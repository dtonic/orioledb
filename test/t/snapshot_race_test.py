#!/usr/bin/env python3
# coding: utf-8

import os
import random
import string
import threading
import time

import psycopg2

from .base_test import BaseTest


def connect(node):
	return psycopg2.connect(host=node.host,
	                        port=node.port,
	                        dbname='postgres',
	                        user=os.environ.get('USER') or None)


class SnapshotRaceTest(BaseTest):
	"""
	A REPEATABLE READ snapshot reads the same rows, TOASTed values and all,
	through a sequential scan and through the primary key, before and after
	other transactions update, delete, insert again and roll back while
	checkpoints run.

	The updates resize rows that are large enough to split and merge the
	primary key's pages.  An ordered scan of the primary key used to drop
	whole key ranges there: a merged page split again, and a differential
	merge image reconstructed from one half of it took the merge's boundary
	for the end of that half (see reconstruct_merge_diff_half()).
	"""

	def race(self,
	         conf="",
	         seconds=60,
	         checkpoints=True,
	         deletes=True,
	         savepoints=False,
	         kept_values=False,
	         readers=2,
	         updaters=2,
	         rows=50):
		node = self.node
		node.append_conf(conf + """
			max_connections = 30
		""")
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, val text NOT NULL,
				grp int NOT NULL DEFAULT 0) USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, repeat(md5(g::text), 100)
				FROM generate_series(1, {rows}) g;
		""")
		stop = threading.Event()
		errors = []
		counts = {'reads': 0, 'writes': 0}

		def value(rng):
			n = rng.choice((2100, 2500, 4000, 8000, 16000, 30000))
			return ''.join(rng.choices(string.ascii_letters, k=n))

		def updater(u):
			rng = random.Random(u)
			conn = connect(node)
			cur = conn.cursor()
			while not stop.is_set():
				k = rng.randint(1, rows)
				try:
					if deletes and rng.random() < 0.2:
						cur.execute("DELETE FROM o_t WHERE id = %s", (k, ))
						cur.execute(
						    "INSERT INTO o_t VALUES (%s, %s) "
						    "ON CONFLICT (id) DO UPDATE SET val = excluded.val",
						    (k, value(rng)))
					elif kept_values and rng.random() < 0.4:
						# the TOASTed value stays, another column changes
						cur.execute("UPDATE o_t SET grp = %s WHERE id = %s",
						            (rng.randint(0, 15), k))
					else:
						cur.execute("UPDATE o_t SET val = %s WHERE id = %s",
						            (value(rng), k))
					if savepoints:
						# the same row again under a savepoint, or another
						k2 = k if rng.random() < 0.5 else rng.randint(1, rows)
						cur.execute("SAVEPOINT s")
						cur.execute("UPDATE o_t SET val = %s WHERE id = %s",
						            (value(rng), k2))
						if rng.random() < 0.5:
							cur.execute("ROLLBACK TO SAVEPOINT s")
						else:
							cur.execute("RELEASE SAVEPOINT s")
					if rng.random() < 0.1:
						conn.rollback()
					else:
						conn.commit()
					counts['writes'] += 1
				except (psycopg2.errors.SerializationFailure,
				        psycopg2.errors.DeadlockDetected):
					conn.rollback()
				except Exception as e:
					errors.append(f"updater {u}: {e!r}")
					return

		def reader(r):
			conn = connect(node)
			conn.set_session(isolation_level='REPEATABLE READ')
			cur = conn.cursor()
			q = "SELECT id, length(val), md5(val) FROM o_t ORDER BY id"
			seq = ("SET LOCAL enable_indexscan = off; "
			       "SET LOCAL enable_bitmapscan = off; "
			       "SET LOCAL enable_seqscan = on")
			pk = ("SET LOCAL enable_seqscan = off; "
			      "SET LOCAL enable_bitmapscan = off; "
			      "SET LOCAL enable_indexscan = on")
			plans = {
			    'seq': (seq, q),
			    'pk': (pk, q),
			    # a backward scan steps by the low keys of the images
			    'pk desc': (pk, q + " DESC"),
			}
			while not stop.is_set():
				try:
					got = []
					for _ in range(2):
						for name, (plan, query) in plans.items():
							cur.execute(plan)
							cur.execute(query)
							rows_got = cur.fetchall()
							if query != q:
								rows_got.reverse()
							got.append((name, rows_got))
						stop.wait(0.3)
					conn.rollback()
					counts['reads'] += 1
				except Exception as e:
					errors.append(f"reader {r}: {e!r}")
					return
				base = dict((k, v) for k, *v in got[0][1])
				for i, (name, rows_got) in enumerate(got[1:], 1):
					if rows_got == got[0][1]:
						continue
					b = dict((k, v) for k, *v in rows_got)
					errors.append(f"reader {r}, read {i} ({name}): " +
					              "; ".join(
					                  f"id {k}: {base.get(k)} -> {b.get(k)}"
					                  for k in sorted(set(base) | set(b))
					                  if base.get(k) != b.get(k)))
					return

		def checkpointer():
			while not stop.wait(2):
				try:
					node.safe_psql("CHECKPOINT")
				except Exception as e:
					errors.append(f"checkpoint: {e!r}")
					return

		threads = [
		    threading.Thread(target=updater, args=(u, ))
		    for u in range(updaters)
		]
		threads += [
		    threading.Thread(target=reader, args=(r, )) for r in range(readers)
		]
		if checkpoints:
			threads.append(threading.Thread(target=checkpointer))
		for t in threads:
			t.start()
		deadline = time.time() + seconds
		while time.time() < deadline and not errors:
			time.sleep(0.5)
		stop.set()
		for t in threads:
			t.join(60)
		self.assertEqual([], errors, counts)
		self.assertGreater(counts['reads'], 10)
		node.stop()

	def test_snapshot_race(self):
		self.race()

	def test_snapshot_race_small_buffers(self):
		self.race("orioledb.main_buffers = 8MB")

	def test_snapshot_race_savepoints(self):
		"""Updates under savepoints rolled back and released, the same row
		updated before them too."""
		self.race(savepoints=True)

	def test_snapshot_race_kept_values(self):
		"""Updates that leave the TOASTed value as it is and change another,
		indexed, column."""
		self.race(kept_values=True, savepoints=True, updaters=4)
