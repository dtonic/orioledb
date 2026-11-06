#!/usr/bin/env python3
# coding: utf-8
"""
Workload scenarios in S3 mode: reads, writes and transactions while the data
is evicted to the bucket and read back, across crashes of the writer and of
readers, a reader taking the bucket over, and loads into fresh data
directories.

Every worker owns a key range of o_kv and runs a deterministic stream of
transactions (inserts, updates, deletes, savepoints rolled back or released,
transactions rolled back), each committed one bumping the worker's row in
o_progress in the same transaction.  So the expected contents for any
progress number can be computed again, and after a crash a worker's progress
is either its last acknowledged commit or the one in flight.  Transfer
workers move money between o_bank accounts under row locks, keeping the sum
fixed, and snapshot checkers verify that a REPEATABLE READ snapshot stays
the same while checkpoints and eviction go on.
"""

import hashlib
import os
import random
import string
import threading
import time

import psycopg2
from psycopg2 import errors as pgerrors
from testgres.enums import NodeStatus

from .s3_base_test import S3BaseTest

NACCOUNTS = 100
BALANCE = 1000
KEY_SPAN = 200000000
NGROUPS = 16


def gen_value(rng):
	if rng.random() < 0.25:
		n = rng.randint(2500, 9000)
	else:
		n = rng.randint(10, 300)
	return ''.join(rng.choices(string.ascii_letters + string.digits, k=n))


def gen_txn(seed, w, i, state):
	"""The ops of transaction i of worker w, given the worker's committed
	rows; a pure function of its arguments."""
	rng = random.Random((seed * 1000003 + w) * 1000003 + i)
	work = dict(state)
	next_id = w * KEY_SPAN + 1 + i * 100

	def ops_on(work, n):
		nonlocal next_id
		ops = []
		for _ in range(n):
			r = rng.random()
			ids = sorted(work)
			if r < 0.45 or len(ids) < 10:
				op = ('ins', next_id, rng.randrange(NGROUPS), gen_value(rng))
				next_id += 1
			elif r < 0.8:
				k = rng.choice(ids)
				if rng.random() < 0.5:
					op = ('upd', k, rng.randrange(NGROUPS), gen_value(rng))
				else:
					# value kept, only the indexed column changes
					op = ('upd', k, rng.randrange(NGROUPS), None)
			else:
				op = ('del', rng.choice(ids))
			apply_op(work, op)
			ops.append(op)
		return ops

	body = []
	for _ in range(rng.randint(1, 3)):
		if rng.random() < 0.3:
			inner = dict(work)
			sp_ops = ops_on(inner, rng.randint(1, 4))
			rollback = rng.random() < 0.5
			if not rollback:
				work = inner
			body.append(('sp', sp_ops, rollback))
		else:
			body.extend(ops_on(work, rng.randint(1, 4)))
	commit = rng.random() >= 0.15
	return body, commit, (work if commit else state)


def apply_op(state, op):
	if op[0] == 'ins':
		state[op[1]] = (op[2], op[3], 0)
	elif op[0] == 'upd':
		grp, val, ver = state[op[1]]
		state[op[1]] = (op[2], val if op[3] is None else op[3], ver + 1)
	else:
		del state[op[1]]


def model_upto(seed, w, n):
	"""The rows of worker w once its transactions up to n are applied."""
	state = {}
	for i in range(1, n + 1):
		_, _, state = gen_txn(seed, w, i, state)
	return state


def run_ops(cur, ops):
	for op in ops:
		if op[0] == 'ins':
			cur.execute(
			    "INSERT INTO o_kv (id, grp, val, ver) "
			    "VALUES (%s, %s, %s, 0)", (op[1], op[2], op[3]))
		elif op[0] == 'upd' and op[3] is None:
			cur.execute(
			    "UPDATE o_kv SET grp = %s, ver = ver + 1 WHERE id = %s",
			    (op[2], op[1]))
			assert cur.rowcount == 1, op
		elif op[0] == 'upd':
			cur.execute(
			    "UPDATE o_kv SET grp = %s, val = %s, ver = ver + 1 "
			    "WHERE id = %s", (op[2], op[3], op[1]))
			assert cur.rowcount == 1, op
		elif op[0] == 'del':
			cur.execute("DELETE FROM o_kv WHERE id = %s", (op[1], ))
			assert cur.rowcount == 1, op
		else:
			_, sp_ops, rollback = op
			cur.execute("SAVEPOINT s")
			run_ops(cur, sp_ops)
			cur.execute("ROLLBACK TO SAVEPOINT s"
			            if rollback else "RELEASE SAVEPOINT s")


def crash_errors(errors):
	"""The errors left once a crash's own are taken out: lost connections,
	and the checkpoint that ran into the shutdown."""
	return [
	    e for e in errors if 'Operational' not in e and 'Interface' not in e
	    and not e.startswith('checkpoint: ')
	]


def connect(node):
	return psycopg2.connect(host=node.host,
	                        port=node.port,
	                        dbname='postgres',
	                        user=os.environ.get('USER') or None,
	                        connect_timeout=10)


class Workload:
	"""Workers, transfers and snapshot checkers against one writer."""

	def __init__(self, test, seed, nworkers):
		self.test = test
		self.seed = seed
		self.nworkers = nworkers
		# per worker: last acknowledged commit, and the one in flight
		self.acked = [0] * nworkers
		self.inflight = [0] * nworkers
		self.next_txn = [1] * nworkers
		self.states = [{} for _ in range(nworkers)]
		self.errors = []
		self.counts = {
		    'commits': 0,
		    'rollbacks': 0,
		    'transfers': 0,
		    'conflicts': 0,
		    'snapshots': 0
		}
		self.lock = threading.Lock()

	def setup(self, node):
		node.safe_psql(f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_kv (
				id int PRIMARY KEY,
				grp int NOT NULL,
				val text NOT NULL,
				ver int NOT NULL
			) USING orioledb;
			CREATE INDEX o_kv_grp ON o_kv (grp, id);
			CREATE TABLE o_progress (w int PRIMARY KEY, n int NOT NULL)
				USING orioledb;
			INSERT INTO o_progress SELECT g, 0 FROM generate_series(0, {self.nworkers - 1}) g;
			CREATE TABLE o_bank (id int PRIMARY KEY, bal int NOT NULL)
				USING orioledb;
			INSERT INTO o_bank SELECT g, {BALANCE} FROM generate_series(1, {NACCOUNTS}) g;
		""")

	def resume_from(self, progress):
		"""Go on from what a node holds after a crash or a takeover."""
		for w in range(self.nworkers):
			n = progress[w]
			self.acked[w] = self.inflight[w] = n
			self.next_txn[w] = n + 1
			self.states[w] = model_upto(self.seed, w, n)

	def worker(self, node, w, stop, max_txns):
		try:
			conn = connect(node)
		except Exception as e:
			if not stop.is_set():
				self.errors.append(f"worker {w}: connect: {e!r}")
			return
		cur = conn.cursor()
		done = 0
		while not stop.is_set() and done < max_txns:
			i = self.next_txn[w]
			body, commit, new_state = gen_txn(self.seed, w, i, self.states[w])
			try:
				run_ops(cur, body)
				if commit:
					cur.execute("UPDATE o_progress SET n = %s WHERE w = %s",
					            (i, w))
					self.inflight[w] = i
					conn.commit()
					self.acked[w] = i
					with self.lock:
						self.counts['commits'] += 1
				else:
					conn.rollback()
					with self.lock:
						self.counts['rollbacks'] += 1
			except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
				if not stop.is_set():
					self.errors.append(f"worker {w} txn {i}: {e!r}")
				return
			except Exception as e:
				self.errors.append(f"worker {w} txn {i}: {e!r}")
				return
			self.states[w] = new_state
			self.next_txn[w] = i + 1
			done += 1
		conn.close()

	def transfers(self, node, t, stop):
		rng = random.Random(self.seed * 7 + t)
		try:
			conn = connect(node)
		except Exception as e:
			if not stop.is_set():
				self.errors.append(f"transfer {t}: connect: {e!r}")
			return
		cur = conn.cursor()
		while not stop.is_set():
			a, b = sorted(rng.sample(range(1, NACCOUNTS + 1), 2))
			amount = rng.randint(1, 50)
			try:
				if rng.random() < 0.3:
					cur.execute(
					    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
				cur.execute(
				    "SELECT id, bal FROM o_bank WHERE id IN (%s, %s) "
				    "ORDER BY id FOR UPDATE", (a, b))
				assert len(cur.fetchall()) == 2
				cur.execute("UPDATE o_bank SET bal = bal - %s WHERE id = %s",
				            (amount, a))
				cur.execute("UPDATE o_bank SET bal = bal + %s WHERE id = %s",
				            (amount, b))
				if rng.random() < 0.1:
					conn.rollback()
				else:
					conn.commit()
				with self.lock:
					self.counts['transfers'] += 1
			except pgerrors.SerializationFailure:
				conn.rollback()
				with self.lock:
					self.counts['conflicts'] += 1
			except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
				if not stop.is_set():
					self.errors.append(f"transfer {t}: {e!r}")
				return
			except Exception as e:
				self.errors.append(f"transfer {t}: {e!r}")
				return
		conn.close()

	def snapshots(self, node, stop, hold=3.0, standby=False):
		"""A REPEATABLE READ snapshot reads the same, before and after the
		checkpoints and the eviction that go on while it is held, and the
		bank's sum is fixed in each."""
		try:
			conn = connect(node)
		except Exception as e:
			if not stop.is_set():
				self.errors.append(f"snapshot: connect: {e!r}")
			return
		conn.set_session(isolation_level='REPEATABLE READ')
		cur = conn.cursor()
		q = ("SELECT count(*), coalesce(sum(ver), 0), "
		     "coalesce(sum(length(val)), 0), "
		     "(SELECT sum(bal) FROM o_bank), "
		     "(SELECT string_agg(n::text, ',' ORDER BY w) FROM o_progress) "
		     "FROM o_kv")
		while not stop.is_set():
			try:
				cur.execute(q)
				first = cur.fetchone()
				stop.wait(hold)
				cur.execute(q)
				second = cur.fetchone()
				cur.execute("SELECT count(*) FROM o_kv WHERE grp >= 0")
				by_index = cur.fetchone()[0]
				diff = None
				if by_index != second[0]:
					cur.execute("SET LOCAL enable_seqscan = off")
					cur.execute("SET LOCAL enable_bitmapscan = off")
					cur.execute("SELECT id, grp, ver FROM o_kv WHERE grp >= 0")
					via_index = set(cur.fetchall())
					cur.execute("SET LOCAL enable_seqscan = on")
					cur.execute("SET LOCAL enable_indexscan = off")
					cur.execute("SET LOCAL enable_indexonlyscan = off")
					cur.execute("SELECT id, grp, ver FROM o_kv")
					via_seq = set(cur.fetchall())
					diff = (sorted(via_seq - via_index)[:5],
					        sorted(via_index - via_seq)[:5])
				conn.rollback()
			except pgerrors.SerializationFailure:
				# a reader cancels a query that conflicts with what it replays
				conn.rollback()
				continue
			except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
				if not stop.is_set():
					self.errors.append(f"snapshot: {e!r}")
				return
			except Exception as e:
				self.errors.append(f"snapshot: {e!r}")
				return
			if first != second:
				self.errors.append(f"snapshot changed: {first} -> {second}")
			if first[3] != NACCOUNTS * BALANCE:
				self.errors.append(f"bank sum {first[3]}")
			if by_index != second[0]:
				self.errors.append(
				    f"{node.name}: index count {by_index} != {second[0]}; "
				    f"only in the primary, only in the index: {diff}")
			with self.lock:
				self.counts['snapshots'] += 1
		conn.close()

	def start(self,
	          node,
	          stop,
	          max_txns=10**9,
	          ntransfers=3,
	          nsnapshots=1,
	          checkpoints=True):
		threads = [
		    threading.Thread(target=self.worker,
		                     args=(node, w, stop, max_txns))
		    for w in range(self.nworkers)
		]
		threads += [
		    threading.Thread(target=self.transfers, args=(node, t, stop))
		    for t in range(ntransfers)
		]
		threads += [
		    threading.Thread(target=self.snapshots, args=(node, stop))
		    for _ in range(nsnapshots)
		]
		if checkpoints:
			threads.append(
			    threading.Thread(target=self.checkpointer, args=(node, stop)))
		for t in threads:
			t.start()
		return threads

	def checkpointer(self, node, stop):
		while not stop.wait(4):
			try:
				node.safe_psql("CHECKPOINT")
			except Exception as e:
				if not stop.is_set():
					self.errors.append(f"checkpoint: {e!r}")
				return

	def expected(self):
		rows = {}
		for w in range(self.nworkers):
			rows.update(self.states[w])
		return rows


class S3WorkloadTest(S3BaseTest):
	maxDiff = None

	def s3_conf(self, desired_size='24MB'):
		return f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3
			orioledb.s3_desired_size = {desired_size}
			orioledb.s3_headers_buffers = 128kB
			orioledb.main_buffers = 8MB
			orioledb.recovery_pool_size = 1
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
			archive_mode = on
			archive_library = 'orioledb'
			max_connections = 50
			max_wal_size = 1GB
		"""

	def reader_conf(self):
		return """
			orioledb.s3_reader = on
			hot_standby = on
			orioledb.main_buffers = 8MB
			orioledb.recovery_pool_size = 1
			max_connections = 50
			max_standby_archive_delay = -1
			max_standby_streaming_delay = -1
		"""

	def data_size(self, node):
		total = 0
		for top, _, files in os.walk(
		    os.path.join(node.data_dir, 'orioledb_data')):
			for f in files:
				fp = os.path.join(top, f)
				# the server renames and removes files meanwhile
				# (*.gcstate.tmp)
				try:
					if not os.path.islink(fp):
						total += os.stat(fp).st_blocks * 512
				except FileNotFoundError:
					pass
		return total

	def wait_eviction(self, node, limit, timeout=180):
		node.safe_psql("CHECKPOINT")
		deadline = time.time() + timeout
		while self.data_size(node) > limit:
			self.assertLess(
			    time.time(), deadline, f"local data stayed at "
			    f"{self.data_size(node) >> 20} MB")
			time.sleep(1)

	def wait_ready(self, node, timeout=120):
		deadline = time.time() + timeout
		while True:
			try:
				node.execute("SELECT 1")
				return
			except Exception:
				self.assertLess(time.time(), deadline, "node never answered")
				time.sleep(0.5)

	def progress(self, node):
		return [
		    n
		    for _, n in node.execute("SELECT w, n FROM o_progress ORDER BY w")
		]

	def run_for(self, wl, node, seconds, **kwargs):
		stop = threading.Event()
		threads = wl.start(node, stop, **kwargs)
		deadline = time.time() + seconds
		while time.time() < deadline and not wl.errors:
			time.sleep(0.5)
		stop.set()
		for t in threads:
			t.join(120)
			self.assertFalse(t.is_alive(), "a workload thread hung")
		self.assertEqual([], wl.errors, '\n'.join(wl.errors))

	def assert_contents(self, node, rows, nworkers=None):
		"""o_kv holds `rows`, read through a sequential scan, the primary key
		and the secondary index alike; the bank's sum is fixed."""
		seq = node.execute("""
			SET enable_indexscan = off; SET enable_bitmapscan = off;
			SET enable_indexonlyscan = off;
			SELECT id, grp, md5(val), length(val), ver FROM o_kv ORDER BY id;
		""")
		want = sorted((k, g, hashlib.md5(v.encode()).hexdigest(), len(v), ver)
		              for k, (g, v, ver) in rows.items())
		self.assertEqual(len(want), len(seq), "row count, sequential scan")
		self.assertEqual(want, seq)

		by_grp = {}
		for k, (g, _, _) in rows.items():
			by_grp.setdefault(g, []).append(k)
		got = node.execute("""
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SELECT grp, array_agg(id ORDER BY id) FROM o_kv
				WHERE grp >= 0 GROUP BY grp ORDER BY grp;
		""")
		want_idx = {(g, k) for g, ks in by_grp.items() for k in ks}
		got_idx = {(g, k) for g, ks in got for k in ks}
		self.assertEqual(
		    (set(), set()), (want_idx - got_idx, got_idx - want_idx),
		    "(grp, id) missing from the secondary index, extra in it")

		sample = random.Random(len(rows)).sample(sorted(rows),
		                                         min(200, len(rows)))
		got = node.execute(f"""
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SELECT id, ver FROM o_kv WHERE id = ANY(ARRAY{sample or [0]}::int[])
				ORDER BY id;
		""")
		self.assertEqual(sorted((k, rows[k][2]) for k in sample), got)

		self.assertEqual([(NACCOUNTS * BALANCE, NACCOUNTS)],
		                 node.execute("SELECT sum(bal), count(*) FROM o_bank"))

	def diagnose(self, node, wl, progress):
		"""For each worker: do its rows match its progress, the transaction
		after it, or neither -- part of one transaction kept?"""
		lines = []
		for w, n in enumerate(progress):
			got = {
			    k: (g, hashlib.md5(v.encode()).hexdigest(), ver)
			    for k, g, v, ver in node.execute(
			        f"SET enable_indexscan = off; SET enable_bitmapscan = off; "
			        f"SET enable_indexonlyscan = off; "
			        f"SELECT id, grp, val, ver FROM o_kv WHERE id >= "
			        f"{w * KEY_SPAN} AND id < {(w + 1) * KEY_SPAN}")
			}

			def digest(state):
				return {
				    k: (g, hashlib.md5(v.encode()).hexdigest(), ver)
				    for k, (g, v, ver) in state.items()
				}

			at_n = digest(model_upto(wl.seed, w, n))
			at_next = digest(model_upto(wl.seed, w, n + 1))
			if got == at_n:
				continue
			bad = sorted(k for k in set(got) | set(at_n)
			             if got.get(k) != at_n.get(k))
			by_pk = {
			    k
			    for (k, ) in node.execute(
			        "SET enable_seqscan = off; SET enable_bitmapscan = off; "
			        f"SELECT id FROM o_kv WHERE id = ANY(ARRAY{bad[:20]}::int[])"
			    )
			}
			lines.append(
			    f"worker {w}: by the primary key {sorted(by_pk)[:4]}; "
			    f"progress {n} (acked {wl.acked[w]}, in flight "
			    f"{wl.inflight[w]}); rows like the next transaction: "
			    f"{got == at_next}; differing rows {len(bad)}: " +
			    "; ".join(f"{k} db={got.get(k)} model={at_n.get(k)} "
			              f"next={at_next.get(k)}" for k in bad[:4]))
		return '\n'.join(lines)

	def assert_progress_in(self, wl, progress):
		for w, n in enumerate(progress):
			self.assertIn(
			    n, (wl.acked[w], wl.inflight[w]),
			    f"worker {w}: progress {n}, acknowledged "
			    f"{wl.acked[w]}, in flight {wl.inflight[w]}")

	def load_and_check(self, port, suffix, rows):
		with self.initNode(port, suffix) as loaded:
			self.loader.download(loaded.data_dir)
			loaded.append_conf(port=loaded.port)
			loaded.append_conf("orioledb.main_buffers = 8MB")
			loaded.start()
			self.assert_contents(loaded, rows)
			loaded.stop()
			loaded.cleanup()

	def start_reader(self, port, suffix):
		reader = self.initNode(port, suffix)
		self.loader.download(reader.data_dir)
		reader.append_conf(port=reader.port)
		reader.append_conf(self.reader_conf())
		open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
		reader.start()
		self.wait_ready(reader)
		return reader

	def wait_caught_up(self, reader, progress, timeout=180):
		deadline = time.time() + timeout
		while True:
			try:
				if self.progress(reader) == progress:
					return
			except Exception:
				pass
			self.assertLess(time.time(), deadline,
			                f"reader stayed behind {progress}")
			time.sleep(0.5)

	def crash_scenario(self, s3, crashes=3, nsnapshots=1):
		node = self.node
		if s3:
			node.append_conf(self.s3_conf())
		else:
			node.append_conf("""
				orioledb.main_buffers = 8MB
				orioledb.recovery_pool_size = 1
				max_connections = 50
				max_wal_size = 1GB
			""")
		node.start()
		wl = Workload(self, seed=1, nworkers=6)
		wl.setup(node)

		self.run_for(wl, node, 60, nsnapshots=nsnapshots)
		self.assertGreater(wl.counts['commits'], 100, wl.counts)
		if nsnapshots:
			self.assertGreater(wl.counts['snapshots'], 3, wl.counts)
		self.assertEqual([wl.acked[w] for w in range(6)], self.progress(node))
		if s3:
			self.wait_eviction(node, 32 << 20)
		# read back from the bucket
		self.assert_contents(node, wl.expected())

		for round in range(crashes):
			# crash while the workers are in the middle of their
			# transactions
			stop = threading.Event()
			threads = wl.start(node, stop, nsnapshots=nsnapshots)
			time.sleep(15)
			self.assertEqual([], wl.errors)
			node.stop(['-m', 'immediate'])
			stop.set()
			for t in threads:
				t.join(120)
			wl.errors = crash_errors(wl.errors)
			self.assertEqual([], wl.errors)

			node.start()
			progress = self.progress(node)
			self.assert_progress_in(wl, progress)
			self.assertEqual('', self.diagnose(node, wl, progress),
			                 f"crash {round}")
			wl.resume_from(progress)
			self.assert_contents(node, wl.expected())

		# goes on after the crash restarts
		self.run_for(wl, node, 20 if crashes else 180, nsnapshots=nsnapshots)
		rows = wl.expected()
		self.assert_contents(node, rows)
		node.stop()
		if s3:
			self.load_and_check(self.getBasePort() + 1, 'tgsl', rows)

	def test_workload_eviction_crash_and_load(self):
		"""Concurrent transactions while the data is evicted and read back;
		a crash in the middle keeps every acknowledged commit, and a load
		from the bucket gives the same rows."""
		self.crash_scenario(s3=True)

	def test_workload_local_no_crash(self):
		self.crash_scenario(s3=False, crashes=0)

	def test_workload_crash_local_no_snapshots(self):
		self.crash_scenario(s3=False, nsnapshots=0)

	def test_workload_crash_s3_no_snapshots(self):
		self.crash_scenario(s3=True, nsnapshots=0)

	def test_workload_crash_local(self):
		"""The same workload and crashes without S3 mode, to tell an S3 mode
		failure from an engine one."""
		self.crash_scenario(s3=False)

	def test_isolation_on_evicted_data(self):
		"""Row locks, update conflicts and old snapshots on rows whose pages
		were evicted to the bucket."""
		node = self.node
		node.append_conf(self.s3_conf('16MB'))
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, v int NOT NULL, pad text)
				USING orioledb;
			INSERT INTO o_t SELECT g, 0, repeat(md5(g::text), 30)
				FROM generate_series(1, 60000) g;
		""")
		self.wait_eviction(node, 24 << 20)

		old = connect(node)
		old.set_session(isolation_level='REPEATABLE READ')
		oc = old.cursor()
		oc.execute("SELECT sum(v), count(*) FROM o_t")
		self.assertEqual((0, 60000), oc.fetchone())

		a = connect(node)
		b = connect(node)
		ac, bc = a.cursor(), b.cursor()
		ac.execute("UPDATE o_t SET v = v + 1 WHERE id = 30000")
		# the row is locked: NOWAIT fails, a waiting update goes after it
		with self.assertRaises(pgerrors.LockNotAvailable):
			bc.execute("SELECT * FROM o_t WHERE id = 30000 FOR UPDATE NOWAIT")
		b.rollback()
		done = threading.Event()

		def waiter():
			bc.execute("UPDATE o_t SET v = v + 10 WHERE id = 30000")
			done.set()

		t = threading.Thread(target=waiter)
		t.start()
		time.sleep(2)
		self.assertFalse(done.is_set(), "the update did not wait")
		a.commit()
		t.join(30)
		self.assertTrue(done.is_set())
		b.commit()
		# read committed re-reads the row it waited for
		self.assertEqual([(11, )],
		                 node.execute("SELECT v FROM o_t WHERE id = 30000"))

		# repeatable read: a concurrent update is a serialization failure
		rr = connect(node)
		rr.set_session(isolation_level='REPEATABLE READ')
		rc = rr.cursor()
		rc.execute("SELECT v FROM o_t WHERE id = 40000")
		node.safe_psql("UPDATE o_t SET v = 5 WHERE id = 40000")
		with self.assertRaises(pgerrors.SerializationFailure):
			rc.execute("UPDATE o_t SET v = v + 1 WHERE id = 40000")
		rr.rollback()

		# many updates and a checkpoint while the old snapshot is held
		node.safe_psql("UPDATE o_t SET v = v + 1 WHERE id % 7 = 0")
		node.safe_psql("DELETE FROM o_t WHERE id % 11 = 0")
		self.wait_eviction(node, 24 << 20)
		oc.execute("SELECT sum(v), count(*) FROM o_t")
		self.assertEqual((0, 60000), oc.fetchone())
		oc.execute("SET LOCAL enable_seqscan = off")
		oc.execute("SELECT v FROM o_t WHERE id IN (7, 11, 30000, 40000) "
		           "ORDER BY id")
		self.assertEqual([(0, ), (0, ), (0, ), (0, )], oc.fetchall())
		old.rollback()

		v = {i: 0 for i in range(1, 60001)}
		v[30000] = 11
		v[40000] = 5
		for i in v:
			if i % 7 == 0:
				v[i] += 1
		v = {i: x for i, x in v.items() if i % 11 != 0}
		expected = [(sum(v.values()), len(v))]
		self.assertEqual(expected,
		                 node.execute("SELECT sum(v), count(*) FROM o_t"))
		for c in (a, b, rr, old):
			c.close()
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(expected,
		                 node.execute("SELECT sum(v), count(*) FROM o_t"))
		node.stop()

	def test_ddl_in_transactions(self):
		"""DDL committed, rolled back and rolled back to a savepoint, on tables
		whose data was evicted; a crash and a load keep what committed."""
		node = self.node
		node.append_conf(self.s3_conf('16MB'))
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_a (id int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO o_a SELECT g, repeat(md5(g::text), 20)
				FROM generate_series(1, 40000) g;
			CREATE TABLE o_b (id int PRIMARY KEY, v int) USING orioledb;
			INSERT INTO o_b SELECT g, g FROM generate_series(1, 1000) g;
		""")
		self.wait_eviction(node, 24 << 20)
		node.safe_psql("""
			BEGIN;
			ALTER TABLE o_a ADD COLUMN c int NOT NULL DEFAULT 7;
			CREATE INDEX o_a_c ON o_a (c, id);
			UPDATE o_a SET c = id % 5 WHERE id <= 10000;
			COMMIT;

			BEGIN;
			TRUNCATE o_b;
			INSERT INTO o_b VALUES (1, -1);
			ROLLBACK;

			BEGIN;
			DROP TABLE o_b;
			ROLLBACK;

			BEGIN;
			CREATE TABLE o_c (id int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO o_c SELECT g, 'c' || g FROM generate_series(1, 500) g;
			SAVEPOINT s;
			CREATE TABLE o_d (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_d VALUES (1);
			ALTER TABLE o_c ADD COLUMN w int DEFAULT 1;
			ROLLBACK TO SAVEPOINT s;
			CREATE INDEX o_c_v ON o_c (v);
			COMMIT;

			BEGIN;
			CREATE TABLE o_e (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_e SELECT generate_series(1, 100);
			ROLLBACK;

			BEGIN;
			TRUNCATE o_b;
			INSERT INTO o_b SELECT g, -g FROM generate_series(1, 300) g;
			COMMIT;
		""")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("""
			INSERT INTO o_c VALUES (501, 'after');
			UPDATE o_a SET c = 99 WHERE id = 40000;
		""")

		def check(n):
			self.assertEqual(['o_a', 'o_b', 'o_c'], [
			    r[0] for r in n.execute(
			        "SELECT relname FROM pg_class WHERE relname LIKE 'o\\__' "
			        "AND relkind = 'r' ORDER BY relname")
			])
			self.assertEqual(
			    [(40000, 10000 // 5 * 1 + 10000 // 5 * 2 + 10000 // 5 * 3 +
			      10000 // 5 * 4 + 30000 * 7 - 7 + 99)],
			    n.execute("SELECT count(*), sum(c) FROM o_a"))
			self.assertEqual([(2000, )],
			                 n.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT count(*) FROM o_a WHERE c = 0"""))
			self.assertEqual([(300, -45150)],
			                 n.execute("SELECT count(*), sum(v) FROM o_b"))
			self.assertEqual([(501, )], n.execute("SELECT count(*) FROM o_c"))
			self.assertEqual([(501, )],
			                 n.execute("""
				SET enable_seqscan = off;
				SELECT id FROM o_c WHERE v = 'after'"""))
			self.assertEqual(
			    [],
			    n.execute(
			        "SELECT attname FROM pg_attribute "
			        "WHERE attrelid = 'o_c'::regclass AND attname = 'w'"))

		check(node)
		node.stop(['-m', 'immediate'])
		node.start()
		check(node)
		node.stop()
		with self.initNode(self.getBasePort() + 1, 'tgsl') as loaded:
			self.loader.download(loaded.data_dir)
			loaded.append_conf(port=loaded.port)
			loaded.start()
			check(loaded)
			loaded.stop()
			loaded.cleanup()

	def test_readers_follow_and_survive_crashes(self):
		"""Two readers follow the writer from the bucket under the workload,
		with their snapshots consistent; one crashes and catches up after its
		restart, and the writer's crash leaves them serving reads and
		following again once it is back."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		wl = Workload(self, seed=2, nworkers=4)
		wl.setup(node)
		self.run_for(wl, node, 15)
		node.safe_psql("CHECKPOINT")

		r1 = self.start_reader(self.getBasePort() + 1, 'tgr1')
		r2 = self.start_reader(self.getBasePort() + 2, 'tgr2')
		try:
			stop = threading.Event()
			threads = wl.start(node, stop)
			# reads on the readers while they replay
			threads += [
			    threading.Thread(target=wl.snapshots, args=(r, stop, 2.0))
			    for r in (r1, r2)
			]
			for t in threads[-2:]:
				t.start()

			time.sleep(20)
			# reader 1 crashes in the middle and comes back
			r1.stop(['-m', 'immediate'])
			time.sleep(10)
			r1.start()
			self.wait_ready(r1)
			threads.append(
			    threading.Thread(target=wl.snapshots, args=(r1, stop, 2.0)))
			threads[-1].start()
			time.sleep(20)
			stop.set()
			for t in threads:
				t.join(120)
			wl.errors = [
			    e for e in wl.errors
			    if not (e.startswith('snapshot: ') and
			            ('Operational' in e or 'Interface' in e))
			]
			during = wl.errors
			wl.errors = []
			self.assertGreater(wl.counts['snapshots'], 10, wl.counts)

			progress = self.progress(node)
			rows = wl.expected()
			self.assert_contents(node, rows)
			after = []
			for r in (r1, r2):
				self.wait_caught_up(r, progress)
				try:
					self.assert_contents(r, rows)
				except AssertionError as e:
					after.append(f"{r.name}: {e}")
			self.assertEqual(([], []), (during, after))

			# the writer crashes: the readers go on serving what they have
			node.stop(['-m', 'immediate'])
			for r in (r1, r2):
				self.assert_contents(r, rows)
			# readers do not write: their transactions are read only
			with self.assertRaises(Exception):
				r2.safe_psql("INSERT INTO o_bank VALUES (0, 0)")

			node.start()
			self.assert_progress_in(wl, self.progress(node))
			self.run_for(wl, node, 15)
			progress = self.progress(node)
			rows = wl.expected()
			for r in (r1, r2):
				self.wait_caught_up(r, progress)
				self.assert_contents(r, rows)
		finally:
			for r in (r1, r2):
				r.stop(['-m', 'immediate'])
				r.cleanup()
		node.stop()

	def test_reader_takes_over_from_a_crashed_writer(self):
		"""The writer crashes under the workload; a reader is loaded again to
		write and takes the bucket over with every acknowledged commit, the
		workload goes on there, the old writer is fenced, and a load from the
		bucket gives the new writer's rows."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		wl = Workload(self, seed=3, nworkers=6)
		wl.setup(node)
		self.run_for(wl, node, 20)
		node.safe_psql("CHECKPOINT")

		reader = self.start_reader(self.getBasePort() + 1, 'tgrt')
		try:
			stop = threading.Event()
			threads = wl.start(node, stop)
			time.sleep(20)
			self.assertEqual([], wl.errors)
			node.stop(['-m', 'immediate'])
			stop.set()
			for t in threads:
				t.join(120)
			wl.errors = crash_errors(wl.errors)
			self.assertEqual([], wl.errors)

			# the reader still answers, with what it replayed so far
			for w, n in enumerate(self.progress(reader)):
				self.assertLessEqual(n, wl.inflight[w], f"worker {w}")
			reader.stop()

			self.loader.download(reader.data_dir, keep_parts=True)
			reader.append_conf(port=reader.port)
			signal_file = os.path.join(reader.data_dir, "standby.signal")
			if os.path.exists(signal_file):
				os.unlink(signal_file)
			reader.append_conf(self.s3_conf())
			reader.append_conf("orioledb.s3_reader = off")
			reader.start()
			self.wait_ready(reader)
			progress = self.progress(reader)
			self.assert_progress_in(wl, progress)
			self.assertEqual('', self.diagnose(reader, wl, progress))
			wl.resume_from(progress)
			self.assert_contents(reader, wl.expected())

			# the workload goes on on the new writer
			self.run_for(wl, reader, 30)
			self.wait_eviction(reader, 32 << 20)
			rows = wl.expected()
			self.assert_contents(reader, rows)

			# the old writer, started again, finds the lease taken and
			# acknowledges nothing
			try:
				node.start()
			except Exception:
				pass
			if node.status() == NodeStatus.Running:
				done = []
				t = threading.Thread(target=lambda: done.append(
				    node.execute("INSERT INTO o_bank VALUES (0, 0)")),
				                     daemon=True)
				t.start()
				deadline = time.time() + 60
				while node.status() == NodeStatus.Running and \
                    time.time() < deadline:
					time.sleep(0.5)
				self.assertEqual([], done)
				self.assertNotEqual(NodeStatus.Running, node.status())
			self.assert_contents(reader, rows)
			reader.stop()
		finally:
			try:
				reader.stop(['-m', 'immediate'])
			except Exception:
				pass
			reader.cleanup()
		self.load_and_check(self.getBasePort() + 2, 'tgsl', rows)
