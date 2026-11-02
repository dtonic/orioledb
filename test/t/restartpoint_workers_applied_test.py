#!/usr/bin/env python3
# coding: utf-8

import os
import re
import time

from .base_test import BaseTest


def wait_replayed(master, timeout=60):
	"""Waits for the standby to replay the WAL so far, seen from the
	primary: with no hot standby there is no connecting to the standby."""
	lsn = master.execute("SELECT pg_current_wal_lsn()::text")[0][0]
	deadline = time.time() + timeout
	while time.time() < deadline:
		r = master.execute("SELECT replay_lsn >= '%s'::pg_lsn "
		                   "FROM pg_stat_replication" % lsn)
		if r and r[0][0]:
			return
		time.sleep(0.2)
	raise AssertionError("the standby did not replay")


def count_restartpoints(node):
	with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
		return len(re.findall(r"restartpoint complete", f.read()))


class RestartpointWorkersAppliedTest(BaseTest):
	"""
	A standby's restartpoint replays from no further than its recovery
	workers have applied the WAL.  The startup process replays ahead of them,
	and a change they had not applied when the trees were written is in no
	image: a restart from the restartpoint, replaying from where the startup
	process had got to, lost it -- for a transaction committed later too.

	A clean shutdown is such a restartpoint as well, written once the startup
	process and the workers are gone: they have to leave nothing unapplied.

	With no hot standby, the startup process keeps the messages to the
	workers buffered, and the workers took the replay position for their own
	while they had not got them yet.
	"""

	def run_case(self,
	             restarts,
	             restartpoint_each_time,
	             stop_mode='immediate',
	             restartpoint_first=True):
		master = self.node
		master.append_conf("checkpoint_timeout = 1d\nmax_wal_size = 4GB\n")
		master.start()
		master.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 10 FROM generate_series(1, 1000) g;
			CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
		""")
		with self.getReplica() as replica:
			replica.append_conf("checkpoint_timeout = 1d\n")
			replica.start()
			self.catchup_orioledb(replica)
			con = master.connect()
			con.begin()
			con.execute("UPDATE o_t SET grp = 20 WHERE id = 500")
			# more than the local WAL buffer holds: the update goes out
			# before the checkpoint
			con.execute("INSERT INTO o_pad SELECT g, repeat(md5(g::text), 10) "
			            "FROM generate_series(1, 100) g")
			master.safe_psql("CHECKPOINT")
			master.safe_psql("INSERT INTO o_pad VALUES (-1, 'x')")
			self.catchup_orioledb(replica)
			if restartpoint_first:
				replica.safe_psql("CHECKPOINT")
			for i in range(restarts):
				replica.stop(['-m', stop_mode])
				replica.start()
				self.catchup_orioledb(replica)
				if restartpoint_each_time:
					master.safe_psql("CHECKPOINT")
					master.safe_psql(
					    f"INSERT INTO o_pad VALUES ({-2 - i}, 'x')")
					self.catchup_orioledb(replica)
					replica.safe_psql("CHECKPOINT")
			con.commit()
			con.close()
			master.safe_psql("INSERT INTO o_pad VALUES (-100, 'x')")
			self.catchup_orioledb(replica)
			for node in (master, replica):
				self.assertEqual(
				    [(20, )],
				    node.execute("SELECT grp FROM o_t WHERE id = 500"))
				self.assertEqual([(1000, )],
				                 node.execute("""
						SET enable_seqscan = off; SET enable_bitmapscan = off;
						SELECT count(*) FROM o_t WHERE grp >= 0"""))
				self.assertEqual([(500, )],
				                 node.execute("""
						SET enable_seqscan = off; SET enable_bitmapscan = off;
						SELECT id FROM o_t WHERE grp = 20"""))
			replica.stop()
		master.stop()

	def test_restart(self):
		self.run_case(1, False)

	def test_restart_after_another_restartpoint(self):
		self.run_case(1, True)

	def test_restarts(self):
		self.run_case(2, True)

	def test_fast_shutdown(self):
		self.run_case(1, False, 'fast', False)

	def test_smart_shutdown(self):
		self.run_case(1, False, 'smart', False)

	def test_restart_no_hot_standby(self):
		master = self.node
		master.append_conf("checkpoint_timeout = 1d\nmax_wal_size = 4GB\n")
		master.start()
		master.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 10 FROM generate_series(1, 1000) g;
			CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
		""")
		with self.getReplica() as replica:
			replica.append_conf(
			    "hot_standby = off\ncheckpoint_timeout = 30s\n")
			replica.start()
			wait_replayed(master)
			con = master.connect()
			con.begin()
			con.execute("UPDATE o_t SET grp = 20 WHERE id = 500")
			con.execute("INSERT INTO o_pad SELECT g, repeat(md5(g::text), 10) "
			            "FROM generate_series(1, 100) g")
			master.safe_psql("CHECKPOINT")
			master.safe_psql("INSERT INTO o_pad VALUES (-1, 'x')")
			wait_replayed(master)
			n = count_restartpoints(replica)
			deadline = time.time() + 120
			while count_restartpoints(replica) == n:
				self.assertLess(time.time(), deadline, "no restartpoint")
				time.sleep(1)
			replica.stop(['-m', 'immediate'])
			replica.append_conf("hot_standby = on\n")
			replica.start()
			con.commit()
			con.close()
			master.safe_psql("INSERT INTO o_pad VALUES (-100, 'x')")
			self.catchup_orioledb(replica)
			for node in (master, replica):
				self.assertEqual(
				    [(20, )],
				    node.execute("SELECT grp FROM o_t WHERE id = 500"))
				self.assertEqual([(500, )],
				                 node.execute("""
						SET enable_seqscan = off; SET enable_bitmapscan = off;
						SELECT id FROM o_t WHERE grp = 20"""))
			replica.stop()
		master.stop()
