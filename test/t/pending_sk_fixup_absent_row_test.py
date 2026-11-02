#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent
from .base_test import wait_stopevent


class PendingSkFixupAbsentRowTest(BaseTest):
	"""
	A row whose primary key change came after the checkpoint wrote the
	primary key, and whose secondary index change was pending when it went
	on to the secondary indexes.  Recovery fixes the secondary indexes up for
	it before it replays the change, so the primary key image does not have
	the row yet: the fix-up must not take another row, or the deleted row
	the key had before, for it.
	"""

	def run_case(self, sql, finish, setup=""):
		node = self.node
		node.append_conf("""
			checkpoint_timeout = 1d
			max_wal_size = 4GB
			orioledb.enable_stopevents = true
		""")
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, tok int NOT NULL)
				USING orioledb;
			CREATE INDEX o_t_tok ON o_t (tok);
			INSERT INTO o_t SELECT g, g FROM generate_series(1, 9) g;
			-- its primary key goes after o_t's, before any secondary index
			CREATE TABLE o_z (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_z VALUES (1);
			{setup}
			CHECKPOINT;
		""")
		ctl = node.connect()
		dml = node.connect()
		chk = node.connect()
		dml.execute("SET application_name = 's_dml'")
		dml.commit()
		pid = dml.execute("SELECT pg_backend_pid()")[0][0]
		dml.commit()

		# the checkpoint has written o_t's primary key
		ctl.execute("SELECT pg_stopevent_set('checkpoint_index_start', "
		            "'$.treeName == \"o_z_pkey\"')")
		ctl.commit()
		tc = ThreadQueryExecutor(chk, "CHECKPOINT")
		tc.start()
		wait_checkpointer_stopevent(node)

		# the change: primary key done, the secondary index pending
		ctl.execute("SELECT pg_stopevent_set('sk_modify_pending', "
		            "'$applicationName == \"s_dml\"')")
		ctl.commit()
		td = ThreadQueryExecutor(dml, f"BEGIN; {sql}")
		td.start()
		wait_stopevent(node, pid)

		# the checkpoint goes on to the secondary indexes and finishes
		ctl.execute("SELECT pg_stopevent_reset('checkpoint_index_start')")
		ctl.commit()
		tc.join()
		ctl.execute("SELECT pg_stopevent_reset('sk_modify_pending')")
		ctl.commit()
		td.join()
		if finish == 'commit':
			dml.commit()
		elif finish == 'rollback':
			dml.rollback()
		# else left in progress
		node.stop(['-m', 'immediate'])
		for c in (ctl, dml, chk):
			try:
				c.close()
			except Exception:
				pass
		node.start()

	def check(self, want):
		node = self.node
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, tok FROM o_t ORDER BY id"""), "sequential scan")
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, tok FROM o_t WHERE tok >= -1000 ORDER BY id"""),
		    "secondary index")
		for i, tok in want:
			self.assertEqual([(i, )],
			                 node.execute(f"""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id FROM o_t WHERE tok = {tok}"""), f"tok {tok}")
		node.stop()

	def base(self):
		return {g: g for g in range(1, 10)}

	def test_insert_over_deleted_row(self):
		"""The key had a deleted row in the primary key image."""
		self.run_case("INSERT INTO o_t VALUES (5, 500)", 'commit',
		              "DELETE FROM o_t WHERE id = 5;")
		rows = self.base()
		rows[5] = 500
		self.check(sorted(rows.items()))

	def test_insert_new_key_committed(self):
		self.run_case("INSERT INTO o_t VALUES (100, 1000)", 'commit',
		              "DELETE FROM o_t WHERE id = 9;")
		rows = self.base()
		del rows[9]
		rows[100] = 1000
		self.check(sorted(rows.items()))

	def test_insert_between_keys_rolled_back(self):
		"""The next key's row is there instead."""
		self.run_case("INSERT INTO o_t VALUES (0, 400)", 'rollback')
		self.check(sorted(self.base().items()))

	def test_insert_between_keys_in_progress(self):
		self.run_case("INSERT INTO o_t VALUES (0, 400)", 'none')
		self.check(sorted(self.base().items()))

	def test_insert_before_deleted_row(self):
		"""The next key is a deleted row: no entry comes back for it, and
		none is there when the key gets a row again."""
		self.run_case("INSERT INTO o_t VALUES (0, 400)", 'commit',
		              "DELETE FROM o_t WHERE id = 1;")
		self.assertTrue(
		    self.node.execute("SELECT orioledb_tbl_check('o_t'::regclass)")[0]
		    [0])
		self.node.safe_psql("INSERT INTO o_t VALUES (1, 50)")
		rows = self.base()
		rows[0] = 400
		rows[1] = 50
		self.check(sorted(rows.items()))
		self.node.start()
		self.assertEqual([],
		                 self.node.execute("""
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SELECT id FROM o_t WHERE tok = 1"""))

	def test_update_in_progress(self):
		self.run_case("UPDATE o_t SET tok = 700 WHERE id = 7", 'none')
		self.check(sorted(self.base().items()))
