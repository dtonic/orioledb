#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent


class CheckpointRollbackToSavepointTest(BaseTest):
	"""
	A rollback to a savepoint while a checkpoint is writing the trees takes
	back changes the checkpoint may have written already.  Recovery from that
	checkpoint has to take them back too, in the primary key and in the
	secondary indexes alike.
	"""

	def setup_tables(self):
		node = self.node
		node.append_conf("""
			checkpoint_timeout = 1d
			orioledb.enable_stopevents = true
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL, val text)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 10, repeat('x', 100)
				FROM generate_series(1, 1000) g;
			-- checkpointed after o_t's trees: a stop there has them written
			CREATE TABLE o_z (id int PRIMARY KEY, v int NOT NULL)
				USING orioledb;
			CREATE INDEX o_z_v ON o_z (v);
			INSERT INTO o_z VALUES (1, 1);
			CHECKPOINT;
		""")

	def check(self, want):
		node = self.node
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""), "sequential scan")
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= -1000 ORDER BY id"""),
		    "secondary index")

	def run_case(self,
	             before,
	             inside,
	             after_rollback,
	             commit=True,
	             stop_at='o_z_v'):
		"""`before` and `inside` (a savepoint) run before the checkpoint,
		the rollback to the savepoint and `after_rollback` while it is
		stopped at the start of tree `stop_at`."""
		self.setup_tables()
		node = self.node
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		if before:
			con1.execute(before)
		con1.execute("SAVEPOINT s")
		con1.execute(inside)
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', "
		             f"'$.treeName == \"{stop_at}\"')")
		t = ThreadQueryExecutor(con2, "CHECKPOINT")
		t.start()
		wait_checkpointer_stopevent(node)
		con1.execute("ROLLBACK TO SAVEPOINT s")
		if after_rollback:
			con1.execute(after_rollback)
		if commit:
			con1.commit()
		else:
			con1.rollback()
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start')")
		t.join()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])
		node.start()

	def base(self):
		return {g: g % 10 for g in range(1, 1001)}

	def want(self, rows):
		return sorted(rows.items())

	def test_secondary_key_change_rolled_back(self):
		self.run_case(None, "UPDATE o_t SET grp = 13 WHERE id <= 100", None)
		self.check(self.want(self.base()))

	def test_secondary_key_change_rolled_back_then_top_level(self):
		self.run_case("UPDATE o_t SET grp = 20 WHERE id = 500",
		              "UPDATE o_t SET grp = 13 WHERE id <= 100",
		              "UPDATE o_t SET grp = 14 WHERE id = 200")
		rows = self.base()
		rows[500] = 20
		rows[200] = 14
		self.check(self.want(rows))

	def test_insert_and_delete_rolled_back(self):
		self.run_case(
		    None, "DELETE FROM o_t WHERE id <= 50; "
		    "INSERT INTO o_t SELECT g, 7 FROM generate_series(2001, 2050) g",
		    "INSERT INTO o_t VALUES (3000, 3)")
		rows = self.base()
		rows[3000] = 3
		self.check(self.want(rows))

	def test_rolled_back_then_aborted(self):
		self.run_case("UPDATE o_t SET grp = 20 WHERE id = 500",
		              "UPDATE o_t SET grp = 13 WHERE id <= 100",
		              "UPDATE o_t SET grp = 14 WHERE id = 200",
		              commit=False)
		self.check(self.want(self.base()))

	def run_before_toast_consistent(self,
	                                after_checkpoint,
	                                finish,
	                                first=True):
		"""A savepoint rolled back and the changes after it reach the WAL
		while the checkpoint is at the primary keys, before the secondary
		indexes, so recovery replays them to the primary key only.
		`after_checkpoint` runs once the checkpoint is done, then `finish`
		ends the transaction.  Without `first` the savepoint comes before
		any change."""
		self.setup_tables()
		node = self.node
		node.safe_psql("""
			CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
			CHECKPOINT;
		""")
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		if first:
			con1.execute("UPDATE o_t SET grp = 20 WHERE id = 500")
		con1.execute("SELECT pg_stopevent_set('checkpoint_index_start', "
		             "'$.treeName == \"o_t_pkey\"')")
		t = ThreadQueryExecutor(con2, "CHECKPOINT")
		t.start()
		wait_checkpointer_stopevent(node)
		con1.execute("SAVEPOINT s")
		con1.execute("UPDATE o_t SET grp = 13 WHERE id <= 10")
		con1.execute("ROLLBACK TO SAVEPOINT s")
		con1.execute("UPDATE o_t SET grp = 14 WHERE id = 200")
		con1.execute("DELETE FROM o_t WHERE id = 201")
		con1.execute("INSERT INTO o_t VALUES (5000, 5)")
		# more than the local WAL buffer holds: all of it goes out now
		con1.execute("INSERT INTO o_pad SELECT g, repeat(md5(g::text), 10) "
		             "FROM generate_series(1, 100) g")
		con1.execute("SELECT pg_stopevent_reset('checkpoint_index_start')")
		t.join()
		for q in after_checkpoint:
			con1.execute(q)
		if finish == 'commit':
			con1.commit()
		else:
			con1.rollback()
		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])
		node.start()

	def test_restarted_savepoint_before_toast_consistent(self):
		self.run_before_toast_consistent([], 'commit')
		rows = self.base()
		rows[500] = 20
		rows[200] = 14
		del rows[201]
		rows[5000] = 5
		self.check(self.want(rows))

	def test_first_savepoint_before_toast_consistent(self):
		"""Rolling back to a savepoint taken before any change rolls back
		all the transaction did, and it goes on."""
		self.run_before_toast_consistent([], 'commit', first=False)
		rows = self.base()
		rows[200] = 14
		del rows[201]
		rows[5000] = 5
		self.check(self.want(rows))

	def test_first_savepoint_rolled_back_again(self):
		self.run_before_toast_consistent([
		    "ROLLBACK TO SAVEPOINT s", "UPDATE o_t SET grp = 15 WHERE id = 300"
		],
		                                 'commit',
		                                 first=False)
		rows = self.base()
		rows[300] = 15
		self.check(self.want(rows))

	def test_first_savepoint_then_aborted(self):
		self.run_before_toast_consistent(
		    ["UPDATE o_t SET grp = 15 WHERE id = 300"],
		    'rollback',
		    first=False)
		self.check(self.want(self.base()))

	def test_restarted_savepoint_rolled_back_again(self):
		"""The changes after the first rollback are rolled back too, after
		the checkpoint."""
		self.run_before_toast_consistent([
		    "ROLLBACK TO SAVEPOINT s", "UPDATE o_t SET grp = 15 WHERE id = 300"
		], 'commit')
		rows = self.base()
		rows[500] = 20
		rows[300] = 15
		self.check(self.want(rows))

	def test_restarted_savepoint_then_released_and_aborted(self):
		self.run_before_toast_consistent(
		    ["RELEASE SAVEPOINT s", "UPDATE o_t SET grp = 15 WHERE id = 300"],
		    'rollback')
		self.check(self.want(self.base()))

	def test_restarted_savepoint_then_nested(self):
		"""A savepoint started inside the restarted one and rolled back
		after the checkpoint takes back only its own changes."""
		self.run_before_toast_consistent([
		    "SAVEPOINT s2", "UPDATE o_t SET grp = 16 WHERE id = 400",
		    "ROLLBACK TO SAVEPOINT s2",
		    "UPDATE o_t SET grp = 17 WHERE id = 401"
		], 'commit')
		rows = self.base()
		rows[500] = 20
		rows[200] = 14
		del rows[201]
		rows[5000] = 5
		rows[401] = 17
		self.check(self.want(rows))

	def test_rolled_back_at_primary_trees(self):
		"""Stopped before the secondary index is written: the primary key
		has the change in its image already."""
		self.run_case(None,
		              "UPDATE o_t SET grp = 13 WHERE id <= 100",
		              None,
		              stop_at='o_t_grp')
		self.check(self.want(self.base()))
