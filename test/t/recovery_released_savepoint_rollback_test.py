#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryReleasedSavepointRollbackTest(BaseTest):
	"""
	A transaction rolled back after releasing savepoints leaves nothing
	behind once recovery replays it: neither its own changes made before the
	savepoints nor the secondary index changes made inside them.
	"""

	def setup_table(self, conf=""):
		node = self.node
		node.append_conf(conf)
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL, val text)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 10, repeat('x', 100)
				FROM generate_series(1, 1000) g;
		""")

	def expected(self):
		return [(g, g % 10) for g in range(1, 1001)]

	def check(self):
		node = self.node
		self.assertEqual(
		    self.expected(),
		    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""))
		self.assertEqual(
		    self.expected(),
		    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""))

	def rollback_txn(self, before_checkpoint):
		node = self.node
		con = node.connect()
		con.execute("UPDATE o_t SET grp = 100 WHERE id = 1")
		con.execute("INSERT INTO o_t VALUES (5000, 1, 'new')")
		con.execute("SAVEPOINT s1")
		con.execute("UPDATE o_t SET grp = 101 WHERE id = 2")
		con.execute("UPDATE o_t SET grp = 102, val = repeat('y', 5000) "
		            "WHERE id = 3")
		con.execute("RELEASE SAVEPOINT s1")
		con.execute("SAVEPOINT s2")
		con.execute("DELETE FROM o_t WHERE id = 4")
		con.execute("INSERT INTO o_t VALUES (5001, 2, 'new')")
		con.execute("RELEASE SAVEPOINT s2")
		con.execute("DELETE FROM o_t WHERE id = 5")
		if before_checkpoint:
			node.safe_psql("CHECKPOINT")
		con.rollback()
		con.close()

	def test_checkpoint_then_savepoint_then_rollback(self):
		"""The changes made before a checkpoint by a transaction that opens a
		savepoint after it and then rolls back."""
		self.setup_table()
		node = self.node
		con = node.connect()
		con.execute("UPDATE o_t SET grp = 100 WHERE id = 1")
		con.execute("INSERT INTO o_t VALUES (5000, 1, 'new')")
		con.execute("DELETE FROM o_t WHERE id = 5")
		node.safe_psql("CHECKPOINT")
		con.execute("SAVEPOINT s1")
		con.execute("UPDATE o_t SET grp = 101 WHERE id = 2")
		con.execute("RELEASE SAVEPOINT s1")
		con.rollback()
		con.close()
		self.check()
		node.stop(['-m', 'immediate'])
		node.start()
		self.check()

	def test_released_savepoint_before_checkpoint_then_rollback(self):
		"""A savepoint released before the checkpoint, another opened after
		it, then the transaction rolls back."""
		self.setup_table()
		node = self.node
		con = node.connect()
		con.execute("SAVEPOINT s1")
		con.execute("UPDATE o_t SET grp = 101 WHERE id = 2")
		con.execute("DELETE FROM o_t WHERE id = 6")
		con.execute("INSERT INTO o_t VALUES (5001, 2, 'new')")
		con.execute("RELEASE SAVEPOINT s1")
		node.safe_psql("CHECKPOINT")
		con.execute("SAVEPOINT s2")
		con.execute("UPDATE o_t SET grp = 102 WHERE id = 3")
		con.execute("RELEASE SAVEPOINT s2")
		con.rollback()
		con.close()
		self.check()
		node.stop(['-m', 'immediate'])
		node.start()
		self.check()

	def test_released_savepoint_before_checkpoint_then_commit(self):
		"""The same transaction committed keeps every change: the savepoint
		opened after the checkpoint does not undo the released one's."""
		self.setup_table()
		node = self.node
		con = node.connect()
		con.execute("UPDATE o_t SET grp = 100 WHERE id = 1")
		con.execute("SAVEPOINT s1")
		con.execute("UPDATE o_t SET grp = 101 WHERE id = 2")
		con.execute("DELETE FROM o_t WHERE id = 6")
		con.execute("INSERT INTO o_t VALUES (5001, 2, 'new')")
		con.execute("RELEASE SAVEPOINT s1")
		node.safe_psql("CHECKPOINT")
		con.execute("SAVEPOINT s2")
		con.execute("UPDATE o_t SET grp = 102 WHERE id = 3")
		con.execute("RELEASE SAVEPOINT s2")
		con.commit()
		con.close()
		expected = [(g, g % 10) for g in range(1, 1001) if g != 6]
		expected = [(1, 100), (2, 101), (3, 102)] + expected[3:] + [(5001, 2)]
		for n in (False, True):
			if n:
				node.stop(['-m', 'immediate'])
				node.start()
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""))
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""))

	def savepoint_open_at_checkpoint(self, commit):
		"""A savepoint open at the checkpoint and released after it, then
		another savepoint, then the end of the transaction."""
		self.setup_table()
		node = self.node
		con = node.connect()
		con.execute("UPDATE o_t SET grp = 100 WHERE id = 1")
		con.execute("SAVEPOINT s1")
		con.execute("UPDATE o_t SET grp = 101 WHERE id = 2")
		con.execute("DELETE FROM o_t WHERE id = 6")
		node.safe_psql("CHECKPOINT")
		con.execute("INSERT INTO o_t VALUES (5001, 2, 'new')")
		con.execute("RELEASE SAVEPOINT s1")
		con.execute("SAVEPOINT s2")
		con.execute("UPDATE o_t SET grp = 102 WHERE id = 3")
		con.execute("RELEASE SAVEPOINT s2")
		if commit:
			con.commit()
		else:
			con.rollback()
		con.close()
		if commit:
			expected = [(g, g % 10) for g in range(1, 1001) if g != 6]
			expected = [(1, 100), (2, 101), (3, 102)] + expected[3:] + \
                [(5001, 2)]
		else:
			expected = self.expected()
		for n in (False, True):
			if n:
				node.stop(['-m', 'immediate'])
				node.start()
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""))
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""))

	def test_savepoint_open_at_checkpoint_commit(self):
		self.savepoint_open_at_checkpoint(True)

	def test_savepoint_open_at_checkpoint_rollback(self):
		self.savepoint_open_at_checkpoint(False)

	def test_rollback_after_released_savepoints_no_checkpoint(self):
		self.setup_table()
		self.node.safe_psql("CHECKPOINT")
		self.rollback_txn(False)
		self.check()
		self.node.stop(['-m', 'immediate'])
		self.node.start()
		self.check()

	def test_rollback_after_released_savepoints_checkpoint_inside(self):
		self.setup_table()
		self.rollback_txn(True)
		self.check()
		self.node.stop(['-m', 'immediate'])
		self.node.start()
		self.check()

	def test_rollback_after_released_savepoints_single_worker(self):
		self.setup_table("orioledb.recovery_pool_size = 1")
		self.rollback_txn(True)
		self.node.stop(['-m', 'immediate'])
		self.node.start()
		self.check()

	def test_rollback_after_released_savepoints_twice(self):
		"""Two crash restarts, the second after more such transactions."""
		self.setup_table("orioledb.recovery_pool_size = 1")
		self.rollback_txn(True)
		self.node.stop(['-m', 'immediate'])
		self.node.start()
		self.check()
		self.rollback_txn(False)
		self.rollback_txn(True)
		self.node.stop(['-m', 'immediate'])
		self.node.start()
		self.check()

	def rolled_back_then_released(self, checkpoint_at):
		"""A savepoint rolled back, another released, then commit: the
		released one's insert stays, the rolled back one's changes do not."""
		self.setup_table()
		node = self.node
		con = node.connect()
		steps = [
		    "SAVEPOINT s1",
		    "UPDATE o_t SET grp = 101 WHERE id = 2",
		    "INSERT INTO o_t VALUES (5000, 1, 'gone')",
		    "ROLLBACK TO SAVEPOINT s1",
		    "SAVEPOINT s2",
		    "INSERT INTO o_t VALUES (5001, 2, 'kept')",
		    "RELEASE SAVEPOINT s2",
		]
		for i, step in enumerate(steps):
			if i == checkpoint_at:
				node.safe_psql("CHECKPOINT")
			con.execute(step)
		if checkpoint_at == len(steps):
			node.safe_psql("CHECKPOINT")
		con.commit()
		con.close()
		expected = self.expected() + [(5001, 2)]
		for n in (False, True):
			if n:
				node.stop(['-m', 'immediate'])
				node.start()
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""), f"crash {n}")
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""), f"crash {n}")

	def test_rolled_back_then_released_checkpoint_0(self):
		self.rolled_back_then_released(0)

	def test_rolled_back_then_released_checkpoint_1(self):
		self.rolled_back_then_released(1)

	def test_rolled_back_then_released_checkpoint_2(self):
		self.rolled_back_then_released(2)

	def test_rolled_back_then_released_checkpoint_3(self):
		self.rolled_back_then_released(3)

	def test_rolled_back_then_released_checkpoint_4(self):
		self.rolled_back_then_released(4)

	def test_rolled_back_then_released_checkpoint_5(self):
		self.rolled_back_then_released(5)

	def test_rolled_back_then_released_checkpoint_6(self):
		self.rolled_back_then_released(6)

	def test_rolled_back_then_released_checkpoint_7(self):
		self.rolled_back_then_released(7)

	def top_level_after_rollback_to(self, checkpoint_at):
		"""A savepoint rolled back, then changes at the top level, then
		commit: those changes reach the secondary index too."""
		self.setup_table()
		node = self.node
		con = node.connect()
		steps = [
		    "SAVEPOINT s1",
		    "UPDATE o_t SET grp = 101 WHERE id = 2",
		    "UPDATE o_t SET grp = 102 WHERE id = 3",
		    "ROLLBACK TO SAVEPOINT s1",
		    "INSERT INTO o_t VALUES (5001, 12, 'kept')",
		    "DELETE FROM o_t WHERE id = 6",
		    "UPDATE o_t SET grp = 105 WHERE id = 7",
		]
		for i, step in enumerate(steps):
			if i == checkpoint_at:
				node.safe_psql("CHECKPOINT")
			con.execute(step)
		if checkpoint_at == len(steps):
			node.safe_psql("CHECKPOINT")
		con.commit()
		con.close()
		expected = [(g, 105 if g == 7 else g % 10)
		            for g in range(1, 1001) if g != 6] + [(5001, 12)]
		for n in (False, True):
			if n:
				node.stop(['-m', 'immediate'])
				node.start()
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""), f"crash {n}")
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""), f"crash {n}")

	def test_top_level_after_rollback_to_checkpoint_0(self):
		self.top_level_after_rollback_to(0)

	def test_top_level_after_rollback_to_checkpoint_1(self):
		self.top_level_after_rollback_to(1)

	def test_top_level_after_rollback_to_checkpoint_2(self):
		self.top_level_after_rollback_to(2)

	def test_top_level_after_rollback_to_checkpoint_3(self):
		self.top_level_after_rollback_to(3)

	def test_top_level_after_rollback_to_checkpoint_4(self):
		self.top_level_after_rollback_to(4)

	def test_top_level_after_rollback_to_checkpoint_5(self):
		self.top_level_after_rollback_to(5)

	def test_top_level_after_rollback_to_checkpoint_6(self):
		self.top_level_after_rollback_to(6)

	def test_top_level_after_rollback_to_checkpoint_7(self):
		self.top_level_after_rollback_to(7)

	# The savepoint rolled back, a second one under the same name released,
	# a third rolled back: the released one's changes stay.  A checkpoint
	# holding the released one open had the next savepoint's record roll the
	# checkpoint stacks back to it.
	SIBLING_STEPS = [
	    "SAVEPOINT s",
	    "UPDATE o_t SET grp = 101 WHERE id = 2",
	    "ROLLBACK TO SAVEPOINT s",
	    "SAVEPOINT s",
	    "DELETE FROM o_t WHERE id = 4",
	    "UPDATE o_t SET grp = 7 WHERE id = 3",
	    "INSERT INTO o_t VALUES (5001, 2, 'kept')",
	    "RELEASE SAVEPOINT s",
	    "SAVEPOINT s",
	    "UPDATE o_t SET grp = 105 WHERE id = 5",
	    "INSERT INTO o_t VALUES (5002, 3, 'gone')",
	    "ROLLBACK TO SAVEPOINT s",
	    "UPDATE o_t SET grp = 106 WHERE id = 6",
	]

	def released_sibling(self, flush):
		"""The checkpoint at each step in turn; with flush, the changes go
		out to WAL before it."""
		self.setup_table()
		node = self.node
		expected = sorted([(g, {
		    3: 7,
		    6: 106
		}.get(g, g % 10)) for g in range(1, 1001) if g != 4] + [(5001, 2)])
		for checkpoint_at in range(len(self.SIBLING_STEPS) + 1):
			node.safe_psql("""
				DROP TABLE IF EXISTS o_pad;
				CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
				TRUNCATE o_t;
				INSERT INTO o_t SELECT g, g % 10, repeat('x', 100)
					FROM generate_series(1, 1000) g;
			""")
			con = node.connect()
			for i, step in enumerate(self.SIBLING_STEPS + [None]):
				if i == checkpoint_at:
					if flush:
						# more than the local WAL buffer holds
						con.execute("INSERT INTO o_pad SELECT g, "
						            "repeat(md5(g::text), 10) "
						            "FROM generate_series(1, 100) g")
					node.safe_psql("CHECKPOINT")
				if step:
					con.execute(step)
			con.commit()
			con.close()
			node.stop(['-m', 'immediate'])
			node.start()
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""), f"checkpoint at {checkpoint_at}")
			self.assertEqual(
			    expected,
			    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id"""),
			    f"checkpoint at {checkpoint_at}")

	def test_released_sibling(self):
		self.released_sibling(False)

	def test_released_sibling_flushed(self):
		self.released_sibling(True)
