#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

CASES = [
    # the indexed column changes, and changes back, in a savepoint
    ("INSERT INTO o_t VALUES (1001, 11, 'a')",
     "UPDATE o_t SET grp = 9 WHERE id = 1001"),
    ("INSERT INTO o_t VALUES (1002, 4, 'a')",
     "DELETE FROM o_t WHERE id = 1002"),
    ("INSERT INTO o_t VALUES (1003, 4, 'a'); DELETE FROM o_t WHERE id = 50",
     "UPDATE o_t SET grp = 7, v = 'bb' WHERE id = 1003"),
    # an existing row's index entry deleted and put back before the savepoint
    ("UPDATE o_t SET grp = 99 WHERE id = 60; UPDATE o_t SET grp = 12 WHERE id = 60",
     "UPDATE o_t SET grp = 5 WHERE id = 60"),
]

EXPECTED_QUERY = """
	SET enable_seqscan = off; SET enable_bitmapscan = off;
	SELECT id, grp FROM o_t WHERE grp >= 0 ORDER BY id
"""


class RecoverySkRollbackToSavepointTest(BaseTest):
	"""
	A transaction changes a row of its own, or one it changed back, in a
	savepoint it rolls back; replay lost the row's entry of the secondary
	index.  Replay took the entry back as one a checkpoint image holds ahead
	of the records, instead of deleting it, so the rollback to the savepoint
	had nothing to put back.  A worker that had no record of the transaction
	yet got none of its savepoints, and found none to roll back to, and left
	out the undo of the changes to the versions of its own, and to a table of
	the transaction's own.
	"""

	def setup_table(self, node):
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL, v text NOT NULL)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 16, 'x' FROM generate_series(1, 100) g;
			CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
		""")

	def run_cases(self, node, checkpoint_at=None, flush_all=False):
		"""Runs the cases in one session each, a checkpoint at the given step
		while they are in progress.  The first session's changes go out to
		WAL before the checkpoint, the others' after it too unless
		flush_all."""
		cons = [node.connect() for _ in CASES]
		for con, (before, _) in zip(cons, CASES):
			con.begin()
			con.execute(before)
		if checkpoint_at == 'before_savepoint':
			self.checkpoint(node, cons, flush_all)
		for con, (_, in_savepoint) in zip(cons, CASES):
			con.execute("SAVEPOINT s")
			con.execute(in_savepoint)
		if checkpoint_at == 'in_savepoint':
			self.checkpoint(node, cons, flush_all)
		for con in cons:
			con.execute("ROLLBACK TO SAVEPOINT s")
			con.commit()
			con.close()
		return node.execute(EXPECTED_QUERY)

	def checkpoint(self, node, cons, flush_all):
		# more than the local WAL buffer holds: the changes go out before
		# the checkpoint
		for i, con in enumerate(cons if flush_all else cons[:1]):
			con.execute(
			    f"INSERT INTO o_pad SELECT g, repeat(md5(g::text), 10) "
			    f"FROM generate_series({i * 1000 + 1}, {i * 1000 + 100}) g")
		node.safe_psql("CHECKPOINT")
		for con in (cons if flush_all else cons[:1]):
			con.execute("DELETE FROM o_pad")

	def crash_case(self, checkpoint_at, flush_all=False):
		node = self.node
		node.start()
		self.setup_table(node)
		expected = self.run_cases(node, checkpoint_at, flush_all)
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(expected, node.execute(EXPECTED_QUERY))
		node.stop()

	def replica_case(self, pool_size):
		master = self.node
		master.start()
		self.setup_table(master)
		with self.getReplica() as replica:
			replica.append_conf(f"orioledb.recovery_pool_size = {pool_size}\n")
			replica.start()
			self.catchup_orioledb(replica)
			expected = self.run_cases(master)
			master.safe_psql("INSERT INTO o_pad VALUES (-1, 'x')")
			self.catchup_orioledb(replica)
			self.assertEqual(expected, replica.execute(EXPECTED_QUERY))
			replica.stop()
		master.stop()

	def test_replica(self):
		self.replica_case(1)

	def test_replica_workers(self):
		self.replica_case(3)

	# A row inserted, changed in a savepoint that is rolled back, committed;
	# then changed again by a transaction that rolls back whole.  A replica
	# lost the row's entry of the secondary index: the worker made no undo
	# for the change to the version of its own, so the rollback to the
	# savepoint had nothing to put back.
	def replica_rolled_back_update_case(self, pool_size):
		master = self.node
		master.start()
		self.setup_table(master)
		with self.getReplica() as replica:
			replica.append_conf(f"orioledb.recovery_pool_size = {pool_size}\n")
			replica.start()
			self.catchup_orioledb(replica)
			con = master.connect()
			con.begin()
			con.execute("INSERT INTO o_t VALUES (5001, 11, 'v')")
			con.execute("SAVEPOINT s")
			con.execute("UPDATE o_t SET grp = 9 WHERE id = 5001")
			con.execute("INSERT INTO o_t VALUES (5003, 9, 'n')")
			con.execute("DELETE FROM o_t WHERE id = 7")
			con.execute("ROLLBACK TO SAVEPOINT s")
			con.commit()
			con.execute("UPDATE o_t SET grp = 6 WHERE id = 5001")
			con.execute("INSERT INTO o_t VALUES (5002, 14, 'w')")
			con.execute("SAVEPOINT s")
			con.execute("INSERT INTO o_t VALUES (5004, 14, 'z')")
			con.execute("ROLLBACK TO SAVEPOINT s")
			con.rollback()
			con.close()
			expected = self.run_cases(master)
			master.safe_psql("INSERT INTO o_pad VALUES (-1, 'x')")
			self.catchup_orioledb(replica)
			self.assertEqual(expected, replica.execute(EXPECTED_QUERY))
			replica.stop()
		master.stop()

	def test_replica_rolled_back_update(self):
		self.replica_rolled_back_update_case(1)

	def test_replica_rolled_back_update_workers(self):
		self.replica_rolled_back_update_case(3)

	def test_crash(self):
		self.crash_case(None)

	def test_crash_checkpoint_before_savepoint(self):
		self.crash_case('before_savepoint')

	def test_crash_checkpoint_in_savepoint(self):
		self.crash_case('in_savepoint')

	def test_crash_flushed_checkpoint_before_savepoint(self):
		self.crash_case('before_savepoint', True)

	def test_crash_flushed_checkpoint_in_savepoint(self):
		self.crash_case('in_savepoint', True)

	# A table the transaction creates: no undo was made for its changes in
	# replay, which knew of no savepoint to roll back to.
	CREATED_STEPS = [
	    """CREATE TABLE o_n (id int PRIMARY KEY, grp int NOT NULL, v text NOT NULL)
			USING orioledb""",
	    "CREATE INDEX o_n_grp ON o_n (grp, id)",
	    "INSERT INTO o_n SELECT g, mod(g, 7), 'n' FROM generate_series(1, 50) g",
	    "SAVEPOINT s",
	    "UPDATE o_n SET grp = grp + 100 WHERE id <= 10",
	    "DELETE FROM o_n WHERE id BETWEEN 20 AND 25",
	    "INSERT INTO o_n VALUES (100, 3, 'z')",
	    "UPDATE o_n SET v = 'w' WHERE id = 30",
	    "ROLLBACK TO SAVEPOINT s",
	    "INSERT INTO o_n VALUES (200, 4, 'after')",
	]
	CREATED_QUERIES = [
	    """SET enable_indexscan = off; SET enable_indexonlyscan = off;
		SET enable_bitmapscan = off; SELECT id, grp, v FROM o_n ORDER BY id""",
	    """SET enable_seqscan = off; SET enable_bitmapscan = off;
		SELECT id, grp FROM o_n WHERE grp >= 0 ORDER BY id""",
	]

	def run_created(self, node, checkpoint_after=None):
		con = node.connect()
		con.begin()
		for i, step in enumerate(self.CREATED_STEPS):
			con.execute(step)
			if i == checkpoint_after:
				self.checkpoint(node, [con], False)
		con.commit()
		con.close()
		return [node.execute(q) for q in self.CREATED_QUERIES]

	def created_crash_case(self, checkpoint_after):
		node = self.node
		node.start()
		self.setup_table(node)
		expected = self.run_created(node, checkpoint_after)
		self.assertEqual(51, len(expected[0]))
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(expected,
		                 [node.execute(q) for q in self.CREATED_QUERIES])
		node.stop()

	def test_created_crash(self):
		self.created_crash_case(None)

	def test_created_crash_checkpoint_before_savepoint(self):
		self.created_crash_case(2)

	def test_created_crash_checkpoint_in_savepoint(self):
		self.created_crash_case(5)

	def test_created_replica(self):
		master = self.node
		master.start()
		self.setup_table(master)
		with self.getReplica() as replica:
			replica.start()
			self.catchup_orioledb(replica)
			expected = self.run_created(master)
			master.safe_psql("INSERT INTO o_pad VALUES (-1, 'x')")
			self.catchup_orioledb(replica)
			self.assertEqual(
			    expected, [replica.execute(q) for q in self.CREATED_QUERIES])
			replica.stop()
		master.stop()
