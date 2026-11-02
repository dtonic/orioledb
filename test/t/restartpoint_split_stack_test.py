#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import struct
import threading

from .base_test import BaseTest
from .base_test import wait_checkpointer_stopevent

# uint32 count, then that many XidFileRec: OXid, XidRecKind (+pad),
# UndoStackLocations (4 x UndoLocation), UndoLocation.
XID_REC_SIZE = 8 + 8 + 4 * 8 + 8
XID_HEADER_SIZE = 4
# XidRecUndoRegularNoSecondary and XidRecUndoRegularSecondary
SPLIT_KINDS = {7, 8}


class RestartpointSplitStackTest(BaseTest):
	"""
	A standby recovering from a checkpoint of the primary can split a
	checkpoint undo stack (see CheckpointUndoStack in recovery.c).  Its
	restartpoint writes the halves down, and a restart from it reads them
	back: the rest of the transaction comes out the same in the primary key
	and the secondary index.
	"""

	def xid_kinds(self, node):
		files = glob.glob(os.path.join(node.data_dir, 'orioledb_data',
		                               '*.xid'))
		newest = max(files, key=lambda f: int(os.path.basename(f)[:-4]))
		with open(newest, 'rb') as f:
			blob = f.read()
		count = struct.unpack('<I', blob[:XID_HEADER_SIZE])[0]
		kinds = set()
		for i in range(count):
			off = XID_HEADER_SIZE + i * XID_REC_SIZE
			kinds.add(struct.unpack('<i', blob[off + 8:off + 12])[0])
		return kinds

	def check(self, node, want):
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_indexscan = off; SET enable_bitmapscan = off;
				SET enable_indexonlyscan = off;
				SELECT id, grp FROM o_t ORDER BY id"""), f"{node.name}: sequential scan")
		self.assertEqual(
		    want,
		    node.execute("""
				SET enable_seqscan = off; SET enable_bitmapscan = off;
				SELECT id, grp FROM o_t WHERE grp >= -1000 ORDER BY id"""),
		    f"{node.name}: secondary index")

	def run_case(self, finish):
		master = self.node
		master.append_conf("""
			checkpoint_timeout = 1d
			max_wal_size = 4GB
			orioledb.enable_stopevents = true
		""")
		master.start()
		master.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL)
				USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t SELECT g, g % 10 FROM generate_series(1, 1000) g;
			-- its primary key goes after o_t's, before any secondary index
			CREATE TABLE o_z (id int PRIMARY KEY) USING orioledb;
			CREATE TABLE o_pad (id int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO o_z VALUES (1);
		""")
		z = master.execute("SELECT 'o_z'::regclass::oid")[0][0]

		con = master.connect()
		con.begin()
		con.execute("UPDATE o_t SET grp = 20 WHERE id = 500")

		# the checkpoint the standby starts from stops having written o_t's
		# primary key; the savepoint goes by meanwhile
		master.safe_psql("SELECT pg_stopevent_set('checkpoint_table_start', "
		                 f"'$.table.reloid == {z}')")
		made = []
		backup = threading.Thread(
		    target=lambda: made.append(self.getReplica()))
		backup.start()
		wait_checkpointer_stopevent(master)
		con.execute("SAVEPOINT s")
		con.execute("UPDATE o_t SET grp = 13 WHERE id <= 10")
		con.execute("ROLLBACK TO SAVEPOINT s")
		con.execute("UPDATE o_t SET grp = 14 WHERE id = 200")
		con.execute("INSERT INTO o_pad SELECT g, repeat(md5(g::text), 10) "
		            "FROM generate_series(1, 200) g")
		# a commit flushes the WAL
		master.safe_psql("INSERT INTO o_z VALUES (2)")
		master.safe_psql("SELECT pg_stopevent_reset('checkpoint_table_start')")
		backup.join()

		with made[0] as replica:
			replica.append_conf("checkpoint_timeout = 1d")
			# its first recovery splits the stack
			replica.start()
			self.catchup_orioledb(replica)

			# its restartpoint writes the halves down
			master.safe_psql("CHECKPOINT")
			self.catchup_orioledb(replica)
			replica.safe_psql("CHECKPOINT")
			self.assertTrue(SPLIT_KINDS <= self.xid_kinds(replica),
			                self.xid_kinds(replica))

			# and a restart from it reads them back
			replica.stop(['-m', 'immediate'])
			replica.start()
			self.catchup_orioledb(replica)

			rows = {g: g % 10 for g in range(1, 1001)}
			if finish == 'rollback to savepoint':
				con.execute("ROLLBACK TO SAVEPOINT s")
				con.execute("UPDATE o_t SET grp = 15 WHERE id = 300")
				con.commit()
				rows[500] = 20
				rows[300] = 15
			elif finish == 'commit':
				con.commit()
				rows[500] = 20
				rows[200] = 14
			else:
				con.rollback()
			con.close()
			self.catchup_orioledb(replica)
			want = sorted(rows.items())
			self.check(master, want)
			self.check(replica, want)
			replica.stop()
		master.stop()

	def test_commit(self):
		self.run_case('commit')

	def test_rollback_to_savepoint(self):
		self.run_case('rollback to savepoint')

	def test_rollback(self):
		self.run_case('rollback')
