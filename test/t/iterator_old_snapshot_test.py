#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class IteratorOldSnapshotTest(BaseTest):
	"""
	An ordered scan of the primary key under a snapshot taken before pages
	were split or merged returns every row the snapshot sees, as a
	sequential scan does.
	"""

	def value_sql(self, size, key='id'):
		# hex digests do not compress much: the rows stay as large as asked
		return (f"(SELECT string_agg(md5({key}::text || '-' || i::text || "
		        f"'-{size}'), '') "
		        f"FROM generate_series(1, {size} / 32) i)")

	def scans(self, con):
		q = "SELECT id, length(val), md5(val) FROM o_t ORDER BY id"
		con.execute("SET LOCAL enable_indexscan = off")
		con.execute("SET LOCAL enable_bitmapscan = off")
		con.execute("SET LOCAL enable_seqscan = on")
		seq = con.execute(q)
		con.execute("SET LOCAL enable_seqscan = off")
		con.execute("SET LOCAL enable_indexscan = on")
		pk = con.execute(q)
		back = con.execute(q.replace("ORDER BY id", "ORDER BY id DESC"))
		return seq, pk, back[::-1]

	def run_case(self, rows, before_size, changes):
		node = self.node
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, val text NOT NULL)
				USING orioledb;
			INSERT INTO o_t SELECT g, {self.value_sql(before_size, 'g')}
				FROM generate_series(1, {rows}) g;
		""")
		reader = node.connect()
		reader.begin('repeatable read')
		seq, pk, back = self.scans(reader)
		self.assertEqual(rows, len(seq))
		self.assertEqual((seq, seq), (pk, back))
		node.safe_psql(changes)
		seq2, pk2, back2 = self.scans(reader)
		self.assertEqual(seq, seq2, "sequential scan")
		self.assertEqual(seq, pk2, "primary key, forward")
		self.assertEqual(seq, back2, "primary key, backward")
		reader.rollback()
		reader.close()
		node.stop()

	def test_split(self):
		"""Small rows on one page grow: the page splits."""
		self.run_case(
		    40, 100, f"""
			UPDATE o_t SET val = {self.value_sql(1500)};
		""")

	def test_merge(self):
		"""Large rows over several pages shrink: the pages merge."""
		self.run_case(
		    40, 1500, f"""
			UPDATE o_t SET val = {self.value_sql(64)};
		""")

	def test_split_some(self):
		"""Rows in the middle grow."""
		self.run_case(
		    60, 200, f"""
			UPDATE o_t SET val = {self.value_sql(1500)} WHERE id BETWEEN 20 AND 40;
		""")

	def test_merge_after_deletes(self):
		"""Rows in the middle go: their pages merge with the neighbours."""
		self.run_case(
		    60, 1500, """
			DELETE FROM o_t WHERE id BETWEEN 10 AND 50;
		""")

	def test_split_then_merge(self):
		self.run_case(
		    60, 200, f"""
			UPDATE o_t SET val = {self.value_sql(1500)} WHERE id BETWEEN 20 AND 40;
			UPDATE o_t SET val = {self.value_sql(64)} WHERE id BETWEEN 20 AND 40;
		""")

	def test_many_rounds(self):
		"""Grow and shrink different ranges over and over."""
		stmts = []
		for i in range(20):
			lo = (i * 7) % 50 + 1
			size = 1500 if i % 2 == 0 else 64
			stmts.append(f"UPDATE o_t SET val = {self.value_sql(size)} "
			             f"WHERE id BETWEEN {lo} AND {lo + 12};")
		self.run_case(60, 600, "\n".join(stmts))

	def test_split_rolled_back(self):
		"""Rows grow (the pages split) and the transaction rolls back (they
		shrink and merge again), over and over."""
		node = self.node
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, val text NOT NULL)
				USING orioledb;
			INSERT INTO o_t SELECT g, {self.value_sql(600, 'g')}
				FROM generate_series(1, 60) g;
		""")
		reader = node.connect()
		reader.begin('repeatable read')
		seq, pk, back = self.scans(reader)
		self.assertEqual((seq, seq), (pk, back))
		writer = node.connect()
		for i in range(40):
			lo = (i * 7) % 50 + 1
			writer.execute(f"UPDATE o_t SET val = {self.value_sql(3000)} "
			               f"WHERE id BETWEEN {lo} AND {lo + 6}")
			if i % 3 == 2:
				writer.commit()
			else:
				writer.rollback()
			seq2, pk2, back2 = self.scans(reader)
			self.assertEqual(seq, seq2, f"round {i}: sequential scan")
			self.assertEqual(seq, pk2, f"round {i}: primary key, forward")
			self.assertEqual(seq, back2, f"round {i}: primary key, backward")
		reader.rollback()
		reader.close()
		writer.close()
		node.stop()

	def test_split_rolled_back_no_seq_scan(self):
		"""The same, the reader reading only through the primary key: no
		sequential scan runs on the tree when the pages split."""
		node = self.node
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, val text NOT NULL)
				USING orioledb;
			INSERT INTO o_t SELECT g, {self.value_sql(600, 'g')}
				FROM generate_series(1, 60) g;
		""")
		q = ("SELECT id, length(val), md5(val) FROM o_t "
		     "WHERE id > 0 ORDER BY id")
		expected = node.execute(q)
		reader = node.connect()
		reader.begin('repeatable read')
		reader.execute("SET LOCAL enable_seqscan = off")
		reader.execute("SET LOCAL enable_bitmapscan = off")
		self.assertEqual(expected, reader.execute(q))
		writer = node.connect()
		for i in range(40):
			lo = (i * 7) % 50 + 1
			writer.execute(f"UPDATE o_t SET val = {self.value_sql(3000)} "
			               f"WHERE id BETWEEN {lo} AND {lo + 6}")
			if i % 3 == 2:
				writer.commit()
			else:
				writer.rollback()
			self.assertEqual(expected, reader.execute(q), f"round {i}")
		reader.rollback()
		reader.close()
		writer.close()
		node.stop()
