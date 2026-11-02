#!/usr/bin/env python3
# coding: utf-8

import random
import string

from .base_test import BaseTest


def incompressible(n, seed):
	rng = random.Random(seed)
	return ''.join(rng.choices(string.ascii_letters + string.digits, k=n))


class ToastReplaceValueTest(BaseTest):
	"""
	A TOASTed value replaced by another reads back as the new value alone:
	the chunks the old value had past the new one's end are gone, whatever
	the update before it was and whatever snapshot reads it.  A new value
	that fills its last chunk exactly used to be read with the old value's
	next chunk appended -- deleted, yet returned -- and failed the size
	check in generic_toast_get().
	"""

	def value_sql(self, size, seed):
		# hex digests do not compress much: the value takes size bytes
		return (f"(SELECT string_agg(md5('{seed}-' || i::text), '') "
		        f"FROM generate_series(1, {size} / 32) i)")

	def run_case(self, kept_update, old_snapshot=False, checkpoint=False):
		node = self.node
		node.start()
		node.safe_psql(f"""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, grp int NOT NULL,
				val text NOT NULL) USING orioledb;
			CREATE INDEX o_t_grp ON o_t (grp, id);
			INSERT INTO o_t VALUES (1, 0, {self.value_sql(8128, 'a')});
			INSERT INTO o_t VALUES (2, 0, 'short');
		""")
		reader = None
		if old_snapshot:
			reader = node.connect()
			reader.begin('repeatable read')
			self.assertEqual(
			    [(8128, )],
			    reader.execute("SELECT length(val) FROM o_t WHERE id = 1"))
		con = node.connect()
		con.execute(kept_update)
		con.rollback()
		if checkpoint:
			node.safe_psql("CHECKPOINT")
		con.execute(f"UPDATE o_t SET val = {self.value_sql(5312, 'b')} "
		            "WHERE id = 1")
		con.commit()
		con.close()
		q = "SELECT length(val), md5(val) FROM o_t WHERE id = 1"
		new = node.execute(f"SELECT 5312, md5({self.value_sql(5312, 'b')})")
		self.assertEqual(new, node.execute(q))
		self.assertEqual([(5312, )],
		                 node.execute("SELECT sum(length(val)) - 5 FROM o_t"))
		rr = node.connect()
		rr.begin('repeatable read')
		self.assertEqual(new, rr.execute(q))
		rr.rollback()
		rr.close()
		if reader:
			self.assertEqual(
			    [(8128, )],
			    reader.execute("SELECT length(val) FROM o_t WHERE id = 1"))
			reader.rollback()
			reader.close()
		node.stop()

	def test_kept_value_update_rolled_back(self):
		self.run_case("UPDATE o_t SET grp = 7 WHERE id = 1")

	def test_kept_value_update_rolled_back_old_snapshot(self):
		self.run_case("UPDATE o_t SET grp = 7 WHERE id = 1", old_snapshot=True)

	def test_kept_value_update_rolled_back_checkpoint(self):
		self.run_case("UPDATE o_t SET grp = 7 WHERE id = 1", checkpoint=True)

	def test_same_value_update_rolled_back(self):
		self.run_case("UPDATE o_t SET val = val WHERE id = 1")

	def test_kept_value_update_twice_rolled_back(self):
		self.run_case("UPDATE o_t SET grp = 7 WHERE id = 1; "
		              "UPDATE o_t SET grp = 8 WHERE id = 1")

	def replace_value(self, old_len, new_len):
		"""The value is replaced by one of new_len characters; nothing of the
		old one is left behind."""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_t (id int PRIMARY KEY, val text NOT NULL)
				USING orioledb;
		""")
		con = node.connect()
		old = incompressible(old_len, 1)
		new = incompressible(new_len, 2)
		con.execute("INSERT INTO o_t VALUES (1, %s)", old)
		con.commit()
		con.execute("UPDATE o_t SET val = %s WHERE id = 1", new)
		con.commit()
		self.assertEqual(
		    [(new_len, )],
		    node.execute("SELECT length(val) FROM o_t WHERE id = 1"))
		self.assertEqual(
		    new,
		    node.execute("SELECT val FROM o_t WHERE id = 1")[0][0])
		con.close()
		node.stop()

	def test_replace_by_two_full_chunks(self):
		# 4 bytes of varlena header: 5314 bytes, two chunks of 2657
		self.replace_value(8123, 5310)

	def test_replace_by_two_chunks_and_a_byte(self):
		self.replace_value(8123, 5311)

	def test_replace_by_one_full_chunk(self):
		self.replace_value(8123, 2653)

	def test_replace_by_three_full_chunks(self):
		self.replace_value(12000, 7967)

	def test_replace_longer_by_two_full_chunks(self):
		self.replace_value(20000, 5310)
