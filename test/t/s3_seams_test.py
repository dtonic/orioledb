#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import time

from .s3_base_test import S3BaseTest
from .s3_base_test import s3_test_attrs

from testgres.exceptions import QueryException


class S3SeamsTest(S3BaseTest):
	"""The engine's S3 touch points in btree/io.c that have no test of their
	own: the part floor file of a dropped tree, the 48-bit address guard."""

	def s3_conf(self):
		return f"""
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_mode = true
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3

			archive_mode = on
			archive_library = 'orioledb'
		"""

	def truncate_in_place(self, node):
		"""A table whose tree was emptied in place: the second TRUNCATE of
		a tree the same transaction made keeps its relnode, and leaves the
		part floor file (s3floor_<relnode>) beside the tree's files."""
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int, v text) USING orioledb;
		""")
		con = node.connect()
		con.begin()
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('a', 200)
				FROM generate_series(1, 2000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('b', 200)
				FROM generate_series(1, 2000) g;
		""")
		con.commit()
		con.close()
		floors = self.floor_files(node)
		self.assertEqual(1, len(floors), floors)
		return floors[0]

	def floor_files(self, node):
		return glob.glob(
		    os.path.join(node.data_dir, 'orioledb_data', '*', 's3floor_*'))

	def test_s3_part_past_the_address_space_is_refused(self):
		"""A part whose blocks would lie past the 48 bits a downlink holds
		stops the server instead of being stored under another part's
		address.  Two EiB of parts is out of reach, so the floor file is
		made to say the tree handed out 2^41 of them."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		floor = self.truncate_in_place(node)
		node.safe_psql("CHECKPOINT;")
		node.stop()
		with open(floor, 'w') as f:
			f.write(f"{1 << 41}\n")
		node.start()
		# the write that meets the guard is the page's, at the checkpoint or
		# an eviction before it: the server stops there, so the statements
		# may or may not come back
		try:
			node.safe_psql("INSERT INTO o_test VALUES (1, 'c'); CHECKPOINT;")
		except QueryException:
			pass
		deadline = time.time() + 60
		while time.time() < deadline:
			with open(node.pg_log_file) as f:
				log = f.read()
			if "past the 48-bit address space of a downlink" in log:
				break
			time.sleep(0.5)
		self.assertIn("past the 48-bit address space of a downlink", log)
		self.assertIn("PANIC", log)
		try:
			node.stop(['-m', 'immediate'])
		except Exception:
			pass
