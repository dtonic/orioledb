#!/usr/bin/env python3
# coding: utf-8

import re

from .base_test import BaseTest

# A table rewrite copies the table into its new relnode.  Logical decoding
# must not emit the copy as inserts, while the changes made around it in the
# same transaction are emitted as usual.


class LogicalRewriteTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.start()
		self.node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_rw (id int PRIMARY KEY, w int, note text) USING orioledb;
			INSERT INTO o_rw SELECT g, g, 'n' || g FROM generate_series(1, 1000) g;
		""")
		self.node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('rw', 'test_decoding');"
		)

	def changes(self):
		counts = {}
		for (data, ) in self.node.execute(
		    "SELECT data FROM pg_logical_slot_get_changes('rw', NULL, NULL);"):
			m = re.match(r"table public\.(\w+): (\w+):", data)
			if m:
				counts[m.groups()] = counts.get(m.groups(), 0) + 1
		return counts

	def test_rewrite_is_not_decoded(self):
		node = self.node
		node.safe_psql('postgres',
		               "ALTER TABLE o_rw ALTER COLUMN w TYPE bigint;")
		node.safe_psql(
		    'postgres',
		    "ALTER TABLE o_rw ADD COLUMN ts timestamptz DEFAULT clock_timestamp();"
		)
		node.safe_psql(
		    'postgres', """
			BEGIN;
			UPDATE o_rw SET note = 'changed' WHERE id <= 3;
			ALTER TABLE o_rw ALTER COLUMN note TYPE varchar(64);
			INSERT INTO o_rw (id, w, note) VALUES (2001, 1, 'after');
			COMMIT;
		""")
		self.assertEqual(self.changes(), {
		    ('o_rw', 'UPDATE'): 3,
		    ('o_rw', 'INSERT'): 1
		})

	def test_table_created_and_filled_in_one_transaction(self):
		node = self.node
		node.safe_psql(
		    'postgres', """
			BEGIN;
			CREATE TABLE o_new (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_new SELECT generate_series(1, 10);
			COMMIT;
		""")
		self.assertEqual(self.changes(), {('o_new', 'INSERT'): 10})

	def test_failed_rewrite_leaves_no_mark(self):
		node = self.node
		con = node.connect()
		con.execute("SAVEPOINT a;")
		try:
			con.execute("ALTER TABLE o_rw ALTER COLUMN note TYPE int "
			            "USING note::int;")
		except Exception:
			con.execute("ROLLBACK TO SAVEPOINT a;")
		con.execute("INSERT INTO o_rw (id, w, note) VALUES (3001, 1, 'x');")
		con.commit()
		con.close()
		self.assertEqual(self.changes(), {('o_rw', 'INSERT'): 1})
