#!/usr/bin/env python3
# coding: utf-8

import re

from .base_test import BaseTest

# Decoded changes of transactions that release and roll back savepoints must
# be the committed rows: nothing a released subtransaction wrote may be lost,
# and nothing a rolled back one wrote may be emitted.


class LogicalSubxactDecodingTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.start()
		self.node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_sx (id int PRIMARY KEY, v int) USING orioledb;
			CREATE TABLE h_sx (id int PRIMARY KEY, v int) USING heap;
		""")
		self.node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('sx', 'test_decoding');"
		)

	def decoded(self):
		rows = {'o_sx': {}, 'h_sx': {}}
		changes = self.node.execute(
		    "SELECT data FROM pg_logical_slot_get_changes('sx', NULL, NULL);")
		for (data, ) in changes:
			m = re.match(r"table public\.(\w+): (INSERT|UPDATE|DELETE): (.*)",
			             data)
			if not m:
				continue
			table, op, rest = m.groups()
			key = int(re.search(r"id\[integer\]:(\d+)", rest).group(1))
			if op == 'DELETE':
				rows[table].pop(key, None)
			else:
				rows[table][key] = int(
				    re.search(r"v\[integer\]:(-?\d+)", rest).group(1))
		return rows

	def assertDecodedIsTable(self, *transactions):
		for transaction in transactions:
			self.node.safe_psql('postgres', transaction)
		decoded = self.decoded()
		for table in ('o_sx', 'h_sx'):
			real = dict(self.node.execute(f"SELECT id, v FROM {table};"))
			self.assertEqual(decoded[table], real, table)

	def test_release_then_rollback(self):
		self.assertDecodedIsTable(
		    """BEGIN; INSERT INTO o_sx VALUES (1, 0); SAVEPOINT a;
			INSERT INTO o_sx VALUES (2, 0); RELEASE a; SAVEPOINT b;
			INSERT INTO o_sx VALUES (3, 0); ROLLBACK TO b; COMMIT;""",
		    """BEGIN; INSERT INTO o_sx VALUES (4, 0); SAVEPOINT a;
			INSERT INTO o_sx VALUES (5, 0); RELEASE a; SAVEPOINT b;
			ROLLBACK TO b; COMMIT;""", """DO $$BEGIN INSERT INTO o_sx VALUES (6, 0);
			BEGIN INSERT INTO o_sx VALUES (7, 0);
			EXCEPTION WHEN others THEN NULL; END;
			BEGIN INSERT INTO o_sx VALUES (8, 0); RAISE EXCEPTION 'x';
			EXCEPTION WHEN others THEN NULL; END; END$$;""")

	def test_released_child_of_rolled_back_savepoint(self):
		self.assertDecodedIsTable(
		    """BEGIN; INSERT INTO o_sx VALUES (1, 0); SAVEPOINT a;
			INSERT INTO o_sx VALUES (2, 0); SAVEPOINT b;
			INSERT INTO o_sx VALUES (3, 0); RELEASE b; ROLLBACK TO a;
			COMMIT;""")

	def test_empty_released_child_number_reused(self):
		# An empty subtransaction released and rolled back with its parent,
		# then its number taken by a later savepoint.
		self.assertDecodedIsTable(
		    """BEGIN; SAVEPOINT s0; INSERT INTO o_sx VALUES (1, 0);
			SAVEPOINT s1; SAVEPOINT s2; RELEASE s2; ROLLBACK TO s1;
			UPDATE o_sx SET v = v + 1 WHERE id = 1; RELEASE s0;
			INSERT INTO o_sx VALUES (2, 0); SAVEPOINT s0; ROLLBACK TO s0;
			COMMIT;""")

	def test_savepoints_before_first_change(self):
		self.assertDecodedIsTable("""BEGIN; SAVEPOINT s0; SAVEPOINT s1;
			INSERT INTO o_sx VALUES (1, 0); ROLLBACK TO s1;
			INSERT INTO o_sx VALUES (2, 0); RELEASE s0; SAVEPOINT s0;
			INSERT INTO o_sx VALUES (3, 0); ROLLBACK TO s0;
			INSERT INTO o_sx VALUES (4, 0); COMMIT;""")
		self.assertEqual(self.node.execute("SELECT id FROM o_sx ORDER BY id;"),
		                 [(2, ), (4, )])

	def test_heap_xid_after_oriole_change(self):
		self.assertDecodedIsTable(
		    """BEGIN; INSERT INTO o_sx VALUES (1, 0); SAVEPOINT a;
			INSERT INTO h_sx VALUES (1, 0); RELEASE a; COMMIT;""",
		    """BEGIN; INSERT INTO o_sx VALUES (2, 0); SAVEPOINT a;
			INSERT INTO o_sx VALUES (3, 0); INSERT INTO h_sx VALUES (2, 0);
			ROLLBACK TO a; INSERT INTO o_sx VALUES (4, 0); COMMIT;""",
		    """BEGIN; INSERT INTO o_sx VALUES (5, 0); SAVEPOINT a;
			INSERT INTO h_sx VALUES (3, 0); INSERT INTO o_sx VALUES (6, 0);
			SAVEPOINT b; INSERT INTO o_sx VALUES (7, 0); RELEASE b;
			INSERT INTO o_sx VALUES (8, 0); ROLLBACK TO a; COMMIT;""")

	def test_rollback_after_released_subxact_with_heap_xid(self):
		# The subtransaction's joint commit must not let the transaction's
		# rollback go without a rollback record.  Without one, its changes
		# stay in the reorder buffer under its logical xid, and the next
		# transaction of the same backend that gets that xid commits them.
		con = self.node.connect()
		con.execute("""INSERT INTO o_sx VALUES (1, 0); SAVEPOINT a;
			INSERT INTO h_sx VALUES (1, 0); INSERT INTO o_sx VALUES (2, 0);
			RELEASE a;""")
		con.rollback()
		for i in range(3, 13):
			con.execute(f"INSERT INTO o_sx VALUES ({i}, 0);")
			con.commit()
		con.close()
		self.assertDecodedIsTable()
