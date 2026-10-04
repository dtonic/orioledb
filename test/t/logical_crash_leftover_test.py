#!/usr/bin/env python3
# coding: utf-8

import re
import threading

from .base_test import BaseTest

# A transaction cut short by a crash leaves the changes it had written in WAL
# and no commit or rollback after them.  Logical xids come from a small pool,
# and a backend that gets the same slot after the restart gets the same
# logical xid; its changes must not be decoded together with the dead
# transaction's.


class LogicalCrashLeftoverTest(BaseTest):

	def test_transaction_in_flight_at_a_crash_is_not_decoded(self):
		node = self.node
		node.append_conf('postgresql.conf', "wal_level = logical\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_cl (id bigint PRIMARY KEY, tag text) USING orioledb;
			CREATE TABLE h_cl (seq bigserial PRIMARY KEY, id bigint) USING heap;
			CREATE FUNCTION o_cl_f() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN INSERT INTO h_cl (id) VALUES (NEW.id); RETURN NEW; END $$;
			CREATE TRIGGER o_cl_t AFTER INSERT ON o_cl
				FOR EACH ROW EXECUTE FUNCTION o_cl_f();
		""")
		node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('cl', 'test_decoding');"
		)
		node.safe_psql('postgres', "CHECKPOINT;")

		# Transactions in flight at the crash, big enough to write WAL
		# before their commit.
		conns = [node.connect() for _ in range(4)]
		for i, c in enumerate(conns):
			c.begin()
			c.execute("INSERT INTO o_cl SELECT %d + g, 'dead' "
			          "FROM generate_series(1, 40000) g" % ((i + 1) * 1000000))
		node.stop(['-m', 'immediate'])
		for c in conns:
			try:
				c.close()
			except Exception:
				pass
		node.start()

		# Backends after the restart take the same slots, and with them the
		# same logical xids.
		after = [node.connect() for _ in range(8)]
		for i, c in enumerate(after):
			c.begin()
			c.execute("INSERT INTO o_cl VALUES (%d, 'live')" % i)
			c.commit()
		for c in after:
			c.close()

		tags = {}
		for (data, ) in node.execute(
		    "SELECT data FROM pg_logical_slot_get_changes('cl', NULL, NULL);"):
			m = re.match(r"table public\.o_cl: INSERT: .*tag\[text\]:'(\w+)'",
			             data)
			if m:
				tags[m.group(1)] = tags.get(m.group(1), 0) + 1
		self.assertEqual(tags, {'live': 8})
		self.assertEqual(node.execute("SELECT count(*) FROM o_cl;")[0][0], 8)
		node.stop()
