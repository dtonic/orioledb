#!/usr/bin/env python3
# coding: utf-8

import re
import threading

from .base_test import BaseTest

# A transaction cut short by a crash leaves the changes it had written in WAL
# and no commit or rollback after them.  Logical xids come from a small pool,
# and a backend that gets the same slot after the restart gets the same
# logical xid; its changes must not be decoded together with the dead
# transaction's.  A logical xid is also the value of a heap xid that has
# ended -- on a young cluster the backend's slot number itself -- so it can
# name a dead heap transaction too.


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

	def test_heap_rows_of_a_transaction_in_flight_at_a_crash_are_not_decoded(
	        self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "wal_level = logical\nmax_connections = 100\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_cl (id bigint PRIMARY KEY, tag text) USING orioledb;
			CREATE TABLE h_cl (seq bigserial PRIMARY KEY, id bigint, tag text)
				USING heap;
			CREATE FUNCTION o_cl_f() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN INSERT INTO h_cl (id, tag) VALUES (NEW.id, NEW.tag);
			RETURN NEW; END $$;
			CREATE TRIGGER o_cl_t AFTER INSERT ON o_cl
				FOR EACH ROW EXECUTE FUNCTION o_cl_f();
		""")
		node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('cl', 'test_decoding');"
		)
		node.safe_psql('postgres', "CHECKPOINT;")

		# Mixed transactions in flight at the crash, on 40 consecutive heap
		# xids: one of them is a multiple of 32, the logical xid a backend
		# gets on a young cluster (slot number * 32).
		conns = [node.connect() for _ in range(40)]
		for i, c in enumerate(conns):
			c.begin()
			c.execute("INSERT INTO o_cl SELECT %d + g, 'dead' "
			          "FROM generate_series(1, 2000) g" % ((i + 1) * 1000000))
		node.stop(['-m', 'immediate'])
		for c in conns:
			try:
				c.close()
			except Exception:
				pass
		node.start()

		# Mixed transactions after the restart, open together so that each
		# takes a slot of its own.
		after = [node.connect() for _ in range(40)]
		for i, c in enumerate(after):
			c.begin()
			c.execute("INSERT INTO o_cl VALUES (%d, 'live')" % i)
		for c in after:
			c.commit()
			c.close()

		tags = {}
		for (data, ) in node.execute(
		    "SELECT data FROM pg_logical_slot_get_changes('cl', NULL, NULL);"):
			m = re.match(r"table public\.(\w+): INSERT: .*tag\[text\]:'(\w+)'",
			             data)
			if m:
				key = m.group(1) + ':' + m.group(2)
				tags[key] = tags.get(key, 0) + 1
		self.assertEqual(tags, {'o_cl:live': 40, 'h_cl:live': 40})
		node.stop()

	def test_a_transaction_in_flight_at_a_crash_does_not_hold_the_slot(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "wal_level = logical\nmax_connections = 100\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TABLE o_cl (id bigint PRIMARY KEY, tag text) USING orioledb;
			CREATE TABLE h_cl (id bigint, tag text) USING heap;
			CREATE FUNCTION o_cl_f() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN INSERT INTO h_cl VALUES (NEW.id, NEW.tag); RETURN NEW; END $$;
			CREATE TRIGGER o_cl_t AFTER INSERT ON o_cl
				FOR EACH ROW EXECUTE FUNCTION o_cl_f();
		""")
		node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('cl', 'test_decoding');"
		)

		# The transaction in flight at the crash takes a backend slot that no
		# backend after the restart takes again, so its logical xid is not
		# reused.
		idle = [node.connect() for _ in range(30)]
		for c in idle:
			c.execute("SELECT 1;")
		dead = node.connect()
		dead.begin()
		dead.execute("INSERT INTO o_cl SELECT g, 'dead' "
		             "FROM generate_series(1, 2000) g")
		# A commit flushes WAL up to its record, the dead transaction's too
		node.safe_psql('postgres', "INSERT INTO o_cl VALUES (0, 'flush');")
		crash_lsn = node.execute("SELECT pg_current_wal_lsn();")[0][0]
		node.stop(['-m', 'immediate'])
		for c in idle + [dead]:
			try:
				c.close()
			except Exception:
				pass
		node.start()

		# One backend, on one slot: none of its logical xids is the dead
		# transaction's.
		live = node.connect()
		for i in range(50):
			live.execute("INSERT INTO o_cl VALUES (%d, 'live');" % (10000 + i))
			live.commit()
		# Running xacts records: the first drops the heap half of the dead
		# transaction, the next can take a restart point past it.
		for i in range(2):
			live.execute("CHECKPOINT;")
			live.commit()
			live.execute("INSERT INTO o_cl VALUES (%d, 'last');" % (i + 1))
			live.commit()
		live.close()
		node.execute(
		    "SELECT count(*) FROM pg_logical_slot_get_changes('cl', NULL, NULL);"
		)
		moved = node.execute(
		    "SELECT restart_lsn > '%s'::pg_lsn FROM pg_replication_slots "
		    "WHERE slot_name = 'cl';" % crash_lsn)[0][0]
		self.assertTrue(moved)
		node.stop()
