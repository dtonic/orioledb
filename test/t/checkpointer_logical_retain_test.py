#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

# Under wal_level = logical, a process keeps the global xmin back while the
# WAL records it writes may still be decoded.  The checkpointer deletes system
# cache entries in autonomous transactions after a checkpoint, and nothing
# released what their records held: the global xmin stayed there until the
# checkpointer exited, so VACUUM of a bridged index, which removes dead bridge
# entries only below the global xmin, kept every entry deleted since.


class CheckpointerLogicalRetainTest(BaseTest):

	def test_checkpointer_cleanup_does_not_hold_the_global_xmin(self):
		node = self.node
		node.append_conf('postgresql.conf', "wal_level = logical\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE TYPE o_enum AS ENUM ('a', 'b', 'c');
			ALTER TYPE o_enum ADD VALUE 'd';
			ALTER TYPE o_enum RENAME VALUE 'd' TO 'e';
			CREATE TYPE o_range AS RANGE (subtype = int8);
			CREATE TYPE o_composite AS (x timestamp, y float);
			CREATE TABLE o_sys_cache (
				key o_enum,
				key2 o_range,
				key3 o_composite,
				key4 int[],
				PRIMARY KEY (key, key2, key3, key4)
			) USING orioledb;
			DROP TYPE o_range CASCADE;
			DROP TABLE o_sys_cache;
			DROP TYPE o_enum;
			DROP TYPE o_composite;
		""")
		# The checkpointer deletes the dropped types' system cache entries
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql('postgres', "CHECKPOINT;")
		run_xmin = node.execute(
		    "SELECT runxmin FROM orioledb_get_xid_meta();")[0][0]

		# VACUUM of a bridged index brings the global xmin up to date
		node.safe_psql(
		    'postgres', """
			CREATE TABLE o_bridged (id serial PRIMARY KEY, p point)
				USING orioledb;
			INSERT INTO o_bridged (p)
				SELECT point(0.01 * i, 0.02 * i) FROM generate_series(1, 5) i;
			CREATE INDEX o_bridged_p_idx ON o_bridged USING gist (p);
			DELETE FROM o_bridged;
		""")
		node.safe_psql('postgres', "VACUUM o_bridged;")
		global_xmin = node.execute(
		    "SELECT globalxmin FROM orioledb_get_xid_meta();")[0][0]
		self.assertGreaterEqual(global_xmin, run_xmin)
		node.stop()
