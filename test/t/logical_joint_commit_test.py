#!/usr/bin/env python3
# coding: utf-8

import re

from .base_test import BaseTest

# A transaction that writes both an OrioleDB table and a heap table commits
# twice in WAL: OrioleDB's joint commit record, then the heap COMMIT record.
# A slot can be confirmed up to a point between the two -- the end of WAL
# when a decoding session stopped.  The next session must still send the
# whole transaction, OrioleDB changes included.


class LogicalJointCommitTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.start()
		self.node.safe_psql(
		    'postgres', """
			CREATE EXTENSION orioledb;
			CREATE EXTENSION pg_walinspect;
			CREATE TABLE o_jc (id int PRIMARY KEY, v text) USING orioledb;
			CREATE TABLE h_jc (id int PRIMARY KEY, v text) USING heap;
		""")
		self.node.safe_psql(
		    'postgres',
		    "SELECT pg_create_logical_replication_slot('jc', 'test_decoding');"
		)

	def changes(self, upto_lsn=None):
		rows = self.node.execute(
		    "SELECT data FROM pg_logical_slot_get_changes('jc', %s, NULL);" %
		    ("'%s'" % upto_lsn if upto_lsn else "NULL"))
		return [
		    m.groups() for (data, ) in rows
		    for m in [re.match(r"table public\.(\w+): (\w+):", data)] if m
		]

	def test_slot_confirmed_between_the_two_commit_records(self):
		node = self.node
		before = node.execute("SELECT pg_current_wal_insert_lsn();")[0][0]
		node.safe_psql(
		    'postgres', """
			BEGIN;
			INSERT INTO o_jc VALUES (1, 'oriole');
			INSERT INTO h_jc VALUES (1, 'heap');
			UPDATE o_jc SET v = 'oriole2' WHERE id = 1;
			COMMIT;
		""")
		after = node.execute("SELECT pg_current_wal_insert_lsn();")[0][0]
		# the end of the last OrioleDB record before the heap COMMIT record
		cut = node.execute("""
			SELECT max(end_lsn) FROM pg_get_wal_records_info('%s', '%s')
			WHERE starts_with(resource_manager, 'OrioleDB')
			  AND start_lsn < (SELECT min(start_lsn)
			                   FROM pg_get_wal_records_info('%s', '%s')
			                   WHERE resource_manager = 'Transaction'
			                     AND starts_with(record_type, 'COMMIT'));
		""" % (before, after, before, after))[0][0]
		self.assertIsNotNone(cut)
		self.assertEqual(self.changes(cut), [])
		self.assertEqual(
		    node.execute(
		        "SELECT confirmed_flush_lsn FROM pg_replication_slots "
		        "WHERE slot_name = 'jc';")[0][0], cut)
		self.assertEqual(sorted(self.changes()), [('h_jc', 'INSERT'),
		                                          ('o_jc', 'INSERT'),
		                                          ('o_jc', 'UPDATE')])

	def test_transaction_already_sent_is_not_sent_again(self):
		node = self.node
		node.safe_psql(
		    'postgres', """
			BEGIN;
			INSERT INTO o_jc VALUES (2, 'oriole');
			INSERT INTO h_jc VALUES (2, 'heap');
			COMMIT;
		""")
		self.assertEqual(sorted(self.changes()), [('h_jc', 'INSERT'),
		                                          ('o_jc', 'INSERT')])
		node.safe_psql('postgres', "INSERT INTO o_jc VALUES (3, 'later');")
		self.assertEqual(self.changes(), [('o_jc', 'INSERT')])
