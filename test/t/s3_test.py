import collections
import logging
import os
import time
import re
import shutil
import runpy
import signal
import struct
import subprocess
import sys
from tempfile import mkdtemp, mkstemp

import threading
from threading import Thread
from unittest.mock import patch

import boto3
import botocore.client
import testgres
from testgres.defaults import default_dbname
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException, QueryException

from .base_test import wait_checkpointer_stopevent
from .s3_base_test import S3BaseTest, s3_test_attrs, mock_put_object, \
 orig_put_object, \
 mock_put_object_conflict, mock_moto_unkown_error, FlakyPutObject, \
 RevokedKeyPut, AmbiguousPut, HoldChunksOf, HoldFirstGet, ConflictingPut, \
 FailingGet, RevokedKey, orig_get_object, orig_list_objects_v2

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

dir_path = os.path.dirname(os.path.realpath(__file__))


class S3Test(S3BaseTest):

	def test_s3_put_get(self):
		fd, s3_test_file = mkstemp()
		with os.fdopen(fd, 'wt') as fp:
			fp.write("HELLO\nIT'S A ME\nMARIO\n")

		self.client.upload_file(Bucket=self.bucket_name,
		                        Filename=s3_test_file,
		                        Key="wal/314159")
		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(list(x["Key"] for x in objects))
		self.assertEqual(objects, ['wal/314159'])

		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(f"SELECT s3_put('wal/926535', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('5/LICENSE', '{s3_test_file}');")
		node.safe_psql(f"SELECT s3_put('LICENSE', '{s3_test_file}');")

		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		objects = sorted(
		    list(x["Key"] for x in objects if '/chunks/' not in x["Key"] and
		         '/terminators/' not in x["Key"]))
		self.assertEqual(objects, [
		    '5/LICENSE', 'LICENSE', 'root', 'wal/314159', 'wal/926535',
		    'writer'
		])
		object = self.client.get_object(Bucket=self.bucket_name,
		                                Key="5/LICENSE")
		boto_object_body = object["Body"].readlines()
		boto_object_body = [x.decode("utf-8") for x in boto_object_body]
		boto_object_body = ''.join(boto_object_body)
		orioledb_object_body = node.execute(f"SELECT s3_get('5/LICENSE');")
		orioledb_object_body = orioledb_object_body[0][0]
		self.assertEqual(boto_object_body, orioledb_object_body)
		with open(f"{s3_test_file}", "r") as f:
			file_content = ''.join(f.readlines())
			self.assertEqual(file_content, orioledb_object_body)
		node.stop(['-m', 'immediate'])
		os.unlink(s3_test_file)

	def test_s3_credential_check(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = 'BOB:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
		""")
		with self.assertRaises(StartNodeException) as e:
			node.start()
		self.assertEqual(e.exception.message, "Cannot start node")
		with open(node.pg_log_file) as f:
			log = f.readlines()
		message = log[0].split('] ')[-1].strip()
		# the first request reads the bucket's root
		self.assertEqual(message,
		                 'FATAL:  could not read "root" from the S3 bucket')

	def test_s3_checkpoint(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")
		node.start()
		datname = default_dbname()
		datoid = node.execute(f"""
			SELECT oid from pg_database WHERE datname = '{datname}'
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT")
		node.safe_psql("""
			CREATE TABLE o_test_2 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_2 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))

		node.stop(['-m', 'immediate'])

		# Every map the last checkpoint wrote is in the bucket under its
		# tree, and every tree with data has parts there.
		orioledb_dir = node.data_dir + "/orioledb_data"
		chkp_num = [
		    x.split('.')[0] for x in os.listdir(orioledb_dir)
		    if x.endswith('.xid')
		][0]
		objects = self.client.list_objects(Bucket=self.bucket_name,
		                                   Prefix=f'trees/{datoid}/')
		objects = sorted(x["Key"] for x in objects.get("Contents", []))
		maps = [x for x in objects if '/maps/' in x]
		parts = [x for x in objects if '/parts/' in x]
		self.assertTrue(maps)
		self.assertTrue(parts)
		for name in os.listdir(f"{orioledb_dir}/{datoid}"):
			base, _, ext = name.partition('.')
			if ext == 'map' and base.endswith(f"-{chkp_num}"):
				relnode = base.split('-')[0]
				self.assertIn(
				    f"trees/{datoid}/{relnode}/maps/"
				    f"{self.checkpoint_token(int(chkp_num))}-{chkp_num}", maps)
			elif ext == '' and base.isdigit() and \
			    os.path.getsize(f"{orioledb_dir}/{datoid}/{name}") > 8192:
				self.assertTrue(
				    any(x.startswith(f"trees/{datoid}/{base}/parts/") for x in parts),
				    f"no part of tree {base} in the bucket")
		node.start()
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop()

	def test_s3_checkpoint_unchanged(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1

			autovacuum = off
		""")
		node.start()

		# 1st CHECKPOINT
		node.safe_psql("""
			CREATE TABLE test_1 (
				val_1 int
			) USING heap;
			INSERT INTO test_1 SELECT * FROM generate_series(1, 2000);
			CREATE TABLE test_2 (
				val_2 int
			) USING heap;
			INSERT INTO test_2 SELECT * FROM generate_series(2000, 4000);
		""")

		test_1_filepath = node.execute(
		    "SELECT pg_catalog.pg_relation_filepath('test_1'::regclass)")[0][0]
		test_2_filepath = node.execute(
		    "SELECT pg_catalog.pg_relation_filepath('test_2'::regclass)")[0][0]

		node.safe_psql("CHECKPOINT")

		objects_1 = self.client.list_objects(Bucket=self.bucket_name)
		objects_1 = objects_1.get("Contents", [])
		objects_1 = sorted(list(x["Key"] for x in objects_1))

		dir_1 = self.checkpoint_dir(1)
		self.assertIn(dir_1 + "orioledb_data/file_checksums", objects_1)
		self.assertIn(dir_1 + test_1_filepath, objects_1)
		self.assertIn(dir_1 + test_2_filepath, objects_1)

		# 2nd CHECKPOINT
		node.safe_psql("""
			INSERT INTO test_2 SELECT * FROM generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT")

		objects_2 = self.client.list_objects(Bucket=self.bucket_name)
		objects_2 = objects_2.get("Contents", [])
		objects_2 = sorted(list(x["Key"] for x in objects_2))

		dir_2 = self.checkpoint_dir(2)
		self.assertIn(dir_2 + "orioledb_data/file_checksums", objects_2)
		self.assertNotIn(dir_2 + test_1_filepath, objects_2)
		self.assertIn(dir_2 + test_2_filepath, objects_2)

		node.stop()

	def test_s3_checkpoint_checksum_error(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1

			autovacuum = off
		""")
		node.start()

		small_file_checksums = os.path.join(node.data_dir, "orioledb_data",
		                                    "small_file_checksums")

		# Test that CHECKPOINT will fail in case of lack of permissions
		open(small_file_checksums, "a").close()
		os.chmod(small_file_checksums, 0)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  could not read file "orioledb_data/small_file_checksums": Permission denied',
			    f.read())

		os.unlink(small_file_checksums)

		# Test that CHECKPOINT will fail in case of incorrect checkpoint number
		node.safe_psql("CHECKPOINT")
		shutil.copy(small_file_checksums, f"{small_file_checksums}.copy")

		pattern_str = r"^FILE: (?P<filename>.+), CHECKSUM: (?P<checksum>.+), CHECKPOINT: (?P<checkpoint>\d+)$"
		pattern = re.compile(pattern_str)

		with open(small_file_checksums, "r+") as f:
			m = pattern.search(f.readline().rstrip())
			assert m is not None

			f.seek(0, os.SEEK_END)
			f.write(
			    f"FILE: {m.groups()[0]}, CHECKSUM: {m.groups()[1]}, CHECKPOINT: 100"
			)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  unexpected checkpoint number in the checksum file',
			    f.read())

		# Test that CHECKPOINT will fail in case of duplicated entries
		shutil.copy(f"{small_file_checksums}.copy", small_file_checksums)

		with open(small_file_checksums, "r+") as f:
			line = f.readline().rstrip()
			f.seek(0, os.SEEK_END)
			f.write(line)

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn(
			    'ERROR:  the file name is duplicated in the checksum file',
			    f.read())

		# Test that CHECKPOINT will fail in case of invalid file
		shutil.copy(f"{small_file_checksums}.copy", small_file_checksums)

		with open(small_file_checksums, "r+") as f:
			m = pattern.search(f.readline().rstrip())
			assert m is not None

			f.seek(0, os.SEEK_END)
			f.write(f"FILE: {m.groups()[0]}, CHECKSUM: {m.groups()[1]}")

		with self.assertRaises(QueryException) as e:
			node.safe_psql("CHECKPOINT")

		assert e.exception.message is not None
		self.assertTrue(
		    self.stripErrorMsg(e.exception.message).startswith(
		        "ERROR:  checkpoint request failed"))

		with open(node.pg_log_file) as f:
			self.assertIn('ERROR:  invalid line format of the checksum file',
			              f.read())

	def test_s3_ddl_recovery(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 1
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		node.safe_psql("""
			CREATE TABLE o_test_1(
				val_1 text NOT NULL COLLATE "C",
				PRIMARY KEY (val_1)
			) USING orioledb;
			ALTER TABLE o_test_1 ALTER val_1 TYPE text COLLATE "POSIX";
		""")

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("""
			ALTER TABLE o_test_1
				DROP CONSTRAINT o_test_1_pkey;
		""")

		node.stop(['-m', 'immediate'])
		node.start()

	def get_file_occupied_size(self, path):
		try:
			result = 0
			zero = b'\0' * 8192
			f = open(path, "rb")
			data = f.read(8192)
			while len(data) > 0:
				if data != zero:
					result = result + len(data)
				data = f.read(8192)
			f.close()
			return result
		except:  # We could be here due to concurrent operation, e.g. file removal
			return 0

	def get_data_size(self):
		node = self.node
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(
		    f"{node.data_dir}/orioledb_data"):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp):
					total_size += self.get_file_occupied_size(fp)
		return total_size

	def test_s3_data_eviction(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_desired_size = 20MB

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		node.safe_psql("""
			BEGIN;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO o_test (id, value) (SELECT id, repeat('x', 2500) FROM generate_series(1, 20000) id);
			COMMIT;
		""")
		node.safe_psql("CHECKPOINT")
		deadline = time.time() + 120
		while self.get_data_size() > 20 * 1024 * 1024:
			self.assertLess(time.time(), deadline, "the data was never evicted")
			time.sleep(1)
		self.assertEqual(20000,
		                 node.execute("SELECT COUNT(*) FROM o_test")[0][0])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(20000,
		                 node.execute("SELECT COUNT(*) FROM o_test")[0][0])
		node.stop()

	def s3_small_buffers_conf(self, desired_size):
		self.node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_desired_size = {desired_size}
			orioledb.s3_headers_buffers = 128kB
			orioledb.main_buffers = 8MB

			orioledb.s3_num_workers = 3
			checkpoint_timeout = 1h
			max_wal_size = 4GB
		""")

	def test_s3_blocks_written_past_checkpoint_256(self):
		"""
		Blocks written by checkpoint 256 and later are read back from their
		own checkpoint's objects once evicted.  The checkpoint number was read
		back from the extent offset with an 8-bit mask, so such a block was
		looked for under the number modulo 256: the S3 worker failed on the
		missing object over and over, and whoever needed the block waited for
		it forever.
		"""
		node = self.node
		self.s3_small_buffers_conf('1MB')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO o_test SELECT id, repeat('a', 300)
				FROM generate_series(1, 20000) id;
		""")
		con = node.connect(autocommit=True)
		# A broken read hangs: fail instead
		con.execute("SET statement_timeout = '120s';")
		for _ in range(260):
			con.execute("CHECKPOINT;")
		con.execute("UPDATE o_test SET value = repeat('b', 300);")
		con.execute("CHECKPOINT;")
		for _ in range(60):
			if self.get_data_size() <= 2 * 1024 * 1024:
				break
			time.sleep(1)
		# evicted: the reads below go to the bucket
		self.assertLessEqual(self.get_data_size(), 2 * 1024 * 1024)
		self.assertEqual([(20000, 20000)],
		                 con.execute("""
			SELECT count(*), count(*) FILTER (WHERE value = repeat('b', 300))
			FROM o_test;
		"""))
		con.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual([(20000, )],
		                 node.execute("""
			SET statement_timeout = '120s';
			SELECT count(*) FROM o_test WHERE value = repeat('b', 300);
		"""))
		node.stop()

	def test_s3_transient_errors_are_retried(self):
		"""
		A store that answers 5xx for a while is waited out: the uploads are
		retried and the checkpoint completes, instead of the S3 worker dying
		on the first error and taking the checkpoint (or the postmaster)
		with it.
		"""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 1000);
		""")
		flaky = FlakyPutObject(5)
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			node.safe_psql("CHECKPOINT;")
		self.assertEqual(5, flaky.failed)
		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertIn("failed, retrying", log)
		self.assertNotIn("could not put object to S3", log)
		self.assertTrue(any('/parts/' in k for k in self.part_etags()))
		self.assertEqual([(1000, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop()

	def local_checkpoint_number(self, node):
		"""lastCheckpointNumber of the local OrioleDB control file."""
		with open(os.path.join(node.data_dir, 'orioledb_data', 'control'),
		          'rb') as f:
			return struct.unpack('<QI', f.read(12))[1]

	def crash_while_uploading(self, flaky, before_control=False):
		"""Crash while a checkpoint waits for uploads that `flaky` refuses,
		restart, checkpoint, and return the rows of a node loaded from the
		bucket."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_lease_duration = 15")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			CREATE TABLE h_test (id int PRIMARY KEY);
			INSERT INTO o_test SELECT generate_series(1, 1000);
			INSERT INTO h_test SELECT generate_series(1, 1000);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("""
			INSERT INTO o_test SELECT generate_series(1001, 2000);
			INSERT INTO h_test SELECT generate_series(1001, 2000);
		""")
		before = self.local_checkpoint_number(node)
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			checkpoint = Thread(target=lambda: node.execute("CHECKPOINT;"))
			checkpoint.daemon = True
			checkpoint.start()
			deadline = time.time() + 60
			while flaky.failed < 3 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreaterEqual(flaky.failed, 3)
			# parts: the checkpoint waits for them before its local control
			for _ in range(15 if before_control else 0):
				self.assertEqual(before, self.local_checkpoint_number(node))
				time.sleep(0.2)
			node.stop(['-m', 'immediate'])
			checkpoint.join(10)
		node.start()
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(2000, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			rows = (new_node.execute("SELECT count(*) FROM o_test")[0][0],
			        new_node.execute("SELECT count(*) FROM h_test")[0][0])
			new_node.stop()
			new_node.cleanup()
		return rows

	def test_s3_crash_while_parts_upload(self):
		"""The local checkpoint must not complete before its parts are up:
		after a restart the flags that would upload them are gone, and the
		next checkpoint commits a bucket naming parts that were never up."""
		self.assertEqual((2000, 2000),
		                 self.crash_while_uploading(FlakyPutObject(10**9),
		                                            before_control=True))

	def test_s3_crash_while_pg_files_upload(self):
		"""The same for the PostgreSQL files of a checkpoint."""
		self.assertEqual((2000, 2000),
		                 self.crash_while_uploading(
		                     FlakyPutObject(10**9, prefix='data/')))

	def test_s3_commits_after_the_checkpoint_survive(self):
		"""Commits after the last checkpoint, in a WAL segment that was never
		archived, are restored from the bucket: the WAL shipper put them as
		chunks before the commits returned."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			CREATE TABLE h_test (id int PRIMARY KEY);
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		for i in range(101, 111):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});"
			               f"INSERT INTO h_test VALUES ({i});")
		segment = node.execute(
		    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
		node.stop(['-m', 'immediate'])
		self.assertFalse(
		    self.client.list_objects(Bucket=self.bucket_name,
		                             Prefix=f'wal/{segment}').get("Contents"))

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(110, 1, 110)],
			                 new_node.execute("SELECT count(*), min(id), max(id) "
			                                  "FROM o_test"))
			self.assertEqual([(10, )],
			                 new_node.execute("SELECT count(*) FROM h_test"))
			# the restored writer goes on shipping where the bucket ends
			new_node.safe_psql("INSERT INTO o_test VALUES (1000);")
			new_node.stop()
			new_node.cleanup()

	def test_s3_commit_waits_for_the_bucket(self):
		"""With synchronous_standby_names = 's3' a commit returns only once
		its WAL is in the bucket."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		flaky = FlakyPutObject(10**9, contains='/chunks/')
		done = []
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			commit = Thread(target=lambda: done.append(
			    node.execute("INSERT INTO o_test VALUES (1) RETURNING id;")))
			commit.daemon = True
			commit.start()
			# this commit waits for the bucket, not for anything else
			watch = node.connect(autocommit=True)
			deadline = time.time() + 30
			while time.time() < deadline and (flaky.failed == 0 or not watch.execute("""
				SELECT 1 FROM pg_stat_activity
				WHERE wait_event = 'SyncRep' AND query LIKE 'INSERT INTO o_test%%'
			""")):
				time.sleep(0.2)
			flushed = int(watch.execute(
			    "SELECT pg_wal_lsn_diff(pg_current_wal_flush_lsn(), '0/0')")[0][0])
			watch.close()
			self.assertGreater(flaky.failed, 0)
			self.assertEqual(done, [])
			# no chunk has its WAL yet
			self.assertLess(max(e for _, e in self.chunk_ranges().values()), flushed)
			flaky.failures = 0
			commit.join(60)
		self.assertEqual(len(done), 1)
		self.assertGreaterEqual(max(e for _, e in self.chunk_ranges().values()),
		                        flushed)
		self.assertEqual([(1, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop()

	def test_s3_restart_waits_for_unshipped_wal(self):
		"""A commit that was waiting for the bucket when the server died is
		visible after local recovery: clients are held until the shipper has
		put it, so none sees a commit the bucket lacks."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		flaky = FlakyPutObject(10**9, contains='/chunks/')
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			commit = Thread(target=lambda: node.execute(
			    "INSERT INTO o_test VALUES (1) RETURNING id;"))
			commit.daemon = True
			commit.start()
			deadline = time.time() + 30
			while flaky.failed == 0 and time.time() < deadline:
				time.sleep(0.2)
			node.stop(['-m', 'immediate'])
			commit.join(10)

			node.start()
			seen = []
			reader = Thread(target=lambda: seen.append(
			    node.execute("SELECT count(*) FROM o_test")))
			reader.daemon = True
			reader.start()
			time.sleep(5)
			# the shipper cannot put the commit's WAL: the client waits
			self.assertEqual(seen, [])
			flaky.failures = 0
			reader.join(60)
		self.assertEqual(seen, [[(1, )]])
		node.stop()

	def bucket_objects(self):
		result = {}
		paginator = self.client.get_paginator('list_objects_v2')
		for page in paginator.paginate(Bucket=self.bucket_name):
			for obj in page.get("Contents", []):
				result[obj["Key"]] = obj["ETag"]
		return result

	def test_s3_reader_writes_nothing_from_start_to_stop(self):
		"""Across its start, the replay of the bucket's WAL, a restartpoint,
		a restart and its stop, a reader changes no object but its pin."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.main_buffers = 8MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('x', 500) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("UPDATE o_test SET pad = 'y' WHERE id % 7 = 0;")
		node.stop(['-m', 'immediate'])

		def bucket():
			result = {}
			paginator = self.client.get_paginator('list_objects_v2')
			for page in paginator.paginate(Bucket=self.bucket_name):
				for obj in page.get("Contents", []):
					result[obj["Key"]] = obj["ETag"]
			return result

		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			before = bucket()
			for _ in range(2):
				reader.start()
				deadline = time.time() + 60
				count = None
				while time.time() < deadline and count != [(20000, 2857)]:
					try:
						count = reader.execute(
						    "SELECT count(*), count(*) FILTER (WHERE pad = 'y') FROM o_test")
					except Exception:
						pass
					time.sleep(0.5)
				self.assertEqual([(20000, 2857)], count)
				reader.safe_psql("CHECKPOINT;")
				reader.stop()
			after = bucket()
			changed = {k for k in set(before) | set(after)
			           if before.get(k) != after.get(k)}
			self.assertEqual({'readers'}, changed)
			reader.cleanup()

	def test_s3_reader_reads_without_writing(self):
		"""A reader opens what the bucket holds, as a hot standby on a data
		directory the loader made, and writes nothing to the bucket while the
		writer goes on."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.main_buffers = 8MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('x', 500) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		# only in chunks: after the checkpoint, in a segment not archived
		node.safe_psql("INSERT INTO o_test VALUES (100000, 'after');")

		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			if os.environ.get('DEBUG_CHUNKS'):
				lsn = node.execute("SELECT pg_current_wal_lsn()")[0][0]
				print("writer lsn", lsn, flush=True)
				for key in self.chunk_keys():
					body = self.s3_object(key)
					print(key, struct.unpack('<IIQQQ', body[:32])[3:5], flush=True)
				for x in self.client.list_objects(Bucket=self.bucket_name, Prefix='wal/').get("Contents", []):
					if '/chunks/' not in x["Key"]:
						print("segment", x["Key"], flush=True)
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			writer_before = self.s3_body('writer')
			root_before = self.s3_body('root')
			reader.start()
			deadline = time.time() + 60
			while time.time() < deadline:
				try:
					reader.execute("SELECT 1")
					break
				except Exception:
					time.sleep(0.5)
			# every part is read from the bucket, not the writer's disk; the
			# standby applies what it replayed in the background
			deadline = time.time() + 60
			while time.time() < deadline and reader.execute(
			    "SELECT count(*) FROM o_test")[0][0] != 20001:
				time.sleep(0.5)
			self.assertEqual([(20001, 100000)],
			                 reader.execute("SELECT count(*), max(id) FROM o_test"))
			self.assertEqual([("after", )],
			                 reader.execute(
			                     "SELECT pad FROM o_test WHERE id = 100000"))

			# the writer goes on; the reader follows from the bucket
			node.safe_psql("INSERT INTO o_test VALUES (100001, 'later');")
			node.safe_psql("CHECKPOINT;")
			node.safe_psql("INSERT INTO o_test VALUES (100002, 'latest');")
			deadline = time.time() + 60
			while time.time() < deadline and reader.execute(
			    "SELECT count(*) FROM o_test")[0][0] != 20003:
				time.sleep(0.5)
			self.assertEqual([(20003, 100002)],
			                 reader.execute("SELECT count(*), max(id) FROM o_test"))
			# in recovery the WAL position is the replayed one
			self.assertEqual([('reader', True)],
			                 reader.execute("SELECT role, wal_flush_lsn > '0/0' "
			                                "FROM orioledb_s3_status()"))

			self.assertEqual(self.s3_body('writer')['token'],
			                 writer_before['token'])
			# with the writer stopped, whatever changes in the bucket is the
			# reader's doing: its pin, nothing else
			node.stop()
			before = self.bucket_objects()
			reader.execute("SELECT count(*) FROM o_test WHERE length(pad) = 500")
			reader.stop()
			after = self.bucket_objects()
			changed = {k for k in set(before) | set(after)
			           if before.get(k) != after.get(k)}
			self.assertTrue(changed <= {'readers'}, changed)
			self.assertEqual(self.s3_body('root')['token'],
			                 root_before['token'])
			with open(reader.pg_log_file) as f:
				log = f.read()
			self.assertIn("reading the S3 bucket without writing to it", log)
			self.assertNotIn("took the S3 writer lease", log)
			reader.cleanup()

	def test_s3_takeover_reads_what_came_after_the_load(self):
		"""A data directory loaded while the writer still committed is behind
		the bucket: the writer it starts as puts a terminator after the last
		chunk, recovers from the bucket up to it, and goes on on a new
		timeline."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test VALUES (101);")

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			# the writer goes on after the load, and dies (no checkpoint: a
			# clean stop's checkpoint is refused before any WAL is looked at)
			node.safe_psql("INSERT INTO o_test VALUES (102);")
			node.stop(['-m', 'immediate'])

			new_node.start()
			self.assertEqual([(102, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			self.assertGreater(
			    new_node.execute("SELECT timeline_id FROM pg_control_checkpoint()")[0][0],
			    1)
			with open(new_node.pg_log_file) as f:
				self.assertIn("put a WAL terminator", f.read())
			new_node.safe_psql("INSERT INTO o_test VALUES (103);")
			new_node.stop(['-m', 'immediate'])
			new_node.cleanup()
		with self.initNode(self.getBasePort() + 2, 'tgsc') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(103, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_keeps_what_a_retired_timeline_lends_the_next(self):
		"""After a takeover the new timeline's first segment starts with the
		old timeline's bytes up to the switch, and a restore assembles them
		from the old timeline's chunks.  The GC removes those chunks once the
		checkpoint that retired the timeline is collected; a later load must
		still have every commit."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			archive_mode = off
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test VALUES (101);")
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as b:
			self.loader.download(b.data_dir)
			b.append_conf(port=b.port)
			b.start()
			self.assertEqual([(101, )], b.execute("SELECT count(*) FROM o_test"))
			for i in range(102, 106):
				b.safe_psql(f"INSERT INTO o_test VALUES ({i});")
				b.safe_psql("CHECKPOINT;")
			# the old timeline's chunks before the terminator go
			deadline = time.time() + 90
			while time.time() < deadline and [
			    k for k in self.chunk_keys()
			    if k.startswith('wal/00000001/chunks/')]:
				b.safe_psql("CHECKPOINT;")
				time.sleep(1)
			b.safe_psql("INSERT INTO o_test VALUES (106);")
			b.stop(['-m', 'immediate'])
			b.cleanup()
		with self.initNode(self.getBasePort() + 2, 'tgsc') as c:
			self.loader.download(c.data_dir)
			c.append_conf(port=c.port)
			c.start()
			self.assertEqual([(106, 106)],
			                 c.execute("SELECT count(*), max(id) FROM o_test"))
			c.stop()
			c.cleanup()

	def test_s3_local_wal_load(self):
		"""--local-wal puts the bucket's WAL in pg_wal, for a server that
		cannot run the loader: a load has every commit; one the writer went
		on after is refused at start rather than missing commits."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test VALUES (101);")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as behind:
			self.loader.download(behind.data_dir, local_wal=True)
			behind.append_conf(port=behind.port)
			node.safe_psql("INSERT INTO o_test VALUES (102);")
			node.stop(['-m', 'immediate'])
			with self.assertRaises(StartNodeException):
				behind.start()
			with open(behind.pg_log_file) as f:
				self.assertIn("the S3 bucket has WAL past this data directory",
				              f.read())
			behind.cleanup()
		with self.initNode(self.getBasePort() + 2, 'tgsc') as new_node:
			self.loader.download(new_node.data_dir, local_wal=True)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(102, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.safe_psql("INSERT INTO o_test VALUES (103);")
			new_node.stop()
			new_node.cleanup()

	def test_s3_terminator_fences_the_old_writer(self):
		"""Once a terminator follows its last chunk, a writer that was fenced
		off can acknowledge no commit: its next chunk finds the key taken."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test SELECT generate_series(1, 10);")
		keys = self.chunk_keys(terminators=True)
		last = int(keys[-1].rsplit('/', 1)[1])
		tli = int(keys[-1].split('/')[1], 16)
		token = int(self.s3_body('writer')['token']) + 1
		# after the last chunk, never over one the shipper put meanwhile
		seq = last + 1
		while True:
			try:
				self.client.put_object(
				    Bucket=self.bucket_name,
				    Key=f"wal/{tli:08X}/chunks/{seq:020d}",
				    Body=struct.pack('<IIQQQ', 0x3154574f, tli, token, 0, 0),
				    IfNoneMatch='*')
				break
			except self.client.exceptions.ClientError:
				seq += 1
		done = []
		commit = Thread(target=lambda: done.append(
		    node.execute("INSERT INTO o_test VALUES (11) RETURNING id;")))
		commit.daemon = True
		commit.start()
		time.sleep(5)
		self.assertEqual(done, [])
		# the fenced writer stops on its own
		deadline = time.time() + 30
		while node.status() == NodeStatus.Running and time.time() < deadline:
			time.sleep(0.5)
		self.assertNotEqual(node.status(), NodeStatus.Running)
		with open(node.pg_log_file) as f:
			self.assertIn("belongs to another writer", f.read())

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(10, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_archive_is_fenced_by_the_lease(self):
		"""A writer whose lease another writer took puts no archived segment:
		before each it makes sure the lease is still its own for a while, and
		stops when it is not.  Without the WAL shipper the archive is the only
		way WAL reaches the bucket."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_wal_shipping = off
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		writer = self.s3_body('writer')
		writer['token'] = str(int(writer['token']) + 1)
		writer['owner'] = 'another-writer'
		writer['expires_at'] = str(int(time.time()) + 3600)
		self.client.put_object(
		    Bucket=self.bucket_name, Key='writer',
		    Body=''.join(f"{k} {v}\n" for k, v in writer.items()).encode())
		# the renewals stop: only the archive's own check can notice
		lease_pids = [
		    int(line.split()[0]) for line in os.popen(
		        f"ps -o pid=,args= --ppid {node.pid}").read().splitlines()
		    if 'orioledb s3 lease' in line]
		self.assertEqual(len(lease_pids), 1)
		os.kill(lease_pids[0], signal.SIGSTOP)
		try:
			time.sleep(16)
			node.safe_psql("INSERT INTO o_test VALUES (1);")
			segment = node.execute(
			    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
			node.safe_psql("SELECT pg_switch_wal();")
			deadline = time.time() + 30
			while node.status() == NodeStatus.Running and time.time() < deadline:
				time.sleep(0.5)
		finally:
			try:
				os.kill(lease_pids[0], signal.SIGCONT)
			except ProcessLookupError:
				pass
		self.assertNotEqual(node.status(), NodeStatus.Running)
		with open(node.pg_log_file) as f:
			self.assertIn("lost the S3 writer lease", f.read())
		self.assertNotIn("Contents", self.client.list_objects(
		    Bucket=self.bucket_name, Prefix=f'wal/{segment}'))

	def test_s3_shipping_turned_on_keeps_the_segment_a_stop_left(self):
		"""A stop without the shipper puts the segment the WAL ends in into
		the bucket.  Started again with the shipper, the instance ships the
		rest of that segment as chunks, and a restore reads them: the segment
		the stop left is no complete archived one."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_wal_shipping = off\n")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.stop()
		node.append_conf("""
			orioledb.s3_wal_shipping = on
			synchronous_standby_names = 's3'
		""")
		node.start()
		segment = node.execute(
		    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
		# the stop put it up
		self.wait_for_object(f'wal/{segment}')
		for i in range(11, 21):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
		self.assertEqual(segment, node.execute(
		    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0])
		node.stop(['-m', 'immediate'])
		self.assertNotIn("Contents", self.client.list_objects(
		    Bucket=self.bucket_name, Prefix=f'wal/{segment}', Delimiter='.'))

		with self.initNode(self.getBasePort() + 1, 'tgsp') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(20, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_restore_reads_the_segment_a_stop_left(self):
		"""Without the shipper, a restore of a bucket whose last stop left a
		partial segment reads that segment: it holds the stop's
		checkpoint."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_wal_shipping = off\n")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.stop()
		self.assertEqual(1, len([
		    o['Key'] for o in self.client.list_objects(
		        Bucket=self.bucket_name, Prefix='wal/')['Contents']
		    if o['Key'].endswith('.partial')]))

		with self.initNode(self.getBasePort() + 1, 'tgsq') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(10, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_reader_becomes_the_writer(self):
		"""A reader keeps its parts when it is loaded again to write: once the
		writer is gone, it takes the bucket over with every commit."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
			orioledb.main_buffers = 8MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('x', 500) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			deadline = time.time() + 60
			while True:
				try:
					if reader.execute("SELECT count(*) FROM o_test")[0][0] == 20000:
						break
				except Exception:
					pass
				self.assertLess(time.time(), deadline)
				time.sleep(0.5)
			node.safe_psql("INSERT INTO o_test VALUES (100000, 'last');")
			node.stop(['-m', 'immediate'])
			reader.stop()

			parts = [f for top, _, files in os.walk(
			    os.path.join(reader.data_dir, 'orioledb_data')) for f in files
			         if f.isdigit()]
			self.assertTrue(parts)
			self.loader.download(reader.data_dir, keep_parts=True)
			reader.append_conf(port=reader.port)
			reader.append_conf("orioledb.main_buffers = 8MB")
			reader.start()
			self.assertEqual([(20001, 100000)],
			                 reader.execute("SELECT count(*), max(id) FROM o_test"))
			reader.safe_psql("INSERT INTO o_test VALUES (100001, 'next');")
			reader.stop(['-m', 'immediate'])
			reader.cleanup()

		with self.initNode(self.getBasePort() + 2, 'tgsc') as last:
			self.loader.download(last.data_dir)
			last.append_conf(port=last.port)
			last.start()
			self.assertEqual([(20002, )],
			                 last.execute("SELECT count(*) FROM o_test"))
			last.stop()
			last.cleanup()

	def chunk_keys(self, terminators=False):
		# every start puts a terminator, and it stays: WAL chunks alone by
		# default
		keys = self.all_keys('wal/')
		marks = {k.replace('/terminators/', '/chunks/') for k in keys
		         if '/terminators/' in k}
		return sorted(k for k in keys if '/chunks/' in k and
		              (terminators or k not in marks))

	def chunk_ranges(self, keys=None):
		"""[start, end) of WAL chunks, by key."""
		result = {}
		for k in self.chunk_keys() if keys is None else keys:
			try:
				head = self.client.get_object(Bucket=self.bucket_name, Key=k,
				                              Range='bytes=0-31')['Body'].read()
			except self.client.exceptions.NoSuchKey:
				continue
			_, _, _, start, end = struct.unpack('<IIQQQ', head)
			result[k] = (start, end)
		return result

	def assert_removed_chunks_archived(self, ranges, seg_size=16 * 1024 * 1024):
		"""Every chunk of ranges the bucket no longer has held WAL that
		archived segments hold, its start and its end alike."""
		present = set(self.chunk_keys())
		segments = {k.split('/')[-1] for k in self.all_keys('wal/')
		            if re.fullmatch(r'wal/[0-9A-F]{24}', k)}
		for key, (start, end) in ranges.items():
			if key in present:
				continue
			tli = int(key.split('/')[1], 16)
			for lsn in (start, end - 1):
				seg = lsn // seg_size
				name = f"{tli:08X}{seg // 256:08X}{seg % 256:08X}"
				self.assertIn(name, segments, f"{key} {start:X}-{end:X} removed")

	def test_s3_gc_removes_chunks_when_segments_are_not_archived(self):
		"""With archiving off no segment is archived, so the shipper keeps
		every chunk; the GC removes the ones before the segment of R's REDO,
		and a restore still has what it needs."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			archive_mode = off
			synchronous_standby_names = 's3'
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		early = None
		for r in range(4):
			for i in range(10):
				node.safe_psql(f"INSERT INTO o_test VALUES ({r * 10 + i});")
			node.safe_psql("SELECT pg_switch_wal();")
			node.safe_psql("CHECKPOINT;")
			if early is None:
				early = set(self.chunk_keys())
		self.assertTrue(early)
		deadline = time.time() + 90
		while time.time() < deadline and early & set(self.chunk_keys()):
			time.sleep(1)
		self.assertEqual(set(), early & set(self.chunk_keys()))
		node.safe_psql("INSERT INTO o_test VALUES (1000);")
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(41, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_keys_rotate_on_a_reload(self):
		"""Once the store refuses the old key, requests are retried for
		orioledb.s3_auth_retry_time; new keys set meanwhile and reloaded are
		taken by every process, and what waited goes on."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		revoked = RevokedKeyPut(self.access_key_id)
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: revoked(response)):
			done = []
			commit = Thread(target=lambda: done.append(
			    node.safe_psql("INSERT INTO o_test VALUES (11);")))
			commit.start()
			deadline = time.time() + 60
			while revoked.refused < 2 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreaterEqual(revoked.refused, 2)
			self.assertFalse(done)
			node.append_conf(f"orioledb.s3_accesskey = '{self.access_key_id}NEW'")
			node.reload()
			commit.join(60)
			self.assertTrue(done)
			# no checkpoint renews the lease meanwhile: its own worker does,
			# with the new key
			time.sleep(20)
			with open(node.pg_log_file) as f:
				self.assertNotIn("lost the S3 writer lease", f.read())
			node.safe_psql("CHECKPOINT;")
			self.assertGreater(revoked.accepted, 0)
		self.assertEqual([(11, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop()

	def test_s3_keys_rotate_on_a_reader(self):
		"""A reader takes rotated keys on a reload too: its pin goes on, and
		restore_command, run per segment, reads the keys the server has
		now, not the ones in the environment it started with."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_gc_interval = 0
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				orioledb.s3_reader_pin_ttl = 30
				hot_standby = on
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			new_key = f"orioledb.s3_accesskey = '{self.access_key_id}NEW'"
			for n in (node, reader):
				n.append_conf(new_key)
				n.reload()
			time.sleep(2)
			revoked = RevokedKey(self.access_key_id)
			pin = self.s3_object('readers')
			# the test reads with a key of its own
			own = boto3.client(
			    "s3", endpoint_url=self.client.meta.endpoint_url,
			    verify=self.s3_cainfo, region_name=self.region,
			    aws_access_key_id='TESTREADER',
			    aws_secret_access_key=self.secret_access_key)

			def readers():
				return own.get_object(Bucket=self.bucket_name,
				                      Key='readers')['Body'].read()

			with patch('moto.s3.responses.S3Response.put_object',
			           new=lambda r: revoked(r, orig_put_object)), \
			    patch('moto.s3.responses.S3Response.get_object',
			          new=lambda r: revoked(r, orig_get_object)), \
			    patch('moto.s3.responses.S3Response.list_objects_v2',
			          new=lambda r: revoked(r, orig_list_objects_v2)):
				for i in range(11, 21):
					node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
				node.safe_psql("SELECT pg_switch_wal();")
				node.safe_psql("INSERT INTO o_test VALUES (21);")
				deadline = time.time() + 60
				count = None
				while time.time() < deadline and count != [(21, )]:
					try:
						count = reader.execute("SELECT count(*) FROM o_test")
					except Exception:
						pass
					time.sleep(0.5)
				self.assertEqual([(21, )], count)
				# the pin is renewed with the new key
				deadline = time.time() + 30
				while time.time() < deadline and readers() == pin:
					time.sleep(1)
				self.assertNotEqual(pin, readers())
			self.assertEqual(NodeStatus.Running, reader.status())
			reader.stop()
			reader.cleanup()
		node.stop()

	def test_s3_a_refused_key_fails_with_no_retry_time(self):
		"""With orioledb.s3_auth_retry_time = 0 a refusal is the answer: the
		request fails at once instead of being retried for the key to catch
		up, and the worker that died on it tries again once a second, not
		at once."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_auth_retry_time = 0
			orioledb.s3_lease_duration = 30
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		# the lease goes on: only the refusal can show
		revoked = RevokedKeyPut(self.access_key_id, spare=('writer', ))
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: revoked(response)):
			checkpoint = Thread(target=lambda: node.safe_psql("CHECKPOINT;"))
			checkpoint.daemon = True
			checkpoint.start()
			deadline = time.time() + 10
			refused = False
			while time.time() < deadline and not refused:
				with open(node.pg_log_file) as f:
					refused = "http code = 403" in f.read()
				time.sleep(0.2)
			self.assertTrue(refused)
			before = revoked.refused
			time.sleep(3)
			self.assertLess(revoked.refused - before, 3 * 3 * 2)
		# the key works again: the checkpoint goes through
		checkpoint.join(60)
		self.assertFalse(checkpoint.is_alive())
		with open(node.pg_log_file) as f:
			self.assertNotIn("lost the S3 writer lease", f.read())
		node.stop()

	def test_s3_status(self):
		"""orioledb_s3_status() says where the writer and the bucket stand,
		from shared memory."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_gc_interval = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		row = node.execute("""
			SELECT role, token, lease_remaining > 0, root_checkpoint,
			       wal_ship_lag, wal_shipper_running, unsealed_parts
			FROM orioledb_s3_status();
		""")[0]
		self.assertEqual(('writer', int(self.kv('root')['token']), True,
		                  int(self.kv('root')['checkpoint']), 0, True, 0), row)
		deadline = time.time() + 30
		while node.execute("SELECT gc_retain FROM orioledb_s3_status()")[0][0] == 0:
			self.assertLess(time.time(), deadline)
			time.sleep(0.5)
		node.stop()

	def test_s3_a_damaged_root_is_put_back(self):
		"""Each commit leaves a copy of root under roots/; a damaged root is
		put back from the newest copy whose manifest is there, and the
		writer goes on from it.  A root that reads is left alone."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test SELECT generate_series(11, 20);")
		node.safe_psql("CHECKPOINT;")
		node.stop()
		good = self.client.get_object(Bucket=self.bucket_name,
		                              Key='root')['Body'].read()
		chkp = int(self.kv('root')['checkpoint'])
		self.wait_for_object(f'roots/{chkp}')
		self.loader.download(None, repair_root=True)
		self.assertEqual(good, self.client.get_object(
		    Bucket=self.bucket_name, Key='root')['Body'].read())

		self.client.put_object(Bucket=self.bucket_name, Key='root',
		                       Body=b'\x00garbage')
		with self.assertRaises(StartNodeException):
			node.start()
		self.loader.download(None, repair_root=True)
		self.assertEqual(chkp, int(self.kv('root')['checkpoint']))
		node.start()
		self.assertEqual([(20, )], node.execute("SELECT count(*) FROM o_test"))
		node.safe_psql("CHECKPOINT;")
		self.assertGreater(int(self.kv('root')['checkpoint']), chkp)
		node.stop()

	def test_s3_a_root_put_back_from_an_older_copy_loads_on(self):
		"""The newest copy of root is missing and root is damaged:
		--repair-root puts back an older copy.  A data directory loaded from
		it replays the bucket's WAL past that checkpoint, every commit the
		writer acknowledged included, and writes on."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		older = int(self.kv('root')['checkpoint'])
		node.safe_psql("INSERT INTO o_test SELECT generate_series(11, 20);")
		node.safe_psql("CHECKPOINT;")
		newest = int(self.kv('root')['checkpoint'])
		self.wait_for_object(f'roots/{newest}')
		for i in range(21, 31):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
		node.stop(['-m', 'immediate'])
		self.client.delete_object(Bucket=self.bucket_name, Key=f'roots/{newest}')
		self.client.put_object(Bucket=self.bucket_name, Key='root',
		                       Body=b'\x00garbage')
		self.loader.download(None, repair_root=True)
		self.assertEqual(older, int(self.kv('root')['checkpoint']))

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(30, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.safe_psql("INSERT INTO o_test VALUES (31);")
			new_node.safe_psql("CHECKPOINT;")
			self.assertGreater(int(self.kv('root')['checkpoint']), newest)
			new_node.stop()
			new_node.cleanup()

	def test_s3_puts_whose_answer_is_lost(self):
		"""A PUT that went through but whose answer was lost is retried and
		judged by what the bucket holds: a chunk, a part, a manifest, root and
		the lease each come out as if answered, the writer keeps its lease,
		and a load from the bucket has every acknowledged commit."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_gc_interval = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
		""")
		ambiguous = AmbiguousPut()
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: ambiguous(response)):
			for r in range(3):
				for i in range(20):
					node.safe_psql(f"INSERT INTO o_test VALUES ({r * 1000 + i}, "
					               f"repeat('x', 300));")
				node.safe_psql(f"INSERT INTO o_test SELECT g, repeat('y', 300) "
				               f"FROM generate_series({r * 1000 + 100}, "
				               f"{r * 1000 + 900}) g;")
				node.safe_psql("CHECKPOINT;")
			node.safe_psql("INSERT INTO o_test VALUES (99999, 'last');")
		self.assertTrue(all(ambiguous.landed.values()), ambiguous.landed)
		self.assertEqual([(2464, )], node.execute("SELECT count(*) FROM o_test"))
		with open(node.pg_log_file) as f:
			self.assertNotIn("lost the S3 writer lease", f.read())
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(2464, 'last')],
			                 new_node.execute("SELECT count(*), max(pad) FILTER "
			                                  "(WHERE id = 99999) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_chunks_are_removed_once_archived(self):
		"""A chunk is removed once an archived segment holds its WAL; a
		restore takes the segments and the chunks after them."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		for i in range(1, 6):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
			wal_file = node.execute(
			    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
			node.safe_psql("SELECT pg_switch_wal();")
			self.wait_for_object(f'wal/{wal_file}')
		ranges = self.chunk_ranges()
		shipped = len(ranges)
		node.safe_psql("INSERT INTO o_test VALUES (6);")
		deadline = time.time() + 60
		while time.time() < deadline and len(self.chunk_keys()) >= shipped:
			time.sleep(1)
		self.assertLess(len(self.chunk_keys()), shipped)
		self.assert_removed_chunks_archived(ranges)
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(6, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_chunks_are_removed_under_load(self):
		"""Chunks go once archived while commits keep coming and checkpoints
		recycle the archived segments (an archived segment the checkpoint
		removed has no archive status file left to say so), and the instance
		restarts over the chunks its previous run left."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			wal_keep_size = 0
			min_wal_size = 32MB
			max_wal_size = 64MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id serial PRIMARY KEY, pad text) USING orioledb;
		""")
		script = os.path.join(node.base_dir, 'insert.sql')
		with open(script, 'w') as f:
			f.write("INSERT INTO o_test (pad) VALUES (repeat('x', 500));\n")
		bench = subprocess.Popen(
		    [testgres.get_bin_path('pgbench'),
		     '-n', '-f', script, '-c', '16', '-j', '4', '-T', '90',
		     '-h', node.host, '-p', str(node.port), default_dbname()],
		    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		try:
			early = None
			deadline = time.time() + 80
			removed = False
			while time.time() < deadline and not removed:
				# a checkpoint recycles archived segments, and their
				# archive status files go with them
				node.safe_psql("SELECT pg_switch_wal();")
				node.safe_psql("CHECKPOINT;")
				time.sleep(2)
				keys = set(self.chunk_keys())
				if early is None:
					early = keys or None
					early_ranges = self.chunk_ranges(keys)
					continue
				# the chunks seen first are gone while commits still come
				removed = not early & keys
				self.assertIsNone(bench.poll())
			self.assertTrue(removed)
			self.assert_removed_chunks_archived(early_ranges)
		finally:
			bench.kill()
			bench.wait()
		node.stop(['-m', 'immediate'])
		node.start()
		count = node.execute("SELECT count(*) FROM o_test")[0][0]
		self.assertGreater(count, 0)
		node.stop(['-m', 'immediate'])
		# every commit the restart has, the bucket has: no chunk removed
		# took WAL that was not archived
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(count, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_load_sees_commits_behind_a_long_transaction(self):
		"""A transaction open across a checkpoint holds its xmin: the ones that
		finished after it started and before the checkpoint are known as
		finished only from the xidmap, which never left the data directory.
		Here the open transaction creates a table, and the catalog cache
		entries go in by autonomous transactions of their own; loaded from the
		bucket they read as still in progress, and replaying the table's rows
		did not find them."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		hold = node.connect()
		hold.begin()
		hold.execute("""
			CREATE TABLE o_test (id int PRIMARY KEY, v text) USING orioledb;
			INSERT INTO o_test SELECT g, 'v' || g FROM generate_series(1, 1000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		hold.commit()
		hold.close()
		node.safe_psql("UPDATE o_test SET v = v || 'x' WHERE id <= 10;")
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(1000, 10)],
			                 new_node.execute(
			                     "SELECT count(*), count(*) FILTER (WHERE right(v, 1) = 'x') FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_dead_extents_are_recorded(self):
		"""Pages a checkpoint wrote and the next one relocates are recorded as
		dead from that next checkpoint on, for the GC; nothing is deleted."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int) USING orioledb;
			INSERT INTO o_test SELECT g, 0 FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		parts_before = self.part_etags()
		node.safe_psql("UPDATE o_test SET v = 1;")
		node.safe_psql("CHECKPOINT;")
		dead = [x["Key"] for x in self.client.list_objects(
		    Bucket=self.bucket_name, Prefix='trees/').get("Contents", [])
		        if '/dead/' in x["Key"]]
		self.assertTrue(dead)
		extents = 0
		for key in dead:
			extents += len(self.s3_object(key)) // 8
		self.assertGreater(extents, 10)
		# recording deletes nothing
		self.assertTrue(set(parts_before) <= set(self.part_etags()))
		self.assertEqual([(20000, 20000)],
		                 node.execute("SELECT count(*), sum(v) FROM o_test"))
		node.stop()

	def test_s3_restart_then_crash_keeps_commits(self):
		"""A clean stop puts the WAL the shipper had not shipped as chunks --
		never its partial segment under the archive's key, which a restore
		would take as complete and read no chunk over: the commits of the next
		run, in the same segment, would be lost."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.stop()
		node.start()
		node.safe_psql("INSERT INTO o_test SELECT generate_series(11, 20);")
		node.stop(['-m', 'immediate'])
		# an archived segment is a complete one: the next run did not write
		# on in it
		for obj in self.client.list_objects(Bucket=self.bucket_name,
		                                    Prefix='wal/').get("Contents", []):
			name = obj["Key"].split('/')[-1]
			if '/chunks/' in obj["Key"] or len(name) != 24:
				continue
			local = os.path.join(node.data_dir, "pg_wal", name)
			if os.path.exists(local):
				with open(local, 'rb') as f:
					self.assertEqual(f.read(), self.s3_object(obj["Key"]),
					                 f"archived {name} differs from pg_wal")
		if os.environ.get('DEBUG_CHUNKS'):
			print("root", self.s3_object('root').decode().replace('\n', ' | '), flush=True)
			for obj in self.client.list_objects(Bucket=self.bucket_name,
			                                    Prefix='wal/').get("Contents", []):
				if '/chunks/' in obj["Key"]:
					body = self.s3_object(obj["Key"])
					print(obj["Key"], struct.unpack('<IIQQQ', body[:32])[2:5], flush=True)
					with open('/tmp/' + obj["Key"].split('/')[-1] + '.chunk', 'wb') as f:
						f.write(body)
				else:
					print("segment", obj["Key"], flush=True)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(20, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def gc_world(self, extra_conf=""):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
			orioledb.main_buffers = 8MB
		""" + extra_conf)
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, 0, repeat('x', 200) FROM generate_series(1, 30000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		return node

	def assert_bucket_restores(self, node, expected,
	                           query="SELECT count(*), sum(v) FROM o_test"):
		"""What the GC left serves the writer, and a load: it reads every
		part it needs from the bucket, which the writer's local copies do
		not."""
		self.assertEqual(expected, node.execute(query))
		node.stop()
		with self.initNode(self.getBasePort() + 4, 'tgsr') as restored:
			self.loader.download(restored.data_dir)
			restored.append_conf(port=restored.port)
			restored.append_conf("orioledb.main_buffers = 8MB")
			restored.start()
			self.assertEqual(expected, restored.execute(query))
			restored.stop()
			restored.cleanup()

	def wait_for_gc(self, before, timeout=60):
		deadline = time.time() + timeout
		while time.time() < deadline:
			if set(before) - set(self.part_etags()):
				return True
			time.sleep(1)
		return False

	def test_s3_gc_removes_dead_parts(self):
		"""Parts whose every block died at a checkpoint no longer retained are
		removed; what is left restores every row."""
		node = self.gc_world()
		first = self.part_etags()
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		self.assertTrue(self.wait_for_gc(first))
		# the pass logs once it is through, after its deletes
		deadline = time.time() + 30
		while True:
			with open(node.pg_log_file) as f:
				if "S3 GC removed" in f.read():
					break
			self.assertLess(time.time(), deadline)
			time.sleep(0.5)
		self.assertEqual([(30000, 90000)],
		                 node.execute("SELECT count(*), sum(v) FROM o_test"))
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, 90000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_crash_restart_takes_a_new_token(self):
		"""A crash restart of the same postmaster takes a new writer token:
		the checkpoint numbers the run takes again never name what the
		crashed run left, nor one of its PUTs still on the way.  Its own
		lease it takes over at once."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 60
			restart_after_crash = on
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		token = int(self.kv('root')['token'])
		postmaster = node.pid
		with node.connect() as con:
			backend = con.execute("SELECT pg_backend_pid();")[0][0]
			os.kill(backend, signal.SIGKILL)
		deadline = time.time() + 30
		while True:
			try:
				node.safe_psql("INSERT INTO o_test SELECT generate_series(11, 20);")
				break
			except Exception:
				self.assertLess(time.time(), deadline)
				time.sleep(0.5)
		self.assertEqual(postmaster, node.pid)
		self.assertEqual(token + 1, int(self.kv('writer')['token']))
		node.safe_psql("CHECKPOINT;")
		self.assertEqual(token + 1, int(self.kv('root')['token']))
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsr') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(20, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_removes_parts_written_before_a_crash(self):
		"""Blocks the next checkpoint's stream wrote while a checkpoint ran
		are no longer referenced once a crash goes back to that checkpoint;
		their parts still go."""
		node = self.gc_world("""
			orioledb.enable_stopevents = true
			orioledb.debug_disable_pools_limit = true
			orioledb.main_buffers = 1MB
		""")
		node.safe_psql("UPDATE o_test SET v = v + 1;")
		con = node.connect()
		con.execute("SELECT pg_stopevent_set('checkpoint_step', "
		            "'$.action == \"walkDownwards\" && "
		            "$.treeName == \"o_test_pkey\" && $.lokey.id > 15000');")
		con.commit()
		checkpoint = Thread(target=lambda: node.safe_psql("CHECKPOINT;"))
		checkpoint.start()
		wait_checkpointer_stopevent(node)
		# the checkpoint holds halfway: what it passed goes to the next stream
		node.safe_psql("UPDATE o_test SET v = v + 1;")
		node.safe_psql("UPDATE o_test SET v = v + 1;")
		con.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		con.commit()
		con.close()
		checkpoint.join()
		node.stop(['-m', 'immediate'])
		node.start()
		total = node.execute("SELECT sum(v) FROM o_test")[0][0]

		seen = set(self.part_etags())
		rounds = []
		for v in range(4):
			node.safe_psql("UPDATE o_test SET v = v + 1;")
			node.safe_psql("CHECKPOINT;")
			now = set(self.part_etags())
			rounds.append(now - seen)
			seen |= now
		# the table's tree: every round rewrites each of its blocks
		tree = self.primary_relnode(node)
		recent = rounds[-1] | rounds[-2]
		self.assertTrue([k for k in recent if k.split('/')[2] == tree])

		def left():
			return {k for k in self.part_etags()
			        if k.split('/')[2] == tree} - recent

		deadline = time.time() + 90
		while time.time() < deadline and left():
			time.sleep(1)
		self.assertEqual(set(), left())
		self.assert_bucket_restores(node, [(30000, total + 4 * 30000)])

	def test_s3_gc_folds_the_lists_of_a_checkpoint_with_no_manifest(self):
		"""A checkpoint that finished locally but never got its manifest up
		has its dead lists in the bucket all the same; the GC folds them, so
		the blocks that died there do not live on."""
		node = self.gc_world("orioledb.s3_lease_duration = 15")
		parts = self.part_etags()
		tree = self.primary_relnode(node)
		first = {k for k in parts if k.split('/')[2] == tree}
		self.assertTrue(first)
		node.safe_psql("UPDATE o_test SET v = v + 1;")
		flaky = FlakyPutObject(10**9, prefix='manifests/')
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			checkpoint = Thread(target=lambda: node.execute("CHECKPOINT;"))
			checkpoint.daemon = True
			checkpoint.start()
			deadline = time.time() + 60
			while flaky.failed < 3 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreaterEqual(flaky.failed, 3)
			node.stop(['-m', 'immediate'])
			checkpoint.join(10)
		node.start()
		for v in range(2, 5):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 90
		while time.time() < deadline and first & set(self.part_etags()):
			time.sleep(1)
		self.assertEqual(set(), first & set(self.part_etags()))
		self.assert_bucket_restores(node, [(30000, 120000)])

	def test_s3_gc_keeps_checkpoints_by_time(self):
		"""orioledb.s3_gc_keep_time keeps the checkpoints committed within it
		restorable however many came after; then they go."""
		node = self.gc_world("orioledb.s3_gc_keep_time = 40")
		start = time.time()
		parts = self.part_etags()
		tree = self.primary_relnode(node)
		first = {k for k in parts if k.split('/')[2] == tree}
		self.assertTrue(first)
		kept = int(self.kv('root')['checkpoint'])
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		# within the window the first checkpoint loads and reads
		with self.initNode(self.getBasePort() + 1, 'tgsb') as old:
			self.loader.download(old.data_dir, as_of=kept)
			old.append_conf(port=old.port)
			old.append_conf("""
				orioledb.s3_reader = on
				orioledb.s3_reader_pin_ttl = 30
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(old.data_dir, "standby.signal"), "w").close()
			old.start()
			deadline = time.time() + 30
			while True:
				try:
					got = old.execute("SELECT count(*), sum(v) FROM o_test")
					break
				except Exception:
					self.assertLess(time.time(), deadline)
					time.sleep(0.5)
			self.assertEqual([(30000, 0)], got)
			old.stop()
			old.cleanup()
		self.assertLess(time.time(), start + 35)
		while time.time() < start + 35:
			self.assertEqual(first, first & set(self.part_etags()),
			                 f"{time.time() - start:.1f}s, gc {self.kv('gc')}, "
			                 f"kept {kept}")
			time.sleep(1)
		# past the window, and past the reader's pin
		deadline = time.time() + 90
		while time.time() < deadline and first & set(self.part_etags()):
			time.sleep(1)
		self.assertGreater(time.time(), start + 40)
		self.assertEqual(set(), first & set(self.part_etags()))
		self.assert_bucket_restores(node, [(30000, 90000)])

	def test_s3_gc_prunes_writer_epochs(self):
		"""Every run that commits a checkpoint adds an epoch to root.  The GC
		drops the ones no retained checkpoint names an object under, so the
		table stays small however often the instance restarts."""
		node = self.gc_world()

		def epochs():
			body = self.client.get_object(Bucket=self.bucket_name,
			                              Key='root')['Body'].read().decode()
			return [l for l in body.splitlines() if l.startswith('epoch ')]

		for v in range(1, 7):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
			node.stop(['-m', 'immediate'])
			node.start()
		node.safe_psql("UPDATE o_test SET v = 7;")
		node.safe_psql("CHECKPOINT;")
		grown = len(epochs())
		deadline = time.time() + 60
		while time.time() < deadline and len(epochs()) >= grown:
			node.safe_psql("CHECKPOINT;")
			time.sleep(1)
		self.assertLess(len(epochs()), grown)
		self.assertEqual([(30000, 210000)],
		                 node.execute("SELECT count(*), sum(v) FROM o_test"))
		node.stop()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, 210000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_as_of_an_old_epoch_after_a_takeover(self):
		"""Restarts add epochs and the GC prunes them; another data directory
		takes the bucket over and commits on.  The oldest retained
		checkpoint, written under a token of a run long gone, still loads
		with --as-of and reads as it was."""
		node = self.gc_world("orioledb.s3_gc_keep_checkpoints = 4")
		values = {}

		def note(v):
			c = int(self.kv('root')['checkpoint'])
			for k in range(max(values, default=0) + 1, c + 1):
				values[k] = v

		def epochs():
			return [l for l in self.s3_object('root').decode().splitlines()
			        if l.startswith('epoch ')]

		note(0)
		for v in range(1, 7):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
			note(v)
			node.stop(['-m', 'immediate'])
			node.start()
			note(v)
		grown = len(epochs())
		deadline = time.time() + 60
		while time.time() < deadline and len(epochs()) >= grown:
			node.safe_psql("CHECKPOINT;")
			note(6)
			time.sleep(1)
		self.assertLess(len(epochs()), grown)
		node.stop()
		note(6)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as b:
			self.loader.download(b.data_dir)
			b.append_conf(port=b.port)
			b.append_conf("""
				orioledb.s3_gc_interval = 1
				orioledb.s3_gc_keep_checkpoints = 4
				orioledb.main_buffers = 8MB
			""")
			b.start()
			note(6)
			for v in range(7, 9):
				b.safe_psql(f"UPDATE o_test SET v = {v};")
				b.safe_psql("CHECKPOINT;")
				note(v)
			time.sleep(3)
			oldest = int(self.kv('gc')['retain'])
			b.stop()
			self.assertIn(oldest, values)
			# written before the takeover
			self.assertLessEqual(values[oldest], 6)

			with self.initNode(self.getBasePort() + 2, 'tgsc') as old:
				self.loader.download(old.data_dir, as_of=oldest)
				old.append_conf(port=old.port)
				old.append_conf("""
					orioledb.s3_reader = on
					hot_standby = on
					orioledb.main_buffers = 8MB
				""")
				open(os.path.join(old.data_dir, "standby.signal"), "w").close()
				old.start()
				deadline = time.time() + 60
				while True:
					try:
						got = old.execute("SELECT count(*), sum(v) FROM o_test")
						break
					except Exception:
						self.assertLess(time.time(), deadline)
						time.sleep(0.5)
				self.assertEqual([(30000, 30000 * values[oldest])], got)
				old.stop()
				old.cleanup()
			b.cleanup()

	def test_s3_gc_goes_on_from_a_tree_state_folded_before_a_takeover(self):
		"""The old writer's GC folded the blocks that died in the first half
		of a table and removed those dead lists; the part the boundary falls
		in is half dead.  The writer that takes over kills the second half:
		its GC finds the first half's blocks in the tree state the bucket
		keeps, and removes that part too."""
		node = self.gc_world()
		first = {k for k in self.part_etags()
		         if k.split('/')[2] == self.primary_relnode(node)}
		self.assertTrue(first)
		node.safe_psql("UPDATE o_test SET v = 1 WHERE id <= 15000;")
		node.safe_psql("CHECKPOINT;")
		half = int(self.kv('root')['checkpoint'])
		for _ in range(2):
			node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 60
		while time.time() < deadline and int(self.kv('gc').get('folded', 0)) < half:
			node.safe_psql("CHECKPOINT;")
			time.sleep(1)
		self.assertGreaterEqual(int(self.kv('gc')['folded']), half)
		# the parts of the first half go; the one the boundary falls in is
		# half dead, and stays with the second half
		deadline = time.time() + 60
		while time.time() < deadline and first == first & set(self.part_etags()):
			time.sleep(1)
		left = first & set(self.part_etags())
		self.assertTrue(left)
		self.assertNotEqual(first, left)
		node.stop()
		self.assertTrue(self.all_keys('trees/') and
		                [k for k in self.all_keys('trees/') if k.endswith('/gc')])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as b:
			self.loader.download(b.data_dir)
			b.append_conf(port=b.port)
			b.append_conf("""
				orioledb.s3_gc_interval = 1
				orioledb.s3_gc_keep_checkpoints = 1
				orioledb.main_buffers = 8MB
			""")
			b.start()
			b.safe_psql("UPDATE o_test SET v = 1 WHERE id > 15000;")
			deadline = time.time() + 90
			while time.time() < deadline and first & set(self.part_etags()):
				b.safe_psql("CHECKPOINT;")
				time.sleep(1)
			self.assertEqual(set(), first & set(self.part_etags()))
			self.assert_bucket_restores(b, [(30000, 30000)])
			b.cleanup()

	def test_s3_gc_spares_a_reader_pin(self):
		"""A reader's pin keeps the GC below the checkpoint it opened."""
		node = self.gc_world()
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			pinned = self.part_etags()
			pinned_checkpoint = int(self.kv('root')['checkpoint'])
			for v in range(1, 4):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			# the GC ran past the pinned checkpoint's R, and its pass is through
			deadline = time.time() + 60
			while time.time() < deadline and \
			    int(self.kv('gc').get('retain', 0)) <= pinned_checkpoint:
				time.sleep(0.5)
			self.assertGreater(int(self.kv('gc')['retain']), pinned_checkpoint)
			node.safe_psql("CHECKPOINT;")
			time.sleep(3)
			# nothing the reader's checkpoint needs is gone
			self.assertTrue(set(pinned) <= set(self.part_etags()))
			# it reads its pinned parts, and follows the writer from the bucket
			deadline = time.time() + 60
			count = None
			while time.time() < deadline:
				try:
					count = reader.execute("SELECT count(*), sum(v) FROM o_test")
					if count == [(30000, 90000)]:
						break
				except Exception:
					pass
				time.sleep(0.5)
			self.assertEqual([(30000, 90000)], count)
			reader.stop()
			reader.cleanup()
		node.stop()

	def test_s3_reader_pinned_while_the_gc_reads_pins(self):
		"""The GC publishes R, then reads the pins.  A reader that pins its
		checkpoint in between finds R past it and does not start; the GC,
		reading the pins after, counts that pin and goes no further."""
		node = self.gc_world()
		hold = HoldFirstGet('readers')
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader, \
		    patch('moto.s3.responses.S3Response.get_object',
		          new=lambda response: hold(response)):
			self.loader.download(reader.data_dir)
			pinned = int(self.kv('root')['checkpoint'])
			for v in range(1, 4):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			# the GC's next pass stops after it publishes R
			hold.armed = True
			self.assertTrue(hold.entered.wait(60))
			self.assertGreater(int(self.kv('gc')['retain']), pinned)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			try:
				reader.start()
			except StartNodeException:
				pass
			with open(reader.pg_log_file) as f:
				self.assertIn(f"the S3 bucket no longer keeps checkpoint {pinned}",
				              f.read())
			# the pin is in the readers object the GC reads now
			self.assertIn(f" {pinned} ", self.s3_object('readers').decode())
			before = self.kv('gc')
			hold.release.set()
			node.safe_psql("UPDATE o_test SET v = 4;")
			node.safe_psql("CHECKPOINT;")
			time.sleep(5)
			# the pin holds the GC where it was
			after = self.kv('gc')
			for name in ('folded', 'collected'):
				self.assertLessEqual(int(after.get(name, 0)),
				                     max(int(before.get(name, 0)), pinned))
			reader.cleanup()
		node.stop()

	def test_s3_gc_spares_a_loading_checkpoint(self):
		"""The loader pins the checkpoint it reads, as a reader does: the
		writer goes on and its GC passes while the load is under way, and
		every object the load reads after that is still there."""
		node = self.gc_world()
		paused = []
		pausing = threading.Lock()

		def go_on_writing():
			for v in range(1, 4):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			time.sleep(5)

		real = botocore.client.BaseClient._make_api_call

		def call(client, operation, params):
			# the load reads maps in parallel: one of them pauses it
			if operation == 'GetObject' and '/maps/' in params.get('Key', ''):
				with pausing:
					if not paused:
						paused.append(True)
						go_on_writing()
			return real(client, operation, params)

		loaded = int(self.s3_body('root')['checkpoint'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			# a load reads every object its manifest names, or fails
			with patch('botocore.client.BaseClient._make_api_call', call):
				self.loader.download(new_node.data_dir, checkpoint_only=True)
			self.assertEqual(paused, [True])
			# the GC wanted to go past it meanwhile
			self.assertGreater(int(self.s3_body('gc')['retain']), loaded)
			self.assertNotIn('loader-', self.s3_object('readers').decode())
			new_node.cleanup()
		node.stop()

	def primary_relnode(self, node, table='o_test'):
		"""The relnode of a table's primary tree, as its part keys name it."""
		return str(node.execute(f"""
			SELECT index_relnode FROM orioledb_index_oids()
			WHERE table_reloid = '{table}'::regclass AND index_type = 'primary'
		""")[0][0])

	def part_relnodes(self):
		return {k.split('/')[2] for k in self.part_etags()}

	def test_s3_gc_removes_the_objects_of_an_aborted_build(self):
		"""A build puts its parts up before its transaction commits.  One that
		rolls back leaves a tree no manifest ever names; the GC removes what
		it put up all the same."""
		node = self.gc_world()
		before = self.part_relnodes()
		con = node.connect()
		con.begin()
		con.execute("CREATE INDEX o_test_v ON o_test (v, pad);")
		deadline = time.time() + 60
		while time.time() < deadline and not self.part_relnodes() - before:
			time.sleep(0.5)
		built = self.part_relnodes() - before
		self.assertTrue(built)
		con.rollback()
		con.close()

		def left():
			keys = self.client.list_objects_v2(Bucket=self.bucket_name,
			                                   Prefix='trees/')
			return {k["Key"] for k in keys.get("Contents", [])
			        if k["Key"].split('/')[2] in built}

		for _ in range(3):
			node.safe_psql("UPDATE o_test SET v = v + 1;")
			node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 90
		while time.time() < deadline and left():
			time.sleep(1)
		self.assertEqual(set(), left())
		self.assertTrue(before <= self.part_relnodes())
		self.assert_bucket_restores(node, [(30000, 90000)])

	def test_s3_gc_removes_a_dropped_tree(self):
		"""Once the checkpoint that first left a dropped tree out of the
		catalog is no longer retained, its parts go; the other trees stay."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE kept (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO kept SELECT g, repeat('k', 200) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_relnodes()
		node.safe_psql("""
			CREATE TABLE dropped (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO dropped SELECT g, repeat('d', 200) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		dropped = self.part_relnodes() - before
		self.assertTrue(dropped)
		node.safe_psql("DROP TABLE dropped;")
		for _ in range(3):
			node.safe_psql("CHECKPOINT;")
			time.sleep(2)
		deadline = time.time() + 60
		while time.time() < deadline and dropped & self.part_relnodes():
			time.sleep(1)
		self.assertFalse(dropped & self.part_relnodes())
		self.assertTrue(before <= self.part_relnodes())
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(20000, )],
			                 new_node.execute("SELECT count(*) FROM kept"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_removes_every_object_of_a_dropped_tree(self):
		"""A tree dropped by a writer whose GC never ran goes whole under the
		writer that takes the bucket over, which knows of the drop only from
		the manifests: the maps and parts no manifest named (a run that
		crashed before its checkpoint put them) go with the ones they
		named."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_gc_interval = 0\n")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE kept (id int PRIMARY KEY) USING orioledb;
			INSERT INTO kept SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		before = {k.rsplit('/', 2)[0] for k in self.part_etags()}
		node.safe_psql("""
			CREATE TABLE dropped (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO dropped SELECT g, repeat('d', 200) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		trees = {k.rsplit('/', 2)[0] for k in self.part_etags()} - before
		self.assertTrue(trees)
		for tree in trees:
			self.client.put_object(Bucket=self.bucket_name,
			                       Key=f"{tree}/maps/999-999", Body=b"x")
			self.client.put_object(Bucket=self.bucket_name,
			                       Key=f"{tree}/parts/999999", Body=b"x")
		node.safe_psql("DROP TABLE dropped;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		node.stop()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("""
				orioledb.s3_gc_interval = 1
				orioledb.s3_gc_keep_checkpoints = 1
			""")
			new_node.start()
			deadline = time.time() + 90
			while time.time() < deadline and \
			    any(self.all_keys(f"{tree}/parts/0") for tree in trees):
				new_node.safe_psql("CHECKPOINT;")
				time.sleep(1)
			time.sleep(2)
			self.assertEqual(
			    [], [k for tree in trees for k in self.all_keys(f"{tree}/")])
			self.assertEqual([(100, )],
			                 new_node.execute("SELECT count(*) FROM kept"))
			new_node.stop()
			new_node.cleanup()

	def all_keys(self, prefix=''):
		keys = []
		paginator = self.client.get_paginator('list_objects_v2')
		for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
			keys += [obj["Key"] for obj in page.get("Contents", [])]
		return keys

	def kv(self, key):
		result = {}
		for line in self.s3_object(key).decode().splitlines():
			k, _, v = line.partition(' ')
			result.setdefault(k, v)
		return result

	def wait_for_collected(self, checkpoint, timeout=90):
		deadline = time.time() + timeout
		while time.time() < deadline:
			try:
				if int(self.kv('gc').get('collected', 0)) >= checkpoint:
					return True
			except Exception:
				pass
			time.sleep(1)
		return False

	def manifest(self, chkp):
		root = self.s3_object('root').decode().splitlines()
		token = None
		for line in root:
			parts = line.split(' ')
			if parts[0] == 'epoch' and int(parts[1]) <= chkp:
				token = parts[2]
		body = self.s3_object(f"manifests/{token}-{chkp}").decode()
		m = {'files': {}, 'bundles': {}, 'trees': {}, 'undo': {}}
		for line in body.splitlines():
			kind, _, rest = line.partition(' ')
			if kind == 'file':
				n, _, path = rest.partition(' ')
				m['files'][path] = int(n)
			elif kind == 'bundles':
				n, count = rest.split(' ')
				m['bundles'][int(n)] = int(count)
			elif kind == 'tree':
				d, r, _, mapnum = rest.split(' ')
				m['trees'][(d, r)] = int(mapnum)
			elif kind == 'undoref':
				n, _, name = rest.partition(' ')
				m['undo'][name] = int(n)
			elif kind == 'redo':
				hi, lo = rest.split('/')
				m['redo'] = (int(hi, 16) << 32) | int(lo, 16)
		return m

	def test_s3_gc_removes_old_checkpoints(self):
		"""Once a checkpoint is no longer retained, its manifest goes, and so
		does every object of it the oldest retained checkpoint does not refer
		to: PG files, small file bundles, undo, maps, dead lists, the WAL
		segments before the retained REDO.  What is left restores every
		row."""
		node = self.gc_world()
		for v in range(1, 5):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		committed = int(self.kv('root')['checkpoint'])
		self.assertTrue(self.wait_for_collected(committed))
		r = int(self.kv('gc')['collected'])
		m = self.manifest(r)

		for key in self.all_keys('manifests/'):
			self.assertGreaterEqual(int(key.rsplit('-', 1)[1]), r, key)
		for key in self.all_keys('data/'):
			dirname, _, path = key[len('data/'):].partition('/')
			k = int(dirname.split('-')[1])
			if k >= r:
				continue
			if path.startswith('orioledb_data/small_files_'):
				self.assertIn(k, m['bundles'], key)
			else:
				self.assertEqual(m['files'].get(path), k, key)
		for key in self.all_keys('orioledb_undo/'):
			self.assertGreaterEqual(int(key.rsplit('-', 1)[1]), r, key)
		for key in self.all_keys('trees/'):
			fields = key.split('/')
			if fields[3] == 'maps':
				mapnum = int(fields[4].split('-')[1])
				if mapnum < r:
					self.assertEqual(m['trees'].get((fields[1], fields[2])),
					                 mapnum, key)
			elif fields[3] == 'dead':
				self.assertGreater(int(fields[4].split('-')[1].split('.')[0]), r,
				                   key)
		segment = m['redo'] // (16 * 1024 * 1024)
		for key in self.all_keys('wal/'):
			name = key.split('/')[-1]
			if '/chunks/' in key or len(name) != 24:
				continue
			self.assertGreaterEqual(
			    int(name[8:16], 16) * 0x100 + int(name[16:24], 16), segment, key)

		self.assertEqual([(30000, 120000)],
		                 node.execute("SELECT count(*), sum(v) FROM o_test"))
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, 120000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_compaction_frees_sparse_parts(self):
		"""Updates that leave most of a part dead leave the part to the GC
		for good; compaction moves its live pages out, and then the part
		goes."""
		node = self.gc_world()
		# the table's trees; the system trees are another story
		first = {k for k in self.part_etags() if k.split('/')[1] != '1'}
		# every fifth run of 100 rows stays where it is: a fifth of every part
		node.safe_psql("UPDATE o_test SET v = 1 WHERE (id / 100) % 5 <> 0;")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		time.sleep(5)
		self.assertTrue(first & set(self.part_etags()))
		marked = node.execute("SELECT orioledb_s3_compact('o_test');")[0][0]
		self.assertGreater(marked, 0)
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 90
		while time.time() < deadline and first & set(self.part_etags()):
			time.sleep(1)
		self.assertFalse(first & set(self.part_etags()))
		self.assertEqual([(30000, 24000)],
		                 node.execute("SELECT count(*), sum(v) FROM o_test"))
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, 24000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_sweep_removes_what_the_gc_could_not_see(self):
		"""Objects of collected checkpoints that no manifest names (a crash
		before the manifest) go with --sweep; nothing a restore needs does."""
		node = self.gc_world()
		token = self.kv('root')['token']
		orphans = [f"data/{token}-1/orphan", f"trees/5/999999/maps/{token}-1",
		           f"orioledb_undo/00000000AArow/{token}-1"]
		for key in orphans:
			self.client.put_object(Bucket=self.bucket_name, Key=key, Body=b"x")
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		committed = int(self.kv('root')['checkpoint'])
		self.assertTrue(self.wait_for_collected(committed))
		self.assertTrue(set(orphans) <= set(self.all_keys()))
		self.loader.download(None)
		self.assertFalse(set(orphans) & set(self.all_keys()))
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, 90000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_compaction_runs_periodically(self):
		"""With orioledb.s3_compact_database set, a worker compacts the
		database's orioledb tables on its own."""
		node = self.gc_world("""
			orioledb.s3_compact_database = 'postgres'
			orioledb.s3_compact_interval = 3
		""")
		first = {k for k in self.part_etags() if k.split('/')[1] != '1'}
		node.safe_psql("UPDATE o_test SET v = 1 WHERE (id / 100) % 5 <> 0;")
		deadline = time.time() + 120
		while time.time() < deadline and first & set(self.part_etags()):
			node.safe_psql("CHECKPOINT;")
			time.sleep(3)
		self.assertFalse(first & set(self.part_etags()))
		with open(node.pg_log_file) as f:
			self.assertIn("S3 compaction marked", f.read())
		self.assert_bucket_restores(node, [(30000, 24000)])

	def test_s3_reader_too_far_behind_stops(self):
		"""A reader whose pin falls further behind than
		orioledb.s3_gc_max_pin_lag allows stops before the GC passes it over,
		and the GC then goes on."""
		node = self.gc_world("orioledb.s3_gc_max_pin_lag = 8")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				orioledb.s3_reader_pin_ttl = 30
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			pinned = set(self.part_etags())
			for v in range(1, 12):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			deadline = time.time() + 90
			while reader.status() == NodeStatus.Running and time.time() < deadline:
				time.sleep(1)
			self.assertNotEqual(reader.status(), NodeStatus.Running)
			with open(reader.pg_log_file) as f:
				self.assertIn("this S3 reader is too far behind the writer", f.read())
			self.assertTrue(self.wait_for_gc(pinned))
			reader.cleanup()
		self.assert_bucket_restores(node, [(30000, 330000)])

	def test_s3_reader_whose_pin_lapsed_stops(self):
		"""A reader whose renewals stopped for longer than its pin lives is
		passed by the GC.  Renewing again, it finds the GC folded or collected
		past its checkpoint, and stops rather than read parts that are
		gone."""
		node = self.gc_world()
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				orioledb.s3_reader_pin_ttl = 30
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			pinned = int(self.kv('root')['checkpoint'])
			lease = [int(l.split()[0]) for l in os.popen(
			    f"ps -o pid=,args= --ppid {reader.pid}").read().splitlines()
			    if 'orioledb s3 lease' in l]
			self.assertEqual(1, len(lease))
			os.kill(lease[0], signal.SIGSTOP)
			try:
				# the pin runs out, and the GC goes past it
				for v in range(1, 4):
					node.safe_psql(f"UPDATE o_test SET v = {v};")
					node.safe_psql("CHECKPOINT;")
				time.sleep(31)
				deadline = time.time() + 60
				while time.time() < deadline and \
				    int(self.kv('gc').get('collected', 0)) <= pinned:
					node.safe_psql("CHECKPOINT;")
					time.sleep(1)
				self.assertGreater(int(self.kv('gc')['collected']), pinned)
			finally:
				os.kill(lease[0], signal.SIGCONT)
			deadline = time.time() + 60
			while reader.status() == NodeStatus.Running and time.time() < deadline:
				time.sleep(1)
			self.assertNotEqual(reader.status(), NodeStatus.Running)
			with open(reader.pg_log_file) as f:
				self.assertIn("the S3 GC passed this reader's checkpoint", f.read())
			reader.cleanup()
		node.stop()

	def test_s3_reader_refuses_a_checkpoint_the_gc_collected(self):
		"""R the GC publishes is not the only bound: a GC that keeps more
		again (a setting raised) publishes an R below what it already
		collected.  A reader is refused a checkpoint the GC folded or
		collected past, and the GC publishes no R below them."""
		node = self.gc_world()
		for v in range(1, 6):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 60
		while time.time() < deadline and int(self.kv('gc').get('collected', 0)) < 4:
			node.safe_psql("CHECKPOINT;")
			time.sleep(1)
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			checkpoint = int(self.kv('root')['checkpoint'])
			# keep every checkpoint from now on
			node.append_conf(
			    f"orioledb.s3_gc_keep_checkpoints = {checkpoint}")
			node.restart()
			published = self.s3_object('gc')
			node.safe_psql("CHECKPOINT;")
			deadline = time.time() + 30
			while time.time() < deadline and self.s3_object('gc') == published:
				time.sleep(0.5)
			gc = self.kv('gc')
			self.assertGreaterEqual(int(gc['collected']), 4)
			self.assertGreaterEqual(int(gc['retain']), int(gc['collected']))
			node.stop()
			# the reader opens a checkpoint the GC has folded past
			gc = self.kv('gc')
			body = ''.join(f"{k} {checkpoint + 1 if k == 'folded' else v}\n"
			               for k, v in gc.items())
			self.client.put_object(Bucket=self.bucket_name, Key='gc',
			                       Body=body.encode())
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			try:
				reader.start()
			except StartNodeException:
				pass
			with open(reader.pg_log_file) as f:
				self.assertIn(f"the S3 bucket no longer keeps checkpoint {checkpoint}",
				              f.read())
			reader.cleanup()

	def test_s3_two_takeovers_one_writer(self):
		"""Two data directories loaded from the bucket start as writers at
		once: one takes the bucket, the other is refused, and the bucket has
		one history."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test VALUES (101);")
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as a, \
		    self.initNode(self.getBasePort() + 2, 'tgsc') as b:
			for n in (a, b):
				self.loader.download(n.data_dir)
				n.append_conf(port=n.port)
			results = {}

			def start(n, name):
				try:
					n.start()
					results[name] = True
				except Exception:
					results[name] = False
			ta = Thread(target=start, args=(a, 'a'))
			tb = Thread(target=start, args=(b, 'b'))
			ta.start()
			tb.start()
			ta.join(120)
			tb.join(120)
			running = [n for n in (a, b) if n.status() == NodeStatus.Running]
			self.assertEqual(len(running), 1, results)
			winner = running[0]
			self.assertEqual([(101, )],
			                 winner.execute("SELECT count(*) FROM o_test"))
			winner.safe_psql("INSERT INTO o_test VALUES (102);")
			winner.stop(['-m', 'immediate'])
			for n in (a, b):
				n.cleanup()
		with self.initNode(self.getBasePort() + 3, 'tgsd') as last:
			self.loader.download(last.data_dir)
			last.append_conf(port=last.port)
			last.start()
			self.assertEqual([(102, )],
			                 last.execute("SELECT count(*) FROM o_test"))
			last.stop()
			last.cleanup()

	def test_s3_late_chunk_of_a_fenced_writer_is_ignored(self):
		"""A chunk the fenced writer puts after the terminator is not read:
		the old timeline's WAL restored from the bucket is the WAL that writer
		had shipped, byte for byte."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		node.safe_psql("CHECKPOINT;")
		for i in range(101, 111):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
		old_token = int(self.s3_body('writer')['token'])
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			keys = sorted(k for k in self.chunk_keys(terminators=True)
			              if k.startswith('wal/00000001/'))
			chunks = [(k, struct.unpack('<IIQQQ', self.s3_object(k)[:32]))
			          for k in keys]
			real = [(k, h) for k, h in chunks if h[0] == 0x3143574f]
			last_key, last = real[-1]
			terminator = max(int(k.rsplit('/', 1)[1]) for k, _ in chunks)
			# garbage over the last real chunk, after the terminator
			self.client.put_object(
			    Bucket=self.bucket_name,
			    Key=f"wal/00000001/chunks/{terminator + 1:020d}",
			    Body=struct.pack('<IIQQQ', 0x3143574f, 1, old_token, last[3],
			                     last[3] + 8192) + b'\xff' * 8192)
			new_node.stop(['-m', 'immediate'])

			segment_size = 16 * 1024 * 1024
			segno = last[3] // segment_size
			name = f"00000001{segno >> 8:08X}{segno & 0xFF:08X}"
			restored = os.path.join(new_node.data_dir, 'restored-segment')
			old_argv = sys.argv
			sys.argv = [os.path.join(os.path.dirname(os.path.realpath(__file__)),
			                         '../../orioledb_s3_loader.py'),
			            '--endpoint', self.loader._endpoint_url,
			            '--cert-file', self.s3_cainfo,
			            '-d', new_node.data_dir, '--restore', name, restored]
			try:
				runpy.run_path(sys.argv[0], None, "__main__")
			except SystemExit as e:
				self.assertEqual(e.code, 0)
			finally:
				sys.argv = old_argv
			with open(os.path.join(node.data_dir, 'pg_wal', name), 'rb') as f:
				original = f.read()
			with open(restored, 'rb') as f:
				got = f.read()
			upto = last[4] - segno * segment_size
			self.assertEqual(original[:upto], got[:upto])
			new_node.cleanup()

	def test_s3_terminator_goes_after_the_last_chunk(self):
		"""The fenced writer went on after the data directory was loaded, and
		an archived segment let its chunks from the load's place on go: the
		terminator is put after its last chunk, found by listing, and every
		commit it acknowledged is recovered.  Put where the hint said, in the
		gap, it ended the restore there."""
		self.terminator_after_the_last_chunk(path_style=False)

	def test_s3_terminator_goes_after_the_last_chunk_path_style(self):
		"""The same with the bucket given as the prefix's first segment: the
		listing goes to the bucket, not the store's root (which answers with
		its buckets, and so with no chunk at all)."""
		self.terminator_after_the_last_chunk(path_style=True)

	def terminator_after_the_last_chunk(self, path_style):
		node = self.node
		node.append_conf(self.s3_conf(path_style=path_style))
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			with open(os.path.join(new_node.data_dir, 'orioledb_data',
			                       's3_wal_seq')) as f:
				hint = int(f.read().split()[1])
			hint_key = f'wal/00000001/chunks/{hint:020d}'
			for i in range(1, 6):
				node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
				wal_file = node.execute(
				    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
				node.safe_psql("SELECT pg_switch_wal();")
				self.wait_for_object(f'wal/{wal_file}')
			# the chunks from the hint on are gone, later ones are not
			deadline = time.time() + 60
			while time.time() < deadline and \
			    any(k <= hint_key for k in self.chunk_keys()
			        if k.startswith('wal/00000001/')):
				node.safe_psql("SELECT 1;")
				time.sleep(1)
			self.assertFalse(any(k <= hint_key for k in self.chunk_keys()
			                     if k.startswith('wal/00000001/')))
			for i in range(6, 9):
				node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
			self.assertTrue(self.chunk_keys())
			os.kill(node.pid, signal.SIGKILL)

			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(8, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_copied_data_dir_does_not_ship_over_the_bucket(self):
		"""A copy of the data directory, taken while the writer ran, starts
		once that writer is gone.  The chunks the writer put after the copy
		carry the token the copy has as its previous one, but they hold whole
		records past where the copy's recovery ended: the copy stops instead
		of shipping its own WAL over them, and a load from the bucket has
		every commit the writer acknowledged."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 10
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		with self.initNode(self.getBasePort() + 1, 'tgsc') as copy:
			# a crash-consistent copy: every process stopped while it is taken
			self.copy_while_stopped(node, copy)
			for i in range(11, 21):
				node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
			os.kill(node.pid, signal.SIGKILL)

			copy.append_conf(port=copy.port)
			try:
				copy.start()
			except StartNodeException:
				pass
			deadline = time.time() + 120
			stopped = False
			while time.time() < deadline and not stopped:
				with open(copy.pg_log_file) as f:
					stopped = "another copy of it went on writing" in f.read()
				time.sleep(1)
			self.assertTrue(stopped)
			try:
				copy.stop(['-m', 'immediate'])
			except Exception:
				pass

			with self.initNode(self.getBasePort() + 2, 'tgsd') as last:
				self.loader.download(last.data_dir)
				last.append_conf(port=last.port)
				last.start()
				self.assertEqual([(20, )],
				                 last.execute("SELECT count(*) FROM o_test"))
				last.stop()
				last.cleanup()
			copy.cleanup()

	def test_s3_copy_and_a_paused_original_do_not_splice(self):
		"""A copy of a running writer's data directory starts while the
		original is paused past its lease, and takes the bucket over.  The
		original, resumed, ships a chunk where the copy's first one was to go
		before the copy's lands.  Neither history may be spliced onto the
		other: a load has the copy's history whole, and every commit the
		original acknowledged."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, who text) USING orioledb;
			INSERT INTO o_test SELECT g, 'a' FROM generate_series(1, 10) g;
		""")
		node.safe_psql("CHECKPOINT;")
		token = int(self.kv('root')['token'])
		hold = HoldChunksOf()
		with self.initNode(self.getBasePort() + 1, 'tgsc') as copy, \
		    patch('moto.s3.responses.S3Response.put_object',
		          new=lambda response: hold(response)):
			self.copy_while_stopped(node, copy)
			copy.append_conf(port=copy.port)
			pids = [node.pid] + [
			    int(p) for p in os.popen(
			        f"ps -o pid= --ppid {node.pid}").read().split()]
			lease = [int(l.split()[0]) for l in os.popen(
			    f"ps -o pid=,args= --ppid {node.pid}").read().splitlines()
			    if 'orioledb s3 lease' in l]
			for pid in pids:
				os.kill(pid, signal.SIGSTOP)
			# the copy's chunks wait until the original has shipped
			hold.token = token + 1
			copy.start()
			copied = []

			def copy_writes():
				for i in range(11, 21):
					copy.safe_psql(f"INSERT INTO o_test VALUES ({i}, 'b');")
					copied.append(i)

			writer_b = Thread(target=copy_writes)
			writer_b.daemon = True
			writer_b.start()
			deadline = time.time() + 60
			while hold.held < 2 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreaterEqual(hold.held, 2)
			# the original, but its lease worker, goes on
			for pid in pids:
				if pid not in lease:
					os.kill(pid, signal.SIGCONT)
			acked = []

			def original_writes():
				try:
					for i in range(100, 105):
						node.safe_psql(f"INSERT INTO o_test VALUES ({i}, 'a');")
						acked.append(i)
				except Exception:
					pass

			writer_a = Thread(target=original_writes)
			writer_a.daemon = True
			writer_a.start()
			writer_a.join(20)
			hold.release = True
			for pid in lease:
				try:
					os.kill(pid, signal.SIGCONT)
				except ProcessLookupError:
					pass
			writer_b.join(60)
			# the original, stopping on its lost lease, left the copy's alone
			self.assertEqual(token + 1, int(self.kv('writer')['token']))
			copy.stop(['-m', 'immediate'])
			try:
				node.stop(['-m', 'immediate'])
			except Exception:
				pass

			with self.initNode(self.getBasePort() + 2, 'tgsd') as last:
				self.loader.download(last.data_dir)
				last.append_conf(port=last.port)
				last.start()
				rows = dict(last.execute("SELECT id, who FROM o_test"))
				self.assertEqual([], [i for i in acked if i not in rows])
				# the copy goes on: every commit of it acknowledged and loaded
				self.assertEqual(list(range(11, 21)), copied)
				self.assertEqual([], [i for i in copied if rows.get(i) != 'b'])
				last.stop()
				last.cleanup()
			copy.cleanup()

	def test_s3_takeover_of_a_writer_stalled_mid_commit(self):
		"""The writer stalls while a commit waits for its chunk; another data
		directory loads the bucket and takes it over.  Woken, the old writer
		acknowledges nothing, stops, and the bucket has the new writer's
		history alone."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, who text) USING orioledb;
			INSERT INTO o_test SELECT g, 'a' FROM generate_series(1, 10) g;
		""")
		node.safe_psql("CHECKPOINT;")
		flaky = FlakyPutObject(10**9, contains='/chunks/')
		done = []
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: flaky(response)):
			def commit():
				try:
					node.safe_psql("INSERT INTO o_test VALUES (11, 'a');")
					done.append('acked')
				except Exception:
					done.append('failed')
			waiting = Thread(target=commit)
			waiting.daemon = True
			waiting.start()
			deadline = time.time() + 30
			while flaky.failed == 0 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreater(flaky.failed, 0)
			pids = [node.pid] + [int(p) for p in os.popen(
			    f"ps -o pid= --ppid {node.pid}").read().split()]
			for pid in pids:
				os.kill(pid, signal.SIGSTOP)
		# the store serves the old writer's chunk again; it is asleep
		with self.initNode(self.getBasePort() + 1, 'tgsb') as b:
			self.loader.download(b.data_dir)
			b.append_conf(port=b.port)
			b.start()
			b.safe_psql("INSERT INTO o_test SELECT g, 'b' FROM generate_series(20, 29) g;")
			for pid in pids:
				try:
					os.kill(pid, signal.SIGCONT)
				except ProcessLookupError:
					pass
			waiting.join(60)
			self.assertNotEqual(['acked'], done)
			deadline = time.time() + 60
			while node.status() == NodeStatus.Running and time.time() < deadline:
				time.sleep(0.5)
			self.assertNotEqual(NodeStatus.Running, node.status())
			b.safe_psql("INSERT INTO o_test VALUES (30, 'b');")
			b.stop(['-m', 'immediate'])

			with self.initNode(self.getBasePort() + 2, 'tgsc') as last:
				self.loader.download(last.data_dir)
				last.append_conf(port=last.port)
				last.start()
				self.assertEqual(
				    [(21, 0)],
				    last.execute("SELECT count(*), count(*) FILTER (WHERE id = 11) FROM o_test"))
				last.stop()
				last.cleanup()
			b.cleanup()

	def copy_while_stopped(self, node, copy):
		"""A crash-consistent copy of node's data directory into copy's:
		the postmaster is stopped first, so no process starts while its
		children are listed and stopped."""
		os.kill(node.pid, signal.SIGSTOP)
		pids = [node.pid]
		try:
			children = [int(p) for p in os.popen(
			    f"ps -o pid= --ppid {node.pid}").read().split()]
			for pid in children:
				try:
					os.kill(pid, signal.SIGSTOP)
					pids.append(pid)
				except ProcessLookupError:
					pass
			shutil.rmtree(copy.data_dir)
			shutil.copytree(node.data_dir, copy.data_dir, symlinks=True)
		finally:
			for pid in pids:
				try:
					os.kill(pid, signal.SIGCONT)
				except ProcessLookupError:
					pass
		os.unlink(os.path.join(copy.data_dir, 'postmaster.pid'))

	def test_s3_copy_is_stopped_after_its_chunks_are_archived(self):
		"""The writer goes on long after the copy was taken: the chunks the
		copy would have to read back to where its recovery ended are archived
		and gone.  The copy reads that WAL from the archived segments and
		still stops; if it could read it from nowhere it stops too, rather
		than ship."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		with self.initNode(self.getBasePort() + 1, 'tgsc') as copy:
			self.copy_while_stopped(node, copy)
			first = len(self.chunk_keys())
			for i in range(11, 31):
				node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
				# no checkpoint: the bucket's root stays the copy's
				if i % 5 == 0:
					node.safe_psql("SELECT pg_switch_wal();")
			# the copy's chunks are gone
			deadline = time.time() + 60
			while time.time() < deadline and \
			    min(int(k.rsplit('/', 1)[1]) for k in self.chunk_keys()) < first:
				time.sleep(1)
			self.assertGreaterEqual(
			    min(int(k.rsplit('/', 1)[1]) for k in self.chunk_keys()), first)
			os.kill(node.pid, signal.SIGKILL)

			copy.append_conf(port=copy.port)
			try:
				copy.start()
			except StartNodeException:
				pass
			deadline = time.time() + 120
			stopped = False
			while time.time() < deadline and not stopped:
				with open(copy.pg_log_file) as f:
					log = f.read()
				stopped = "another copy of it went on writing" in log or \
				    "could not check the S3 bucket's WAL" in log
				time.sleep(1)
			self.assertTrue(stopped)
			try:
				copy.stop(['-m', 'immediate'])
			except Exception:
				pass

			with self.initNode(self.getBasePort() + 2, 'tgsd') as last:
				self.loader.download(last.data_dir)
				last.append_conf(port=last.port)
				last.start()
				self.assertEqual([(30, )],
				                 last.execute("SELECT count(*) FROM o_test"))
				last.stop()
				last.cleanup()
			copy.cleanup()

	def test_s3_copy_is_stopped_by_a_segment_archived_past_its_chunks(self):
		"""The writer goes on past the copy with WAL its shipper has not put
		up yet, and archives the segment the copy's recovery ends in.  The
		chunks then reach no further than the copy does, but a restore reads
		that segment, not the chunks the copy would ship into it: the copy
		stops."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		token = int(self.kv('root')['token'])
		hold = HoldChunksOf()
		with self.initNode(self.getBasePort() + 1, 'tgsc') as copy, \
		    patch('moto.s3.responses.S3Response.put_object',
		          new=lambda response: hold(response)):
			self.copy_while_stopped(node, copy)
			segment = node.execute(
			    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
			# the writer's shipper puts nothing more; its commits wait for
			# no chunk
			hold.token = token
			node.safe_psql("""
				SET synchronous_commit = local;
				INSERT INTO o_test SELECT generate_series(11, 30);
				SELECT pg_switch_wal();
			""")
			self.wait_for_object(f'wal/{segment}')
			os.kill(node.pid, signal.SIGKILL)

			copy.append_conf(port=copy.port)
			try:
				copy.start()
			except StartNodeException:
				pass
			deadline = time.time() + 120
			stopped = False
			while time.time() < deadline and not stopped:
				with open(copy.pg_log_file) as f:
					log = f.read()
				stopped = "another copy of it went on writing" in log
				time.sleep(1)
			self.assertTrue(stopped)
			try:
				copy.stop(['-m', 'immediate'])
			except Exception:
				pass
			copy.cleanup()

	def test_s3_restart_keeps_the_chunk_across_a_segment_end(self):
		"""The last chunk a clean stop puts up reaches past the end of a
		segment that is archived.  The next run must keep it: the restore
		of the next segment reads its start from it."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
		""")
		node.safe_psql("INSERT INTO o_test SELECT g, md5(g::text) FROM generate_series(1, 300000) g;")
		node.stop()
		ranges = self.chunk_ranges()
		# segments archived, the last chunk past the end of one of them
		self.assertTrue([k for k in self.all_keys('wal/0') if len(k) == 28])
		node.start()
		# the shipper's GC of what the last run left comes round
		time.sleep(14)
		self.assert_removed_chunks_archived(ranges)
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(300000, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_restore_lists_the_chunks_once(self):
		"""A recovery asks restore_command for its segments in order.  Each
		lists the chunks from the last one it has that ends before the
		segment, not the timeline's whole list again: across a recovery the
		chunks are listed about once."""
		from moto.s3.responses import S3Response
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			archive_mode = off
			synchronous_standby_names = 's3'
			checkpoint_timeout = 1h
			max_wal_size = 1GB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		start = node.execute("SELECT pg_current_wal_lsn();")[0][0]
		script = os.path.join(node.base_dir, 'insert.sql')
		with open(script, 'w') as f:
			f.write("INSERT INTO o_test SELECT nextval('s'), (SELECT "
			        "string_agg(md5(random()::text), '') FROM generate_series(1, 400));\n")
		node.safe_psql("CREATE SEQUENCE s;")
		subprocess.run(
		    [testgres.get_bin_path('pgbench'), '-n', '-f', script,
		     '-c', '4', '-t', '2500', '-h', node.host, '-p', str(node.port),
		     default_dbname()], check=True,
		    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		segments = node.execute(
		    f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{start}') / "
		    "(16 * 1024 * 1024)")[0][0]
		self.assertGreaterEqual(segments, 7)
		chunks = len(self.all_keys('wal/00000001/chunks/'))
		node.stop(['-m', 'immediate'])

		listed = [0]
		orig = S3Response.list_objects_v2

		def counting(response):
			result = orig(response)
			prefix = response.querystring.get('prefix', [''])[0]
			if '/chunks/' in prefix:
				body = result[2] if isinstance(result, tuple) else result
				if isinstance(body, bytes):
					body = body.decode()
				listed[0] += body.count('<Key>')
			return result

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node, \
		    patch.object(S3Response, 'list_objects_v2', counting):
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(10000, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()
		# the load reads them twice, and the first segment once
		self.assertLess(listed[0], 4 * chunks,
		                f"{chunks} chunks, {segments} segments")

	def test_s3_restart_goes_on_past_a_lagging_hint(self):
		"""The hint of where this directory's chunks end is not made durable
		after every chunk: after a machine crash it may name a chunk the GC
		has removed since.  The restart lists the chunks and goes on from the
		first left, instead of shipping under them."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			synchronous_standby_names = 's3'
			wal_keep_size = 0
			min_wal_size = 32MB
			max_wal_size = 64MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		seq_file = os.path.join(node.data_dir, 'orioledb_data', 's3_wal_seq')
		with open(seq_file) as f:
			old_hint = f.read()
		old_seq = int(old_hint.split()[1])
		for i in range(1, 21):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
			if i % 5 == 0:
				node.safe_psql("SELECT pg_switch_wal();")
				node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 60
		while time.time() < deadline and \
		    min(int(k.rsplit('/', 1)[1]) for k in self.chunk_keys()) <= old_seq:
			time.sleep(1)
		self.assertGreater(
		    min(int(k.rsplit('/', 1)[1]) for k in self.chunk_keys()), old_seq)
		node.stop(['-m', 'immediate'])
		# what a crash of the machine may leave: the hint of long ago
		with open(seq_file, 'w') as f:
			f.write(old_hint)
		node.start()
		node.safe_psql("INSERT INTO o_test VALUES (21);")
		self.assertEqual([(21, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(21, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_restore_refuses_a_gap_in_the_chunks(self):
		"""A chunk missing between two that are there is WAL that was
		acknowledged and is not in the bucket: restoring the segment up to the
		gap would end recovery there and leave the rest out without a word.
		Recovery stops instead."""
		self.gap_in_the_chunks(lambda keys, ranges, seg_start: keys[len(keys) // 2])

	def test_s3_restore_command_that_cannot_run_stops_recovery(self):
		"""A restore_command that cannot run at all (no interpreter, no
		module, a wrong path) answers PostgreSQL with a code that reads as
		"no such file", and recovery would end the WAL there, without the
		commits in the bucket after it.  The restore_command the loader
		writes turns every failure other than its own "not there" into a
		stop."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("SELECT pg_switch_wal();")
		for i in range(1, 11):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			auto = os.path.join(new_node.data_dir, 'postgresql.auto.conf')
			with open(auto) as f:
				conf = f.read()
			self.assertIn('orioledb_s3_loader.py', conf)
			with open(auto, 'w') as f:
				f.write(conf.replace('orioledb_s3_loader.py', 'no_such_loader.py'))
			try:
				new_node.start()
			except StartNodeException:
				pass
			deadline = time.time() + 60
			while True:
				with open(new_node.pg_log_file) as f:
					log = f.read()
				if "could not restore file" in log:
					break
				self.assertLess(time.time(), deadline)
				time.sleep(0.5)
			self.assertNotIn("database system is ready to accept connections",
			                 log)
			try:
				new_node.stop(['-m', 'immediate'])
			except Exception:
				pass
			new_node.cleanup()

	def test_s3_restore_answers_a_standby_with_not_there(self):
		"""--restore that cannot reach the bucket stops a writer's recovery
		(128) but tells a standby the file is not there yet (99), so a reader
		asks again later rather than stop."""
		loader = os.path.join(dir_path, '..', '..', 'orioledb_s3_loader.py')
		for standby, expected in ((False, 128), (True, 99)):
			d = mkdtemp()
			try:
				if standby:
					open(os.path.join(d, 'standby.signal'), 'w').close()
				rc = subprocess.run(
				    [sys.executable, loader, '--endpoint', 'http://127.0.0.1:9',
				     '--resolved-endpoint', 'http://127.0.0.1:9',
				     '--resolved-bucket', self.bucket_name, '--resolved-prefix', '',
				     '--resolved-style', 'path', '--segment-size', '16777216',
				     '--restore', '000000010000000000000009', os.path.join(d, 'x')],
				    cwd=d, capture_output=True,
				    env=dict(os.environ, AWS_MAX_ATTEMPTS='1')).returncode
				self.assertEqual(expected, rc)
			finally:
				shutil.rmtree(d)

	def test_s3_restore_command_maps_the_loader_answers(self):
		"""The restore_command a load writes passes the loader's answers on as
		PostgreSQL reads them: found (0) is 0, not there (99) is 1, and any
		other end (an error, a crash, a signal) is 128, which stops recovery
		rather than end the WAL there.  A standby takes them all as not
		there yet."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("CHECKPOINT;")
		node.stop()
		loader = os.path.abspath(
		    os.path.join(dir_path, '..', '..', 'orioledb_s3_loader.py'))
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			with open(os.path.join(new_node.data_dir,
			                       'postgresql.auto.conf')) as f:
				line = [l for l in f if l.startswith('restore_command')][-1]
			command = line.split('=', 1)[1].strip()[1:-1].replace("''", "'")
			self.assertIn(loader, command)
			stub = os.path.join(new_node.base_dir, 'stub.py')
			with open(stub, 'w') as f:
				f.write("import os, signal\n"
				        "code = os.environ['STUB_ANSWER']\n"
				        "if code == 'kill':\n"
				        "    os.kill(os.getpid(), signal.SIGKILL)\n"
				        "raise SystemExit(int(code))\n")
			command = command.replace(loader, stub).replace(
			    '%f', '000000010000000000000009').replace('%p', 'x')
			for standby, answers in (
			    (False, {'0': 0, '99': 1, '1': 128, '2': 128, 'kill': 128}),
			    (True, {'0': 0, '99': 1, '1': 1, '2': 1, 'kill': 1})):
				if standby:
					open(os.path.join(new_node.data_dir, 'standby.signal'),
					     'w').close()
				for answer, expected in answers.items():
					rc = subprocess.run(
					    ['sh', '-c', command], cwd=new_node.data_dir,
					    env=dict(os.environ, STUB_ANSWER=answer)).returncode
					self.assertEqual(expected, rc, (standby, answer))
			new_node.cleanup()

	def test_s3_restore_stops_when_the_store_fails(self):
		"""A chunk the store does not serve (503 past the retries) is no
		end of the WAL: --restore answers 128, which stops recovery, not 1;
		served, the same request answers 0."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test SELECT generate_series(1, 10);")
		segment = node.execute(
		    "SELECT pg_walfile_name(pg_current_wal_lsn());")[0][0]
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			with open(os.path.join(new_node.data_dir,
			                       'postgresql.auto.conf')) as f:
				line = [l for l in f if l.startswith('restore_command')][-1]
			command = line.split('=', 1)[1].strip()[1:-1].replace("''", "'")
			# the loader's own answer, without the wrapper's mapping
			command = command.split(';')[0].replace('%f', segment).replace(
			    '%p', os.path.join(new_node.base_dir, 'seg'))
			shutil.rmtree(os.path.join(new_node.data_dir, 'pg_wal',
			                           'orioledb_s3_chunks'), ignore_errors=True)
			failing = FailingGet('/chunks/')
			with patch('moto.s3.responses.S3Response.get_object',
			           new=lambda response: failing(response)):
				rc = subprocess.run(['sh', '-c', command], cwd=new_node.data_dir,
				                    capture_output=True,
				                    env=dict(os.environ, AWS_MAX_ATTEMPTS='1')).returncode
			self.assertGreater(failing.failed, 0)
			self.assertEqual(128, rc)
			rc = subprocess.run(['sh', '-c', command], cwd=new_node.data_dir,
			                    capture_output=True).returncode
			self.assertEqual(0, rc)
			new_node.cleanup()

	def test_s3_a_conflicting_part_put_is_put_again(self):
		"""AWS answers a conditional PUT that races another write of the key
		with 409.  A part PUT answered so, with no object under the key, is
		put again, and the checkpoint goes through."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g;
		""")
		conflicting = ConflictingPut(5)
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: conflicting(response)):
			node.safe_psql("CHECKPOINT;")
		self.assertEqual(5, conflicting.answered)
		node.stop()
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(20000, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_restore_refuses_a_gap_at_a_segment_start(self):
		"""The chunk that a segment starts in missing is a gap too: the
		segment is not assembled from the next chunk on."""
		self.gap_in_the_chunks(lambda keys, ranges, seg_start: next(
		    k for k in keys if ranges[k][0] <= seg_start < ranges[k][1]))

	def gap_in_the_chunks(self, pick):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
		""")
		node.safe_psql("CHECKPOINT;")
		# the commits go to the next segment: the load takes the
		# checkpoint's own, and recovery meets the gap
		node.safe_psql("SELECT pg_switch_wal();")
		for i in range(1, 11):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")
		lsn = node.execute("SELECT pg_current_wal_lsn() - '0/0'")[0][0]
		seg_size = int(node.execute("SHOW wal_segment_size")[0][0][:-2]) * 1024 * 1024
		seg_start = int(lsn) // seg_size * seg_size
		node.stop(['-m', 'immediate'])
		ranges = {}
		for k in self.chunk_keys():
			if k.startswith('wal/00000001/chunks/'):
				head = self.client.get_object(Bucket=self.bucket_name,
				                              Key=k)['Body'].read()[:32]
				_, _, _, start, end = struct.unpack('<IIQQQ', head)
				if end > seg_start:
					ranges[k] = (start, end)
		keys = sorted(ranges, key=lambda k: ranges[k][0])
		self.assertGreater(len(keys), 4)
		self.client.delete_object(Bucket=self.bucket_name,
		                          Key=pick(keys, ranges, seg_start))
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			try:
				new_node.start()
			except StartNodeException:
				pass
			# recovery stops at the gap; it never promotes
			deadline = time.time() + 60
			while True:
				with open(new_node.pg_log_file) as f:
					log = f.read()
				if "could not restore file" in log:
					break
				self.assertLess(time.time(), deadline)
				time.sleep(0.5)
			self.assertNotIn("database system is ready to accept connections",
			                 log)
			try:
				new_node.stop(['-m', 'immediate'])
			except Exception:
				pass
			new_node.cleanup()

	def test_s3_truncate_in_place_numbers_parts_on(self):
		"""TRUNCATE of a table whose trees the same transaction made empties
		them in place, files and maps with them.  The trees go on numbering
		their parts from where they were: a part number handed out again would
		be put over the part of that number, and the GC, which keeps the dead
		blocks of each part by number, would count the new part's blocks as
		the dead ones of the old."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int, v text) USING orioledb;
		""")
		# the first TRUNCATE gives the table a new tree, the second empties
		# that in place (a table with no primary key keeps its tree's relnode)
		con = node.connect()
		con.begin()
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('a', 200)
				FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_etags()
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('b', 200)
				FROM generate_series(1, 20000) g;
		""")
		con.commit()
		con.close()
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CHECKPOINT;")
		after = self.part_etags()
		self.assertGreater(len(after), len(before))
		for key, etag in before.items():
			if key in after:
				self.assertEqual(etag, after[key], key)
		self.assertEqual([(20000, 'b' * 200)],
		                 node.execute("SELECT count(*), min(v) FROM o_test"))
		node.stop()

	def test_s3_gc_removes_parts_a_truncate_in_place_left(self):
		"""The parts a tree had before TRUNCATE emptied it in place are named
		by nothing after: the GC removes them, and keeps the ones after."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int, v text) USING orioledb;
		""")
		con = node.connect()
		con.begin()
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('a', 200)
				FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_etags()
		tree = collections.Counter(k.split('/')[2]
		                           for k in before).most_common(1)[0][0]
		old = {k for k in before if k.split('/')[2] == tree}
		con.execute("""
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, repeat('b', 200)
				FROM generate_series(1, 20000) g;
		""")
		con.commit()
		con.close()
		for _ in range(3):
			node.safe_psql("CHECKPOINT;")
		new = {k for k in self.part_etags()
		       if k.split('/')[2] == tree} - old
		self.assertTrue(new)
		deadline = time.time() + 90
		while time.time() < deadline and old & set(self.part_etags()):
			time.sleep(1)
		self.assertEqual(set(), old & set(self.part_etags()))
		self.assertTrue(new <= set(self.part_etags()))
		self.assertEqual([(20000, 'b' * 200)],
		                 node.execute("SELECT count(*), min(v) FROM o_test"))
		node.stop()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(20000, 'b' * 200)],
			                 new_node.execute("SELECT count(*), min(v) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_restarted_shipper_does_not_rewind(self):
		"""Where recovery ended is where the first shipper of a run starts;
		one started again later in the run starts where the chunks are, not
		back there."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("synchronous_standby_names = 's3'")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.stop(['-m', 'immediate'])
		node.start()
		for i in range(11, 31):
			node.safe_psql(f"INSERT INTO o_test VALUES ({i});")

		def starts():
			with open(node.pg_log_file) as f:
				return re.findall(r"shipping WAL to S3 from ([0-9A-F]+)/([0-9A-F]+)",
				                  f.read())

		first = starts()
		node.safe_psql("""
			SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE backend_type = 'orioledb s3 wal shipper';
		""")
		deadline = time.time() + 60
		while time.time() < deadline and len(starts()) == len(first):
			time.sleep(0.5)
		again = starts()
		self.assertGreater(len(again), len(first))

		def lsn(x):
			return (int(x[0], 16) << 32) | int(x[1], 16)

		self.assertGreater(lsn(again[-1]), lsn(first[-1]))
		node.safe_psql("INSERT INTO o_test VALUES (31);")
		node.stop()

	def test_s3_skewed_writer_clock_does_not_hold_the_lease(self):
		"""A writer whose clock runs ten minutes ahead still gives up its lease
		on the store's clock: once it dies, another instance takes the bucket
		within the lease duration, not ten minutes later."""
		libfaketime = None
		for path in ('/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1',
		             '/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1'):
			if os.path.exists(path):
				libfaketime = path
		if libfaketime is None:
			self.skipTest("libfaketime is not installed")
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_lease_duration = 15")
		os.environ['LD_PRELOAD'] = libfaketime
		os.environ['FAKETIME'] = '+600'
		try:
			node.start()
		finally:
			del os.environ['LD_PRELOAD']
			del os.environ['FAKETIME']
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		# the writer's clock is ahead indeed
		self.assertGreater(
		    float(node.execute("SELECT extract(epoch FROM now())")[0][0]) -
		    time.time(), 500)
		# a crash: the lease is not given back, it runs out
		os.kill(node.pid, signal.SIGKILL)
		time.sleep(2)
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			t0 = time.time()
			new_node.start()
			self.assertLess(time.time() - t0, 50)
			with open(new_node.pg_log_file) as f:
				self.assertIn("waiting for the S3 writer lease", f.read())
			self.assertEqual([(10, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_after_a_takeover_removes_an_aborted_build(self):
		"""A build rolled back between two checkpoints leaves a tree no
		manifest names.  Its writer stops before its GC removes it; the
		manifest carries the drop, and the writer that takes the bucket
		over removes what the build put up."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 0
			orioledb.main_buffers = 8MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, 0, repeat('x', 200) FROM generate_series(1, 30000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_relnodes()
		con = node.connect()
		con.begin()
		con.execute("CREATE INDEX o_test_v ON o_test (v, pad);")
		deadline = time.time() + 60
		while time.time() < deadline and not self.part_relnodes() - before:
			time.sleep(0.5)
		built = self.part_relnodes() - before
		self.assertTrue(built)
		con.rollback()
		con.close()
		node.safe_psql("CHECKPOINT;")
		node.stop()

		def left():
			return [k for k in self.all_keys('trees/') if k.split('/')[2] in built]

		self.assertTrue(left())
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("""
				orioledb.s3_gc_interval = 1
				orioledb.s3_gc_keep_checkpoints = 1
				orioledb.main_buffers = 8MB
			""")
			new_node.start()
			deadline = time.time() + 90
			while time.time() < deadline and left():
				new_node.safe_psql("CHECKPOINT;")
				time.sleep(1)
			self.assertEqual([], left())
			self.assertEqual([(30000, )],
			                 new_node.execute("SELECT count(*) FROM o_test"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_removes_build_parts(self):
		"""A tree build says where its parts end, so once every block of one
		is dead the part goes like any other."""
		node = self.gc_world()
		before = set(self.part_etags())
		node.safe_psql("CREATE INDEX o_test_v ON o_test (v);")
		node.safe_psql("CHECKPOINT;")
		built = set(self.part_etags()) - before
		index_parts = {k for k in built
		               if k.split('/')[2] not in {b.split('/')[2] for b in before}}
		self.assertTrue(index_parts)
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		deadline = time.time() + 90
		while time.time() < deadline and index_parts & set(self.part_etags()):
			time.sleep(1)
		self.assertFalse(index_parts & set(self.part_etags()))
		self.assertEqual([(30000, )],
		                 node.execute("SELECT count(*) FROM o_test WHERE v = 3"))
		node.stop(['-m', 'immediate'])
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("orioledb.main_buffers = 8MB")
			new_node.start()
			self.assertEqual([(30000, )],
			                 new_node.execute("SET enable_seqscan = off; "
			                                  "SELECT count(*) FROM o_test WHERE v = 3"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_gc_goes_on_after_a_takeover(self):
		"""A writer that loads the bucket goes on collecting from where the
		last writer stopped: the dead extents the last one recorded come from
		the bucket."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 0
			orioledb.main_buffers = 8MB
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, 0, repeat('x', 200) FROM generate_series(1, 30000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		first = self.part_etags()
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		self.assertTrue(set(first) <= set(self.part_etags()))
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.append_conf("""
				orioledb.s3_gc_interval = 1
				orioledb.s3_gc_keep_checkpoints = 1
				orioledb.main_buffers = 8MB
				orioledb.s3_lease_duration = 10
			""")
			new_node.start()
			self.assertTrue(self.wait_for_gc(first))
			self.assertEqual([(30000, 90000)],
			                 new_node.execute("SELECT count(*), sum(v) FROM o_test"))
			new_node.stop(['-m', 'immediate'])
			new_node.cleanup()

		with self.initNode(self.getBasePort() + 2, 'tgsc') as last:
			self.loader.download(last.data_dir)
			last.append_conf(port=last.port)
			last.append_conf("orioledb.main_buffers = 8MB")
			last.start()
			self.assertEqual([(30000, 90000)],
			                 last.execute("SELECT count(*), sum(v) FROM o_test"))
			last.stop()
			last.cleanup()

	def test_s3_as_of_restores_an_older_checkpoint(self):
		"""A retained older checkpoint loads with --as-of, and starts only as
		a reader."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_gc_keep_checkpoints = 5")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("INSERT INTO o_test SELECT generate_series(11, 20);")
		node.safe_psql("CHECKPOINT;")
		older = int(self.kv('root')['checkpoint'])
		node.safe_psql("INSERT INTO o_test SELECT generate_series(21, 30);")
		node.safe_psql("CHECKPOINT;")
		self.assertGreater(int(self.kv('root')['checkpoint']), older)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as old:
			self.loader.download(old.data_dir, as_of=older)
			old.append_conf(port=old.port)
			with self.assertRaises(Exception):
				old.start()
			with open(old.pg_log_file) as f:
				self.assertIn("loaded as of an older checkpoint", f.read())
			old.append_conf("""
				orioledb.s3_reader = on
				hot_standby = on
			""")
			open(os.path.join(old.data_dir, "standby.signal"), "w").close()
			old.start()
			deadline = time.time() + 60
			while True:
				try:
					count = old.execute("SELECT count(*) FROM o_test")
					break
				except Exception:
					self.assertLess(time.time(), deadline)
					time.sleep(0.5)
			self.assertEqual([(20, )], count)
			old.stop()
			old.cleanup()
		self.assertEqual([(30, )], node.execute("SELECT count(*) FROM o_test"))
		node.stop()

	def part_etags(self):
		result = {}
		paginator = self.client.get_paginator('list_objects_v2')
		for page in paginator.paginate(Bucket=self.bucket_name,
		                               Prefix='trees/'):
			for obj in page.get("Contents", []):
				if '/parts/' in obj["Key"]:
					result[obj["Key"]] = obj["ETag"]
		return result

	def test_s3_parts_are_written_once(self):
		"""
		A tree's part is uploaded once its stream's checkpoint sealed it and
		never again: updates and later checkpoints write new parts.  A
		checkpoint the bucket holds therefore stays readable whatever the
		instance does after it.
		"""
		node = self.node
		self.s3_small_buffers_conf('1MB')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO o_test SELECT id, repeat('a', 300)
				FROM generate_series(1, 20000) id;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_etags()
		self.assertTrue(before)
		for ch in "bcd":
			node.safe_psql(f"UPDATE o_test SET value = repeat('{ch}', 300) WHERE id % 3 = 0;")
			node.safe_psql("CHECKPOINT;")
		after = self.part_etags()
		self.assertEqual({k: before[k] for k in before},
		                 {k: after.get(k) for k in before})
		self.assertGreater(len(after), len(before))
		self.assertEqual([(20000, 6666)],
		                 node.execute("""
			SELECT count(*), count(*) FILTER (WHERE value = repeat('d', 300))
			FROM o_test;
		"""))
		node.stop()

	def test_s3_a_lost_map_header_puts_no_part_again(self):
		"""A map whose header is lost loads its tree empty and does not say
		how many parts the tree handed out; the tree numbers on past every
		part the bucket has instead of putting one of them again."""
		self.lost_map_header(0)

	def test_s3_a_lost_map_header_lists_every_part(self):
		"""The parts past a lost map header are found by a listing that
		goes on past its first page of 1000 keys."""
		self.lost_map_header(1500)

	def lost_map_header(self, extra):
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		before = self.part_etags()
		tree = self.primary_relnode(node)
		datoid = next(k.split('/')[1] for k in before if k.split('/')[2] == tree)
		node.stop()
		maps = [f for f in os.listdir(f"{node.data_dir}/orioledb_data/{datoid}")
		        if f.startswith(f"{tree}-") and f.endswith('.map')]
		self.assertTrue(maps)
		for f in maps:
			with open(f"{node.data_dir}/orioledb_data/{datoid}/{f}", 'r+b') as m:
				m.write(bytes(44))
		old = max(int(k.split('/')[-1]) for k in before if k.split('/')[2] == tree)
		# parts of the tree the map no longer counts, past a listing page
		for p in range(old + 1, old + 1 + extra):
			self.client.put_object(Bucket=self.bucket_name,
			                       Key=f"trees/{datoid}/{tree}/parts/{p}", Body=b"x")
		placed = {k: v for k, v in self.part_etags().items() if k not in before}
		self.assertEqual(extra, len(placed))
		node.start()
		node.safe_psql("INSERT INTO o_test SELECT g, repeat('y', 200) "
		               "FROM generate_series(1, 20000) g ON CONFLICT DO NOTHING;")
		node.safe_psql("CHECKPOINT;")
		after = self.part_etags()
		self.assertEqual(before, {k: after.get(k) for k in before})
		self.assertEqual(placed, {k: after.get(k) for k in placed})
		new = [int(k.split('/')[-1]) for k in set(after) - set(before) - set(placed)
		       if k.split('/')[2] == tree]
		self.assertTrue(new)
		self.assertGreater(min(new), old + extra)
		node.stop()

	def test_s3_unsealed_parts_request_a_checkpoint(self):
		"""
		Parts stay local until the checkpoint of the stream that wrote them
		seals them, so past orioledb.s3_max_unsealed_size the server asks for
		a checkpoint instead of filling the disk.
		"""
		node = self.node
		self.s3_small_buffers_conf('1MB')
		node.append_conf("""
			orioledb.s3_max_unsealed_size = 1MB
		""")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		before = node.execute(
		    "SELECT root_checkpoint FROM orioledb_s3_status()")[0][0]
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO o_test SELECT id, repeat('a', 300)
				FROM generate_series(1, 40000) id;
		""")
		deadline = time.time() + 60
		while time.time() < deadline:
			with open(node.pg_log_file) as f:
				if re.search("requesting (a|an immediate) checkpoint", f.read()):
					break
			time.sleep(0.5)
		else:
			self.fail("no checkpoint was requested")
		# a checkpoint follows, and seals what it asked for
		deadline = time.time() + 60
		while time.time() < deadline and \
		    node.execute("SELECT root_checkpoint FROM orioledb_s3_status()")[0][0] <= before:
			time.sleep(0.5)
		self.assertGreater(
		    node.execute("SELECT root_checkpoint FROM orioledb_s3_status()")[0][0], before)
		self.assertEqual([(40000, )],
		                 node.execute("SELECT count(*) FROM o_test"))
		deadline = time.time() + 60
		while time.time() < deadline and \
		    node.execute("SELECT unsealed_parts FROM orioledb_s3_status()")[0][0] > 8:
			time.sleep(0.5)
		self.assertLessEqual(
		    node.execute("SELECT unsealed_parts FROM orioledb_s3_status()")[0][0], 8)
		node.stop()

	def load_tables_after_checkpoint(self, tables, rows):
		node = self.node
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("CHECKPOINT")
		for t in range(tables):
			node.safe_psql(f"""
				CREATE TABLE o_test{t} (
					id int PRIMARY KEY,
					value text NOT NULL
				) USING orioledb;
				INSERT INTO o_test{t}
					SELECT id, repeat(md5(id::text), 8)
					FROM generate_series(1, {rows}) id;
			""")

	def assert_tables(self, tables, rows):
		for t in range(tables):
			self.assertEqual(
			    rows,
			    self.node.execute(f"SELECT count(*) FROM o_test{t}")[0][0])

	def test_s3_evicted_data_of_checkpoint_in_progress(self):
		"""
		Data gets uploaded and evicted while the tables it belongs to are
		new.  Once every part of such a file is evicted and its header
		buffer goes to another file, the file must still say its parts are
		in S3, not look like a new file whose parts are all local.  Parts
		are uploaded once a checkpoint seals them: unsealed ones are never
		evicted.
		"""
		node = self.node
		self.s3_small_buffers_conf('20MB')
		node.start()
		self.load_tables_after_checkpoint(8, 40000)
		node.safe_psql("CHECKPOINT")
		for _ in range(60):
			if self.get_data_size() <= 30 * 1024 * 1024:
				break
			time.sleep(1)
		self.assertLessEqual(self.get_data_size(), 30 * 1024 * 1024)
		# Let later eviction cycles hand the header buffers of the evicted
		# files to others
		time.sleep(10)
		self.assert_tables(8, 40000)
		node.stop()

	def test_s3_crash_before_checkpoint_of_new_tables(self):
		"""
		Tables created and filled after the last checkpoint live in data files
		of the next checkpoint number, and recovery writes them again.  Those
		files have to survive until the end-of-recovery checkpoint reads them.
		"""
		node = self.node
		self.s3_small_buffers_conf('1000MB')
		node.start()
		self.load_tables_after_checkpoint(8, 40000)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assert_tables(8, 40000)
		node.stop()

	def test_s3_index_built_right_after_a_rebuild(self):
		"""
		ADD PRIMARY KEY rebuilds the table into files of the current
		checkpoint without loading the tree in shared memory.  A CREATE INDEX
		right after it -- the order pg_restore runs them in -- must still see
		the rows: in S3 mode the data-exists check looked for a file name only
		local mode uses and built an empty index.
		"""
		node = self.node
		self.s3_small_buffers_conf('1000MB')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (id text NOT NULL, val int) USING orioledb;
			INSERT INTO o_test SELECT g::text, g FROM generate_series(1, 1000) g;
		""")
		node.safe_psql("ALTER TABLE o_test ADD PRIMARY KEY (id)")
		node.safe_psql("CREATE INDEX o_test_val ON o_test (val)")
		count_by_index = """
			SET enable_seqscan = off;
			SET enable_bitmapscan = off;
			SELECT count(*) FROM o_test WHERE val > 0;
		"""
		self.assertEqual(1000, node.execute(count_by_index)[0][0])
		self.assertEqual(1000,
		                 node.execute("SELECT count(*) FROM o_test")[0][0])
		node.stop()

	def test_s3_data_dir_load(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.append_conf(f"""
			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		self.wait_for_object('wal/')
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()

	def test_s3_data_dir_load_two_checkpoints(self):
		"""A data directory loaded from a bucket that holds more than one
		checkpoint: files unchanged since an earlier checkpoint are found by
		walking back from the latest one (download_unchanged_small_files),
		which used to iterate the checksum dict without .items() and fail
		before the undo and WAL files were fetched."""
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.append_conf(f"""
			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		# A second checkpoint that leaves o_test_1 untouched: its files stay
		# at checkpoint 1 and the loader has to walk back for them.
		node.safe_psql("""
			CREATE TABLE o_test_2 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_2 SELECT * FROM generate_series(6, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		node.stop(['--no-wait'])

		self.wait_for_object('wal/')
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			self.assertEqual([(6, ), (7, ), (8, ), (9, ), (10, )],
			                 new_node.execute("SELECT * FROM o_test_2"))
			new_node.stop()
			new_node.cleanup()

	def s3_conf(self, path_style=False):
		# path_style: the host alone, the bucket as the prefix's first segment
		location = f"""
			orioledb.s3_host = '{self.host}:{self.port}'
			orioledb.s3_prefix = '{self.bucket_name}'
		""" if path_style else f"""
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
		"""
		return location + f"""
			orioledb.s3_mode = true
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3

			archive_mode = on
			archive_library = 'orioledb'
		"""

	def wait_for_object(self, key, timeout=60):
		# list_objects returns the response, never [], so test for Contents
		deadline = time.time() + timeout
		while "Contents" not in self.client.list_objects(
		    Bucket=self.bucket_name, Prefix=key):
			self.assertLess(time.time(), deadline, f"{key} never arrived")
			time.sleep(0.1)

	def wait_stopped(self, node, timeout=120):
		deadline = time.time() + timeout
		while node.status() == NodeStatus.Running:
			self.assertLess(time.time(), deadline, "the node never stopped")
			time.sleep(0.1)

	def s3_object(self, key):
		return self.client.get_object(Bucket=self.bucket_name,
		                              Key=key)["Body"].read()

	def test_s3_small_file_checksums_list_every_file(self):
		"""Every small file a checkpoint uploads is listed in that checkpoint's
		small_file_checksums.  The entries used to be written out only in full
		batches, so the last ones of each checkpoint were lost: an unchanged
		file that fell among them was neither uploaded with the checkpoint nor
		recorded as unchanged, and a data directory loaded from it lacked the
		file (PG_VERSION, global/*)."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (val_1 int) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		node.safe_psql("CREATE TABLE test_2 (val_1 int) USING heap;")
		node.safe_psql("CHECKPOINT;")
		chkp_nums = sorted({
		    int(x["Key"].split('/')[1].split('-')[1])
		    for x in self.client.list_objects(Bucket=self.bucket_name,
		                                      Prefix='data/').get(
		                                          "Contents", [])
		    if re.match(r'data/\d+-\d+/orioledb_data/small_file_checksums$',
		                x["Key"])
		})
		self.assertGreaterEqual(len(chkp_nums), 2)
		for chkp_num in chkp_nums:
			listed = set(
			    re.findall(
			        r'^FILE: (.+), CHECKSUM: \w+, CHECKPOINT: \d+$',
			        self.s3_object(
			            self.checkpoint_dir(chkp_num) +
			            "orioledb_data/small_file_checksums").
			        decode(), re.MULTILINE))
			bundled = set()
			i = 0
			while True:
				try:
					data = self.s3_object(
					    self.checkpoint_dir(chkp_num) +
					    f"orioledb_data/small_files_{i}")
				except self.client.exceptions.NoSuchKey:
					break
				for j in range(struct.unpack('i', data[0:4])[0]):
					(name_offset, _, _) = struct.unpack(
					    'iii', data[4 + j * 12:16 + j * 12])
					bundled.add(data[name_offset:data.find(b'\0', name_offset
					                                         )].decode())
				i += 1
			self.assertTrue(bundled, f"checkpoint {chkp_num} uploaded no small files")
			self.assertEqual(set(), bundled - listed,
			                 f"checkpoint {chkp_num}")
		node.stop()

	def test_s3_data_dir_load_retained_undo(self):
		"""A checkpoint taken while a transaction holds undo across more than
		one undo file keeps that range, and the loader has to fetch it: the
		row undo files the control file names, under the data directory.  The
		loader read the range from stale control file offsets, asked for
		object names the server no longer writes, left out the last file and
		wrote the rest relative to the current directory."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf(f"""
			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				pad text
			) USING orioledb;
			INSERT INTO o_test SELECT g, repeat('a', 300)
				FROM generate_series(1, 200000) g;
		""")
		# An open writing transaction keeps every undo record written after
		# it in the range the checkpoint retains.
		con = node.connect()
		con.begin()
		con.execute("INSERT INTO o_test VALUES (-1, 'uncommitted');")
		for ch in "bcd":
			node.safe_psql(
			    f"UPDATE o_test SET pad = repeat('{ch}', 300) WHERE id > 0;")
		node.safe_psql("CHECKPOINT;")
		# The checkpoint's REDO segment has to be in the archive.  Not the
		# current one: the checkpoint switched to a new segment already, and
		# that one is archived only by the next checkpoint.
		redo = node.execute(
		    "SELECT redo_wal_file FROM pg_control_checkpoint();")[0][0]
		node.safe_psql("SELECT pg_switch_wal();")
		self.wait_for_object(f'wal/{redo}', timeout=600)
		retained = sorted(
		    f for f in os.listdir(os.path.join(node.data_dir, "orioledb_undo"))
		    if f.endswith("row"))
		self.assertGreater(len(retained), 1)
		# An immediate stop: no shutdown checkpoint to release the range
		node.stop(['-m', 'immediate'])
		con.close()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			loaded = sorted(
			    f for f in os.listdir(
			        os.path.join(new_node.data_dir, "orioledb_undo"))
			    if f.endswith("row"))
			self.assertGreater(len(loaded), 1)
			for f in loaded:
				versions = sorted(
				    (int(x["Key"].rsplit('-', 1)[1]), x["Key"])
				    for x in self.client.list_objects(
				        Bucket=self.bucket_name,
				        Prefix=f"orioledb_undo/{f}/").get("Contents", []))
				self.assertEqual(
				    self.s3_object(versions[-1][1]),
				    open(os.path.join(new_node.data_dir, "orioledb_undo", f),
				         'rb').read())
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(200000, 1)],
			                 new_node.execute(
			                     "SELECT count(*), count(DISTINCT pad) FROM o_test"))
			self.assertEqual([(0, )],
			                 new_node.execute(
			                     "SELECT count(*) FROM o_test WHERE id = -1"))
			self.assertEqual([('d' * 300, )],
			                 new_node.execute(
			                     "SELECT pad FROM o_test WHERE id = 1"))
			new_node.stop()
			new_node.cleanup()

	def load_after_archived_commits(self, checkpoint_only):
		"""Commit three rows after the last checkpoint, each in a WAL segment
		that gets archived, lose the node, and return the rows of a node loaded
		from the bucket.  Each restore runs on its own bucket: a loaded node
		writes its own checkpoints and WAL to the bucket it was loaded from."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf(f"""
			orioledb.recovery_pool_size = 1
			orioledb.recovery_idx_pool_size = 1
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int PRIMARY KEY
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		archived = []
		for i in range(3):
			node.safe_psql(f"INSERT INTO o_test_1 VALUES ({10 + i});")
			archived.append(
			    node.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
			    [0][0])
			node.safe_psql("SELECT pg_switch_wal();")
		for wal_file in archived:
			self.wait_for_object(f'wal/{wal_file}')
		# An immediate stop: no shutdown checkpoint
		node.stop(['-m', 'immediate'])

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir,
			                     checkpoint_only=checkpoint_only)
			new_node.append_conf(port=new_node.port)
			if checkpoint_only:
				# An older state of the bucket: it is read, not written to --
				# writing on from it would give the bucket two histories
				new_node.append_conf("""
					orioledb.s3_reader = on
					hot_standby = on
				""")
				open(os.path.join(new_node.data_dir, "standby.signal"),
				     "w").close()
			new_node.start()
			deadline = time.time() + 60
			while True:
				try:
					rows = new_node.execute("SELECT * FROM o_test_1 ORDER BY 1")
					break
				except Exception:
					if time.time() > deadline:
						raise
					time.sleep(0.5)
			new_node.stop()
			new_node.cleanup()
		return rows

	def test_s3_data_dir_load_replays_archived_wal(self):
		"""Commits made after the last checkpoint are back when their WAL was
		archived: the loader fetches the archived segments that follow the
		checkpoint's REDO segment, and the server replays them."""
		self.assertEqual([(x, ) for x in [1, 2, 3, 4, 5, 10, 11, 12]],
		                 self.load_after_archived_commits(False))

	def test_s3_data_dir_load_checkpoint_only(self):
		"""With --checkpoint-only the loaded node, a reader, holds the state of
		the last checkpoint."""
		self.assertEqual([(x, ) for x in [1, 2, 3, 4, 5]],
		                 self.load_after_archived_commits(True))

	@s3_test_attrs(
	    http=True,
	    prefix=f'{S3BaseTest.bucket_name}/{S3BaseTest.optional_prefix}')
	def test_s3_http_and_prefix(self):
		prefix = f'{self.bucket_name}/{self.optional_prefix}'
		node = self.node
		self.client.put_object(Bucket=self.bucket_name,
		                       Key=self.optional_prefix)

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}'
			orioledb.s3_prefix = '{prefix}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = false

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		wal_prefix = os.path.join(self.optional_prefix, 'wal/')
		self.wait_for_object(wal_prefix)
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}',
	               prefix=S3BaseTest.optional_prefix)
	def test_s3_prefix_no_bucket(self):
		node = self.node
		self.client.put_object(Bucket=self.bucket_name,
		                       Key=self.optional_prefix)

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_prefix = {self.optional_prefix}
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = true

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		wal_prefix = os.path.join(self.optional_prefix, 'wal/')
		self.wait_for_object(wal_prefix)
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(http=True,
	               host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}')
	def test_s3_http_virtual_host(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = false

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		wal_prefix = 'wal/'
		self.wait_for_object(wal_prefix)
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	@s3_test_attrs(host=f'{S3BaseTest.bucket_name}.{S3BaseTest.host}')
	def test_s3_https_virtual_host(self):
		node = self.node

		node.append_conf(
		    'postgresql.conf', f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.bucket_name}.{self.host}:{self.port}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_use_https = true

			archive_mode = on
			archive_library = 'orioledb'
		""")
		node.start()
		archiver_pid = node.execute("""
			SELECT pid FROM pg_stat_activity WHERE backend_type = 'archiver';
		""")[0][0]
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test_1 (
				val_1 int
			) USING orioledb;
			INSERT INTO o_test_1 SELECT * FROM generate_series(1, 5);
		""")
		node.safe_psql("CHECKPOINT;")
		self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
		                 node.execute("SELECT * FROM o_test_1"))
		node.stop(['--no-wait'])

		wal_prefix = 'wal/'
		self.wait_for_object(wal_prefix)
		os.kill(archiver_pid, signal.SIGUSR2)
		self.wait_stopped(node)

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)

			new_node.start()
			self.assertEqual([(1, ), (2, ), (3, ), (4, ), (5, )],
			                 new_node.execute("SELECT * FROM o_test_1"))
			new_node.stop()
			new_node.cleanup()
		node.stop()

	def test_s3_check_control(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			control_filename = os.path.join(node.data_dir, "orioledb_data",
			                                "control")
			shutil.copy(control_filename, f"{control_filename}.copy")

			node.stop()

			# Test that PostgreSQL won't start in case of absent control file
			new_node = self.initNode(self.getBasePort() + 1, 'tgsb')
			new_node.append_conf(f"""
				orioledb.s3_mode = true
				orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
				orioledb.s3_region = '{self.region}'
				orioledb.s3_accesskey = '{self.access_key_id}'
				orioledb.s3_secretkey = '{self.secret_access_key}'
				orioledb.s3_cainfo = '{self.s3_cainfo}'
			""")

			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  OrioleDB can be incompatible with the S3 bucket because the control file exists on the S3 bucket',
				    f.read())

			# Test that PostgreSQL won't start in case of different control identifier
			new_node.append_conf("orioledb.s3_prefix = 'temp_prefix'")
			new_node.start()
			new_node.safe_psql("CHECKPOINT")
			new_node.stop()

			new_node.append_conf("orioledb.s3_prefix = ''")

			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn('differs from the S3 bucket identifier',
				              f.read())

			# Test that PostgreSQL won't start in case of wrong CRC of the control file
			with open(control_filename, "rb+") as f:
				f.write(b'\x11\x11\x11\x11\x11\x11\x11\x11')

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn('FATAL:  Wrong CRC in control file', f.read())

			# Test that PostgreSQL won't start in case of corrupted control file
			with open(control_filename, "w+") as f:
				f.truncate(10)

			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not read data from control file "orioledb_data/control"',
				    f.read())

			# Test that PostgreSQL won't start in case of different checkpoint number
			shutil.copy(f"{control_filename}.copy", control_filename)
			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    "FATAL:  OrioleDB misses new changes and checkpoints from the S3 bucket and they are incompatible with each other",
				    f.read())

			# Test that PostgreSQL won't start in case of lack of permissions
			os.chmod(control_filename, 0)
			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")

			with open(node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  could not open file "orioledb_data/control": Permission denied',
				    f.read())

		# The lease is taken with conditional writes: a refused one on a node
		# whose data directory matches its bucket
		new_node.append_conf("orioledb.s3_prefix = 'temp_prefix'")
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object_conflict):
			with self.assertRaises(StartNodeException) as e:
				new_node.start()

			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  another writer replaced "writer" in the S3 bucket while this instance was starting',
				    f.read())

		# Test unkown error handling
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_moto_unkown_error):
			with self.assertRaises(StartNodeException) as e:
				new_node.start()

			self.assertEqual(e.exception.message, "Cannot start node")

			with open(new_node.pg_log_file) as f:
				self.assertIn('FATAL:  could not put object to S3', f.read())

	def test_s3_token_file(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		token_filename = os.path.join(node.data_dir, "orioledb_data",
		                              "s3_token")
		shutil.copy(token_filename, f"{token_filename}.copy")
		node.stop()

		def refused(message):
			with self.assertRaises(StartNodeException) as e:
				node.start()
			self.assertEqual(e.exception.message, "Cannot start node")
			with open(node.pg_log_file) as f:
				self.assertIn(message, f.read())

		with open(token_filename, "w") as f:
			f.write("0 0\n")
		refused(
		    'FATAL:  invalid S3 writer token in file "orioledb_data/s3_token"')

		with open(token_filename, "w") as f:
			f.truncate(0)
		refused('FATAL:  could not read file "orioledb_data/s3_token"')

		shutil.copy(f"{token_filename}.copy", token_filename)
		os.chmod(token_filename, 0)
		refused('FATAL:  could not open file "orioledb_data/s3_token": '
		        'Permission denied')

		# Without the token file the data directory is not the bucket's writer
		os.remove(token_filename)
		refused('FATAL:  another writer has committed to the S3 bucket since '
		        'this data directory last did')

		shutil.copy(f"{token_filename}.copy", token_filename)
		node.start()
		node.stop()

	def checkpoint_token(self, chkp_num):
		"""The writer token root names for checkpoint chkp_num."""
		body = self.client.get_object(Bucket=self.bucket_name,
		                              Key='root')['Body'].read().decode()
		token = None
		for line in body.splitlines():
			if line.startswith('epoch '):
				_, first, epoch_token = line.split(' ')
				if int(first) <= chkp_num:
					token = epoch_token
		return token

	def checkpoint_dir(self, chkp_num):
		return f"data/{self.checkpoint_token(chkp_num)}-{chkp_num}/"

	def s3_body(self, key):
		body = self.client.get_object(Bucket=self.bucket_name,
		                              Key=key)['Body'].read().decode()
		return dict(line.split(' ', 1) for line in body.splitlines())

	def test_s3_second_writer_is_refused(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_lease_duration = 15")
		node.start()
		node.safe_psql("CHECKPOINT;")
		writer = self.s3_body('writer')
		self.assertEqual(writer['token'], self.s3_body('root')['token'])

		# A copy of the data directory carries the writer's own token: it
		# waits for the lease, sees it renewed, and refuses
		with self.getReplica() as new_node:
			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			self.assertEqual(e.exception.message, "Cannot start node")
			with open(new_node.pg_log_file) as f:
				log = f.read()
			self.assertIn('FATAL:  the S3 bucket is in use by another writer',
			              log)
			self.assertIn('a copy of this data directory is running', log)
			new_node.cleanup()

		# Another instance is refused at once
		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			new_node.append_conf(self.s3_conf())
			with self.assertRaises(StartNodeException) as e:
				new_node.start()
			with open(new_node.pg_log_file) as f:
				self.assertIn(
				    'FATAL:  OrioleDB can be incompatible with the S3 bucket',
				    f.read())
			new_node.cleanup()

		self.assertEqual(node.execute("SELECT 1"), [(1, )])
		node.stop()
		# A clean stop gives the lease back
		self.assertEqual(self.s3_body('writer')['expires_at'], '0')

	def test_s3_restart_takes_a_new_token(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_lease_duration = 15")
		node.start()
		first = int(self.s3_body('writer')['token'])
		node.stop()
		node.start()
		second = int(self.s3_body('writer')['token'])
		self.assertEqual(second, first + 1)
		self.assertEqual(int(self.s3_body('root')['token']), second)
		# A crash leaves the lease to run out; the restart waits for it
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(int(self.s3_body('writer')['token']), second + 1)
		node.stop()

	def test_s3_root_names_the_last_checkpoint(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 100);
		""")
		previous = int(self.s3_body('root')['checkpoint'])
		for _ in range(3):
			node.safe_psql("CHECKPOINT;")
			root = self.s3_body('root')
			self.assertEqual(int(root['checkpoint']), previous + 1)
			previous = int(root['checkpoint'])
			# everything the checkpoint needs is up before root names it
			self.client.head_object(
			    Bucket=self.bucket_name,
			    Key=self.checkpoint_dir(previous) + "orioledb_data/control")
			self.assertEqual(root['token'], self.s3_body('writer')['token'])
		node.stop()

	def test_s3_lost_lease_stops_the_instance(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("orioledb.s3_lease_duration = 15")
		node.start()
		writer = self.s3_body('writer')
		# Another writer takes the bucket over
		self.client.put_object(
		    Bucket=self.bucket_name,
		    Key='writer',
		    Body=(f"format 1\nbucket {writer['bucket']}\n"
		          f"token {int(writer['token']) + 1}\nattempt 1\n"
		          f"owner other\nexpires_at 9999999999\n").encode())
		deadline = time.time() + 30
		while node.status() == NodeStatus.Running and time.time() < deadline:
			time.sleep(0.5)
		self.assertNotEqual(node.status(), NodeStatus.Running)
		with open(node.pg_log_file) as f:
			self.assertIn('lost the S3 writer lease, shutting down', f.read())

	def test_s3_control_consistency(self):
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_lease_duration = 15
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])
			objects = sorted(list(x["Key"] for x in objects))
			self.assertIn('writer', objects)
			self.assertIn('root', objects)
			token = int(self.s3_body('writer')['token'])

			with self.getReplica() as new_node:
				node.stop(["-m", "immediate"])

				# The copy waits for the dead writer's lease, then takes over
				new_node.start()
				self.assertEqual(int(self.s3_body('writer')['token']),
				                 token + 1)

				new_node.stop()
				new_node.cleanup()

	def test_s3_control_inconsistency(self):
		node = self.node
		node.append_conf(f"""
			orioledb.s3_mode = true
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'

			orioledb.s3_num_workers = 3
			orioledb.recovery_pool_size = 1
		""")

		# Patch put_object() to test "If-None-Match"
		with patch("moto.s3.responses.S3Response.put_object",
		           new=mock_put_object):
			node.start()
			node.safe_psql("CHECKPOINT;")

			with self.getReplica() as new_node:

				node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
				node.safe_psql("""
					CREATE TABLE o_test (
						val_1 int
					) USING orioledb;
					INSERT INTO o_test SELECT * FROM generate_series(1, 5);
				""")
				node.stop()

				with self.assertRaises(StartNodeException) as e:
					new_node.start()
				self.assertEqual(e.exception.message, "Cannot start node")
				with open(new_node.pg_log_file) as f:
					log = f.readlines()
				message = log[0].split('] ')[-1].strip()
				self.assertEqual(
				    message,
				    "FATAL:  OrioleDB misses new changes and checkpoints from the S3 bucket and they are incompatible with each other"
				)

				new_node.cleanup()
