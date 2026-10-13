import os
import re
import signal
import threading
import time
from threading import Thread
from unittest.mock import patch

import werkzeug.serving
from moto.s3.responses import S3Response
from testgres.enums import NodeStatus
from testgres.exceptions import StartNodeException

from .s3_base_test import S3BaseTest, HoldWrites, FlakyPutObject, \
 orig_list_objects_v2

orig_head_object = S3Response.head_object


class S3LeaseTest(S3BaseTest):
	"""The writer lease, root and its copies, and the fences around them."""

	def s3_conf(self):
		return f"""
			orioledb.s3_host = '{self.host}:{self.port}/{self.bucket_name}'
			orioledb.s3_mode = true
			orioledb.s3_region = '{self.region}'
			orioledb.s3_accesskey = '{self.access_key_id}'
			orioledb.s3_secretkey = '{self.secret_access_key}'
			orioledb.s3_cainfo = '{self.s3_cainfo}'
			orioledb.s3_num_workers = 3
			orioledb.s3_lease_duration = 15

			archive_mode = on
			archive_library = 'orioledb'
		"""

	def s3_object(self, key):
		return self.client.get_object(Bucket=self.bucket_name,
		                              Key=key)["Body"].read()

	def kv(self, key):
		result = {}
		for line in self.s3_object(key).decode().splitlines():
			k, _, v = line.partition(' ')
			result.setdefault(k, v)
		return result

	def keys(self, prefix):
		result = []
		paginator = self.client.get_paginator('list_objects_v2')
		for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
			result += [x["Key"] for x in page.get("Contents", [])]
		return sorted(result)

	def chunk_keys(self, tli=1):
		"""The WAL chunks of a timeline, the terminators (every start puts
		one, and it stays) left out."""
		keys = self.keys(f'wal/{tli:08X}/')
		marks = {k.replace('/terminators/', '/chunks/') for k in keys
		         if '/terminators/' in k}
		return sorted(k for k in keys if '/chunks/' in k and k not in marks)

	def log(self, node):
		with open(node.pg_log_file) as f:
			return f.read()

	def wait_stopped(self, node, timeout):
		deadline = time.time() + timeout
		while node.status() == NodeStatus.Running:
			if time.time() >= deadline:
				return False
			time.sleep(0.2)
		return True

	def wait_logged(self, node, text, timeout):
		deadline = time.time() + timeout
		while text not in self.log(node):
			if time.time() >= deadline:
				return False
			time.sleep(0.2)
		return True

	def child_pid(self, node, name):
		for line in os.popen(
		    f"ps -o pid=,args= --ppid {node.pid}").read().splitlines():
			if name in line:
				return int(line.split()[0])
		self.fail(f"{node.name} has no child {name!r}")

	def kill_quietly(self, node):
		if node.status() == NodeStatus.Running:
			try:
				node.stop(['-m', 'immediate'])
			except Exception:
				pass

	def checkpoint_in_background(self, node):
		"""A CHECKPOINT the test cuts short (the node dies, or the request
		holds): its connection error is the expected end."""

		def run():
			try:
				node.execute("CHECKPOINT;")
			except Exception:
				pass

		thread = Thread(target=run)
		thread.daemon = True
		thread.start()
		return thread

	def root_copies(self):
		"""(token, checkpoint, key, body) of every copy under roots/, a
		copy from before the token was in the key as token 0."""
		copies = []
		for key in self.keys('roots/'):
			m = re.fullmatch(r'roots/(?:(\d+)-)?(\d+)', key)
			if m:
				copies.append((int(m.group(1) or 0), int(m.group(2)), key,
				               self.kv(key)))
		return copies

	def test_s3_fenced_put_leaves_a_newer_writers_object(self):
		"""A fenced PUT finds its key holding other bytes of a smaller token
		(a writer this one fenced off) and replaces them.  Between its HEAD
		and the replacement a newer writer wrote the key: the replacement
		goes under If-Match on the ETag the HEAD saw, fails, and the newer
		token read next stops this instance.  An unconditional delete took
		the newer writer's object away with the old one, and the fenced
		writer went on."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.stop()
		node.start()  # token 2: a token of 1 is one it fenced off
		self.assertEqual('2', self.kv('writer')['token'])
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		segment = node.execute(
		    "SELECT pg_walfile_name(pg_current_wal_lsn())")[0][0]
		key = f'wal/{segment}'
		self.client.put_object(Bucket=self.bucket_name, Key=key,
		                       Body=b'left by a fenced writer',
		                       Metadata={'token': '1'})
		newer = b'written by a newer writer'
		planted = threading.Event()
		test = self

		def head_then_overwrite(response, bucket_name, query, key_name,
		                        headers):
			result = orig_head_object(response, bucket_name, query, key_name,
			                          headers)
			if key_name.endswith(segment) and not planted.is_set():
				planted.set()
				test.client.put_object(Bucket=test.bucket_name, Key=key,
				                       Body=newer, Metadata={'token': '3'})
			return result

		try:
			with patch('moto.s3.responses.S3Response.head_object',
			           new=head_then_overwrite):
				node.safe_psql("SELECT pg_switch_wal();")
				self.assertTrue(planted.wait(60), "the archive never asked")
				stopped = self.wait_stopped(node, 60)
			self.assertTrue(stopped, "the fenced writer went on")
			self.assertIn('lost the S3 writer lease', self.log(node))
			head = self.client.head_object(Bucket=self.bucket_name, Key=key)
			self.assertEqual('3', head['Metadata']['token'])
			self.assertEqual(newer, self.s3_object(key))
		finally:
			self.kill_quietly(node)

	def test_s3_stop_outlasting_the_lease_keeps_it(self):
		"""The lease worker renews through the shutdown checkpoint: a stop
		whose checkpoint takes longer than the lease does not let a loaded
		copy take the bucket over meanwhile.  A regular background worker
		was stopped before the checkpoint, and the lease ran out in it."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		token = self.kv('writer')['token']
		hold = HoldWrites(r'/data/\d+-\d+/')
		with self.initNode(self.getBasePort() + 1, 'tgsb') as other:
			self.loader.download(other.data_dir)
			other.append_conf(port=other.port)
			self.s3_server.before_write = hold
			stopper = Thread(target=lambda: node.stop(['-t', '300']))
			stopper.daemon = True
			stopper.start()
			try:
				self.assertTrue(hold.entered.wait(90),
				                "the stop checkpoint put no file")
				# the copy waits for the lease to run out, and sees it renewed
				with self.assertRaises(StartNodeException):
					other.start()
				self.assertIn('a copy of this data directory is running',
				              self.log(other))
			finally:
				hold.release.set()
				self.s3_server.before_write = None
				stopper.join(300)
				self.kill_quietly(other)
			self.assertNotEqual(NodeStatus.Running, node.status())
			self.assertEqual(token, self.kv('writer')['token'])
			self.assertEqual('0', self.kv('writer')['expires_at'])
			self.assertNotIn('lost the S3 writer lease', self.log(node))
			other.cleanup()

	def test_s3_stop_ends_when_the_lease_is_lost_in_it(self):
		"""The lease worker is stuck through a stop: the stop reaches its
		checkpoint all the same (a system worker is not waited for), the
		lease lapses in it and a loaded copy takes the bucket over.  The
		stop checkpoint, shipping the WAL the shipper left, checks the lease
		before it puts and stops the instance with the lease lost.  Before,
		a stuck lease worker held the stop before its checkpoint, and a stop
		whose lease lapsed shipped into the new writer's bucket until it met
		the terminator."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 10);
		""")
		node.safe_psql("CHECKPOINT;")
		token = int(self.kv('writer')['token'])
		lease = self.child_pid(node, 'orioledb s3 lease')
		hold = HoldWrites(r'/data/\d+-\d+/')
		with self.initNode(self.getBasePort() + 1, 'tgsb') as other:
			self.loader.download(other.data_dir)
			other.append_conf(port=other.port)
			os.kill(lease, signal.SIGSTOP)
			self.s3_server.before_write = hold
			stopper = Thread(target=lambda: node.stop(['-t', '300']))
			stopper.daemon = True
			stopper.start()
			try:
				self.assertTrue(hold.entered.wait(90),
				                "the stop did not reach its checkpoint")
				other.start()
				self.assertEqual(token + 1, int(self.kv('writer')['token']))
				hold.release.set()
				self.s3_server.before_write = None
				self.assertTrue(
				    self.wait_logged(node, 'lost the S3 writer lease', 90),
				    "the stop went on after the lease was lost")
			finally:
				hold.release.set()
				self.s3_server.before_write = None
				try:
					os.kill(lease, signal.SIGCONT)
				except ProcessLookupError:
					pass
			self.assertTrue(self.wait_stopped(node, 60), "the stop never ended")
			stopper.join(60)
			# the checkpointer saw the lease lost, before it put a chunk
			log = self.log(node)
			checkpointer = re.search(r'\[(\d+)\] LOG:  checkpoint starting: shutdown',
			                         log).group(1)
			self.assertIn(f'[{checkpointer}] LOG:  lost the S3 writer lease', log)
			self.assertNotIn('belongs to another writer', log)
			self.assertEqual(token + 1, int(self.kv('writer')['token']))
			self.assertEqual([(10, )],
			                 other.execute("SELECT count(*) FROM o_test"))
			other.stop()
			other.cleanup()

	def test_s3_repair_root_takes_the_newest_writers_copy(self):
		"""A writer's copy of root lands late, after the writer that took
		the bucket over left its own under the same checkpoint number.  The
		copies are keyed by token too, so neither replaces the other, and
		--repair-root takes the newest token's: the old writer's would let
		the fenced writer in again."""
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
		hold = HoldWrites(r'/roots/')
		self.s3_server.before_write = hold
		checkpoint = self.checkpoint_in_background(node)
		try:
			self.assertTrue(hold.entered.wait(60), "no copy of root was put")
			# root names the checkpoint; its copy is on the way; the writer dies
			node.stop(['-m', 'immediate'])
			checkpoint.join(30)
			chkp = int(self.kv('root')['checkpoint'])
			token_a = int(self.kv('root')['token'])
			with self.initNode(self.getBasePort() + 1, 'tgsb') as other:
				self.loader.download(other.data_dir)
				other.append_conf(port=other.port)
				other.start()
				token_b = int(self.kv('root')['token'])
				self.assertEqual(token_a + 1, token_b)
				# the old writer's copy lands now
				hold.release.set()
				self.s3_server.before_write = None
				deadline = time.time() + 60
				while not any(c[1] == chkp and int(c[3]['token']) == token_a
				              for c in self.root_copies()):
					self.assertLess(time.time(), deadline,
					                "the late copy never landed")
					time.sleep(0.2)
				other.stop(['-m', 'immediate'])
				# the copies after the takeover's are gone, and root is damaged
				for _, k, key, _ in self.root_copies():
					if k > chkp:
						self.client.delete_object(Bucket=self.bucket_name,
						                          Key=key)
				self.client.put_object(Bucket=self.bucket_name, Key='root',
				                       Body=b'\x00garbage')
				self.loader.download(None, repair_root=True)
				self.assertEqual(token_b, int(self.kv('root')['token']))
				self.assertEqual(chkp, int(self.kv('root')['checkpoint']))
				# the new writer goes on from it: the local state is ahead
				other.start()
				self.assertEqual([(20, )],
				                 other.execute("SELECT count(*) FROM o_test"))
				other.stop()
				other.cleanup()
		finally:
			hold.release.set()
			self.s3_server.before_write = None
			self.kill_quietly(node)

	def test_s3_start_refuses_a_store_with_no_usable_date(self):
		"""The lease's expiry is judged on the store's clock, read from the
		Date header: a store whose answers carry none that reads is refused
		at start, instead of being judged on the local clock."""
		node = self.node
		node.append_conf(self.s3_conf())
		with patch.object(werkzeug.serving.WSGIRequestHandler,
		                  'date_time_string',
		                  new=lambda self, timestamp=None: 'some day'):
			with self.assertRaises(StartNodeException):
				node.start()
		self.assertIn('no usable Date', self.log(node))
		node.start()
		self.assertEqual([(1, )], node.execute("SELECT 1"))
		node.stop()

	def test_s3_status_answers_while_the_store_stalls(self):
		"""The lease's renewal is a request to the store; while it waits,
		orioledb_s3_status() reads the shared state without waiting for
		it.  The state was locked across the request, and every reader of
		it stood behind a stalled store."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		hold = HoldWrites(r'/writer$', seconds=6)
		self.s3_server.before_write = hold
		try:
			self.assertTrue(hold.entered.wait(30), "no renewal came")
			started = time.time()
			row = node.execute("SELECT token, lease_remaining FROM orioledb_s3_status()")
			elapsed = time.time() - started
			self.assertEqual(int(self.kv('writer')['token']), row[0][0])
			self.assertLess(elapsed, 1.0)
		finally:
			self.s3_server.before_write = None
		time.sleep(7)
		self.assertNotIn('lost the S3 writer lease', self.log(node))
		node.stop()
