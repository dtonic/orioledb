import logging
import os
import re
import threading
import time
from threading import Thread
from typing import Any
from unittest.mock import patch

from testgres.exceptions import QueryException
from moto.s3.exceptions import S3ClientError, MissingKey, InvalidRequest

from .s3_base_test import S3BaseTest, orig_get_object, orig_put_object, \
 AccessDenied

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class RequestTimeout(S3ClientError):
	"""AWS's 400 for a body that did not arrive in time."""
	code = 400

	def __init__(self, **kwargs: Any):
		super().__init__(
		    "RequestTimeout",
		    "Your socket connection to the server was not read from or "
		    "written to within the timeout period.", **kwargs)


class S3DatapathTest(S3BaseTest):
	"""The data path: part downloads, the request layer, the queue and
	eviction."""

	def conf(self, desired_size='16MB', extra=''):
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
			orioledb.s3_gc_interval = 0
			checkpoint_timeout = 1h
			max_wal_size = 4GB
			{extra}
		""")

	def incompressible_table(self, name, rows):
		self.node.safe_psql(f"""
			CREATE TABLE {name} (
				id int PRIMARY KEY,
				value text NOT NULL
			) USING orioledb;
			INSERT INTO {name} SELECT id,
				(SELECT string_agg(md5(id::text || g::text), '')
				 FROM generate_series(1, 30) g)
				FROM generate_series(1, {rows}) id;
		""")

	def get_file_occupied_size(self, path):
		try:
			result = 0
			zero = b'\0' * 8192
			with open(path, "rb") as f:
				data = f.read(8192)
				while len(data) > 0:
					if data != zero:
						result += len(data)
					data = f.read(8192)
			return result
		except:  # a file removed meanwhile
			return 0

	def get_data_size(self):
		total = 0
		for dirpath, _, filenames in os.walk(
		    f"{self.node.data_dir}/orioledb_data"):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				if not os.path.islink(fp):
					total += self.get_file_occupied_size(fp)
		return total

	def wait_evicted(self, below_mb, timeout=120):
		deadline = time.time() + timeout
		while self.get_data_size() > below_mb * 1024 * 1024:
			self.assertLess(time.time(), deadline, "the data was not evicted")
			time.sleep(0.5)

	def evicted_table(self, rows=40000, below_mb=16):
		"""A table larger than the local bound, checkpointed and evicted."""
		self.node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb")
		self.incompressible_table('o_test', rows)
		self.node.safe_psql("CHECKPOINT")
		self.wait_evicted(below_mb)
		time.sleep(1)

	def part_keys(self):
		keys = []
		paginator = self.client.get_paginator('list_objects_v2')
		for page in paginator.paginate(Bucket=self.bucket_name,
		                               Prefix='trees/'):
			keys += [
			    o["Key"] for o in page.get("Contents", [])
			    if '/parts/' in o["Key"]
			]
		return keys

	def server_log(self):
		with open(self.node.pg_log_file) as f:
			return f.read()

	def lookup_error(self, timeout='60s'):
		"""The error a lookup of evicted parts ends in, and how long it took.
		Five keys spread over the table: the eviction spares a file's last
		part most cycles, so one key could find its part still here."""
		started = time.time()
		with self.assertRaises(QueryException) as e:
			self.node.safe_psql(f"SET statement_timeout = '{timeout}'; "
			                    f"SELECT length(value) FROM o_test "
			                    f"WHERE id IN (2000, 10000, 20000, 30000, 38000)")
		return e.exception.message, time.time() - started

	def test_s3_a_part_missing_from_the_bucket_is_an_error(self):
		"""
		A part the bucket does not hold hung the query: the backend's own
		GET answered 404, the part went to the S3 workers, and the worker
		died on the 404, restarted and died on the same task for ever while
		the backend waited for the part's status with no message.  The
		backend now reports the part, naming the object, and the server
		goes on.
		"""
		node = self.node
		self.conf('8MB')
		node.start()
		self.evicted_table()
		saved = {}
		for key in self.part_keys():
			saved[key] = self.client.get_object(Bucket=self.bucket_name,
			                                    Key=key)["Body"].read()
		keys = list(saved)
		for i in range(0, len(keys), 1000):
			self.client.delete_objects(
			    Bucket=self.bucket_name,
			    Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
		workers_started = self.server_log().count("s3 worker") \
		    - self.server_log().count("is shut down")

		message, elapsed = self.lookup_error()
		self.assertIn("is not in the S3 bucket", message)
		self.assertIn("trees/", message)
		self.assertLess(elapsed, 20, f"the error took {elapsed:.0f} s")

		# the server is healthy: the parts put back are read, and no S3
		# worker died on the way
		for key, body in saved.items():
			self.client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
		self.assertEqual([(960, )] * 5,
		                 node.execute("SELECT length(value) FROM o_test "
		                              "WHERE id IN (2000, 10000, 20000, "
		                              "30000, 38000)"))
		log = self.server_log()
		self.assertEqual(
		    workers_started,
		    log.count("s3 worker") - log.count("is shut down"))
		self.assertNotIn("TRAP", log)
		self.assertNotIn("PANIC", log)
		node.stop()

	def test_s3_a_part_the_store_refuses_is_an_error(self):
		"""
		A part the store answers with a definitive 4xx other than 404 is
		left to the S3 workers, and the worker that cannot download it
		marks the part failed instead of dying on it: the backend waiting
		for the part gets an error, and the next reader tries the download
		again.
		"""
		node = self.node
		self.conf('8MB')
		node.start()
		self.evicted_table()

		def refusing_get(response):
			if '/parts/' in response.parse_key_name():
				raise InvalidRequest("GET")
			return orig_get_object(response)

		with patch('moto.s3.responses.S3Response.get_object',
		           new=refusing_get):
			message, elapsed = self.lookup_error()
		self.assertIn("could not read part", message)
		self.assertIn("from the S3 bucket", message)
		self.assertLess(elapsed, 20, f"the error took {elapsed:.0f} s")
		# the store gives the part again: the part is read
		self.assertEqual([(960, )] * 5,
		                 node.execute("SELECT length(value) FROM o_test "
		                              "WHERE id IN (2000, 10000, 20000, "
		                              "30000, 38000)"))
		self.assertNotIn("could not get object from S3", self.server_log())
		node.stop()

	def test_s3_a_part_refused_past_the_retry_time_is_an_error(self):
		"""
		With no auth retry time a 403 is the answer: a backend whose own
		GET of a part is refused reports it, instead of handing the part to
		a worker that dies on the same refusal once a second.
		"""
		node = self.node
		self.conf('8MB', "orioledb.s3_auth_retry_time = 0")
		node.start()
		self.evicted_table()

		def refusing_get(response):
			if '/parts/' in response.parse_key_name():
				raise AccessDenied()
			return orig_get_object(response)

		with patch('moto.s3.responses.S3Response.get_object',
		           new=refusing_get):
			message, elapsed = self.lookup_error()
		self.assertIn("refused part", message)
		self.assertIn("trees/", message)
		self.assertLess(elapsed, 20, f"the error took {elapsed:.0f} s")
		self.assertEqual([(960, )] * 5,
		                 node.execute("SELECT length(value) FROM o_test "
		                              "WHERE id IN (2000, 10000, 20000, "
		                              "30000, 38000)"))
		node.stop()

	def test_s3_a_stalled_get_is_given_up_and_retried(self):
		"""
		A store that accepts a request and never answers held the request
		for ever: no transfer timeout was set unless the caller had a
		deadline.  A transfer that makes no progress for a minute is given
		up and retried like any transport failure.
		"""
		node = self.node
		self.conf('8MB')
		node.start()
		self.evicted_table()

		release = threading.Event()
		armed = [True]

		def stalling_get(response):
			if armed[0] and '/parts/' in response.parse_key_name():
				armed[0] = False
				release.wait(400)
			return orig_get_object(response)

		try:
			with patch('moto.s3.responses.S3Response.get_object',
			           new=stalling_get):
				started = time.time()
				node.safe_psql("SET statement_timeout = '240s'; "
				               "SELECT length(value) FROM o_test "
				               "WHERE id IN (2000, 10000, 20000, 30000, 38000)")
				elapsed = time.time() - started
		finally:
			release.set()
		self.assertGreaterEqual(elapsed, 50)
		self.assertLess(elapsed, 150, f"the stalled read took {elapsed:.0f} s")
		log = self.server_log()
		self.assertIn("failed, retrying", log)
		self.assertIn("return code = 28", log)  # CURLE_OPERATION_TIMEDOUT
		node.stop()

	def test_s3_a_request_timeout_is_retried(self):
		"""
		AWS answers 400 RequestTimeout to a PUT whose body did not arrive
		in time, as it answers BadDigest to one damaged on the way: the
		body is sent again rather than the worker dying on it.
		"""
		node = self.node
		self.conf('1000MB')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test SELECT generate_series(1, 1000);
		""")
		failed = [0]

		def timing_out_put(response):
			if '/parts/' in response.parse_key_name() and failed[0] < 3:
				failed[0] += 1
				raise RequestTimeout()
			return orig_put_object(response)

		with patch('moto.s3.responses.S3Response.put_object',
		           new=timing_out_put):
			node.safe_psql("CHECKPOINT")
		self.assertEqual(3, failed[0])
		log = self.server_log()
		self.assertIn("failed, retrying", log)
		self.assertNotIn("could not put object to S3", log)
		node.stop()

	def test_s3_a_backend_takes_rotated_keys_while_it_is_refused(self):
		"""
		A client backend whose request is refused retries it for
		orioledb.s3_auth_retry_time, but read the configuration only
		between commands: new keys reloaded meanwhile did not reach it,
		and it waited the whole retry time with the revoked key.  It takes
		the reload while it waits, as the background workers do.
		"""
		node = self.node
		self.conf('8MB', "orioledb.s3_auth_retry_time = 120")
		node.start()
		self.evicted_table()
		refused = [0]

		def revoked_get(response):
			if '/parts/' in response.parse_key_name() and \
			    f"Credential={self.access_key_id}/" in \
			    response.headers.get("Authorization", ""):
				refused[0] += 1
				raise AccessDenied()
			return orig_get_object(response)

		with patch('moto.s3.responses.S3Response.get_object',
		           new=revoked_get):
			done = []
			read = Thread(target=lambda: done.append(
			    node.safe_psql("SET statement_timeout = '150s'; "
			                   "SELECT length(value) FROM o_test "
			                   "WHERE id IN (2000, 10000, 20000, 30000, 38000)")))
			read.start()
			deadline = time.time() + 60
			while refused[0] < 2 and time.time() < deadline:
				time.sleep(0.2)
			self.assertGreaterEqual(refused[0], 2)
			self.assertFalse(done)
			node.append_conf(f"orioledb.s3_accesskey = '{self.access_key_id}NEW'")
			node.reload()
			started = time.time()
			read.join(40)
			elapsed = time.time() - started
			self.assertFalse(read.is_alive(),
			                 "the backend went on with the revoked key")
			self.assertTrue(done)
			self.assertLess(elapsed, 30,
			                f"the new key took {elapsed:.0f} s to reach the "
			                f"backend")
		node.stop()

	def test_s3_hints_past_the_tree_do_not_wait_for_a_refused_key(self):
		"""
		A read-ahead hint names parts past the tree's end, which the store
		does not have.  A store that answers those with 403 (a bucket
		policy, a listing denied) put every S3 worker into the auth retry
		window for a hint nobody waits for, and the checkpoint's uploads
		waited behind them.  A hint is best effort: a refusal is a miss.
		"""
		node = self.node
		self.conf("4MB", "orioledb.s3_auth_retry_time = 90")
		node.start()
		# nearly everything evicted, and nothing of the tree in memory: the
		# end of the key space was written last and would still be there
		self.evicted_table(rows=100000, below_mb=8)
		node.stop()
		node.start()
		refused = [0]

		def refusing_missing_get(response):
			try:
				return orig_get_object(response)
			except MissingKey:
				if '/parts/' in response.parse_key_name():
					refused[0] += 1
					raise AccessDenied()
				raise

		checkpoint = None
		try:
			with patch('moto.s3.responses.S3Response.get_object',
			           new=refusing_missing_get):
				with node.connect() as con:
					con.execute("SET enable_seqscan = off")
					con.execute("SET enable_bitmapscan = off")
					# a range scan a few parts before the end of the key
					# space (the last, part-full part is the one the eviction
					# spares) reads ahead past the tree's last part
					self.assertEqual([(3001, )],
					                 con.execute("SELECT count(*) FROM o_test "
					                             "WHERE id BETWEEN 94000 "
					                             "AND 97000"))
				deadline = time.time() + 30
				while refused[0] < 3 and time.time() < deadline:
					time.sleep(0.2)
				self.assertGreaterEqual(refused[0], 3)
				time.sleep(2)
				# the workers are free for the checkpoint's uploads
				node.safe_psql("UPDATE o_test SET value = upper(value) "
				               "WHERE id <= 3000")
				checkpoint = Thread(target=lambda: node.safe_psql("CHECKPOINT"))
				checkpoint.start()
				checkpoint.join(45)
				self.assertFalse(checkpoint.is_alive(),
				                 "the checkpoint waited for the workers")
				self.assertLess(refused[0], 40,
				                f"refused hints were retried {refused[0]} times")
		finally:
			if checkpoint is not None and checkpoint.is_alive():
				node.stop(['-m', 'immediate'])
				checkpoint.join(30)
		node.stop()

	def test_s3_queue_wraps_around_under_read_ahead(self):
		"""
		The task queue is a ring: a producer blocked on a full ring wrote
		its task where the erased task had been, and the erase, zeroing
		the slot after it had advanced the erased location, could zero the
		new task's length: a length-0 task at the head stalled the lane.
		A small ring under a scan's read-ahead hints, with slow downloads,
		wraps the ring many times; everything queued must get done.  The
		race itself is a narrow window: this is a stress test, not a
		deterministic reproduction.
		"""
		node = self.node
		self.conf('16MB', "orioledb.s3_queue_size = 128kB")
		node.start()
		self.evicted_table(rows=100000, below_mb=32)

		def slow_get(response):
			if '/parts/' in response.parse_key_name():
				time.sleep(0.03)
			return orig_get_object(response)

		checkpoint = None
		try:
			with patch('moto.s3.responses.S3Response.get_object',
			           new=slow_get):
				for _ in range(3):
					node.safe_psql("SET statement_timeout = '300s'; "
					               "SET enable_indexscan = off; "
					               "SELECT count(*) FROM o_test")
					self.wait_evicted(32, timeout=180)
			node.safe_psql("UPDATE o_test SET value = upper(value) "
			               "WHERE id <= 3000")
			checkpoint = Thread(target=lambda: node.safe_psql("CHECKPOINT"))
			checkpoint.start()
			checkpoint.join(90)
			self.assertFalse(checkpoint.is_alive(), "the queue stalled")
		finally:
			if checkpoint is not None and checkpoint.is_alive():
				node.stop(['-m', 'immediate'])
				checkpoint.join(30)
		self.assertEqual([(100000, )],
		                 node.execute("SELECT count(*) FROM o_test"))
		node.stop()

	def eviction_cycles(self):
		"""(files measured, header loads, parts evicted) per eviction cycle,
		from the DEBUG1 line each cycle logs."""
		cycles = []
		for m in re.finditer(
		    r"S3 eviction cycle: (\d+) files measured, (\d+) header "
		    r"loads, (\d+) parts evicted", self.server_log()):
			cycles.append(tuple(int(g) for g in m.groups()))
		return cycles

	def test_s3_eviction_cycles_read_each_header_once(self):
		"""
		With many small trees an eviction cycle that could evict nothing
		(every part unsealed) ran three passes over every file's header,
		reloading each into the small header cache three times, and
		stat()ed every file, once a second.  A file's size on disk is kept
		between cycles, and the second cold pass and the clock sweep run
		only over files the first pass found something for.  The trees
		hold a full part each: a file whose only part is a small tail is
		passed over by the fill draw before its header is read.
		"""
		node = self.node
		self.conf('2MB', "log_min_messages = debug1")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb")
		for i in range(400):
			self.incompressible_table(f"t{i}", 1100)
		files = len([
		    f for _, _, fs in os.walk(f"{node.data_dir}/orioledb_data")
		    for f in fs if re.fullmatch(r"\d+(\.\d+)?", f)
		])
		self.assertGreater(files, 400)
		# nothing is sealed: cycles run and evict nothing
		before = len(self.eviction_cycles())
		deadline = time.time() + 60
		while len(self.eviction_cycles()) < before + 6:
			self.assertLess(time.time(), deadline, "no eviction cycles ran")
			time.sleep(1)
		cycles = self.eviction_cycles()[before + 1:before + 6]
		for measured, loads, evicted in cycles:
			self.assertLessEqual(
			    loads, files * 1.2,
			    f"a cycle over {files} files loaded {loads} headers")
		self.assertLessEqual(
		    sum(c[0] for c in cycles), files * 1.2,
		    f"five cycles over {files} files measured "
		    f"{[c[0] for c in cycles]}")
		node.stop()

	def test_s3_a_part_full_last_part_is_evicted_by_its_fill(self):
		"""
		A file's last, part-full part is evicted with the probability of
		its fill (an empty tail frees little for a GET later); a cycle
		that skips it also leaves its usage count unhalved.  The draw was
		a signed int32 compared with the fill as an unsigned number: a
		negative draw counted as larger than any fill, so a part 88% full
		was skipped 56% of the time instead of 12%.  Two hundred trees
		whose one part is 88% full start at the same usage count; with
		the bug the clock sweep halves each of them in less than half of
		its cycles, and most are not cold after twice the cycles the fix
		needs to evict them all.
		"""
		node = self.node
		self.conf('1000MB', "log_min_messages = debug1")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb")
		for i in range(200):
			self.incompressible_table(f"t{i}", 760)
		node.safe_psql("CHECKPOINT")
		before = len(self.eviction_cycles())
		node.safe_psql("ALTER SYSTEM SET orioledb.s3_desired_size = '4MB'")
		node.reload()
		deadline = time.time() + 120
		while sum(c[2] for c in self.eviction_cycles()[before:]) < 190:
			self.assertLess(time.time(), deadline, "the parts were not evicted")
			time.sleep(0.5)
		cycles = [c[2] for c in self.eviction_cycles()[before:]]
		self.assertLessEqual(
		    len(cycles), 15,
		    f"evicting 190 of the 200 part-full parts took {len(cycles)} "
		    f"cycles: {cycles}")
		node.stop()

	def test_s3_a_large_object_is_not_read_into_memory(self):
		"""
		A GET was read into a StringInfo whatever its size, and failed
		inside the curl callback past 1 GB.  A body past the bound is
		refused with an error naming the object, before it is in memory.
		"""
		node = self.node
		self.conf('1000MB')
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb")
		self.client.put_object(Bucket=self.bucket_name, Key='big',
		                       Body=b'x' * (257 << 20))
		with self.assertRaises(QueryException) as e:
			node.safe_psql("SELECT length(s3_get('big'))")
		self.assertIn("too large", e.exception.message)
		self.assertIn("big", e.exception.message)
		# the backend is fine afterwards
		self.assertEqual([(1, )], node.execute("SELECT 1"))
		node.stop()
