import os
import time
from unittest.mock import patch

from moto.core.common_types import TYPE_RESPONSE

from .s3_base_test import S3BaseTest, AccessDenied, orig_put_object
# the module, not the class: a TestCase imported into this namespace would
# be run as this module's
from . import s3_test


class DenyingPut:
	"""A put_object() that refuses (403) the first `failures` PUTs of keys
	holding `contains`, as a store does to a key its policy forbids."""

	def __init__(self, contains, failures):
		self.contains = contains
		self.failures = failures
		self.refused = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.contains in response.parse_key_name() and \
		    self.refused < self.failures:
			self.refused += 1
			raise AccessDenied()
		return orig_put_object(response)


class S3GcTest(S3BaseTest):
	"""The GC, the manifests and the dead lists under crashes, takeovers and
	a store that refuses."""

	s3_conf = s3_test.S3Test.s3_conf
	s3_object = s3_test.S3Test.s3_object
	gc_world = s3_test.S3Test.gc_world
	part_etags = s3_test.S3Test.part_etags
	all_keys = s3_test.S3Test.all_keys
	kv = s3_test.S3Test.kv
	primary_relnode = s3_test.S3Test.primary_relnode
	wait_for_gc = s3_test.S3Test.wait_for_gc
	wait_for_collected = s3_test.S3Test.wait_for_collected
	wait_for_object = s3_test.S3Test.wait_for_object
	wait_stopped = s3_test.S3Test.wait_stopped
	chunk_keys = s3_test.S3Test.chunk_keys
	assert_bucket_restores = s3_test.S3Test.assert_bucket_restores

	def datoid(self, node):
		return node.execute(
		    "SELECT oid FROM pg_database WHERE datname = current_database()")[0][0]

	def token_of(self, chkp):
		"""The writer token that wrote checkpoint chkp, from root's epochs."""
		token = None
		for line in self.s3_object('root').decode().splitlines():
			parts = line.split(' ')
			if parts[0] == 'epoch' and int(parts[1]) <= chkp:
				token = parts[2]
		return token

	def manifest_body(self, chkp):
		return self.s3_object(f"manifests/{self.token_of(chkp)}-{chkp}").decode()

	def gc_field(self, name):
		try:
			return int(self.kv('gc').get(name, 0))
		except Exception:
			return 0

	def wait_for_folded(self, checkpoint, timeout=90):
		deadline = time.time() + timeout
		while time.time() < deadline:
			try:
				if int(self.kv('gc').get('folded', 0)) >= checkpoint:
					return True
			except Exception:
				pass
			time.sleep(1)
		return False

	def wait_until(self, condition, timeout=60, step=0.5):
		deadline = time.time() + timeout
		while time.time() < deadline:
			if condition():
				return True
			time.sleep(step)
		return condition()

	def gc_passes_until(self, node, condition, timeout=60):
		"""With orioledb.s3_gc_interval set high, a reload wakes the GC for a
		pass: ask for passes until `condition` holds."""
		deadline = time.time() + timeout
		while True:
			node.reload()
			if self.wait_until(condition, 3, 0.5):
				return True
			if time.time() >= deadline:
				return condition()

	def log_contains(self, node, text):
		with open(node.pg_log_file) as f:
			return text in f.read()

	def node_pids(self, node):
		"""The postmaster and its children."""
		pids = [node.pid]
		for name in os.listdir('/proc'):
			if not name.isdigit():
				continue
			try:
				with open(f'/proc/{name}/stat') as f:
					stat = f.read()
			except OSError:
				continue
			if int(stat.rsplit(')', 1)[1].split()[1]) == node.pid:
				pids.append(int(name))
		return pids

	def worker_pid(self, node, title):
		"""The child of the postmaster whose process title holds `title`: a
		background worker with no database connection is not in
		pg_stat_activity."""
		for pid in self.node_pids(node):
			try:
				with open(f'/proc/{pid}/cmdline', 'rb') as f:
					if title.encode() in f.read():
						return pid
			except OSError:
				continue
		raise Exception(f"no process of {node.pid} is titled {title}")

	def process_ended(self, pid):
		try:
			with open(f'/proc/{pid}/stat') as f:
				return f.read().rsplit(')', 1)[1].split()[0] == 'Z'
		except OSError:
			return True

	def local_dead_lists(self, node, datoid, relnode):
		path = os.path.join(node.data_dir, 'orioledb_data', str(datoid))
		return sorted(f for f in os.listdir(path)
		              if f.startswith(f"{relnode}-") and f.endswith(".dead"))

	def crash_before_the_manifest(self, keep_note):
		"""Crash checkpoint K after it renamed its small file checksums and
		before it wrote its manifest; the next checkpoint must still know how
		many bundles K put up, or its manifest is incomplete and no restore
		takes it."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			restart_after_crash = off
			orioledb.s3_auth_retry_time = 0
			orioledb.s3_lease_duration = 15
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int) USING orioledb;
			CREATE TABLE h_test (id int PRIMARY KEY);
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 1000) g;
			INSERT INTO h_test SELECT generate_series(1, 1000);
		""")
		node.safe_psql("CHECKPOINT;")
		before = int(self.kv('root')['checkpoint'])
		k = before + 1
		# a build list: the checkpoint puts it up right after it renamed the
		# small file checksums, and a refusal there ends the checkpointer
		node.safe_psql("""
			CREATE INDEX o_test_v ON o_test (v);
			INSERT INTO h_test SELECT generate_series(1001, 1100);
		""")
		denying = DenyingPut('.build', failures=1)
		with patch('moto.s3.responses.S3Response.put_object',
		           new=lambda response: denying(response)):
			try:
				node.safe_psql("CHECKPOINT;")
			except Exception:
				pass
			self.assertTrue(self.wait_until(lambda: denying.refused == 1, 60))
			self.wait_stopped(node)
		# the crash fell between the two: K's small files are listed as its
		with open(os.path.join(node.data_dir, 'orioledb_data',
		                       'small_file_checksums')) as f:
			self.assertIn(f"CHECKPOINT: {k}\n", f.read())
		note = os.path.join(node.data_dir, 'orioledb_data', 's3_bundles')
		self.assertTrue(os.path.exists(note))
		if not keep_note:
			os.unlink(note)
		node.is_started = False	# testgres did not see the server go
		node.start()
		node.safe_psql("INSERT INTO h_test VALUES (2000);")
		node.safe_psql("CHECKPOINT;")
		committed = int(self.kv('root')['checkpoint'])
		self.assertGreater(committed, k)
		body = self.manifest_body(committed)
		bundles = self.all_keys(f"data/{self.token_of(k)}-{k}/orioledb_data/small_files_")
		self.assertTrue(bundles)
		self.assertIn(f"\nbundles {k} {len(bundles)}\n", body)
		self.assertIn("\ncomplete 1\n", body)
		counted = self.log_contains(node, "S3 manifest counted")
		self.assertEqual(not keep_note, counted)
		node.stop()

		with self.initNode(self.getBasePort() + 1, 'tgsb') as new_node:
			self.loader.download(new_node.data_dir)
			new_node.append_conf(port=new_node.port)
			new_node.start()
			self.assertEqual([(1000, 1101)],
			                 new_node.execute("SELECT (SELECT count(*) FROM o_test), "
			                                  "(SELECT count(*) FROM h_test)"))
			new_node.stop()
			new_node.cleanup()

	def node_answers(self, node):

		def answers():
			try:
				return node.execute("SELECT 1") == [(1, )]
			except Exception:
				return False

		return answers

	def test_s3_bundle_count_survives_a_crash_before_the_manifest(self):
		"""The count is noted locally before the checksums are renamed, and
		the next manifest takes it from there."""
		self.crash_before_the_manifest(keep_note=True)

	def test_s3_bundle_count_is_listed_when_the_note_is_gone(self):
		"""Without the note (a second crash wrote over it) the bundles are
		counted in the bucket: they are numbered densely from 0."""
		self.crash_before_the_manifest(keep_note=False)
