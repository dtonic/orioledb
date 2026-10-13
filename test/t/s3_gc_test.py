import os
import signal
import struct
import time
from unittest.mock import patch

from testgres.enums import NodeStatus

from moto.core.common_types import TYPE_RESPONSE

from .s3_base_test import S3BaseTest, AccessDenied, HoldFirstGet, \
 orig_put_object, orig_delete_object, orig_list_objects_v2
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


class DenyingDelete:
	"""A delete_object() that refuses (403) every DELETE of one key while
	`denying` holds, and removes nothing then."""

	def __init__(self, key):
		self.key = key
		self.denying = True
		self.refused = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.denying and response.parse_key_name() == self.key:
			self.refused += 1
			raise AccessDenied()
		return orig_delete_object(response)


class ListRecorder:
	"""A list_objects_v2() that records the prefix and start-after of every
	listing, and answers it."""

	def __init__(self):
		self.lists = []

	def __call__(self, response) -> TYPE_RESPONSE:
		querystring = response._get_querystring(response.request, response.uri)
		prefix = querystring.get("prefix", [None])[0]
		after = response.querystring.get("start-after", [None])[0]
		if isinstance(prefix, bytes):
			prefix = prefix.decode()
		self.lists.append((prefix, after))
		return orig_list_objects_v2(response)

	def chunk_lists(self):
		return [(p, a) for p, a in self.lists if p and '/chunks/' in p]


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

	def test_s3_gc_frozen_through_a_takeover_deletes_nothing(self):
		"""A GC pass frozen after it read the pins, while another writer takes
		the bucket over, deletes nothing when it thaws: the new writer's
		readers may pin what it was about to remove."""
		node = self.gc_world("orioledb.s3_lease_duration = 15")
		datoid = self.datoid(node)
		relnode = self.primary_relnode(node)
		ready = []
		hold = HoldFirstGet(f"trees/{datoid}/{relnode}/gc",
		                    when=lambda: bool(ready))
		hold.armed = True
		with patch('moto.s3.responses.S3Response.get_object',
		           new=lambda response: hold(response)):
			node.safe_psql("UPDATE o_test SET v = 1;")
			node.safe_psql("CHECKPOINT;")
			node.safe_psql("UPDATE o_test SET v = 2;")
			ready.append(True)
			node.safe_psql("CHECKPOINT;")
			# the GC is between its read of the pins and its deletes
			self.assertTrue(hold.entered.wait(60))
			parts = set(self.part_etags())
			old_token = self.kv('root')['token']
			gc_pid = self.worker_pid(node, 'orioledb s3 gc')
			frozen = [p for p in self.node_pids(node) if p != gc_pid]
			for pid in frozen:
				os.kill(pid, signal.SIGSTOP)
			try:
				with self.initNode(self.getBasePort() + 1, 'tgsb') as b:
					self.loader.download(b.data_dir)
					b.append_conf(port=b.port)
					b.append_conf("""
						orioledb.s3_gc_interval = 0
						orioledb.main_buffers = 8MB
					""")
					# waits for the frozen writer's lease to run out
					b.start()
					b.safe_psql("CHECKPOINT;")
					self.assertNotEqual(old_token, self.kv('root')['token'])
					hold.release.set()
					# the old GC goes on from where it stopped, and ends
					self.wait_until(lambda: self.process_ended(gc_pid), 20)
					time.sleep(2)
					self.assertEqual(parts, set(self.part_etags()))
					for pid in frozen:
						os.kill(pid, signal.SIGCONT)
					self.wait_stopped(node)
					b.safe_psql("UPDATE o_test SET v = 3;")
					self.assert_bucket_restores(b, [(30000, 90000)])
					b.cleanup()
			finally:
				hold.release.set()
				for pid in frozen:
					try:
						os.kill(pid, signal.SIGCONT)
					except OSError:
						pass

	def test_s3_gc_goes_on_past_a_delete_the_store_refuses(self):
		"""A DELETE the store refuses for good holds up neither the pass nor
		the next: the other parts go, the progress is published, the refused
		part stays in the state and goes once the store lets it."""
		node = self.gc_world("orioledb.s3_auth_retry_time = 0")
		tree = self.primary_relnode(node)
		first = {k for k in self.part_etags() if k.split('/')[2] == tree}
		denied = sorted(first)[0]
		denying = DenyingDelete(denied)
		with patch('moto.s3.responses.S3Response.delete_object',
		           new=lambda response: denying(response)):
			for v in range(1, 4):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			committed = int(self.kv('root')['checkpoint'])
			self.assertTrue(self.wait_for_folded(committed))
			self.assertTrue(self.wait_until(
			    lambda: not (first - {denied}) & set(self.part_etags()), 60))
			self.assertIn(denied, self.part_etags())
			self.assertGreater(denying.refused, 0)
			self.assertTrue(self.log_contains(node, "S3 GC could not delete"))
			self.assertFalse(self.log_contains(node, "could not delete object from S3"))
			denying.denying = False
			# the part stayed in the state: the next fold of the tree removes it
			node.safe_psql("UPDATE o_test SET v = 4;")
			node.safe_psql("CHECKPOINT;")
			self.assertTrue(self.wait_until(
			    lambda: denied not in self.part_etags(), 60))
		self.assert_bucket_restores(node, [(30000, 120000)])

	def test_s3_gc_goes_on_past_a_damaged_manifest(self):
		"""A manifest that does not parse as its checkpoint's is passed over
		like one that was never written, with a LOG, and the pass goes on;
		it goes with the other manifests below R."""
		node = self.gc_world("orioledb.s3_gc_interval = 3600")
		c1 = int(self.kv('root')['checkpoint'])
		node.safe_psql("UPDATE o_test SET v = 1;")
		node.safe_psql("CHECKPOINT;")
		key = f"manifests/{self.token_of(c1)}-{c1}"
		body = self.s3_object(key)
		self.assertIn(f"checkpoint {c1}\n".encode(), body)
		self.client.put_object(Bucket=self.bucket_name, Key=key,
		                       Body=body.replace(f"checkpoint {c1}\n".encode(),
		                                         b"checkpoint 999\n", 1))
		node.safe_psql("UPDATE o_test SET v = 2;")
		node.safe_psql("CHECKPOINT;")
		c3 = int(self.kv('root')['checkpoint'])
		self.assertTrue(self.gc_passes_until(
		    node, lambda: self.gc_field('collected') >= c3))
		self.assertTrue(self.log_contains(node, "S3 GC skips the manifest of checkpoint"))
		self.assertFalse(self.all_keys(key))
		self.assert_bucket_restores(node, [(30000, 60000)])

	def test_s3_gc_lists_chunks_from_where_it_stopped(self):
		"""A pass lists a timeline's chunks from the last one it removed, not
		from the first; a timeline whose first kept chunk still ends past
		R's REDO is not listed at all."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			archive_mode = off
			synchronous_standby_names = 's3'
			orioledb.s3_gc_interval = 3600
			orioledb.s3_gc_keep_checkpoints = 1
		""")
		recorder = ListRecorder()
		with patch('moto.s3.responses.S3Response.list_objects_v2',
		           new=lambda response: recorder(response)):
			node.start()
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			""")

			def rounds(n, start):
				for r in range(start, start + n):
					for i in range(10):
						node.safe_psql(f"INSERT INTO o_test VALUES ({r * 10 + i});")
					node.safe_psql("SELECT pg_switch_wal();")
					node.safe_psql("CHECKPOINT;")

			def chunks():
				return set(self.chunk_keys())

			rounds(1, 0)
			early = chunks()
			self.assertTrue(early)
			rounds(3, 1)
			recorder.lists.clear()
			self.assertTrue(self.gc_passes_until(node, lambda: not early & chunks(), 90))
			self.assertTrue(recorder.chunk_lists())
			removed = early - chunks()
			last_removed = {}
			for k in removed:
				tli, seq = k.split('/')[1], int(k.split('/')[3])
				last_removed[tli] = max(last_removed.get(tli, 0), seq)

			rounds(2, 4)
			later = chunks()
			recorder.lists.clear()
			committed = int(self.kv('root')['checkpoint'])
			self.assertTrue(self.gc_passes_until(
			    node, lambda: self.gc_field('collected') >= committed))
			self.assertTrue(self.wait_until(lambda: later != chunks(), 30))
			for prefix, after in recorder.chunk_lists():
				tli = prefix.split('/')[-3]
				self.assertIsNotNone(after, f"the chunks of {tli} were listed whole again")
				self.assertGreaterEqual(int(after.split('/')[-1]),
				                        last_removed.get(tli, 0))
		node.stop()

	def test_s3_gc_state_is_sized_by_the_trees_blocks_per_part(self):
		"""A tree's GC state holds one bit per block of each part, sized by
		the tree's blocks per part, and names the writer that wrote it."""
		node = self.gc_world()
		datoid = self.datoid(node)
		relnode = self.primary_relnode(node)
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		key = f"trees/{datoid}/{relnode}/gc"
		self.wait_for_object(key)
		body = self.s3_object(key)
		magic, token, bpp, count = struct.unpack('<IQQQ', body[:28])
		self.assertEqual(0x34434753, magic)
		self.assertEqual(int(self.kv('root')['token']), token)
		self.assertEqual(1024 * 1024 // 8192, bpp)
		self.assertGreater(count, 0)
		self.assertEqual(28 + count * (16 + (bpp + 7) // 8), len(body))
		node.stop()

	def test_s3_gc_resets_the_state_of_a_relnode_named_again(self):
		"""A tree dropped by R whose relnode a later manifest names again is a
		new tree under old keys: its GC state starts over, rather than the
		dropped tree's dead bits marking the new tree's parts dead."""
		node = self.gc_world("""
			orioledb.s3_gc_interval = 3600
			orioledb.s3_gc_keep_checkpoints = 2
		""")
		datoid = self.datoid(node)
		relnode = self.primary_relnode(node)
		state_file = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                          f"{relnode}.gcstate")
		state_key = f"trees/{datoid}/{relnode}/gc"
		for v in range(1, 3):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		c3 = int(self.kv('root')['checkpoint'])
		self.assertTrue(self.gc_passes_until(
		    node, lambda: self.gc_field('collected') >= c3 - 1))
		self.assertTrue(self.wait_until(lambda: os.path.exists(state_file), 30))
		self.assertTrue(self.all_keys(state_key))

		# the manifest that becomes R says the tree is gone; the one after
		# names it again
		node.safe_psql("UPDATE o_test SET v = 3;")
		node.safe_psql("CHECKPOINT;")
		c4 = int(self.kv('root')['checkpoint'])
		key = f"manifests/{self.token_of(c4)}-{c4}"
		lines = self.s3_object(key).decode().splitlines(keepends=True)
		forged = [l for l in lines if not l.startswith(f"tree {datoid} {relnode} ")]
		self.assertEqual(len(lines) - 1, len(forged))
		self.client.put_object(Bucket=self.bucket_name, Key=key, Body=''.join(forged))
		node.safe_psql("UPDATE o_test SET v = 4;")
		node.safe_psql("CHECKPOINT;")
		self.assertTrue(self.gc_passes_until(
		    node, lambda: self.gc_field('collected') >= c4))
		self.assertTrue(self.log_contains(node, "a tree has its relnode again"))
		self.assertFalse(os.path.exists(state_file))
		self.assertFalse(self.all_keys(state_key))
		# the tree itself was left alone, and its GC goes on from a clean state
		node.safe_psql("UPDATE o_test SET v = 5;")
		node.safe_psql("CHECKPOINT;")
		self.assertTrue(self.gc_passes_until(
		    node, lambda: self.gc_field('collected') >= c4 + 1))
		self.assert_bucket_restores(node, [(30000, 150000)])

	def test_s3_sweep_keeps_the_lists_of_a_skipped_tree(self):
		"""The GC leaves the dead lists of a tree whose state it cannot read,
		for whoever repairs the state; --sweep leaves them too, since the GC
		names the tree as skipped."""
		node = self.gc_world()
		datoid = self.datoid(node)
		relnode = self.primary_relnode(node)
		self.client.put_object(Bucket=self.bucket_name,
		                       Key=f"trees/{datoid}/{relnode}/gc",
		                       Body=b"not a state")
		for v in range(1, 4):
			node.safe_psql(f"UPDATE o_test SET v = {v};")
			node.safe_psql("CHECKPOINT;")
		self.assertTrue(self.wait_for_folded(int(self.kv('root')['checkpoint'])))
		self.assertTrue(self.log_contains(node, "S3 GC skips tree"))
		lists = set(self.all_keys(f"trees/{datoid}/{relnode}/dead/"))
		self.assertTrue(lists)
		self.assertIn(f"\nskipped {datoid} {relnode}\n", self.s3_object('gc').decode())
		self.loader.download(None)
		self.assertEqual(lists, set(self.all_keys(f"trees/{datoid}/{relnode}/dead/")))
		node.stop()

	def test_s3_gc_removes_the_local_lists_of_a_dropped_tree(self):
		"""The local dead lists of a dropped tree go with the tree: left, a
		tree that has the relnode again would fold them as its own."""
		node = self.node
		node.append_conf(self.s3_conf())
		node.append_conf("""
			orioledb.s3_gc_interval = 1
			orioledb.s3_gc_keep_checkpoints = 1
			orioledb.main_buffers = 1MB
			orioledb.debug_disable_pools_limit = true
		""")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, v int, pad text) USING orioledb;
			INSERT INTO o_test SELECT g, 0, repeat('x', 200) FROM generate_series(1, 50000) g;
			CREATE TABLE o_keep (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_keep VALUES (1);
		""")
		node.safe_psql("CHECKPOINT;")
		datoid = self.datoid(node)
		relnode = self.primary_relnode(node)
		# an update larger than the buffers: the evicted pages' old extents
		# fill a list no checkpoint will upload, since the tree goes first
		node.safe_psql("UPDATE o_test SET v = 1;")
		self.assertTrue(self.local_dead_lists(node, datoid, relnode))
		node.safe_psql("DROP TABLE o_test;")
		for _ in range(3):
			node.safe_psql("INSERT INTO o_keep SELECT max(id) + 1 FROM o_keep;")
			node.safe_psql("CHECKPOINT;")
		self.assertTrue(self.wait_until(
		    lambda: not self.all_keys(f"trees/{datoid}/{relnode}/"), 90))
		self.assertEqual([], self.local_dead_lists(node, datoid, relnode))
		node.stop()

	def test_s3_gc_waits_for_a_pins_renewal_before_passing_it(self):
		"""A pin below the floor is passed over only once its reader has had
		a renewal in which to see pin_warn and stop; until then the GC
		honours it."""
		node = self.gc_world("orioledb.s3_gc_max_pin_lag = 2")
		with self.initNode(self.getBasePort() + 1, 'tgsb') as reader:
			self.loader.download(reader.data_dir)
			reader.append_conf(port=reader.port)
			reader.append_conf("""
				orioledb.s3_reader = on
				orioledb.s3_reader_pin_ttl = 90
				hot_standby = on
				orioledb.main_buffers = 8MB
			""")
			open(os.path.join(reader.data_dir, "standby.signal"), "w").close()
			reader.start()
			pinned = set(self.part_etags())
			pin = int(self.kv('root')['checkpoint'])
			for v in range(1, 5):
				node.safe_psql(f"UPDATE o_test SET v = {v};")
				node.safe_psql("CHECKPOINT;")
			self.assertGreater(int(self.kv('gc')['pin_floor']), pin)
			time.sleep(5)
			# the pin is below the floor, but its reader has not renewed since
			self.assertLessEqual(int(self.kv('gc')['folded']), pin)
			self.assertEqual(set(), pinned - set(self.part_etags()))
			# the renewal sees pin_warn and stops the reader; the pin ages
			# past a renewal interval and the GC passes it
			deadline = time.time() + 120
			while reader.status() == NodeStatus.Running and time.time() < deadline:
				time.sleep(1)
			self.assertNotEqual(reader.status(), NodeStatus.Running)
			self.assertTrue(self.log_contains(reader, "this S3 reader is too far behind the writer"))
			self.assertTrue(self.wait_until(
			    lambda: int(self.kv('gc')['folded']) > pin, 120, 1))
			reader.cleanup()
		self.assert_bucket_restores(node, [(30000, 120000)])
