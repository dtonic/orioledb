#!/usr/bin/env python3

import argparse
import boto3
import os
import re
import struct
import sys
import time
import testgres

from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig

# The most requests the loader runs at once (its download pools).  The
# client's connection pool is sized to match: at botocore's default of 10, the
# rest of the pool's threads opened and dropped a connection per request
# ("Connection pool is full, discarding connection").
MAX_CONCURRENCY = max(16, os.cpu_count() or 1)
from email.utils import parsedate_to_datetime
from threading import Event, Thread
from urllib.parse import urlparse


class OrioledbS3ObjectLoader:

	def parse_args(self):
		epilog = """
			This util uses boto3 under the hood.
			You can set credentials using AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables
			Or by using ~/.aws/config file.
			Read for more details:
			https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables
		"""
		parser = argparse.ArgumentParser(usage=argparse.SUPPRESS,
		                                 epilog=epilog)
		parser.add_argument(
		    '--endpoint',
		    dest='endpoint',
		    required=True,
		    help="AWS url (must contain bucket name if no prefix set)")
		parser.add_argument('-d',
		                    '--data-dir',
		                    dest='data_dir',
		                    help="Destination data directory")
		parser.add_argument(
		    '--prefix',
		    dest='prefix',
		    required=False,
		    default="",
		    help="Prefix to prepend to S3 object name (may contain bucket name)"
		)
		parser.add_argument('--cert-file',
		                    dest='cert_file',
		                    help="Path to crt file")
		parser.add_argument(
		    '--checkpoint-only',
		    dest='checkpoint_only',
		    action='store_true',
		    help="Restore the state of the last checkpoint only: fetch its "
		    "REDO WAL segment, not the archived segments after it")
		parser.add_argument(
		    '--as-of',
		    dest='as_of',
		    type=int,
		    default=None,
		    help="Restore a checkpoint the bucket still retains instead of "
		    "the last one committed; implies --checkpoint-only, and the data "
		    "directory starts only as a reader")
		parser.add_argument(
		    '--keep-parts',
		    dest='keep_parts',
		    action='store_true',
		    help="Load into an existing data directory (a reader to make the "
		    "writer) keeping the tree parts it has: they are copies of "
		    "objects that never change")
		parser.add_argument(
		    '--restore',
		    dest='restore',
		    nargs=2,
		    metavar=('FILE', 'PATH'),
		    help="restore_command mode: put WAL file FILE (%%f) from the "
		    "bucket at PATH (%%p): an archived segment, a segment assembled "
		    "from chunks, or a timeline history")
		parser.add_argument(
		    '--local-wal',
		    dest='local_wal',
		    action='store_true',
		    help="Put every WAL segment the bucket has in pg_wal instead of "
		    "having restore_command read the bucket: for a server whose "
		    "environment cannot run this script.  A reader then does not "
		    "follow the writer")
		parser.add_argument(
		    '--restore-command',
		    dest='restore_command',
		    default=None,
		    help="restore_command to write instead of this script's own "
		    "--restore (as the server would run it)")
		parser.add_argument(
		    '--repair-root',
		    dest='repair_root',
		    action='store_true',
		    help="Put root back when the bucket's is missing or cannot be "
		    "read: from the copy under roots/ of the largest writer token "
		    "(then the latest checkpoint) whose manifest is there.  With "
		    "the writer stopped")
		parser.add_argument(
		    '--sweep',
		    dest='sweep',
		    action='store_true',
		    help="Remove what the GC could not know of: objects of "
		    "checkpoints it has collected that the oldest retained "
		    "checkpoint does not refer to (a crash before a manifest, a "
		    "build rolled back).  Prints the keys; --verbose for each")
		# what --restore is given so it does not probe the bucket again
		parser.add_argument('--resolved-endpoint', dest='resolved_endpoint',
		                    help=argparse.SUPPRESS)
		parser.add_argument('--resolved-bucket', dest='resolved_bucket',
		                    help=argparse.SUPPRESS)
		parser.add_argument('--resolved-prefix', dest='resolved_prefix',
		                    default='', help=argparse.SUPPRESS)
		parser.add_argument('--resolved-style', dest='resolved_style',
		                    help=argparse.SUPPRESS)
		parser.add_argument(
		    '--segment-size',
		    dest='segment_size',
		    type=int,
		    default=16 * 1024 * 1024,
		    help="WAL segment size for --restore")
		parser.add_argument(
		    '--verbose',
		    dest='verbose',
		    action='store_true',
		    help="More verbose output. Downloaded files displayed.")

		try:
			args = parser.parse_args()
		except SystemExit as e:
			if e.code != 0:
				parser.print_help()
			raise

		if 'cert_file' in args:
			verify = args.cert_file
		else:
			verify = None

		if args.restore:
			# restore_command runs in the data directory: take the
			# credentials the server uses, which a reload may have rotated,
			# over the ones in the environment it was started with
			for key, value in server_s3_settings('.').items():
				os.environ[key] = value

		if args.resolved_bucket:
			self.resolved = (args.resolved_endpoint, args.resolved_bucket,
			                 args.resolved_prefix, args.resolved_style)
			s3_client = boto3.client(
			    "s3",
			    endpoint_url=args.resolved_endpoint,
			    verify=verify,
			    config=Config(s3={'addressing_style': args.resolved_style},
			                  max_pool_connections=MAX_CONCURRENCY))
			self.finish_args(args, s3_client, args.resolved_bucket,
			                 args.resolved_prefix)
			return

		parsed_url = urlparse(args.endpoint)
		bucket = parsed_url.netloc.split('.')[0]
		raw_endpoint = f"{parsed_url.scheme}://{'.'.join(parsed_url.netloc.split('.')[1:])}"

		splitted_prefix = args.prefix.strip('/').split('/')
		splitted_path = parsed_url.path.strip('/').split('/')
		prefix = os.path.join(*splitted_path, *splitted_prefix)
		splitted_prefix = prefix.split('/')

		bucket_in_endpoint = True
		bucket_in_prefix = False
		try:
			config = Config(s3={'addressing_style': 'virtual'},
			                max_pool_connections=MAX_CONCURRENCY)
			s3_client = boto3.client("s3",
			                         endpoint_url=raw_endpoint,
			                         verify=verify,
			                         config=config)
			s3_client.head_bucket(Bucket=bucket)
			bucket_name = bucket
		except ValueError:
			bucket_in_endpoint = False
			bucket_in_prefix = True
		if bucket_in_prefix:
			config = None
			bucket = splitted_prefix[0]
			prefix = '/'.join(splitted_prefix[1:])
			s3_client = boto3.client(
			    "s3",
			    endpoint_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
			    verify=verify,
			    config=Config(max_pool_connections=MAX_CONCURRENCY))
			try:
				s3_client.head_bucket(Bucket=bucket)
			except ParamValidationError:
				bucket_in_prefix = False
			except ClientError:
				bucket_in_prefix = False
			bucket_name = bucket

		if not bucket_in_endpoint and not bucket_in_prefix:
			raise Exception("No valid bucket name in endpoint or prefix")

		if bucket_in_endpoint:
			self.resolved = (raw_endpoint, bucket_name, prefix, 'virtual')
		else:
			self.resolved = (f"{parsed_url.scheme}://{parsed_url.netloc}",
			                 bucket_name, prefix, 'path')
		self.finish_args(args, s3_client, bucket_name, prefix)

	def finish_args(self, args, s3_client, bucket_name, prefix):
		self._error_occurred = Event()
		self.data_dir = args.data_dir
		self.bucket_name = bucket_name
		self.prefix = prefix
		self.verbose = args.verbose
		self.checkpoint_only = args.checkpoint_only or args.as_of is not None
		self.as_of = args.as_of
		self.keep_parts = args.keep_parts
		self.restore_args = args.restore
		self.segment_size = args.segment_size
		self.endpoint = args.endpoint
		self.cert_file = args.cert_file
		self.raw_prefix = args.prefix
		self.sweep = args.sweep
		self.repair = args.repair_root
		self.local_wal = args.local_wal
		self.restore_command = args.restore_command
		if args.restore is None and args.data_dir is None and \
		    not args.sweep and not args.repair_root:
			raise Exception("-d/--data-dir is required")
		self.s3 = s3_client

	def run(self):
		if self.restore_args:
			raise SystemExit(self.restore(*self.restore_args))
		if self.sweep:
			return self.sweep_orphans()
		if self.repair:
			return self.repair_root()
		if self.keep_parts:
			self.clear_keeping_parts()
		root = self.read_root(self.bucket_name)
		if root is None or int(root['checkpoint']) == 0:
			raise Exception("The S3 bucket has no committed checkpoint")
		# root names the last checkpoint committed, and which writer token
		# wrote each checkpoint: objects a checkpoint writes under its number
		# carry the token, so what a crashed run left under a number taken
		# again is never read
		chkp_num = int(root['checkpoint'])
		self.epochs = root['epochs']
		if self.as_of is not None:
			if self.as_of > chkp_num:
				raise Exception(f"Checkpoint {self.as_of} is past the last "
				                f"committed one, {chkp_num}")
			chkp_num = self.as_of
		self.chkp_num = chkp_num
		# held while the objects are read, so the GC leaves them
		self.pin = ReaderPin(self, chkp_num)
		try:
			self.load_checkpoint(root, chkp_num)
		finally:
			self.pin.release()

	def load_checkpoint(self, root, chkp_num):
		# the checkpoint's manifest names every object restoring it reads
		manifest_body = self.read_object(
		    f"manifests/{self.token_of(chkp_num)}-{chkp_num}")
		if manifest_body is None:
			raise Exception(f"The S3 bucket keeps no manifest of checkpoint "
			                f"{chkp_num}")
		manifest = parse_manifest(manifest_body.decode())
		if not manifest['complete']:
			raise Exception(f"The manifest of checkpoint {chkp_num} is "
			                "incomplete")
		self.download_manifest(manifest)
		self.apply_xidmap_range(chkp_num)

		control = get_control_data(self.data_dir)
		self.download_wal(control)
		if not self.checkpoint_only:
			self.replay_on_start()
			if self.local_wal:
				self.download_rest_of_wal(control)
			self.recover_from_bucket(int(control["Bytes per WAL segment"]))
		self.write_token_file(root, manifest_body)

	def sweep_orphans(self):
		"""
		The GC removes what manifests name.  An object of a checkpoint below
		`collected` that the manifest of `collected` does not refer to is
		named by no retained manifest: it is garbage the GC could not see.
		Parts are left alone: a writer puts over an uncommitted part number.
		The dead lists of a tree the GC skipped (`skipped` lines of `gc`: its
		state could not be read) are left for whoever repairs the state.
		"""
		root = self.read_root(self.bucket_name)
		gc = self.read_object('gc')
		if root is None or gc is None:
			print("nothing to sweep: no root or no GC progress yet")
			return
		self.epochs = root['epochs']
		lines = [line.split(' ') for line in gc.decode().splitlines() if ' ' in line]
		fields = dict((l[0], ' '.join(l[1:])) for l in lines)
		skipped = {(int(l[1]), int(l[2])) for l in lines if l[0] == 'skipped' and len(l) == 3}
		collected = int(fields.get('collected', 0))
		folded = int(fields.get('folded', 0))
		if collected == 0:
			print("nothing to sweep: the GC has collected nothing yet")
			return
		body = self.read_object(
		    f"manifests/{self.token_of(collected)}-{collected}")
		if body is None:
			print(f"nothing to sweep: no manifest of checkpoint {collected}")
			return
		m = parse_manifest(body.decode())
		removed = 0
		offset = len(self.prefix.strip('/').split('/')) if self.prefix.strip('/') else 0

		def tok_chk(text):
			try:
				token, chk = text.split('-', 1)
				return int(token), int(chk.split('.', 1)[0])
			except ValueError:
				return None

		for key in self.list_objects(self.bucket_name, ''):
			parts = key.split('/')[offset:]
			garbage = False
			if parts[0] == 'manifests' and len(parts) == 2:
				tc = tok_chk(parts[1])
				garbage = tc is not None and tc[1] < collected
			elif parts[0] == 'data' and len(parts) >= 2:
				tc = tok_chk(parts[1])
				if tc is not None and tc[1] < collected:
					path = '/'.join(parts[2:])
					if path.startswith('orioledb_data/small_files_'):
						garbage = tc[1] not in m['bundles']
					else:
						garbage = m['files'].get(path) != tc[1]
			elif parts[0] == 'orioledb_undo' and len(parts) == 3:
				tc = tok_chk(parts[2])
				garbage = tc is not None and tc[1] < collected and \
				    m['undo'].get(parts[1]) != tc[1]
			elif parts[0] == 'trees' and len(parts) == 5 and parts[3] == 'maps':
				tc = tok_chk(parts[4])
				garbage = tc is not None and tc[1] < collected and \
				    m['trees'].get((int(parts[1]), int(parts[2]))) != tc[1]
			elif parts[0] == 'trees' and len(parts) == 5 and parts[3] == 'dead':
				tc = tok_chk(parts[4])
				garbage = tc is not None and tc[1] <= folded and \
				    (int(parts[1]), int(parts[2])) not in skipped
			if garbage:
				if self.verbose:
					print(f"sweep {key}", flush=True)
				self.s3.delete_object(Bucket=self.bucket_name, Key=key)
				removed += 1
		print(f"swept {removed} objects")

	def clear_keeping_parts(self):
		"""Empty the data directory but for the trees' part files."""
		tree_file = re.compile(r'^\d+(\.\d+)?$')
		data = os.path.join(self.data_dir, 'orioledb_data')
		for top, dirs, files in os.walk(self.data_dir, topdown=False):
			for name in files:
				path = os.path.join(top, name)
				parent = os.path.dirname(path)
				if os.path.dirname(parent) == data and \
				    tree_file.match(name) and \
				    os.path.basename(parent).isdigit():
					continue
				os.unlink(path)
			for name in dirs:
				path = os.path.join(top, name)
				if os.path.islink(path):
					os.unlink(path)
				elif not os.listdir(path):
					os.rmdir(path)

	def recover_from_bucket(self, segment_size):
		"""
		Start with an archive recovery that reads the bucket: up to the
		terminator a writer taking over puts, or on and on for a reader
		(standby.signal).  A writer then opens a new timeline.
		"""
		import shlex
		if self.local_wal:
			# pg_wal has it all; archive recovery still opens the timeline
			command = '/bin/false'
		elif self.restore_command:
			command = self.restore_command
		else:
			cmd = [sys.executable, os.path.abspath(__file__),
			       '--endpoint', self.endpoint]
			if self.raw_prefix:
				cmd += ['--prefix', self.raw_prefix]
			if self.cert_file:
				cmd += ['--cert-file', self.cert_file]
			endpoint, bucket, prefix, style = self.resolved
			cmd += ['--segment-size', str(segment_size),
			        '--resolved-endpoint', endpoint, '--resolved-bucket', bucket,
			        '--resolved-prefix', prefix, '--resolved-style', style]
			# only an answer of this script says a file is not there: one
			# that cannot run at all (no interpreter, no module) stops
			# recovery instead of ending the WAL where it is
			# a standby (a reader) asks again later rather than stop
			command = (' '.join(shlex.quote(x) for x in cmd) +
			           ' --restore %f %p; rc=$?; [ $rc -eq 0 ] && exit 0; '
			           f'[ $rc -eq {RESTORE_NOT_FOUND} ] && exit 1; '
			           '[ -f standby.signal ] && exit 1; exit 128')
		if not self.local_wal:
			# the writer's terminator check: recovery reads the bucket
			open(os.path.join(self.data_dir, 'orioledb_data',
			                  's3_restore_bucket'), 'w').close()
		with open(os.path.join(self.data_dir, 'postgresql.auto.conf'),
		          'a') as f:
			f.write("restore_command = '%s'\n" % command.replace("'", "''"))
			f.write("recovery_target_timeline = 'latest'\n")
		open(os.path.join(self.data_dir, 'recovery.signal'), 'w').close()

	def read_object(self, key):
		try:
			obj = self.s3.get_object(Bucket=self.bucket_name,
			                         Key=os.path.join(self.prefix, key))
		except ClientError as e:
			if e.response['Error']['Code'] in ("404", "NoSuchKey"):
				return None
			raise
		return obj['Body'].read()

	def download_manifest(self, m):
		"""
		Every object the manifest of the checkpoint names: the PG files of
		the checkpoint that last uploaded each, the small files from the
		bundles that hold them, the undo it retains and the maps it wrote.  A
		tree whose map is older is fetched by the server when it opens the
		tree, and parts as they are read.
		"""
		k = self.chkp_num
		jobs = []
		bundled = {f"orioledb_data/small_files_{i}"
		           for i in range(m['bundles'].get(k, 0))}
		for path in m['data']:
			if path in bundled:
				continue
			jobs.append((f"{self.checkpoint_dir(k)}/{path}",
			             os.path.join(self.data_dir, path)))
		for path, n in m['files'].items():
			jobs.append((f"{self.checkpoint_dir(n)}/{path}",
			             os.path.join(self.data_dir, path)))
		for name, n in m['undo'].items():
			jobs.append((f"orioledb_undo/{name}/{self.token_of(n)}-{n}",
			             os.path.join(self.data_dir, 'orioledb_undo', name)))
		token = self.token_of(k)
		for datoid, relnode in m['maps']:
			jobs.append((f"trees/{datoid}/{relnode}/maps/{token}-{k}",
			             os.path.join(self.data_dir, "orioledb_data",
			                          str(datoid), f"{relnode}-{k}.map")))
		with ThreadPoolExecutor(MAX_CONCURRENCY) as executor:
			futures = [
			    executor.submit(self.download_file, self.bucket_name,
			                    os.path.join(self.prefix, key), path)
			    for key, path in jobs
			]
			for future in futures:
				future.result()
		if self._error_occurred.is_set():
			raise Exception("Some objects of the checkpoint could not be "
			                "downloaded")

		# small files, by the checkpoint whose bundles hold them
		wanted = {}
		for path, n in m['small'].items():
			wanted.setdefault(n, set()).add(path)
		for n, names in wanted.items():
			if n not in m['bundles']:
				raise Exception(f"The manifest does not say how many bundles "
				                f"checkpoint {n} wrote")
			for i in range(m['bundles'][n]):
				data = self.read_object(
				    f"{self.checkpoint_dir(n)}/orioledb_data/small_files_{i}")
				if data is None:
					raise Exception(f"Small file bundle {i} of checkpoint {n} "
					                "is missing")
				self.extract_small_files(data, names)

	def apply_xidmap_range(self, chkp_num):
		"""Put the checkpoint's copy of the xidmap range [xmin, xmax) back
		where the xidmap keeps it: the transactions in that range that had
		finished are known as such only from there.  A checkpoint written
		before the copy existed has none."""
		path = os.path.join(self.data_dir, 'orioledb_data',
		                    f"{chkp_num}.xidrange")
		if not os.path.exists(path):
			return
		with open(path, 'rb') as f:
			data = f.read()
		xmin, xmax = struct.unpack('<QQ', data[:16])
		if xmin == XIDMAP_RANGE_ENCODED:
			if xmax != 1:
				raise Exception(f"{path} has an unknown format {xmax}")
			xmin, xmax = struct.unpack('<QQ', data[16:32])
			items = decode_xidmap_items(data[32:], xmax - xmin, path)
		else:
			items = data[16:]
		item_size = 16
		file_size = 0x1000000
		if len(items) != (xmax - xmin) * item_size:
			raise Exception(f"{path} holds {len(items)} bytes, not "
			                f"{(xmax - xmin) * item_size}")
		offset = xmin * item_size
		pos = 0
		while pos < len(items):
			file_num = offset // file_size
			in_file = offset % file_size
			n = min(len(items) - pos, file_size - in_file)
			name = os.path.join(self.data_dir, 'orioledb_data',
			                    f"{file_num >> 32:02X}{file_num & 0xFFFFFFFF:08X}.xidmap")
			fd = os.open(name, os.O_WRONLY | os.O_CREAT, 0o600)
			try:
				os.pwrite(fd, items[pos:pos + n], in_file)
				os.fsync(fd)
			finally:
				os.close(fd)
			pos += n
			offset += n

	def replay_on_start(self):
		"""
		A checkpoint taken at a clean stop leaves pg_control saying the
		cluster was shut down, and a server started on it replays no WAL: the
		commits after that checkpoint, in the archived segments and chunks put
		here, would be skipped and written over.  Say it was running instead,
		so the start is a crash recovery from the checkpoint to the end of the
		WAL.
		"""
		DB_SHUTDOWNED, DB_IN_PRODUCTION, STATE_OFFSET = 1, 6, 16
		path = os.path.join(self.data_dir, 'global', 'pg_control')
		with open(path, 'rb') as f:
			data = bytearray(f.read())
		if struct.unpack_from('<i', data, STATE_OFFSET)[0] != DB_SHUTDOWNED:
			return
		# the CRC is the first word that is the CRC of all the bytes before it
		crc_offset = None
		for off in range(STATE_OFFSET + 4, 1024, 4):
			if crc32c(bytes(data[:off])) == struct.unpack_from('<I', data, off)[0]:
				crc_offset = off
				break
		if crc_offset is None:
			raise Exception("Could not find the checksum of pg_control")
		struct.pack_into('<i', data, STATE_OFFSET, DB_IN_PRODUCTION)
		struct.pack_into('<I', data, crc_offset, crc32c(bytes(data[:crc_offset])))
		with open(path, 'wb') as f:
			f.write(data)

	def repair_root(self):
		"""Put root back from the copy under roots/ of the largest writer
		token, then the newest checkpoint, whose checkpoint's manifest is in
		the bucket; a root that reads is left alone.  The token first: a
		writer that took the bucket over leaves its copy under the same
		checkpoint number as the writer it fenced off, and that one's would
		let it in again."""
		try:
			root = self.read_root(self.bucket_name)
			if root is not None and 'checkpoint' in root and \
			    'token' in root and 'bucket' in root:
				print("root reads: nothing to repair")
				return
		except (ValueError, UnicodeDecodeError):
			pass
		copies = []
		for key in self.list_objects(self.bucket_name, 'roots/'):
			name = key.rsplit('/', 1)[-1]
			# roots/<token>-<checkpoint>; a copy from before the token was in
			# the key counts as the smallest token
			m = re.fullmatch(r'(?:(\d+)-)?(\d+)', name)
			if m:
				copies.append((int(m.group(1) or 0), int(m.group(2)), key))
		for _, chkp, key in sorted(copies, reverse=True):
			body = self.s3.get_object(Bucket=self.bucket_name,
			                          Key=key)['Body'].read()
			copy = parse_root_body(body.decode())
			self.epochs = copy['epochs']
			if self.read_object(
			    f"manifests/{self.token_of(chkp)}-{chkp}") is None:
				continue
			# a writer that took over since wrote no copy we can see: putting
			# an older token back would let the fenced writer in again
			writer = self.read_object('writer')
			if writer is not None:
				w = parse_root_body(writer.decode())
				if int(w.get('token', 0)) > int(copy.get('token', 0)):
					raise Exception(
					    f"The writer object names token {w['token']}, newer than "
					    f"{key} (token {copy.get('token')}): root cannot be put "
					    "back from a copy")
			self.s3.put_object(Bucket=self.bucket_name,
			                   Key=os.path.join(self.prefix, 'root'),
			                   Body=body)
			print(f"root put back from {key}")
			return
		raise Exception("No copy of root has its manifest in the bucket")

	def read_root(self, bucket_name):
		"""The bucket's root object as a dict, or None when it has none."""
		try:
			obj = self.s3.get_object(Bucket=bucket_name,
			                         Key=os.path.join(self.prefix, 'root'))
		except ClientError as e:
			if e.response['Error']['Code'] in ("404", "NoSuchKey"):
				return None
			raise
		return parse_root_body(obj['Body'].read().decode())

	def token_of(self, chkp_num):
		"""The writer token that wrote checkpoint chkp_num."""
		token = None
		for first, epoch_token in self.epochs:
			if first <= chkp_num:
				token = epoch_token
		if token is None:
			raise Exception(f"No writer token is known for checkpoint {chkp_num}")
		return token

	def checkpoint_dir(self, chkp_num):
		return f"data/{self.token_of(chkp_num)}-{chkp_num}"

	def extract_small_files(self, data, names):
		numFiles = struct.unpack('i', data[0:4])[0]
		for i in range(0, numFiles):
			(nameOffset, dataOffset,
			 dataLength) = struct.unpack('iii', data[4 + i * 12:16 + i * 12])
			name = data[nameOffset:data.find(b'\0', nameOffset)].decode('ascii')
			if name not in names:
				continue
			fullname = os.path.join(self.data_dir, name)
			self.makedirs(os.path.dirname(fullname), exist_ok=True, mode=0o700)
			with open(fullname, 'wb') as file:
				file.write(data[dataOffset:dataOffset + dataLength])
			os.chmod(fullname, 0o600)

	def write_token_file(self, root, manifest_body):
		"""Give the loaded data directory the token that wrote root: it starts
		as the bucket's writer, after that writer's lease has run out."""
		path = os.path.join(self.data_dir, 'orioledb_data', 's3_token')
		with open(path, 'w') as f:
			f.write(f"{root['token']} {root['bucket']}\n")
		path = os.path.join(self.data_dir, 'orioledb_data', 's3_epochs')
		with open(path, 'w') as f:
			for first, token in root['epochs']:
				f.write(f"{first} {token}\n")
		# the WAL shipper checks the bucket has nothing past what was loaded
		open(os.path.join(self.data_dir, 'orioledb_data', 's3_loaded'),
		     'w').close()
		# the next checkpoint's manifest carries this one's forward
		with open(os.path.join(self.data_dir, 'orioledb_data', 's3_manifest'),
		          'wb') as f:
			f.write(manifest_body)
		# the trees the writer dropped and had not removed: this data
		# directory's GC removes them once a checkpoint after this one is
		# committed (S3DroppedTree: checkpoint, datoid, relnode)
		dropped = [line.split()[1:] for line in manifest_body.decode().splitlines()
		           if line.startswith('dropped ')]
		with open(os.path.join(self.data_dir, 'orioledb_data',
		                       's3_dropped_trees'), 'wb') as f:
			for datoid, relnode in dropped:
				f.write(struct.pack('<III', self.chkp_num, int(datoid),
				                    int(relnode)))
		if self.as_of is not None:
			# an older state than the bucket's: it may only be read
			with open(os.path.join(self.data_dir, 'orioledb_data', 's3_as_of'),
			          'w') as f:
				f.write(f"{self.as_of}\n")

	def download_wal(self, control):
		"""
		Put in pg_wal the WAL from the checkpoint's REDO to its checkpoint
		record, and the timeline histories; restore_command fetches the rest
		from the bucket (recover_from_bucket).  Note for the writer where the
		chunks of the latest timeline end: it puts its terminator there.
		"""
		size = int(control["Bytes per WAL segment"])
		self.segment_size = size
		redo_file = control["Latest checkpoint's REDO WAL file"]
		tli = int(redo_file[0:8], 16)
		redo_seg = self.segno_of_name(redo_file)
		ckpt_seg = parse_lsn(control["Latest checkpoint location"]) // size
		self.makedirs(os.path.join(self.data_dir, 'pg_wal'), exist_ok=True,
		              mode=0o700)
		for segno in range(redo_seg, ckpt_seg + 1):
			name = self.segment_name(tli, segno)
			seg = self.restore_segment(tli, segno)
			if seg is None:
				raise Exception(f"The S3 bucket has no WAL segment {name}")
			if self.checkpoint_only and segno == ckpt_seg:
				# the segment goes on past the checkpoint record: a load of
				# the checkpoint alone stops there
				seg = end_after_record(
				    seg, parse_lsn(control["Latest checkpoint location"]) % size,
				    int(control["WAL block size"]))
			self.write_wal(name, seg)

		latest = tli
		while True:
			history = self.read_object(f"wal/{latest + 1:08X}.history")
			if history is None:
				break
			latest += 1
			self.write_wal(f"{latest:08X}.history", history)

		chunks = self.chunks_of(latest)
		# every key counts for where the chunks end: a terminator too; the
		# cache holds what the bucket listed
		seqs = sorted(int(name) for name in os.listdir(self.chunk_cache(latest))
		              if name.isdigit())
		end = 0
		for seq, magic, start, chunk_end in reversed(chunks):
			if magic == CHUNK_MAGIC:
				end = chunk_end
				break
		# a checkpoint-only load has none of the chunks: a writer started on
		# it without restore_command is refused
		next_seq = seqs[-1] + 1 if seqs and not self.checkpoint_only else 0
		path = os.path.join(self.data_dir, 'orioledb_data', 's3_wal_seq')
		with open(path, 'w') as f:
			f.write(f"{latest} {next_seq} {end}\n")

	def download_rest_of_wal(self, control):
		"""Every segment after the checkpoint record the bucket has, on the
		timeline that covers it."""
		size = self.segment_size
		redo_file = control["Latest checkpoint's REDO WAL file"]
		tli = int(redo_file[0:8], 16)
		starts = []
		t = tli
		while True:
			start = self.timeline_start(t + 1)
			if start is None:
				break
			t += 1
			starts.append((t, start[1]))
		segno = parse_lsn(control["Latest checkpoint location"]) // size + 1
		while True:
			use = tli
			for t, switch in starts:
				if switch < (segno + 1) * size:
					use = t
			seg = self.restore_segment(use, segno)
			if seg is None:
				break
			self.write_wal(self.segment_name(use, segno), seg)
			segno += 1

	def write_wal(self, name, data):
		path = os.path.join(self.data_dir, 'pg_wal', name)
		with open(path, 'wb') as f:
			f.write(data)
		os.chmod(path, 0o600)

	def segment_name(self, tli, segno):
		per = 0x100000000 // self.segment_size
		return f"{tli:08X}{segno // per:08X}{segno % per:08X}"

	def segno_of_name(self, name):
		per = 0x100000000 // self.segment_size
		return int(name[8:16], 16) * per + int(name[16:24], 16)

	def timeline_start(self, tli):
		"""(parent timeline, switch point) of a timeline, or None for the
		first one."""
		if tli <= 1:
			return None
		history = self.read_object(f"wal/{tli:08X}.history")
		if history is None:
			return None
		last = None
		for line in history.decode().splitlines():
			fields = line.split()
			if len(fields) >= 2 and fields[0].isdigit():
				last = (int(fields[0]), parse_lsn(fields[1]))
		return last

	def chunk_seqs(self, tli, after=None):
		prefix = f"wal/{tli:08X}/chunks/"
		start_after = self.chunk_key(tli, after) if after is not None else None
		return sorted(
		    int(key.rsplit('/', 1)[1])
		    for key in self.list_objects(self.bucket_name, prefix, start_after)
		    if re.match(r'.*/chunks/\d+$', key))

	def chunk_head(self, tli, seq):
		"""The 32-byte header of a chunk, from the cache or a ranged GET;
		None when the bucket does not have it."""
		path = os.path.join(self.chunk_cache(tli), str(seq))
		if os.path.exists(path):
			with open(path, 'rb') as f:
				return f.read(32)
		try:
			return self.s3.get_object(
			    Bucket=self.bucket_name, Key=self.chunk_key(tli, seq),
			    Range='bytes=0-31')['Body'].read()
		except ClientError as e:
			if e.response['Error']['Code'] in ("404", "NoSuchKey"):
				return None
			raise

	def first_chunk_seq(self, tli):
		"""The number of a timeline's first chunk of WAL: every number
		before it is a terminator (each writer start puts one).  Read from
		the chunks' own headers: a terminator's mark under terminators/ is
		put after it, and a crash in between leaves none."""
		first = 0
		while True:
			head = self.chunk_head(tli, first)
			if head is None or \
			    struct.unpack('<I', head[0:4])[0] != TERMINATOR_MAGIC:
				return first
			first += 1

	def chunk_key(self, tli, seq):
		return os.path.join(self.prefix, f"wal/{tli:08X}/chunks/{seq:020d}")

	def chunk_cache(self, tli):
		"""Chunks never change once put: their bodies are kept here, so a
		restore_command called again for the segment the server is at fetches
		only the new ones."""
		base = self.data_dir if self.data_dir else '.'
		# under pg_wal: not a PostgreSQL file the checkpoint backs up
		path = os.path.join(base, 'pg_wal', 'orioledb_s3_chunks', f"{tli:08X}")
		os.makedirs(path, mode=0o700, exist_ok=True)
		return path

	def terminator_tokens(self, tli):
		"""(seq, token) of a timeline's terminators, in order: each start
		puts one, and they stay."""
		result = []
		prefix = f"wal/{tli:08X}/terminators/"
		for key in self.list_objects(self.bucket_name, prefix):
			if not re.match(r'.*/terminators/\d+$', key):
				continue
			mark = int(key.rsplit('/', 1)[1])
			path = os.path.join(self.chunk_cache(tli), str(mark))
			if os.path.exists(path):
				with open(path, 'rb') as f:
					head = f.read(32)
			else:
				head = self.s3.get_object(
				    Bucket=self.bucket_name, Key=self.chunk_key(tli, mark),
				    Range='bytes=0-31')['Body'].read()
			magic, _, token, _, _ = struct.unpack('<IIQQQ', head)
			if magic == TERMINATOR_MAGIC:
				result.append((mark, token))
		return sorted(result)

	@staticmethod
	def fence_through(terminators, seq):
		"""The largest token a terminator up to chunk seq fenced off."""
		return max([token for mark, token in terminators if mark <= seq],
		           default=0)

	def cached_before(self, cache, lo, terminators):
		"""The last cached chunk that ends at or before lo and that no
		terminator fenced off: a chunk wins over the ones before it from its
		start on, so none of them puts anything at lo or after."""
		result = None
		for name in os.listdir(cache):
			if not name.isdigit():
				continue
			with open(os.path.join(cache, name), 'rb') as f:
				head = f.read(32)
			if len(head) < 32:
				continue
			magic, _, token, _, end = struct.unpack('<IIQQQ', head)
			seq = int(name)
			if magic == CHUNK_MAGIC and end <= lo and \
			    token >= self.fence_through(terminators, seq) and \
			    (result is None or seq > result):
				result = seq
		return result

	def chunks_of(self, tli, lo=None):
		"""(seq, magic, start, end) of a timeline's chunks in order; the ones
		not cached yet are fetched in parallel, and cached ones the bucket no
		longer has are dropped.  For a segment starting at lo, the chunks
		from the last cached one that ends at or before lo on: the ones
		after it that are cached, and the ones the bucket lists after the
		last cached.  A chunk never changes once put, so a cached one stands
		for itself even when the GC removed it since (its segment is
		archived, and read from there).  A recovery asks for its segments in
		order, and lists each chunk about once."""
		cache = self.chunk_cache(tli)
		after = None
		terminators = None
		if lo is not None:
			terminators = self.terminator_tokens(tli)
			after = self.cached_before(cache, lo, terminators)
		cached = sorted(int(name) for name in os.listdir(cache)
		                if name.isdigit())
		# every chunk up to this one listed was fetched: past it, list again
		marker = os.path.join(cache, 'listed')
		listed = None
		if after is not None and os.path.exists(marker):
			with open(marker) as f:
				listed = int(f.read())
		if listed is None or listed < after:
			after = None

		def terminator(seq):
			with open(os.path.join(cache, str(seq)), 'rb') as f:
				return struct.unpack('<I', f.read(4))[0] == TERMINATOR_MAGIC

		if after is None:
			seqs = self.chunk_seqs(tli)
			present = set(seqs)
			for seq in cached:
				if seq not in present:
					os.unlink(os.path.join(cache, str(seq)))
		else:
			# what is before it puts nothing at lo or after; terminators
			# stay, as their fences are read again
			for seq in cached:
				if seq < after and not terminator(seq):
					os.unlink(os.path.join(cache, str(seq)))
			known = [seq for seq in cached if after < seq <= listed]
			seqs = known + self.chunk_seqs(tli, listed)

		def fetch(seq):
			path = os.path.join(cache, str(seq))
			if os.path.exists(path):
				return
			try:
				body = self.s3.get_object(
				    Bucket=self.bucket_name,
				    Key=self.chunk_key(tli, seq))['Body'].read()
			except ClientError as e:
				if e.response['Error']['Code'] in ("404", "NoSuchKey"):
					raise ChunkGone(tli, seq)
				raise
			tmp = f"{path}.tmp{os.getpid()}"
			with open(tmp, 'wb') as f:
				f.write(body)
			os.replace(tmp, path)

		with ThreadPoolExecutor(MAX_CONCURRENCY) as executor:
			list(executor.map(fetch, seqs))
		if seqs:
			tmp = f"{marker}.tmp{os.getpid()}"
			with open(tmp, 'w') as f:
				f.write(str(max(seqs[-1], listed or 0)))
			os.replace(tmp, marker)
		result = []
		fence = self.fence_through(terminators, after) \
		    if after is not None else 0
		for seq in seqs:
			with open(os.path.join(cache, str(seq)), 'rb') as f:
				magic, _, token, start, end = struct.unpack('<IIQQQ', f.read(32))
			if magic == TERMINATOR_MAGIC:
				# the writer it fenced off put nothing that counts after it
				fence = max(fence, token)
				continue
			if token < fence:
				continue
			result.append((seq, magic, start, end))
		return result

	def chunk_body(self, tli, seq):
		with open(os.path.join(self.chunk_cache(tli), str(seq)), 'rb') as f:
			return f.read()[32:]

	def restore_segment(self, tli, segno):
		"""
		Segment segno of a timeline as the bucket has it: the archived one,
		or one assembled from the chunks, zero padded.  Before a timeline's
		switch point the segment is its parent's.  Chunks are laid in order;
		a later chunk wins over what the ones before it put from its start
		on (a writer that restarted ships again from where its recovery found
		the WAL to end, over a record cut in half).  A terminator ends the
		chunks of the writer it fenced off.  None when the bucket has nothing
		of the segment.
		"""
		# a chunk listed and gone before it was read was removed once its
		# segment was archived: ask for the segment again
		relists = 0
		for _ in range(10):
			try:
				seg = self.assemble_segment(tli, segno)
			except ChunkGone:
				continue
			except ChunkGap:
				# a store whose listing lags its writes (RustFS) can list a
				# chunk and not the one put just before it, for a second or
				# two.  The cache took that listing as complete and never
				# listed the gap again, so a reader stopped there for good:
				# list again, a little later each time
				if relists < 3:
					relists += 1
					self.forget_listing(tli)
					time.sleep(0.5 * relists)
					continue
				seg = self.archived_since(tli, segno)
				if seg is None:
					raise
			except RestoreRefused:
				seg = self.archived_since(tli, segno)
				if seg is None:
					raise
			if seg is None:
				seg = self.archived_since(tli, segno)
			if seg is None:
				seg = self.partial_segment(tli, segno)
			return seg
		raise RestoreRefused(f"the WAL chunks of segment "
		                     f"{self.segment_name(tli, segno)} keep going")

	def forget_listing(self, tli):
		"""The next chunks_of() lists the timeline's chunks again."""
		try:
			os.unlink(os.path.join(self.chunk_cache(tli), 'listed'))
		except FileNotFoundError:
			pass

	def archived_since(self, tli, segno):
		"""The segment archived after assemble_segment looked for it: the
		shipper then removes the chunks inside it, and the chunks listed
		after that can miss the segment's start, or all of it."""
		data = self.read_object(f"wal/{self.segment_name(tli, segno)}")
		return bytearray(data) if data is not None else None

	def partial_segment(self, tli, segno):
		"""The segment the WAL ended in at a shutdown without the shipper:
		a restore takes it only where no chunk has the segment's start."""
		data = self.read_object(f"wal/{self.segment_name(tli, segno)}.partial")
		return bytearray(data) if data is not None else None

	def assemble_segment(self, tli, segno):
		size = self.segment_size
		name = self.segment_name(tli, segno)
		data = self.read_object(f"wal/{name}")
		if data is not None:
			return bytearray(data)
		lo, hi = segno * size, (segno + 1) * size
		# the segment a stop without the shipper left: what no chunk puts
		# over it is that run's WAL (and past its end, what the file held,
		# which recovery reads as the end, as from pg_wal)
		partial = self.partial_segment(tli, segno)
		seg = partial if partial is not None else bytearray(size)
		pos = None
		start_of = self.timeline_start(tli)
		if start_of is not None and lo < start_of[1]:
			parent = self.restore_segment(start_of[0], segno)
			if parent is None:
				return None
			switch = min(start_of[1], hi)
			seg[0:switch - lo] = parent[0:switch - lo]
			pos = switch
		first = None
		for seq, magic, start, end in self.chunks_of(tli, lo):
			if magic == TERMINATOR_MAGIC:
				continue
			if magic != CHUNK_MAGIC:
				raise Exception(f"chunk {seq} of timeline {tli} is not a chunk")
			if end <= lo or start >= hi:
				continue
			if pos is None:
				# the chunks of a timeline follow one another from its first
				# one on, and none that reaches into a segment not archived
				# yet is removed: a later one is the first here only when the
				# one before it is missing
				if first is None:
					first = self.first_chunk_seq(tli)
				pos = lo if seq != first or partial is not None \
				    else max(start, lo)
			if start > pos and partial is not None:
				# what a run whose stop had no shipper wrote before the chunks
				# of the next: the segment it left has it
				pos = start
			if start > pos:
				# a chunk missing between two that are there: the WAL
				# after it was acknowledged and is not here; ending the
				# segment at the gap would end recovery before it
				raise ChunkGap(
				    f"WAL chunks of timeline {tli} miss {pos:X} to {start:X} "
				    f"(before chunk {seq})")
			body = self.chunk_body(tli, seq)
			zero_from = max(start, lo)
			if pos > zero_from:
				seg[zero_from - lo:pos - lo] = bytes(pos - zero_from)
			a, b = max(start, lo), min(end, hi)
			seg[a - lo:b - lo] = body[a - start:b - start]
			pos = end
		if pos is None and partial is None:
			return None
		return seg

	def restore(self, fname, path):
		"""restore_command: 0 when FILE is put at PATH, RESTORE_NOT_FOUND when
		the bucket does not have it, and above 125 when it cannot tell:
		PostgreSQL then stops recovery rather than take the file as the end
		of the WAL.  The restore_command this script writes turns
		RESTORE_NOT_FOUND into 1 and any other failure into 128."""
		try:
			return self.restore_file(fname, path)
		except Exception as e:
			print(f"orioledb_s3_loader: cannot restore {fname}: {e}",
			      file=sys.stderr)
			# a standby only waits: it asks again, and never promotes on it
			if os.path.exists('standby.signal'):
				return RESTORE_NOT_FOUND
			return 128

	def restore_file(self, fname, path):
		if re.fullmatch(r'[0-9A-F]{8}\.history', fname):
			data = self.read_object(f"wal/{fname}")
		elif re.fullmatch(r'[0-9A-F]{24}', fname):
			data = self.restore_segment(int(fname[0:8], 16),
			                            self.segno_of_name(fname))
		else:
			data = None
		if data is None:
			return RESTORE_NOT_FOUND
		with open(path, 'wb') as f:
			f.write(data)
		return 0

	def list_objects(self, bucket_name, directory, start_after=None):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		prefix = os.path.join(self.prefix, directory)
		extra = {'StartAfter': start_after} if start_after else {}
		for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix,
		                               **extra):
			if 'Contents' in page:
				page_objs = [x["Key"] for x in page['Contents']]
				objects.extend(page_objs)

		return objects

	# Reimplement os.dirs so it sets mode for intermediate dirs also
	def makedirs(self, name, mode=0o777, exist_ok=False):
		"""makedirs(name [, mode=0o777][, exist_ok=False])

		Super-mkdir; create a leaf directory and all intermediate ones.  Works like
		mkdir, except that any intermediate path segment (not just the rightmost)
		will be created if it does not exist. If the target directory already
		exists, raise an OSError if exist_ok is False. Otherwise no exception is
		raised.  This is recursive.

		"""
		head, tail = os.path.split(name)
		if not tail:
			head, tail = os.path.split(head)
		if head and tail and not os.path.exists(head):
			try:
				self.makedirs(head, mode, exist_ok=exist_ok)
			except FileExistsError:
				# Defeats race condition when another thread created the path
				pass
			cdir = os.curdir
			if isinstance(tail, bytes):
				cdir = bytes(os.curdir, 'ASCII')
			if tail == cdir:  # xxx/newdir/. exists if xxx/newdir exists
				return
		try:
			os.mkdir(name, mode)
		except OSError:
			# Cannot rely on checking for EEXIST, since the operating system
			# could give priority to other errors like EACCES or EROFS
			if not exist_ok or not os.path.isdir(name):
				raise

	def download_file(self, bucket_name, file_key, local_path) -> bool:
		try:
			transfer_config = TransferConfig(use_threads=False,
			                                 max_concurrency=1)
			if file_key[-1] == '/':
				dirs = local_path
			else:
				dirs = '/'.join(local_path.split('/')[:-1])
			self.makedirs(dirs, exist_ok=True, mode=0o700)
			if file_key[-1] != '/':
				self.s3.download_file(bucket_name,
				                      file_key,
				                      local_path,
				                      Config=transfer_config)
			if self.verbose:
				print(f"{file_key} -> {local_path}", flush=True)
			if re.match(r'.*/orioledb_data/small_files_\d+$', local_path):
				base_dir = '/'.join(local_path.split('/')[:-2])
				with open(local_path, 'rb') as file:
					data = file.read()
				numFiles = struct.unpack('i', data[0:4])[0]
				for i in range(0, numFiles):
					(nameOffset, dataOffset,
					 dataLength) = struct.unpack('iii',
					                             data[4 + i * 12:16 + i * 12])
					name = data[nameOffset:data.find(b'\0', nameOffset
					                                 )].decode('ascii')
					fullname = f"{base_dir}/{name}"
					if self.verbose:
						print(f"{file_key} -> {fullname}", flush=True)
					self.makedirs(os.path.dirname(fullname),
					              exist_ok=True,
					              mode=0o700)
					with open(fullname, 'wb') as file:
						file.write(data[dataOffset:dataOffset + dataLength])
					os.chmod(fullname, 0o600)
				os.unlink(local_path)

		except ClientError as e:
			if e.response['Error']['Code'] == "404":
				print(f"File not found: {file_key}")
			else:
				print(f"An error occurred: {e}")
			self._error_occurred.set()
			return False

		return True


CHUNK_MAGIC = 0x3143574f
TERMINATOR_MAGIC = 0x3154574f


class ChunkGone(Exception):
	"""A chunk listed and gone before it was read."""


class RestoreRefused(Exception):
	"""The bucket's WAL cannot be put together without leaving some out."""


class ChunkGap(RestoreRefused):
	"""A chunk missing between two the listing had."""


PIN_TTL = 600


class ReaderPin:
	"""
	A pin in the bucket's readers object, as a reader's: the GC keeps every
	checkpoint from the lowest unexpired pin on.  Put by a conditional
	replace (every reader edits the object), renewed every third of its TTL
	while held, and checked against the GC's published retention point after
	it is put: pinned before the GC reads the pins, it is counted; pinned
	after, the retention point it sees is the GC's.  Anything that reads a
	checkpoint's objects outside a server takes one the same way.
	"""

	def __init__(self, loader, chkp):
		self.loader = loader
		self.chkp = chkp
		self.id = "loader-" + os.urandom(8).hex()
		self.stop = Event()
		self.put(chkp)
		retain = self.retain()
		if chkp < retain:
			self.remove()
			raise Exception(f"The S3 bucket no longer keeps checkpoint {chkp}: "
			                f"its GC keeps checkpoints from {retain} on")
		self.thread = Thread(target=self.renew, daemon=True)
		self.thread.start()

	def key(self):
		return os.path.join(self.loader.prefix, 'readers')

	def edit(self, line):
		s3 = self.loader.s3
		for _ in range(100):
			try:
				obj = s3.get_object(Bucket=self.loader.bucket_name, Key=self.key())
				body = obj['Body'].read().decode()
				etag = obj['ETag']
				now = int(parsedate_to_datetime(
				    obj['ResponseMetadata']['HTTPHeaders']['date']).timestamp())
			except ClientError as e:
				if e.response['Error']['Code'] not in ("404", "NoSuchKey"):
					raise
				body, etag = "", None
				now = int(parsedate_to_datetime(
				    e.response['ResponseMetadata']['HTTPHeaders']['date']).timestamp())
			kept = []
			for other in body.splitlines():
				fields = other.split()
				if len(fields) >= 3 and fields[0] != self.id and \
				    int(fields[2]) >= now:
					kept.append(other)
			if line is not None:
				kept.append(line(now))
			new = "".join(f"{x}\n" for x in kept).encode()
			cond = {'IfMatch': etag} if etag else {'IfNoneMatch': '*'}
			try:
				s3.put_object(Bucket=self.loader.bucket_name, Key=self.key(),
				              Body=new, **cond)
				return
			except ClientError as e:
				if e.response['Error']['Code'] not in (
				    "PreconditionFailed", "ConditionalRequestConflict", "412",
				    "409", "NoSuchKey", "404"):
					raise
			time.sleep(0.1)
		raise Exception("could not write the loader's pin to the S3 bucket")

	def put(self, chkp):
		# the TTL too: the GC passes a pin below its floor only once a
		# renewal interval (a third of it) has gone by
		self.edit(lambda now: f"{self.id} {chkp} {now + PIN_TTL} {PIN_TTL}")

	def remove(self):
		self.edit(None)

	def retain(self):
		"""The oldest checkpoint the GC keeps whole: R, and what it folded
		and collected up to."""
		body = self.loader.read_object('gc')
		if body is None:
			return 0
		kept = 0
		for line in body.decode().splitlines():
			key, _, value = line.partition(' ')
			if key in ('retain', 'folded', 'collected'):
				kept = max(kept, int(value))
		return kept

	def renew(self):
		while not self.stop.wait(PIN_TTL / 3):
			self.put(self.chkp)

	def release(self):
		self.stop.set()
		self.thread.join()
		self.remove()


XIDMAP_RANGE_ENCODED = 2**64 - 1

# --restore's answer for a file the bucket does not have (below 126, so a
# restore_command that calls it directly reads it as "not there" too)
RESTORE_NOT_FOUND = 99


def decode_xidmap_items(data, count, path):
	"""The xidmap items of a range file: per item the differences of its csn
	and its commit LSN from the item before, as zigzag varints."""
	out = bytearray(count * 16)
	values = [0, 0]
	pos = 0
	n = len(data)
	for i in range(count * 2):
		zz = 0
		shift = 0
		while True:
			if pos >= n:
				raise Exception(f"{path} ends before its {count} items")
			b = data[pos]
			pos += 1
			zz |= (b & 0x7F) << shift
			if b < 0x80:
				break
			shift += 7
		d = (zz >> 1) ^ -(zz & 1)
		v = (values[i & 1] + d) & 0xFFFFFFFFFFFFFFFF
		values[i & 1] = v
		struct.pack_into('<Q', out, i * 8, v)
	if pos != n:
		raise Exception(f"{path} goes on past its {count} items")
	return bytes(out)


def parse_root_body(text):
	root = {'epochs': []}
	for line in text.splitlines():
		key, _, value = line.partition(' ')
		if key == 'epoch':
			first, token = value.split(' ')
			root['epochs'].append((int(first), int(token)))
		else:
			root[key] = value
	return root


def end_after_record(seg, pos, block_size):
	"""SEG with zeros after the WAL record at offset POS of it, if that
	record ends in it.  Each page starts with a header (a long one on the
	segment's first page), which a record runs over."""
	short_header, long_header = 24, 40

	def skip(p, n):
		data = b''
		while n > 0:
			if p % block_size == 0:
				p += long_header if p == 0 else short_header
			take = min(n, block_size - p % block_size)
			data += bytes(seg[p:p + take])
			p += take
			n -= take
		return p, data

	_, head = skip(pos, 4)
	end, _ = skip(pos, struct.unpack('<I', head)[0])
	if end > len(seg):
		return seg
	seg = bytearray(seg)
	seg[end:] = bytes(len(seg) - end)
	return seg


def server_s3_settings(data_dir):
	"""orioledb.s3_accesskey/secretkey/region as AWS_* variables, from the
	server's configuration files and the options it was started with.  The
	files' include, include_if_exists and include_dir directives are
	followed, in order, as the server reads them: the keys are best kept in
	an included file outside the data directory, since the checkpoints put
	postgresql.conf and postgresql.auto.conf into the bucket."""
	names = {
	    'orioledb.s3_accesskey': 'AWS_ACCESS_KEY_ID',
	    'orioledb.s3_secretkey': 'AWS_SECRET_ACCESS_KEY',
	    'orioledb.s3_region': 'AWS_DEFAULT_REGION'
	}
	found = {}
	# a setting starts its line (a commented one does not count); in
	# postmaster.opts each is one quoted argument
	in_conf = re.compile(r"^\s*(orioledb\.s3_(?:accesskey|secretkey|region))"
	                     r"\s*=\s*'?([^'\s\"#]+)'?")
	include = re.compile(r"^\s*(include|include_if_exists|include_dir)"
	                     r"\s*=?\s*'([^']+)'")
	in_opts = re.compile(r'"(orioledb\.s3_(?:accesskey|secretkey|region))'
	                     r'=([^"]+)"')

	def read_conf(path, depth):
		if depth > 10:
			return
		try:
			with open(path) as f:
				lines = f.read().splitlines()
		except OSError:
			return
		here = os.path.dirname(path)
		for line in lines:
			m = in_conf.match(line)
			if m:
				found[names[m.group(1)]] = m.group(2)
				continue
			m = include.match(line)
			if not m:
				continue
			target = os.path.join(here, m.group(2))
			if m.group(1) == 'include_dir':
				try:
					entries = sorted(os.listdir(target))
				except OSError:
					continue
				for entry in entries:
					if entry.endswith('.conf') and not entry.startswith('.'):
						read_conf(os.path.join(target, entry), depth + 1)
			else:
				read_conf(target, depth + 1)

	read_conf(os.path.join(data_dir, 'postgresql.conf'), 0)
	read_conf(os.path.join(data_dir, 'postgresql.auto.conf'), 0)
	try:
		with open(os.path.join(data_dir, 'postmaster.opts')) as f:
			text = f.read()
		for m in in_opts.finditer(text):
			found[names[m.group(1)]] = m.group(2)
	except OSError:
		pass
	return found


def parse_lsn(text):
	hi, lo = text.split('/')
	return (int(hi, 16) << 32) | int(lo, 16)


def parse_manifest(body):
	"""The manifest of a checkpoint (src/s3/manifest.c)."""
	m = {
	    'complete': True,
	    'data': [],
	    'files': {},
	    'small': {},
	    'bundles': {},
	    'undo': {},
	    'maps': [],
	    'trees': {}
	}
	for line in body.splitlines():
		kind, _, rest = line.partition(' ')
		if kind == 'format' and int(rest) != 1:
			raise Exception(f"Unknown manifest format {rest}")
		elif kind == 'checkpoint':
			m['checkpoint'] = int(rest)
		elif kind == 'complete':
			m['complete'] = m['complete'] and rest == '1'
		elif kind == 'data':
			m['data'].append(rest)
		elif kind in ('file', 'small'):
			n, _, path = rest.partition(' ')
			m['files' if kind == 'file' else 'small'][path] = int(n)
		elif kind == 'bundles':
			n, count = rest.split(' ')
			m['bundles'][int(n)] = int(count)
		elif kind == 'undoref':
			n, _, name = rest.partition(' ')
			m['undo'][name] = int(n)
		elif kind == 'map':
			datoid, relnode, _ = rest.split(' ')
			m['maps'].append((int(datoid), int(relnode)))
		elif kind == 'tree':
			datoid, relnode, spcoid, mapnum = rest.split(' ')
			m['trees'][(int(datoid), int(relnode))] = int(mapnum)
	return m


_CRC32C_TABLE = []
for _i in range(256):
	_c = _i
	for _ in range(8):
		_c = (_c >> 1) ^ 0x82F63B78 if _c & 1 else _c >> 1
	_CRC32C_TABLE.append(_c)


def crc32c(data: bytes) -> int:
	crc = 0xFFFFFFFF
	for b in data:
		crc = _CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
	return crc ^ 0xFFFFFFFF


def get_control_data(data_dir: str):
	"""
	Return contents of pg_control file.
	"""

	# this one is tricky (blame PG 9.4)
	_params = [testgres.get_bin_path("pg_controldata")]
	_params += ["-D"]
	_params += [data_dir]

	data = testgres.utils.execute_utility(_params)

	out_dict = {}

	for line in data.splitlines():
		key, _, value = line.partition(':')
		out_dict[key.strip()] = value.strip()

	return out_dict


def get_orioledb_control_data(data_dir: str):
	"""
	Return contents of OrioleDB control file.
	"""

	f = open(f"{data_dir}/orioledb_data/control", 'rb')
	data = f.read(8 * 15)
	# CheckpointControl.undoInfo[i].checkpointRetain{Start,End}Location
	(undoRegularStartLocation,
	 undoRegularEndLocation) = struct.unpack('QQ', data[8 * 10:8 * 12])
	(undoSystemStartLocation,
	 undoSystemEndLocation) = struct.unpack('QQ', data[8 * 13:8 * 15])
	f.close()

	dict = {
	    'undoRegularStartLocation': undoRegularStartLocation,
	    'undoRegularEndLocation': undoRegularEndLocation,
	    'undoSystemStartLocation': undoSystemStartLocation,
	    'undoSystemEndLocation': undoSystemEndLocation
	}

	return dict


def resolve_localhost_names():
	"""The server's curl takes every name under .localhost for the loopback
	(RFC 6761), so a bucket in the host such as bucket.localhost works there;
	connect the same way."""
	from urllib3.util import connection
	create_connection = connection.create_connection

	def create(address, *args, **kwargs):
		host, port = address
		if host.endswith('.localhost'):
			host = '127.0.0.1'
		return create_connection((host, port), *args, **kwargs)

	connection.create_connection = create


if __name__ == '__main__':
	resolve_localhost_names()
	loader = OrioledbS3ObjectLoader()
	loader.parse_args()
	loader.run()
