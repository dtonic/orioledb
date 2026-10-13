import inspect
import json
import logging
import os
import io
import threading
import struct
import re
from re import RegexFlag
import runpy
import socket
from socket import AddressFamily, SocketKind
import sys
import time
import unittest
from threading import Thread
from typing import Optional, Any
from urllib3.util import connection

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from moto.s3.responses import S3Response
from moto.s3.exceptions import InvalidRequest, PreconditionFailed, \
 S3ClientError, InvalidRequest
from moto.core.common_types import TYPE_RESPONSE

from werkzeug.serving import BaseWSGIServer, make_server, make_ssl_devcert
import urllib3

from .base_test import BaseTest

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


def s3_test_attrs(**_):
	""" Stub decorator to parse in S3BaseTest.setUp """

	def attr_decorator(fn):
		return fn

	return attr_decorator


orig_put_object = S3Response.put_object
orig_get_object = S3Response.get_object
orig_list_objects_v2 = S3Response.list_objects_v2
orig_delete_object = S3Response.delete_object


# moto[s3] doesn't implement 409 HTTP error code therefore we need to do that
# by ourselves.
class ConditionalRequestConflict(S3ClientError):
	code = 409

	def __init__(self, failed_condition: str, **kwargs: Any):
		kwargs.setdefault("template", "condition_error")
		super().__init__(
		    "ConditionalRequestConflict",
		    "At least one of the pre-conditions you specified did not hold",
		    condition=failed_condition,
		    **kwargs,
		)


# Patch moto[s3]'s put_object() until it releases support "If-None-Match".
# Relevant PR was merged https://github.com/getmoto/moto/pull/8109 but wasn't
# released yet.
def mock_put_object(self) -> TYPE_RESPONSE:
	key_name = self.parse_key_name()
	if_none_match = self.headers.get("If-None-Match")

	if (if_none_match == "*"
	    and self.backend.get_object(self.bucket_name, key_name) is not None):
		raise PreconditionFailed("If-None-Match")

	return orig_put_object(self)


# Patch moto[s3]'s put_object() to alwasy return 409 code
def mock_put_object_conflict(self) -> TYPE_RESPONSE:
	raise ConditionalRequestConflict("If-None-Match")


def mock_moto_unkown_error(self) -> TYPE_RESPONSE:
	raise InvalidRequest("mock_moto_unkown_error")


class TransientServerError(S3ClientError):
	code = 503

	def __init__(self, **kwargs: Any):
		super().__init__("SlowDown", "Please reduce your request rate.",
		                 **kwargs)


class AccessDenied(S3ClientError):
	code = 403

	def __init__(self, **kwargs: Any):
		super().__init__("AccessDenied", "Access Denied", **kwargs)


class RevokedKeyPut:
	"""
	A put_object() that refuses (403) requests signed with a revoked access
	key, as a store does once a rotated key is withdrawn, and stores the
	others.
	"""

	def __init__(self, revoked, spare=()):
		self.revoked = revoked
		self.spare = spare
		self.refused = 0
		self.accepted = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if f"Credential={self.revoked}/" in response.headers.get(
		    "Authorization", "") and \
		    response.parse_key_name() not in self.spare:
			self.refused += 1
			raise AccessDenied()
		self.accepted += 1
		return orig_put_object(response)


class RevokedKey:
	"""
	Refuses (403) every request signed with a revoked access key: a
	put_object(), get_object() and list_objects_v2() in one, given the
	handler each stands for.
	"""

	def __init__(self, revoked):
		self.revoked = revoked
		self.refused = 0

	def __call__(self, response, handler) -> TYPE_RESPONSE:
		if f"Credential={self.revoked}/" in response.headers.get(
		    "Authorization", ""):
			self.refused += 1
			raise AccessDenied()
		return handler(response)


class AmbiguousPut:
	"""
	A put_object() that stores every `every`-th request for each kind of key
	in `contains` and then answers it 503, as when the answer to a PUT that
	went through is lost: the client cannot tell it from one that did not.
	"""

	def __init__(self, every=3, contains=('root', 'writer', '/chunks/',
	                                      '/parts/', 'manifests/')):
		self.every = every
		self.contains = contains
		self.seen = {c: 0 for c in contains}
		self.landed = {c: 0 for c in contains}

	def __call__(self, response) -> TYPE_RESPONSE:
		key = response.parse_key_name()
		kind = next((c for c in self.contains
		             if (key == c if '/' not in c else c in key)), None)
		if kind is not None:
			self.seen[kind] += 1
			if self.seen[kind] % self.every == 0:
				orig_put_object(response)
				self.landed[kind] += 1
				raise TransientServerError()
		return orig_put_object(response)


class HoldChunksOf:
	"""
	A put_object() that answers 503 to WAL chunks of writer token `token`
	until `release` is set, and stores everything else: one writer's
	shipper waits while another's goes on.
	"""

	def __init__(self):
		self.token = None
		self.release = False
		self.held = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if '/chunks/' in response.parse_key_name() and \
		    self.token is not None and not self.release:
			body = response.body
			if isinstance(body, str):
				body = body.encode()
			if len(body) >= 16 and \
			    struct.unpack('<I', body[0:4])[0] == 0x3143574f and \
			    struct.unpack('<Q', body[8:16])[0] == self.token:
				self.held += 1
				raise TransientServerError()
		return orig_put_object(response)


class ConflictingPut:
	"""
	A put_object() that answers the first `conflicts` PUTs of keys holding
	`contains` with 409 ConditionalRequestConflict, as AWS does to a
	conditional PUT racing another write of the key, without storing.
	"""

	def __init__(self, conflicts, contains='/parts/'):
		self.conflicts = conflicts
		self.contains = contains
		self.answered = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.contains in response.parse_key_name() and \
		    self.answered < self.conflicts:
			self.answered += 1
			raise ConditionalRequestConflict("If-None-Match")
		return orig_put_object(response)


class FailingGet:
	"""A get_object() that answers 503 to GETs of keys holding `contains`."""

	def __init__(self, contains):
		self.contains = contains
		self.failed = 0

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.contains in response.parse_key_name():
			self.failed += 1
			raise TransientServerError()
		return orig_get_object(response)


class FailingDelete:
	"""
	A delete_object() that answers the first `failures` DELETEs of keys
	holding `contains` with 503, removing nothing.
	"""

	def __init__(self, failures, contains='/parts/'):
		self.failures = failures
		self.contains = contains
		self.failed = 0
		self.lock = threading.Lock()

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.contains in response.parse_key_name():
			with self.lock:
				fail = self.failed < self.failures
				if fail:
					self.failed += 1
			if fail:
				raise TransientServerError()
		return orig_delete_object(response)


class HoldFirstGet:
	"""
	A get_object() that, once armed, holds the next GET of `key` (for which
	`when()`, if given, holds) until `release` is set, and serves every
	other request meanwhile: one process stops between two of its requests
	while the others go on.
	"""

	def __init__(self, key, when=None):
		self.key = key
		self.when = when
		self.armed = False
		self.entered = threading.Event()
		self.release = threading.Event()

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.armed and response.parse_key_name() == self.key and \
		    (self.when is None or self.when()):
			self.armed = False
			self.entered.set()
			self.release.wait(120)
		return orig_get_object(response)


class FlakyPutObject:
	"""
	A put_object() that answers the first `failures` requests for a part
	(or for keys under `prefix`) with 503, as a store under load does, then
	stores them.
	"""

	def __init__(self, failures, prefix=None, contains='/parts/'):
		self.failures = failures
		self.failed = 0
		self.prefix = prefix
		self.contains = contains

	def matches(self, key):
		if self.prefix is not None:
			return key.startswith(self.prefix)
		return self.contains in key

	def __call__(self, response) -> TYPE_RESPONSE:
		if self.matches(response.parse_key_name()) and \
		    self.failed < self.failures and not self.terminator(response):
			self.failed += 1
			raise TransientServerError()
		return orig_put_object(response)

	@staticmethod
	def terminator(response):
		# a start's terminator goes: the failures are the shipper's
		body = response.body
		if isinstance(body, str):
			body = body.encode()
		return len(body) >= 4 and \
		    struct.unpack('<I', body[0:4])[0] == 0x3154574f


# TODO: re-enable under valgrind once s3_header_mark_part_loaded race is fixed
@unittest.skipIf(
    os.environ.get('USE_VALGRIND') == '1', 'S3 tests flaky under valgrind')
class S3BaseTest(BaseTest):
	bucket_name = "test-bucket"
	host = "localhost"
	user = "ORDB_USER"
	region = "us-east-1"
	policy_name = "ORDB_POLICY"
	optional_prefix = "prefix/test/"

	def table_contents(self, node):
		"""Every user table's rows (its own, not its children's), and whether
		it is unlogged."""
		result = {}
		for name, persistence in node.execute("""
			SELECT c.oid::regclass::text, c.relpersistence FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind = 'r' AND n.nspname = 'public'
		"""):
			rows = node.execute(
			    f"SELECT coalesce(string_agg(t::text, ';' ORDER BY t::text), '') "
			    f"FROM ONLY {name} t")[0][0]
			result[name] = (persistence, rows)
		return result

	def assert_contents_after_crash(self, node, before):
		"""What a crash restart and a load from the bucket give: the logged
		tables' rows as they were, the unlogged ones empty."""
		expected = {name: (p, '' if p == 'u' else rows)
		            for name, (p, rows) in before.items()}
		self.assertEqual(expected, self.table_contents(node))
		node.stop()
		with self.initNode(self.getBasePort() + 1, 'tgsr') as loaded:
			self.loader.download(loaded.data_dir)
			loaded.append_conf(port=loaded.port)
			loaded.start()
			self.assertEqual(expected, self.table_contents(loaded))
			loaded.stop()
			loaded.cleanup()

	def setUp(self):
		super().setUp()

		# a port of this process's own, so test processes run side by side
		self.port = free_port()
		protocol = 'https'
		prefix = None
		host_param = None

		testMethod = getattr(self, self._testMethodName)
		source = inspect.getsource(testMethod)
		index = source.find("def ")
		attrs = []
		for raw_attrs in re.findall(r"@s3_test_attrs\(((?:\n|.)*?)\)",
		                            source[:index].strip(),
		                            RegexFlag.MULTILINE):
			for attr in raw_attrs.split(','):
				name, val = attr.strip().split('=')
				attrs += [[name.strip(), val.strip()]]
		for name, val in attrs:
			if name == "http" and val == "True":
				protocol = 'http'
			elif name == "prefix":
				prefix = eval(val)
			elif name == "host":
				host_param = eval(val)
		urllib3.util.connection.HAS_IPV6 = False
		host_name = self.host
		if host_param:
			host_name = host_param
		host_port = f"{protocol}://{host_name}:{self.port}"
		self.ssl_key = make_ssl_devcert(f'/tmp/ordb_test_key', cn=host_name)
		if protocol == 'https':
			ssl_context = self.ssl_key
			self.s3_cainfo = self.ssl_key[0]
		else:
			ssl_context = None
			self.s3_cainfo = None
		self.s3_server = MotoServerSSL(port=self.port, ssl_context=ssl_context)
		self.s3_server.start()

		self._orig_create_connection = connection.create_connection

		def s3_test_resolver(host):
			logging.debug(f"s3_test_resolver: {host}")
			if host == host_param:
				return "127.0.0.1"
			else:
				addrlist = socket.getaddrinfo(host, 80)
				for address in addrlist:
					if address[0] == AddressFamily.AF_INET and address[
					    1] == SocketKind.SOCK_STREAM:
						break
				return address[4][0]

		def patched_create_connection(address, *args, **kwargs):
			host, port = address
			hostname = s3_test_resolver(host)

			return self._orig_create_connection((hostname, port), *args,
			                                    **kwargs)

		connection.create_connection = patched_create_connection

		config = None
		if self.bucket_name in host_port:
			config = Config(s3={'addressing_style': 'virtual'})
		self.iam_client = boto3.client('iam',
		                               endpoint_url=host_port,
		                               aws_access_key_id="",
		                               aws_secret_access_key="",
		                               region_name=self.region,
		                               verify=self.s3_cainfo,
		                               config=config)
		self.iam_client.create_user(UserName=self.user)
		policy_document = {
		    "Version": "2012-10-17",
		    "Statement": {
		        "Effect": "Allow",
		        "Action": "*",
		        "Resource": "*"
		    }
		}
		policy = self.iam_client.create_policy(
		    PolicyName=self.policy_name,
		    PolicyDocument=json.dumps(policy_document))
		self.policy_arn = policy["Policy"]["Arn"]
		self.iam_client.attach_user_policy(UserName=self.user,
		                                   PolicyArn=self.policy_arn)
		response = self.iam_client.create_access_key(UserName=self.user)
		self.access_key_id = response["AccessKey"]["AccessKeyId"]
		self.secret_access_key = response["AccessKey"]["SecretAccessKey"]

		session = boto3.Session(aws_access_key_id=self.access_key_id,
		                        aws_secret_access_key=self.secret_access_key,
		                        region_name=self.region)
		host_port = f"{protocol}://{self.host}:{self.port}"
		self.client = session.client("s3",
		                             endpoint_url=host_port,
		                             verify=self.s3_cainfo,
		                             config=config)

		host_port = f"{protocol}://{host_name}:{self.port}"
		if not host_param and not prefix:
			host_port += f"/{self.bucket_name}/"
		self.loader = OrioledbS3Loader(self.access_key_id,
		                               self.secret_access_key, self.region,
		                               host_port, prefix, self.s3_cainfo)
		try:
			self.client.head_bucket(Bucket=self.bucket_name)
		except:
			self.client.create_bucket(Bucket=self.bucket_name)

	def tearDown(self):
		super().tearDown()
		objects = self.client.list_objects(Bucket=self.bucket_name)
		objects = objects.get("Contents", [])
		while objects != []:
			objects = list({"Key": x["Key"]} for x in objects)
			self.client.delete_objects(Bucket=self.bucket_name,
			                           Delete={"Objects": objects})
			objects = self.client.list_objects(Bucket=self.bucket_name)
			objects = objects.get("Contents", [])

		self.client.delete_bucket(Bucket=self.bucket_name)
		self.client.close()

		self.iam_client.detach_user_policy(UserName=self.user,
		                                   PolicyArn=self.policy_arn)
		self.iam_client.delete_policy(PolicyArn=self.policy_arn)
		self.iam_client.delete_user(UserName=self.user)
		self.s3_server.stop()
		connection.create_connection = self._orig_create_connection


class OrioledbS3Loader:

	def __init__(self,
	             aws_access_key_id,
	             aws_secret_access_key,
	             aws_region,
	             endpoint_url,
	             prefix=None,
	             verify=None):
		os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
		os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
		os.environ["AWS_DEFAULT_REGION"] = aws_region

		self._aws_region = aws_region
		self._endpoint_url = endpoint_url
		self._prefix = prefix
		self._verify = verify

	def download(self, path, verbose=False, checkpoint_only=False, as_of=None,
	             keep_parts=False, local_wal=False, repair_root=False):
		dir_path = os.path.dirname(os.path.realpath(__file__))
		old_argv = sys.argv
		sys.argv = [f"{dir_path}/../../orioledb_s3_loader.py"]
		sys.argv += ["--endpoint", self._endpoint_url]
		if self._prefix:
			sys.argv += ["--prefix", self._prefix]
		if self._verify:
			sys.argv += ["--cert-file", self._verify]
		sys.argv += ["-d", path]
		if verbose:
			sys.argv += ["--verbose"]
		if checkpoint_only:
			sys.argv += ["--checkpoint-only"]
		if as_of is not None:
			sys.argv += ["--as-of", str(as_of)]
		if keep_parts:
			sys.argv += ["--keep-parts"]
		if local_wal:
			sys.argv += ["--local-wal"]
		if path is None:
			sys.argv.remove("-d")
			sys.argv.remove(None)
			sys.argv += ["--repair-root" if repair_root else "--sweep"]
		runpy.run_path(sys.argv[0], None, "__main__")
		sys.argv = old_argv


def free_port() -> int:
	"""A TCP port nothing listens on right now."""
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(('', 0))
		return sock.getsockname()[1]


class MotoServerSSL:

	def __init__(self,
	             host: str = "localhost",
	             port: int = 0,
	             ssl_context=None):
		self._host = host
		self._port = port
		self._thread: Optional[Thread] = None
		self._server: Optional[BaseWSGIServer] = None
		self._server_ready = False
		self._ssl_context = ssl_context

	def _server_entry(self) -> None:
		backend = DomainDispatcherApplication(create_backend_app)
		puts = threading.Lock()

		def app(environ, start_response):
			# requests are served concurrently, as a store does; writes
			# (PUT, POST, DELETE) one at a time, so a conditional PUT is
			# judged and stored atomically against deletes too, and the
			# fault injectors count without races. The body is read before
			# the lock: a slow upload must not hold the other writes.
			if environ.get('REQUEST_METHOD') in ('PUT', 'POST', 'DELETE'):
				length = environ.get('CONTENT_LENGTH')
				if length:
					body = environ['wsgi.input'].read(int(length))
				elif environ.get('wsgi.input_terminated'):
					body = environ['wsgi.input'].read()  # chunked
				else:
					body = b''
				environ['wsgi.input'] = io.BytesIO(body)
				with puts:
					return list(backend(environ, start_response))
			return backend(environ, start_response)

		self._server = make_server(self._host,
		                           self._port,
		                           app,
		                           True,
		                           ssl_context=self._ssl_context,
		                           passthrough_errors=True)
		self._server_ready = True
		self._server.serve_forever()

	def start(self) -> None:
		self._thread = Thread(target=self._server_entry, daemon=True)
		self._thread.start()
		while not self._server_ready:
			time.sleep(0.1)

	def stop(self) -> None:
		self._server_ready = False
		if self._server:
			self._server.shutdown()

		self._thread.join()  # type: ignore[union-attr]
