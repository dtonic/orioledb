#!/usr/bin/env python3
# coding: utf-8

import io
import os
import re
import struct
import sys
import tempfile
import unittest

from botocore.exceptions import ClientError
from unittest import mock

import orioledb_s3_loader as loader

SEG = 1 << 20  # a 1 MiB WAL segment keeps the fixtures small


class FakeS3:
	"""The two boto3 calls the loader's restore path makes, over a dict of
	keys: get_object (whole or a byte range; NoSuchKey when absent) and the
	list_objects_v2 paginator (Prefix, StartAfter)."""

	def __init__(self, objects):
		self.objects = dict(objects)

	def get_object(self, Bucket, Key, Range=None):
		if Key not in self.objects:
			raise ClientError({'Error': {'Code': 'NoSuchKey', 'Message': Key}},
			                  'GetObject')
		data = self.objects[Key]
		if Range is not None:
			a, b = re.fullmatch(r'bytes=(\d+)-(\d+)', Range).groups()
			data = data[int(a):int(b) + 1]
		return {'Body': io.BytesIO(data)}

	def get_paginator(self, name):
		assert name == 'list_objects_v2'
		return self

	def paginate(self, Bucket, Prefix, StartAfter=None):
		keys = sorted(k for k in self.objects
		              if k.startswith(Prefix) and (StartAfter is None or k > StartAfter))
		yield {'Contents': [{'Key': k} for k in keys]} if keys else {}


def chunk(token, start, end, body=None, magic=loader.CHUNK_MAGIC):
	"""A WAL chunk as the shipper puts it: a 32-byte header (magic, pad,
	token, start, end) and the bytes of [start, end)."""
	if body is None:
		body = bytes([token & 0xFF]) * (end - start)
	assert len(body) == end - start
	return struct.pack('<IIQQQ', magic, 0, token, start, end) + body


def terminator(token):
	return struct.pack('<IIQQQ', loader.TERMINATOR_MAGIC, 0, token, 0, 0)


class S3LoaderTest(unittest.TestCase):
	"""assemble_segment's decision table (the design doc, *Recovery and the
	loader*) and decode_xidmap_items, on a fake bucket."""

	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()

	def tearDown(self):
		self.tmp.cleanup()

	def make_loader(self, objects):
		l = loader.OrioledbS3ObjectLoader()
		l.s3 = FakeS3(objects)
		l.bucket_name = 'bucket'
		l.prefix = ''
		l.segment_size = SEG
		l.data_dir = self.tmp.name
		return l

	def seg_name(self, tli, segno):
		return f"{tli:08X}{0:08X}{segno:08X}"

	def chunk_key(self, tli, seq):
		return f"wal/{tli:08X}/chunks/{seq:020d}"

	def test_archived_segment_is_taken_as_is(self):
		data = bytes(range(256)) * (SEG // 256)
		l = self.make_loader({f"wal/{self.seg_name(1, 3)}": data})
		self.assertEqual(data, bytes(l.assemble_segment(1, 3)))
		path = os.path.join(self.tmp.name, 'out')
		self.assertEqual(0, l.restore(self.seg_name(1, 3), path))
		with open(path, 'rb') as f:
			self.assertEqual(data, f.read())

	def test_nothing_in_the_bucket_is_not_there(self):
		l = self.make_loader({})
		self.assertIsNone(l.assemble_segment(1, 3))
		self.assertEqual(loader.RESTORE_NOT_FOUND,
		                 l.restore(self.seg_name(1, 3), os.path.join(self.tmp.name, 'out')))
		self.assertEqual(loader.RESTORE_NOT_FOUND,
		                 l.restore("00000002.history", os.path.join(self.tmp.name, 'h')))

	def test_history_file_is_restored_as_is(self):
		l = self.make_loader({"wal/00000002.history": b"1\t0/1000000\tno reason\n"})
		path = os.path.join(self.tmp.name, 'h')
		self.assertEqual(0, l.restore("00000002.history", path))
		with open(path, 'rb') as f:
			self.assertEqual(b"1\t0/1000000\tno reason\n", f.read())

	def test_contiguous_chunks_are_laid_in_order_and_the_later_one_wins(self):
		lo = 2 * SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): chunk(1, lo, lo + 1000),
		    # overlaps the first one's tail: it wins there
		    self.chunk_key(1, 1): chunk(1, lo + 800, lo + 2000, body=b'B' * 1200),
		    self.chunk_key(1, 2): chunk(1, lo + 2000, lo + 2500, body=b'C' * 500),
		})
		seg = l.assemble_segment(1, 2)
		self.assertEqual(SEG, len(seg))
		self.assertEqual(bytes([1]) * 800, bytes(seg[0:800]))
		self.assertEqual(b'B' * 1200, bytes(seg[800:2000]))
		self.assertEqual(b'C' * 500, bytes(seg[2000:2500]))
		self.assertEqual(bytes(SEG - 2500), bytes(seg[2500:]))

	def test_a_chunk_reaching_over_the_segment_end_is_cut(self):
		lo = SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): chunk(1, lo, lo + SEG + 100),
		})
		seg = l.assemble_segment(1, 1)
		self.assertEqual(bytes([1]) * SEG, bytes(seg))
		# and the next segment starts with that chunk's tail
		nxt = l.assemble_segment(1, 2)
		self.assertEqual(bytes([1]) * 100, bytes(nxt[0:100]))
		self.assertEqual(bytes(SEG - 100), bytes(nxt[100:]))

	def test_a_gap_between_chunks_is_refused(self):
		lo = SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): chunk(1, lo, lo + 1000),
		    self.chunk_key(1, 1): chunk(1, lo + 1500, lo + 2000),
		})
		with self.assertRaises(loader.ChunkGap):
			l.assemble_segment(1, 1)
		# restore_command: a gap is "cannot tell", never "not there"
		self.assertEqual(128, l.restore(self.seg_name(1, 1), os.path.join(self.tmp.name, 'o')))

	def test_a_first_chunk_short_of_the_segment_start_is_refused(self):
		"""The chunks of a timeline follow one another from its first chunk
		on; one that is not the first and does not cover the segment's
		start means the chunk before it is missing."""
		lo = SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): chunk(1, 0, 1000),  # the segment before
		    self.chunk_key(1, 2): chunk(1, lo + 500, lo + 1000),  # 1 missing
		})
		with self.assertRaises(loader.ChunkGap):
			l.assemble_segment(1, 1)

	def test_the_timelines_first_chunk_may_start_inside_the_segment(self):
		lo = SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): chunk(1, lo + 500, lo + 1000),
		})
		seg = l.assemble_segment(1, 1)
		self.assertEqual(bytes(500), bytes(seg[0:500]))
		self.assertEqual(bytes([1]) * 500, bytes(seg[500:1000]))

	def test_a_partial_segment_alone_is_taken(self):
		partial = b'P' * SEG
		l = self.make_loader({f"wal/{self.seg_name(1, 4)}.partial": partial})
		self.assertEqual(partial, bytes(l.assemble_segment(1, 4)))

	def test_chunks_are_laid_over_a_partial_segment(self):
		"""What no chunk puts over the .partial is the stopped run's WAL:
		a chunk starting past the partial's content is not a gap."""
		lo = 4 * SEG
		partial = b'P' * SEG
		l = self.make_loader({
		    f"wal/{self.seg_name(1, 4)}.partial": partial,
		    self.chunk_key(1, 0): chunk(1, lo + 3000, lo + 4000, body=b'C' * 1000),
		})
		seg = l.assemble_segment(1, 4)
		self.assertEqual(b'P' * 3000, bytes(seg[0:3000]))
		self.assertEqual(b'C' * 1000, bytes(seg[3000:4000]))
		self.assertEqual(b'P' * (SEG - 4000), bytes(seg[4000:]))

	def test_terminators_are_skipped_and_fence_the_older_writer(self):
		"""Each writer start puts a terminator; the chunks of the writer it
		fenced off (a smaller token) that come after it put nothing."""
		lo = SEG
		l = self.make_loader({
		    self.chunk_key(1, 0): terminator(5),
		    self.chunk_key(1, 1): chunk(5, lo, lo + 1000),
		    self.chunk_key(1, 2): terminator(7),
		    # the old writer went on after being fenced: ignored
		    self.chunk_key(1, 3): chunk(5, lo + 1000, lo + 3000, body=b'X' * 2000),
		    self.chunk_key(1, 4): chunk(7, lo + 1000, lo + 2000, body=b'N' * 1000),
		    "wal/00000001/terminators/0": b'',
		    "wal/00000001/terminators/2": b'',
		})
		seg = l.assemble_segment(1, 1)
		self.assertEqual(bytes([5]) * 1000, bytes(seg[0:1000]))
		self.assertEqual(b'N' * 1000, bytes(seg[1000:2000]))
		self.assertEqual(bytes(SEG - 2000), bytes(seg[2000:]))

	def test_before_the_switch_point_the_segment_is_the_parents(self):
		switch = 2 * SEG + 4096
		parent = bytes([9]) * SEG
		l = self.make_loader({
		    "wal/00000002.history": f"1\t0/{switch:X}\tswitch\n".encode(),
		    f"wal/{self.seg_name(1, 2)}": parent,
		    self.chunk_key(2, 0): chunk(3, switch, switch + 1000, body=b'T' * 1000),
		})
		seg = l.assemble_segment(2, 2)
		self.assertEqual(bytes([9]) * 4096, bytes(seg[0:4096]))
		self.assertEqual(b'T' * 1000, bytes(seg[4096:5096]))
		self.assertEqual(bytes(SEG - 5096), bytes(seg[5096:]))
		# without the parent's segment nothing can be assembled
		del l.s3.objects[f"wal/{self.seg_name(1, 2)}"]
		l.forget_listing(2)
		self.assertIsNone(l.assemble_segment(2, 2))

	def test_a_chunk_removed_between_list_and_get_is_listed_again(self):
		"""A writer going on archives a segment and removes its chunks while
		a load reads the timeline's chunks for where they end: the chunk it
		listed is gone when it asks for it, and a new listing has it no
		more."""

		class RemovedOnGet(FakeS3):

			def __init__(self, objects, key):
				super().__init__(objects)
				self.key = key

			def get_object(self, Bucket, Key, Range=None):
				if Key == self.key:
					del self.objects[Key]
					self.key = None
				return super().get_object(Bucket, Key, Range)

		lo = SEG
		l = self.make_loader({
		    f"wal/{self.seg_name(1, 1)}":
		    bytes([1]) * SEG,
		    self.chunk_key(1, 7):
		    chunk(3, lo, lo + SEG),
		    self.chunk_key(1, 8):
		    chunk(3, lo + SEG, lo + SEG + 500),
		})
		l.s3 = RemovedOnGet(l.s3.objects, self.chunk_key(1, 7))
		l.checkpoint_only = False
		os.makedirs(os.path.join(self.tmp.name, 'orioledb_data'))
		l.download_wal({
		    "Bytes per WAL segment": str(SEG),
		    "Latest checkpoint's REDO WAL file": self.seg_name(1, 1),
		    "Latest checkpoint location": f"0/{lo + 100:X}",
		    "WAL block size": "8192",
		})
		with open(os.path.join(self.tmp.name, 'orioledb_data',
		                       's3_wal_seq')) as f:
			self.assertEqual(f"1 9 {lo + SEG + 500}\n", f.read())

	def test_decode_xidmap_items(self):
		def zigzag(v):
			zz = (v << 1) ^ (v >> 63) if v >= 0 else ((-v) << 1) - 1
			out = bytearray()
			while True:
				b = zz & 0x7F
				zz >>= 7
				if zz:
					out.append(b | 0x80)
				else:
					out.append(b)
					return bytes(out)

		items = [(10, 1000), (10, 1200), (12, 900), (12, 900)]
		data = bytearray()
		prev = (0, 0)
		for csn, lsn in items:
			data += zigzag(csn - prev[0]) + zigzag(lsn - prev[1])
			prev = (csn, lsn)
		out = loader.decode_xidmap_items(bytes(data), len(items), 'x.xidrange')
		self.assertEqual(len(items) * 16, len(out))
		self.assertEqual(items,
		                 [struct.unpack_from('<QQ', out, i * 16) for i in range(len(items))])
		with self.assertRaises(Exception):
			loader.decode_xidmap_items(bytes(data[:-1]), len(items), 'short')
		with self.assertRaises(Exception):
			loader.decode_xidmap_items(bytes(data) + b'\x00', len(items), 'long')
		self.assertEqual(b'', loader.decode_xidmap_items(b'', 0, 'empty'))


def head_bucket_error(code):
	return ClientError({'Error': {'Code': code, 'Message': code}}, 'HeadBucket')


class FakeProbeS3:
	"""boto3.client("s3", ...) as parse_args probes with it: head_bucket
	answers from `answers`, keyed by (endpoint_url, bucket); None is a
	bucket there, anything absent a 404."""

	def __init__(self, answers, endpoint_url):
		self.answers = answers
		self.endpoint_url = endpoint_url

	def head_bucket(self, Bucket):
		answer = self.answers.get((self.endpoint_url, Bucket),
		                          head_bucket_error('404'))
		if answer is not None:
			raise answer


class S3LoaderEndpointTest(unittest.TestCase):
	"""parse_args' choice between a bucket in the endpoint's host
	(virtual-hosted) and the prefix's first segment (path-style)."""

	def resolve(self, endpoint, prefix, answers):

		def client(service, endpoint_url=None, verify=None, config=None):
			return FakeProbeS3(answers, endpoint_url)

		argv = [
		    'orioledb_s3_loader.py', '--endpoint', endpoint, '--prefix', prefix,
		    '--data-dir', 'unused'
		]
		with mock.patch.object(sys, 'argv', argv), \
		     mock.patch.object(loader.boto3, 'client', client):
			l = loader.OrioledbS3ObjectLoader()
			l.parse_args()
		return l.resolved

	def test_a_path_style_host_with_dots_takes_the_bucket_from_the_prefix(
	        self):
		# RustFS behind its Service name: the first label is no bucket, and
		# RustFS answers that HeadBucket with 501
		host = 'http://rustfs.rustfs.svc.cluster.local:9000'
		answers = {
		    ('http://rustfs.svc.cluster.local:9000', 'rustfs'):
		    head_bucket_error('501'),
		    (host, 'bucket'): None,
		}
		self.assertEqual((host, 'bucket', 'orioledb', 'path'),
		                 self.resolve(host, 'bucket/orioledb', answers))

	def test_a_virtual_hosted_endpoint_keeps_the_bucket_in_the_host(self):
		answers = {('https://s3.ap-northeast-2.amazonaws.com', 'bucket'): None}
		self.assertEqual(('https://s3.ap-northeast-2.amazonaws.com', 'bucket',
		                  'orioledb', 'virtual'),
		                 self.resolve(
		                     'https://bucket.s3.ap-northeast-2.amazonaws.com',
		                     'orioledb', answers))

	def test_a_bucket_found_nowhere_reports_both_answers(self):
		# a bucket in the host this client may not read: the 403 is kept
		answers = {
		    ('https://s3.ap-northeast-2.amazonaws.com', 'bucket'):
		    head_bucket_error('403')
		}
		with self.assertRaisesRegex(Exception,
		                            r'endpoint: .*\(403\).*prefix: .*\(404\)'):
			self.resolve('https://bucket.s3.ap-northeast-2.amazonaws.com',
			             'orioledb', answers)


if __name__ == "__main__":
	unittest.main()
