/*-------------------------------------------------------------------------
 *
 * wal_shipper.c
 *		Ship WAL to S3 as it is written, so no commit is lost without a
 *		replica.
 *
 * The shipper connects to its own instance over the replication protocol
 * (application_name "s3", a physical slot) and uploads what it receives as
 * chunks: wal/<tli>/chunks/<seq>, seq dense within a timeline, each holding
 * {token, start_lsn, end_lsn, bytes} and put under If-None-Match.  It reports
 * as flushed only the end of the chunks that are up, in order.  With
 * synchronous_standby_names = 's3' a commit returns once its WAL is in the
 * bucket.  Completed segments still go up through the archive library; a
 * restore takes the segments and assembles what follows them from chunks.
 *
 * A put whose key is taken is judged by reading the chunk back: ours (an
 * earlier attempt that landed) is done; anything else means another writer
 * took over, and the shipper never reports past it.
 *
 * After a restart, clients wait in authentication until the shipper has
 * caught up with the WAL that local recovery replayed: a commit that was
 * waiting for the shipper when the instance went down is visible locally
 * and must not be seen before it is in the bucket.
 *
 * A writer that takes the bucket over (a data directory the loader made)
 * puts a terminator right after the last chunk before it recovers: from then
 * on the writer it fenced off cannot put a chunk, so it acknowledges no
 * commit the new writer's recovery does not see.  The recovery is an archive
 * recovery from the bucket, and it ends on a new timeline, whose history goes
 * up before its first chunk: the WAL the fenced writer still wrote on the old
 * timeline is not on the new one's way.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/wal_shipper.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "orioledb.h"

#include "s3/lease.h"
#include "s3/requests.h"
#include "s3/wal_shipper.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/timeline.h"
#include "access/xlogarchive.h"
#include "access/xlogrecovery.h"
#include "common/controldata_utils.h"
#include "archive/archive_module.h"
#include "catalog/pg_authid.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/condition_variable.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define SHIPPER_SLOT			"orioledb_s3_wal"
#define SHIPPER_APPNAME			"s3"
#define SEQ_FILENAME			ORIOLEDB_DATA_DIR "/s3_wal_seq"
#define CHUNK_MAGIC				0x3143574f	/* "OWC1" */
#define TERMINATOR_MAGIC		0x3154574f	/* "OWT1" */
#define RETIRED_FILENAME		ORIOLEDB_DATA_DIR "/s3_retired"
#define RESTORE_BUCKET_FILENAME	ORIOLEDB_DATA_DIR "/s3_restore_bucket"
#define CHUNK_MAX_BYTES			(16 * 1024 * 1024)
#define CHUNK_GC_INTERVAL_MS	10000
#define CHUNK_GC_SCAN_MAX		100000

bool		s3_wal_shipping = true;
int			s3_wal_chunk_delay = 1;
char	   *s3_wal_conninfo = NULL;

typedef struct
{
	uint32		magic;
	uint32		timeline;
	uint64		token;
	uint64		startLsn;
	uint64		endLsn;
} ChunkHeader;

typedef struct
{
	pg_atomic_uint64 flushedLsn;	/* the end of the chunks that are up */
	pg_atomic_uint64 catchUpLsn;	/* clients wait until flushedLsn reaches it */
	pg_atomic_uint64 replayEndLsn;	/* where recovery found the WAL to end */
	pg_atomic_uint32 running;	/* the shipper is up and streaming */
	ConditionVariable flushedCV;
} S3WalShipperShared;

static S3WalShipperShared *shipper = NULL;

/*
 * The chunks this run knows to be in the bucket, oldest first: removed once
 * an archived segment holds their WAL.
 */
typedef struct
{
	uint64		seq;
	XLogRecPtr	end;
} ShippedChunk;

static ShippedChunk *shipped = NULL;
static int	shippedHead = 0,
			shippedTail = 0,
			shippedCap = 0;

static void
shipped_push(uint64 seq, XLogRecPtr end)
{
	if (shippedTail == shippedCap)
	{
		if (shippedHead > 0)
		{
			memmove(shipped, shipped + shippedHead,
					sizeof(ShippedChunk) * (shippedTail - shippedHead));
			shippedTail -= shippedHead;
			shippedHead = 0;
		}
		if (shippedTail == shippedCap)
		{
			shippedCap = shippedCap ? shippedCap * 2 : 1024;
			shipped = shipped ?
				repalloc(shipped, sizeof(ShippedChunk) * shippedCap) :
				MemoryContextAlloc(TopMemoryContext,
								   sizeof(ShippedChunk) * shippedCap);
		}
	}
	shipped[shippedTail].seq = seq;
	shipped[shippedTail].end = end;
	shippedTail++;
}
static ClientAuthentication_hook_type prev_client_auth_hook = NULL;

Size
s3_wal_shipper_shmem_needs(void)
{
	return sizeof(S3WalShipperShared);
}

void
s3_wal_shipper_shmem_init(Pointer ptr, bool found)
{
	shipper = (S3WalShipperShared *) ptr;
	if (!found)
	{
		pg_atomic_init_u64(&shipper->flushedLsn, InvalidXLogRecPtr);
		pg_atomic_init_u64(&shipper->catchUpLsn, InvalidXLogRecPtr);
		pg_atomic_init_u64(&shipper->replayEndLsn, InvalidXLogRecPtr);
		pg_atomic_init_u32(&shipper->running, 0);
		ConditionVariableInit(&shipper->flushedCV);
	}
}

static char *
chunk_key(TimeLineID tli, uint64 seq)
{
	return psprintf("wal/%08X/chunks/%020llu", tli, (unsigned long long) seq);
}

/*
 * The local hint of where the chunks of this timeline end: "<tli> <next seq>
 * <end lsn>".  It may lag the bucket (a put that landed before a crash); the
 * startup probe walks past what is there.
 */
static bool
read_seq_file(TimeLineID tli, uint64 *nextSeq, XLogRecPtr *endLsn)
{
	FILE	   *f;
	unsigned int fileTli;
	unsigned long long seq,
				lsn;
	bool		ok;

	f = AllocateFile(SEQ_FILENAME, "r");
	if (f == NULL)
		return false;
	ok = fscanf(f, "%u %llu %llu", &fileTli, &seq, &lsn) == 3 && fileTli == tli;
	FreeFile(f);
	if (ok)
	{
		*nextSeq = seq;
		*endLsn = lsn;
	}
	return ok;
}

static void
write_seq_file(TimeLineID tli, uint64 nextSeq, XLogRecPtr endLsn, bool durable)
{
	char		tmp[MAXPGPATH];
	char		buf[96];
	int			fd,
				len;

	snprintf(tmp, sizeof(tmp), "%s.tmp", SEQ_FILENAME);
	len = snprintf(buf, sizeof(buf), "%u %llu %llu\n", tli,
				   (unsigned long long) nextSeq, (unsigned long long) endLsn);
	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmp)));
	if (write(fd, buf, len) != len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	/*
	 * A hint: after each chunk no fsync, a lagging one is walked past at
	 * startup.  Where a run starts shipping is made durable: a hint from
	 * before it would walk into the chunk that starts over the tail of the
	 * last one, and take it for another writer's.
	 */
	if (durable)
	{
		if (pg_fsync(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m", tmp)));
		close(fd);
		durable_rename(tmp, SEQ_FILENAME, ERROR);
		return;
	}
	close(fd);
	if (rename(tmp, SEQ_FILENAME) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\": %m", tmp)));
}

/*
 * Read chunk seq, and its WAL into body when body is not NULL: false when it
 * is not there.
 */
static bool
read_chunk(TimeLineID tli, uint64 seq, ChunkHeader *header, StringInfo body)
{
	StringInfoData buf;
	char	   *key = chunk_key(tli, seq);
	long		code;

	initStringInfo(&buf);
	code = s3_get_object(key, &buf, true);
	pfree(key);
	if (code == S3_RESPONSE_NOT_FOUND)
	{
		pfree(buf.data);
		return false;
	}
	if (buf.len < (int) sizeof(ChunkHeader))
		ereport(ERROR,
				(errmsg("S3 WAL chunk %llu of timeline %u is too short",
						(unsigned long long) seq, tli)));
	memcpy(header, buf.data, sizeof(ChunkHeader));
	if (header->magic != CHUNK_MAGIC && header->magic != TERMINATOR_MAGIC)
		ereport(ERROR,
				(errmsg("S3 WAL chunk %llu of timeline %u is not a chunk",
						(unsigned long long) seq, tli)));
	if (body)
	{
		if (header->magic == CHUNK_MAGIC &&
			buf.len - sizeof(ChunkHeader) != header->endLsn - header->startLsn)
			ereport(ERROR,
					(errmsg("S3 WAL chunk %llu of timeline %u has %d bytes of WAL, not %llu",
							(unsigned long long) seq, tli,
							(int) (buf.len - sizeof(ChunkHeader)),
							(unsigned long long) (header->endLsn - header->startLsn))));
		appendBinaryStringInfo(body, buf.data + sizeof(ChunkHeader),
							   buf.len - sizeof(ChunkHeader));
	}
	pfree(buf.data);
	return true;
}

static bool
read_chunk_header(TimeLineID tli, uint64 seq, ChunkHeader *header)
{
	return read_chunk(tli, seq, header, NULL);
}

/*
 * Copy n bytes of record data from WAL position *pos on, skipping page
 * headers; wal holds the WAL from walStart to walEnd.  False when it ends
 * first.
 */
static bool
wal_record_bytes(const char *wal, XLogRecPtr walStart, XLogRecPtr walEnd,
				 XLogRecPtr *pos, char *dst, Size n)
{
	XLogRecPtr	p = *pos;

	while (n > 0)
	{
		Size		take;

		if (XLogSegmentOffset(p, wal_segment_size) == 0)
			p += SizeOfXLogLongPHD;
		else if (p % XLOG_BLCKSZ == 0)
			p += SizeOfXLogShortPHD;
		take = Min(n, XLOG_BLCKSZ - p % XLOG_BLCKSZ);
		if (p < walStart || p + take > walEnd)
			return false;
		if (dst)
		{
			memcpy(dst, wal + (p - walStart), take);
			dst += take;
		}
		p += take;
		n -= take;
	}
	*pos = p;
	return true;
}

#define DIVERGENCE_SCAN_MAX		1024

/*
 * Whether the chunks of this timeline hold a whole, valid record at
 * replayEnd, where local recovery stopped.  This data directory's own chunks
 * hold only WAL it flushed, and recovery replays every whole record it
 * flushed: a whole record past it was put by a run this directory does not
 * continue -- a copy of the data directory started after the run it was
 * copied from went on, both with the same writer token.  The chunks before
 * nextSeq, ending at end, are walked back to replayEnd; when they cannot be
 * read back that far (removed once archived), nothing is said.
 */
static bool
chunks_pass_replay(TimeLineID tli, uint64 nextSeq, XLogRecPtr end,
				   XLogRecPtr replayEnd)
{
	XLogRecPtr	pos = replayEnd;
	XLogRecPtr	from = end;
	XLogRecPtr	recPos;
	StringInfoData wal;
	uint64		seq = nextSeq;
	XLogRecord	rec;
	char	   *data;
	pg_crc32c	crc;
	bool		result = false;

	/* records start MAXALIGNed */
	pos = pos + (MAXIMUM_ALIGNOF - 1) - (pos + (MAXIMUM_ALIGNOF - 1)) % MAXIMUM_ALIGNOF;
	initStringInfo(&wal);
	while (from > pos)
	{
		ChunkHeader header;
		StringInfoData body;
		XLogRecPtr	upto;

		if (seq == 0 || nextSeq - seq >= DIVERGENCE_SCAN_MAX)
			goto unknown;
		seq--;
		initStringInfo(&body);
		if (!read_chunk(tli, seq, &header, &body) ||
			header.magic != CHUNK_MAGIC)
		{
			pfree(body.data);
			goto unknown;
		}
		/* a later chunk wins where two overlap */
		upto = Min(header.endLsn, from);
		if (upto < from || header.startLsn >= upto)
		{
			pfree(body.data);
			goto unknown;
		}
		{
			StringInfoData joined;

			initStringInfo(&joined);
			appendBinaryStringInfo(&joined, body.data, upto - header.startLsn);
			appendBinaryStringInfo(&joined, wal.data, wal.len);
			pfree(wal.data);
			wal = joined;
		}
		pfree(body.data);
		from = header.startLsn;
	}

	recPos = pos;
	if (!wal_record_bytes(wal.data, from, end, &recPos, (char *) &rec,
						  SizeOfXLogRecord) ||
		rec.xl_tot_len < SizeOfXLogRecord)
		goto done;
	data = palloc(rec.xl_tot_len - SizeOfXLogRecord);
	if (wal_record_bytes(wal.data, from, end, &recPos, data,
						 rec.xl_tot_len - SizeOfXLogRecord))
	{
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, data, rec.xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(crc, (char *) &rec, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(crc);
		result = EQ_CRC32C(crc, rec.xl_crc);
	}
	pfree(data);
	goto done;

unknown:
	ereport(LOG,
			(errmsg("could not read the S3 WAL chunks of timeline %u back to %X/%08X "
					"to check them against local recovery",
					tli, LSN_FORMAT_ARGS(replayEnd))));
done:
	pfree(wal.data);
	return result;
}

/*
 * The seq after the last chunk of the timeline in the bucket.  The shipper's
 * GC never removes the newest chunk of a timeline it ships, so the end is
 * listed even when every earlier chunk is gone; hint, the seq a data
 * directory knows to be reached, is where the listing starts (one before it
 * is the newest at the least) and what is returned when nothing is listed.
 */
static uint64
chunks_end(TimeLineID tli, uint64 hint)
{
	char	   *prefix = psprintf("wal/%08X/chunks/", tli);
	char	   *after = hint >= 2 ? chunk_key(tli, hint - 2) : NULL;
	List	   *keys = s3_list_objects(prefix, after);
	uint64		end = hint;
	ListCell   *lc;

	foreach(lc, keys)
	{
		char	   *key = (char *) lfirst(lc);
		uint64		seq = strtou64(key + strlen(prefix), NULL, 10);

		end = Max(end, seq + 1);
	}
	list_free_deep(keys);
	pfree(prefix);
	if (after)
		pfree(after);
	return end;
}

/* The newest timeline in the bucket from tli on: its history is there. */
static TimeLineID
newest_timeline(TimeLineID tli)
{
	for (;;)
	{
		char		name[MAXFNAMELEN];
		char	   *key;
		StringInfoData buf;
		long		code;

		TLHistoryFileName(name, tli + 1);
		key = psprintf("wal/%s", name);
		initStringInfo(&buf);
		code = s3_get_object(key, &buf, true);
		pfree(key);
		pfree(buf.data);
		if (code == S3_RESPONSE_NOT_FOUND)
			return tli;
		tli++;
	}
}

/*
 * Put one chunk.  Returns false when the key holds a chunk that is not ours:
 * another writer took over.
 */
static bool
put_chunk(TimeLineID tli, uint64 seq, XLogRecPtr start, XLogRecPtr end,
		  const char *data)
{
	ChunkHeader header;
	ChunkHeader existing;
	char	   *body;
	char	   *key = chunk_key(tli, seq);
	long		code;
	Size		len = end - start;

	header.magic = CHUNK_MAGIC;
	header.timeline = tli;
	header.token = s3_lease_token();
	header.startLsn = start;
	header.endLsn = end;

	body = palloc(sizeof(header) + len);
	memcpy(body, &header, sizeof(header));
	memcpy(body + sizeof(header), data, len);

	code = s3_put_object_conditional(key, body, sizeof(header) + len, NULL,
									 true, NULL);
	pfree(body);
	if (code == S3_RESPONSE_OK)
	{
		pfree(key);
		return true;
	}

	/* the key is taken: our own earlier attempt, or another writer's */
	if (!read_chunk_header(tli, seq, &existing))
	{
		pfree(key);
		return put_chunk(tli, seq, start, end, data);
	}
	pfree(key);
	return existing.magic == CHUNK_MAGIC &&
		existing.token == header.token &&
		existing.startLsn == start && existing.endLsn == end;
}

/*
 * A writer taking the bucket over: put a terminator right after the last
 * chunk of the newest timeline in the bucket, so the writer it fenced off
 * cannot put another (its next put finds the key taken and reports nothing
 * past it).  Called by the postmaster once it holds the lease, before
 * recovery, whether or not this instance ships WAL: the writer it fences off
 * may.  The last chunk is found by listing, never taken from the hint: a
 * terminator in a gap the GC left would not stop the writer putting after
 * it, and a restore would end at the gap.  The terminator's place is noted
 * for the GC: the old timeline's chunks go once no retained checkpoint is
 * before the switch.
 */
void
s3_wal_place_terminator(uint64 token)
{
	TimeLineID	tli,
				loadedTli;
	uint64		seq,
				loadedNext;
	ChunkHeader header;
	bool		crc_ok;
	ControlFileData *cf;
	StringInfoData retired;

	cf = get_controlfile(DataDir, &crc_ok);
	if (!crc_ok)
		ereport(FATAL,
				(errmsg("pg_control checksum mismatch")));
	tli = cf->checkPointCopy.ThisTimeLineID;
	pfree(cf);
	{
		FILE	   *f = AllocateFile(SEQ_FILENAME, "r");
		unsigned int fileTli;
		unsigned long long s,
					l;

		seq = 0;
		if (f != NULL)
		{
			if (fscanf(f, "%u %llu %llu", &fileTli, &s, &l) == 3)
			{
				tli = fileTli;
				seq = s;
			}
			FreeFile(f);
		}
	}
	loadedTli = tli;
	loadedNext = seq;
	tli = newest_timeline(loadedTli);
	seq = chunks_end(tli, tli == loadedTli ? loadedNext : 0);

	memset(&header, 0, sizeof(header));
	header.magic = TERMINATOR_MAGIC;
	header.timeline = tli;
	header.token = token;
	for (;; seq++)
	{
		char	   *key = chunk_key(tli, seq);
		long		code;

		/* taken: the fenced writer put one more meanwhile */
		code = s3_put_object_conditional(key, (Pointer) &header, sizeof(header),
										 NULL, true, NULL);
		pfree(key);
		if (code == S3_RESPONSE_OK)
			break;
	}

	/*
	 * Chunks that came after the load, or a timeline started after it, are
	 * WAL this data directory does not have: only a recovery that reads the
	 * bucket gets them.
	 */
	if ((tli != loadedTli || seq > loadedNext) &&
		access(RESTORE_BUCKET_FILENAME, F_OK) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the S3 bucket has WAL past this data directory"),
				 errdetail("The bucket's WAL reaches chunk %llu of timeline %u; "
						   "this data directory was loaded at chunk %llu of timeline %u.",
						   (unsigned long long) seq, tli,
						   (unsigned long long) loadedNext, loadedTli),
				 errhint("Load the data directory from the bucket again, with "
						 "restore_command reading the bucket (the loader's "
						 "default).")));

	initStringInfo(&retired);
	appendStringInfo(&retired, "%u %llu\n", tli, (unsigned long long) seq);
	{
		char		tmp[MAXPGPATH];
		int			fd;

		snprintf(tmp, sizeof(tmp), "%s.tmp", RETIRED_FILENAME);
		fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0 || write(fd, retired.data, retired.len) != retired.len ||
			pg_fsync(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m", tmp)));
		close(fd);
		durable_rename(tmp, RETIRED_FILENAME, FATAL);
	}
	pfree(retired.data);
	ereport(LOG,
			(errmsg("put a WAL terminator at chunk %llu of timeline %u",
					(unsigned long long) seq, tli)));
}

/*
 * The history of a timeline this instance switched to: up before the
 * timeline's first chunk, under If-None-Match, so two writers cannot both
 * start it.
 */
static void
put_history(TimeLineID tli)
{
	char		name[MAXFNAMELEN];
	char		path[MAXPGPATH];
	char	   *key;
	Pointer		data;
	uint64		size;

	TLHistoryFileName(name, tli);
	snprintf(path, sizeof(path), XLOGDIR "/%s", name);
	data = read_file(path, &size);
	if (data == NULL)
		ereport(ERROR,
				(errmsg("the history of timeline %u is not in pg_wal", tli)));
	key = psprintf("wal/%s", name);
	(void) s3_put_object_fenced(key, data, size, NULL);
	pfree(key);
	pfree(data);
}

static void
send_reply(WalReceiverConn *conn, XLogRecPtr flushed, bool requestReply)
{
	StringInfoData buf;

	initStringInfo(&buf);
	pq_sendbyte(&buf, 'r');
	/* written means in the bucket too: remote_write waits for the put */
	pq_sendint64(&buf, flushed);
	pq_sendint64(&buf, flushed);
	/* never applied: remote_apply would wait for nothing that comes */
	pq_sendint64(&buf, InvalidXLogRecPtr);
	pq_sendint64(&buf, GetCurrentTimestamp());
	pq_sendbyte(&buf, requestReply ? 1 : 0);
	walrcv_send(conn, buf.data, buf.len);
	pfree(buf.data);
}

/*
 * Our own server, over the first Unix socket, as the bootstrap superuser
 * (the OS user's name need not be a role).
 */
static char *
default_conninfo(void)
{
	char	   *user;
	char	   *dir = NULL;
	char	   *dirs = Unix_socket_directories ? pstrdup(Unix_socket_directories) : NULL;

	if (dirs != NULL)
	{
		char	   *comma = strchr(dirs, ',');

		if (comma)
			*comma = '\0';
		dir = dirs;
		while (*dir == ' ')
			dir++;
	}
	StartTransactionCommand();
	/* outlives the transaction */
	user = MemoryContextStrdup(TopMemoryContext,
							   GetUserNameFromId(BOOTSTRAP_SUPERUSERID, false));
	CommitTransactionCommand();

	return psprintf("host='%s' port=%d user='%s' dbname=replication",
					dir && *dir ? dir : "/tmp", PostPortNumber, user);
}

/*
 * The physical slot that keeps the WAL the bucket does not have yet; created
 * on the first start.  Returns where it holds WAL from.
 */
static XLogRecPtr
ensure_slot(void)
{
	ReplicationSlot *slot;
	XLogRecPtr	restart;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	slot = SearchNamedReplicationSlot(SHIPPER_SLOT, false);
	LWLockRelease(ReplicationSlotControlLock);

	if (slot == NULL)
	{
		ReplicationSlotCreate(SHIPPER_SLOT, false, RS_PERSISTENT, false, false,
							  false);
		ReplicationSlotReserveWal();
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();

		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
		slot = SearchNamedReplicationSlot(SHIPPER_SLOT, false);
		LWLockRelease(ReplicationSlotControlLock);
		ereport(LOG,
				(errmsg("created replication slot \"%s\" for WAL shipping to S3",
						SHIPPER_SLOT)));
	}

	SpinLockAcquire(&slot->mutex);
	restart = slot->data.restart_lsn;
	SpinLockRelease(&slot->mutex);
	return restart;
}

/*
 * Archiving puts completed segments into the bucket: then and only then a
 * chunk is no longer needed.
 */
static bool
segments_reach_bucket(void)
{
	return XLogArchivingActive() && XLogArchiveLibrary &&
		strcmp(XLogArchiveLibrary, "orioledb") == 0;
}

static bool
segment_archived(TimeLineID tli, XLogRecPtr lsn)
{
	char		xlogfname[MAXFNAMELEN];
	char		path[MAXPGPATH];
	struct stat st;
	XLogSegNo	segno;

	XLByteToSeg(lsn, segno, wal_segment_size);
	XLogFileName(xlogfname, tli, segno, wal_segment_size);
	snprintf(path, sizeof(path), XLOGDIR "/archive_status/%s.done", xlogfname);
	return stat(path, &st) == 0;
}

/*
 * Remove the chunks whose WAL an archived segment has.  A restore reads the
 * segments first and chunks only past them.  The newest chunk stays whatever
 * covers it: a takeover lists the timeline's chunks to find where to put its
 * terminator.
 */
static void
chunks_gc(TimeLineID tli)
{
	while (shippedHead < shippedTail - 1 &&
		   segment_archived(tli, shipped[shippedHead].end - 1))
	{
		char	   *key = chunk_key(tli, shipped[shippedHead].seq);

		s3_delete_object(key);
		pfree(key);
		shippedHead++;
	}
}

/*
 * The chunks a previous run left: walk back from the next one until a chunk
 * is gone (removed already, or never there).
 */
static void
chunks_gc_recover(TimeLineID tli, uint64 nextSeq)
{
	uint64		seq = nextSeq;
	int			found = 0;
	ChunkHeader header;
	uint64		first;

	while (seq > 0 && found < CHUNK_GC_SCAN_MAX &&
		   read_chunk_header(tli, seq - 1, &header))
	{
		seq--;
		found++;
	}
	first = seq;
	for (seq = first; seq < nextSeq; seq++)
	{
		if (!read_chunk_header(tli, seq, &header) ||
			header.magic != CHUNK_MAGIC)
			continue;
		shipped_push(seq, header.endLsn);
	}
}

static void
shipper_exit(int code, Datum arg)
{
	pg_atomic_write_u32(&shipper->running, 0);
	ConditionVariableBroadcast(&shipper->flushedCV);
}

#define SHIPPER_ABSENT_WAIT_MS	30000

/*
 * Wait until the shipper has put the WAL up to lsn.  A shipper not streaming
 * is waited for a while (it starts once recovery is done, and again a second
 * after it failed); false when none came: what the bucket has is not known.
 */
bool
s3_wal_shipper_wait(XLogRecPtr lsn)
{
	bool		reached = true;
	int			absentMs = 0;

	ConditionVariablePrepareToSleep(&shipper->flushedCV);
	while (pg_atomic_read_u64(&shipper->flushedLsn) < lsn)
	{
		if (pg_atomic_read_u32(&shipper->running) == 0)
		{
			if (absentMs >= SHIPPER_ABSENT_WAIT_MS)
			{
				reached = false;
				break;
			}
			absentMs += 1000;
		}
		(void) ConditionVariableTimedSleep(&shipper->flushedCV, 1000,
										   PG_WAIT_EXTENSION);
	}
	ConditionVariableCancelSleep();
	return reached;
}

static void
set_flushed(XLogRecPtr lsn)
{
	pg_atomic_write_u64(&shipper->flushedLsn, lsn);
	ConditionVariableBroadcast(&shipper->flushedCV);
}

void
s3_wal_shipper_main(Datum main_arg)
{
	WalReceiverConn *conn;
	WalRcvStreamOptions options;
	char	   *err = NULL;
	char	   *conninfo;
	TimeLineID	tli;
	uint64		nextSeq = 0;
	XLogRecPtr	start;
	XLogRecPtr	slotRestart;
	XLogRecPtr	replayEnd;
	ChunkHeader existing;
	StringInfoData chunk;
	XLogRecPtr	chunkStart = InvalidXLogRecPtr;
	TimestampTz chunkSince = 0;
	XLogRecPtr	flushed;
	MemoryContext cxt;
	TimestampTz lastReply = 0;
	TimestampTz lastGc = 0;

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();
	/* shared catalogs only: the bootstrap superuser's name */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	cxt = AllocSetContextCreate(TopMemoryContext, "orioledb s3 wal shipper",
								ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(cxt);

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	slotRestart = ensure_slot();

	conninfo = (s3_wal_conninfo && *s3_wal_conninfo) ?
		s3_wal_conninfo : default_conninfo();
	conn = walrcv_connect(conninfo, true, false, false, SHIPPER_APPNAME, &err);
	if (conn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("WAL shipper could not connect to this server: %s", err)));

	(void) walrcv_identify_system(conn, &tli);

	/*
	 * Continue after the last chunk of this timeline.  The hint names where
	 * it was; a chunk past it is an attempt of ours that landed before a
	 * crash, or another writer's: then stop.  A timeline this instance
	 * started is shipped from where it starts, after its history.
	 */
	if (!read_seq_file(tli, &nextSeq, &start))
	{
		nextSeq = 0;
		start = slotRestart - XLogSegmentOffset(slotRestart, wal_segment_size);
		if (tli > 1 && existsTimeLineHistory(tli))
		{
			List	   *history = readTimeLineHistory(tli);
			TimeLineHistoryEntry *entry = linitial(history);

			if (entry->tli == tli && !XLogRecPtrIsInvalid(entry->begin))
				start = entry->begin;
			put_history(tli);
		}
	}
	while (read_chunk_header(tli, nextSeq, &existing))
	{
		/* our own terminator: a load that started without a new timeline */
		if (existing.magic == TERMINATOR_MAGIC && s3_lease_own_token(existing.token))
		{
			nextSeq++;
			continue;
		}
		/*
		 * Past the hint there may be chunks this data directory put before a
		 * crash; anything else is WAL this directory does not have, and
		 * shipping on would give the bucket two histories.
		 */
		if (!s3_lease_own_token(existing.token) ||
			existing.startLsn != start)
			s3_stop_instance("the S3 bucket has WAL past this data directory",
							 psprintf("WAL chunk %llu of timeline %u, %X/%08X to %X/%08X, "
									  "was put by writer token " UINT64_FORMAT
									  ": load the data directory from the bucket again.",
									  (unsigned long long) nextSeq, tli,
									  LSN_FORMAT_ARGS(existing.startLsn),
									  LSN_FORMAT_ARGS(existing.endLsn),
									  existing.token));
		start = existing.endLsn;
		nextSeq++;
	}
	/*
	 * A chunk may end inside a record: recovery stopped at the last whole
	 * one, and the WAL written since starts there.  Ship from there, over the
	 * tail of the last chunk; a restore lets the later chunk win.  No commit
	 * is lost: one is acknowledged only once its whole record is up.
	 */
	replayEnd = pg_atomic_read_u64(&shipper->replayEndLsn);
	if (!XLogRecPtrIsInvalid(replayEnd) && replayEnd < start)
	{
		if (chunks_pass_replay(tli, nextSeq, start, replayEnd))
			s3_stop_instance("the S3 bucket has WAL past this data directory",
							 psprintf("The WAL chunks of timeline %u hold a whole record "
									  "at %X/%08X, where recovery of this data directory "
									  "ended: another copy of it went on writing.  Load "
									  "the data directory from the bucket again.",
									  tli, LSN_FORMAT_ARGS(replayEnd)));
		start = replayEnd;
	}
	if (start < slotRestart - XLogSegmentOffset(slotRestart, wal_segment_size))
		ereport(ERROR,
				(errmsg("the WAL after %X/%08X that the bucket needs is gone",
						LSN_FORMAT_ARGS(start))));

	flushed = start;
	set_flushed(flushed);
	write_seq_file(tli, nextSeq, start, true);

	/*
	 * Where recovery ended is used once: the hint now says where shipping
	 * starts, and a shipper started again in this run starts there.
	 */
	if (!XLogRecPtrIsInvalid(replayEnd))
		(void) pg_atomic_compare_exchange_u64(&shipper->replayEndLsn,
											  &replayEnd, InvalidXLogRecPtr);

	options.logical = false;
	options.slotname = SHIPPER_SLOT;
	options.startpoint = start;
	options.proto.physical.startpointTLI = tli;
	if (!walrcv_startstreaming(conn, &options))
		ereport(ERROR,
				(errmsg("WAL shipper could not start streaming at %X/%08X",
						LSN_FORMAT_ARGS(start))));

	before_shmem_exit(shipper_exit, (Datum) 0);
	pg_atomic_write_u32(&shipper->running, 1);
	ereport(LOG,
			(errmsg("shipping WAL to S3 from %X/%08X, timeline %u, chunk %llu",
					LSN_FORMAT_ARGS(start), tli, (unsigned long long) nextSeq)));
	s3_lease_loaded_done();
	if (segments_reach_bucket())
		chunks_gc_recover(tli, nextSeq);

	initStringInfo(&chunk);

	while (!ShutdownRequestPending)
	{
		char	   *buf;
		pgsocket	wait_fd = PGINVALID_SOCKET;
		int			len;
		bool		replyNow = false;

		CHECK_FOR_INTERRUPTS();

		len = walrcv_receive(conn, &buf, &wait_fd);
		if (len < 0)
		{
			ereport(LOG, (errmsg("WAL shipper stream ended")));
			break;
		}

		if (len > 0)
		{
			StringInfoData msg;

			initReadOnlyStringInfo(&msg, buf, len);
			switch (pq_getmsgbyte(&msg))
			{
				case 'w':
					{
						XLogRecPtr	dataStart = pq_getmsgint64(&msg);
						Size		dataLen;

						(void) pq_getmsgint64(&msg);	/* walEnd */
						(void) pq_getmsgint64(&msg);	/* sendTime */
						dataLen = len - 25;
						if (chunk.len == 0)
						{
							chunkStart = dataStart;
							chunkSince = GetCurrentTimestamp();
						}
						else if (dataStart != chunkStart + chunk.len)
							ereport(ERROR,
									(errmsg("WAL shipper received %X/%08X after %X/%08X",
											LSN_FORMAT_ARGS(dataStart),
											LSN_FORMAT_ARGS(chunkStart + chunk.len))));
						appendBinaryStringInfo(&chunk, buf + 25, dataLen);
						break;
					}
				case 'k':
					(void) pq_getmsgint64(&msg);	/* walEnd */
					(void) pq_getmsgint64(&msg);	/* sendTime */
					replyNow = pq_getmsgbyte(&msg);
					break;
				default:
					ereport(ERROR,
							(errmsg("WAL shipper received an unexpected message")));
			}
			/*
			 * Take what else is ready into this chunk, but not for longer
			 * than orioledb.s3_wal_chunk_delay: under a steady stream of
			 * commits the socket never runs dry, and the put, which every
			 * waiting commit waits for, would be put off on and on.
			 */
			if (chunk.len < CHUNK_MAX_BYTES && !replyNow &&
				(chunk.len == 0 ||
				 !TimestampDifferenceExceeds(chunkSince, GetCurrentTimestamp(),
											 s3_wal_chunk_delay)))
				continue;
		}

		/* nothing more right now: put what we have, one chunk */
		if (chunk.len > 0)
		{
			XLogRecPtr	end = chunkStart + chunk.len;

			if (!put_chunk(tli, nextSeq, chunkStart, end, chunk.data))
				ereport(ERROR,
						(errmsg("WAL chunk %llu of timeline %u belongs to another writer",
								(unsigned long long) nextSeq, tli)));
			if (segments_reach_bucket())
				shipped_push(nextSeq, end);
			nextSeq++;
			flushed = end;
			set_flushed(flushed);
			write_seq_file(tli, nextSeq, flushed, false);
			resetStringInfo(&chunk);
			replyNow = true;
		}

		if (replyNow ||
			GetCurrentTimestamp() >= TimestampTzPlusMilliseconds(lastReply, 1000))
		{
			send_reply(conn, flushed, false);
			lastReply = GetCurrentTimestamp();
		}

		if (len == 0)
		{
			if (shippedHead < shippedTail - 1 &&
				GetCurrentTimestamp() >= TimestampTzPlusMilliseconds(lastGc,
																	CHUNK_GC_INTERVAL_MS))
			{
				chunks_gc(tli);
				lastGc = GetCurrentTimestamp();
			}
			(void) WaitLatchOrSocket(MyLatch,
									 WL_LATCH_SET | WL_SOCKET_READABLE |
									 WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
									 wait_fd, 1000L, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
	}

	walrcv_disconnect(conn);

	/*
	 * Never 0, which would unregister the worker: a shipper told to stop
	 * outside a shutdown (pg_terminate_backend) is started again, as commits
	 * wait for it.
	 */
	proc_exit(1);
}

/*
 * At a shutdown checkpoint the shipper is gone already (background workers
 * stop first): put the WAL it did not ship, the shutdown checkpoint record
 * with it, as the next chunks.  A partial segment is never put under the
 * archive's key: a restore takes an archived segment as complete, and would
 * read no chunk over it.  Returns false when the shipper never ran here, so
 * nothing says what the bucket has.
 */
bool
s3_wal_ship_rest(TimeLineID tli)
{
	uint64		nextSeq;
	XLogRecPtr	start,
				end = GetFlushRecPtr(NULL);
	ChunkHeader existing;
	char	   *buf;

	if (!read_seq_file(tli, &nextSeq, &start))
	{
		/*
		 * The shipper never ran here: the bucket has the archived segments
		 * only.  Ship from the first segment not archived.
		 */
		XLogSegNo	segno;
		XLogRecPtr	redo = GetRedoRecPtr();

		XLByteToSeg(redo, segno, wal_segment_size);
		while (segment_archived(tli, segno * wal_segment_size))
			segno++;
		XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, start);
		nextSeq = chunks_end(tli, 0);
	}
	while (read_chunk_header(tli, nextSeq, &existing) &&
		   s3_lease_own_token(existing.token) &&
		   (existing.magic == TERMINATOR_MAGIC || existing.startLsn == start))
	{
		if (existing.magic == CHUNK_MAGIC)
			start = existing.endLsn;
		nextSeq++;
	}

	buf = palloc(CHUNK_MAX_BYTES);
	while (start < end)
	{
		XLogRecPtr	chunkEnd = Min(end, start + CHUNK_MAX_BYTES);
		XLogRecPtr	pos = start;

		while (pos < chunkEnd)
		{
			XLogSegNo	segno;
			char		fname[MAXFNAMELEN];
			char		path[MAXPGPATH];
			int			fd;
			uint32		off;
			Size		n;

			XLByteToSeg(pos, segno, wal_segment_size);
			XLogFileName(fname, tli, segno, wal_segment_size);
			snprintf(path, sizeof(path), XLOGDIR "/%s", fname);
			off = XLogSegmentOffset(pos, wal_segment_size);
			n = Min(chunkEnd - pos, wal_segment_size - off);
			fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
			if (fd < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", path)));
			if (pg_pread(fd, buf + (pos - start), n, off) != (ssize_t) n)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			close(fd);
			pos += n;
		}
		if (!put_chunk(tli, nextSeq, start, chunkEnd, buf))
			ereport(ERROR,
					(errmsg("WAL chunk %llu of timeline %u belongs to another writer",
							(unsigned long long) nextSeq, tli)));
		nextSeq++;
		start = chunkEnd;
		write_seq_file(tli, nextSeq, start, false);
	}
	pfree(buf);
	return true;
}

void
register_s3_wal_shipper(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3_wal_shipper_main");
	strcpy(worker.bgw_name, "orioledb s3 wal shipper");
	strcpy(worker.bgw_type, "orioledb s3 wal shipper");
	RegisterBackgroundWorker(&worker);
}

/*
 * Called at the end of recovery: WAL up to here may hold commits a client
 * never saw acknowledged, but which are now visible.
 */
void
s3_wal_shipper_set_catch_up(XLogRecPtr lsn)
{
	if (shipper && s3_wal_shipping)
	{
		pg_atomic_write_u64(&shipper->replayEndLsn, GetXLogReplayRecPtr(NULL));
		pg_atomic_write_u64(&shipper->catchUpLsn, lsn);
	}
}

static void
s3_wal_client_auth(Port *port, int status)
{
	XLogRecPtr	target;

	if (prev_client_auth_hook)
		prev_client_auth_hook(port, status);

	if (status != STATUS_OK || am_walsender || shipper == NULL)
		return;

	/*
	 * A writer recovering from the bucket (a takeover) takes no client until
	 * it is done: a hot standby read would show a state short of the commits
	 * the bucket has.
	 */
	while (S3_WRITER_MODE && RecoveryInProgress())
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(100000L);
	}

	target = pg_atomic_read_u64(&shipper->catchUpLsn);
	if (XLogRecPtrIsInvalid(target) ||
		pg_atomic_read_u64(&shipper->flushedLsn) >= target)
		return;

	ConditionVariablePrepareToSleep(&shipper->flushedCV);
	while (pg_atomic_read_u64(&shipper->flushedLsn) < target)
		ConditionVariableTimedSleep(&shipper->flushedCV, 1000,
									PG_WAIT_EXTENSION);
	ConditionVariableCancelSleep();
}

void
s3_wal_shipper_init(void)
{
	prev_client_auth_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = s3_wal_client_auth;
	register_s3_wal_shipper();
}
