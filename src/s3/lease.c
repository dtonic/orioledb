/*-------------------------------------------------------------------------
 *
 * lease.c
 *		The S3 writer lease and the root commit pointer.
 *
 * One instance writes to a bucket at a time.  It holds the lease in the
 * `writer` object, renewed by a background worker, and fences everything it
 * commits through the `root` object: both are replaced only with If-Match on
 * the ETag the holder last saw, so a writer that lost the lease cannot commit.
 * Every start takes a new token, one more than any the bucket or the local
 * data directory has seen; `root` names the token of the writer that wrote it
 * last.  The bucket gets a random id when root is first written; the local
 * data directory keeps its token with that id, so a token from another
 * bucket never counts as ours here.  A copy of a data directory carries the
 * same token as the original: a start whose own token still holds a valid
 * lease waits for it to run out, and refuses if someone renews it meanwhile.  A conditional PUT that ends without a clear answer (no reply, 5xx,
 * 409, 412) is judged by reading the object back: it is ours when it carries
 * our token, our owner string and the attempt number we sent.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/lease.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#include "orioledb.h"

#include "checkpoint/control.h"
#include "utils/seq_buf.h"	/* s3/headers.h needs it */
#include "s3/control.h"
#include "s3/headers.h"
#include "s3/lease.h"
#include "s3/requests.h"
#include "s3/wal_shipper.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "common/controldata_utils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(orioledb_s3_status);

#define WRITER_KEY				"writer"
#define ROOT_KEY				"root"
#define TOKEN_FILENAME			ORIOLEDB_DATA_DIR "/s3_token"
#define EPOCHS_FILENAME			ORIOLEDB_DATA_DIR "/s3_epochs"
#define LOADED_FILENAME			ORIOLEDB_DATA_DIR "/s3_loaded"
#define AS_OF_FILENAME			ORIOLEDB_DATA_DIR "/s3_as_of"
#define LINEAGE_FILENAME		ORIOLEDB_DATA_DIR "/s3_lineage"
#define S3_MAX_LINEAGE			64
#define GC_KEY					"gc"
#define READERS_KEY				"readers"
#define S3_MAX_EPOCHS			4096
#define S3_MAX_SAME_START		64
#define LEASE_FORMAT			1
#define S3_ETAG_LEN				256
#define OWNER_LEN				128

/* the lease is treated as gone this long before the store would expire it */
#define LEASE_MARGIN_SECS		5
#define LEASE_REQUEST_TIMEOUT	10
#define STARTUP_ATTEMPTS		10

int			s3_lease_duration = 30;
int			s3_reader_pin_ttl = 600;

typedef struct
{
	uint64		token;
	uint64		bucket;
	uint32		attempt;
	char		owner[OWNER_LEN];
	int64		expiresAt;		/* the store's clock, seconds since 1970 */
} WriterBody;

typedef struct
{
	uint64		token;
	uint64		bucket;
	uint32		attempt;
	char		owner[OWNER_LEN];
	uint32		checkpoint;
	XLogRecPtr	redoLsn;
	XLogRecPtr	checkpointLsn;
	uint64		controlId;		/* the OrioleDB control identifier, 0: none */
} RootBody;

/*
 * Checkpoints from firstCheckpoint on (up to the next epoch's) were written
 * by token.  Every object a checkpoint writes under its number carries the
 * token in its key, so a checkpoint number taken again after a crash never
 * names the objects the crashed run left.
 */
typedef struct
{
	uint32		firstCheckpoint;
	uint64		token;
} S3Epoch;

typedef struct
{
	LWLock		lock;			/* the state below */
	LWLock		requestLock;	/* the one renewer or committer at a time */
	int			trancheId;
	uint64		token;
	uint64		bucket;
	uint64		prevToken;		/* this data directory's previous run */
	bool		loaded;			/* made by the loader, not run here yet */
	char		owner[OWNER_LEN];
	uint32		writerAttempt;
	char		writerEtag[S3_ETAG_LEN];
	TimestampTz renewedAt;
	TimestampTz deadline;		/* local clock: not ours after this */
	int64		clockOffset;	/* the store's clock minus ours, seconds */
	RootBody	root;
	char		rootEtag[S3_ETAG_LEN];
	int			numEpochs;
	int			numLineage;
	uint64		lineage[S3_MAX_LINEAGE];	/* this directory's runs' tokens */
	uint32		gcPublished[3];	/* retain, folded, collected */
	S3Epoch		epochs[S3_MAX_EPOCHS];
} S3LeaseState;

/* What the postmaster acquired; each shared memory (re)initialization copies it. */
static S3LeaseState pm_lease;

static void reader_pin_and_check(uint32 chkp);

/* the checkpoint this data directory started from, at the postmaster start */
static uint32 start_checkpoint = 0;
static S3LeaseState *lease = NULL;

typedef enum
{
	CAS_DONE,					/* our body is the object now */
	CAS_STALE,					/* an earlier attempt of ours is: retry on its ETag */
	CAS_LOST,					/* someone else's body is the object */
	CAS_UNKNOWN,				/* no answer to judge by */
	CAS_ERROR					/* the store refused the request itself */
} CasResult;

/*
 * Bodies are "key value" lines.
 */
static bool
kv_get(const char *body, const char *key, char *out, size_t outlen)
{
	size_t		keylen = strlen(key);
	const char *line = body;

	while (line && *line)
	{
		const char *end = strchr(line, '\n');
		size_t		linelen = end ? (size_t) (end - line) : strlen(line);

		if (linelen > keylen && strncmp(line, key, keylen) == 0 &&
			line[keylen] == ' ')
		{
			size_t		len = Min(linelen - keylen - 1, outlen - 1);

			memcpy(out, line + keylen + 1, len);
			out[len] = '\0';
			return true;
		}
		line = end ? end + 1 : NULL;
	}
	return false;
}

static bool
kv_get_u64(const char *body, const char *key, uint64 *value)
{
	char		buf[64];
	char	   *end;

	if (!kv_get(body, key, buf, sizeof(buf)))
		return false;
	errno = 0;
	*value = strtou64(buf, &end, 10);
	return errno == 0 && *end == '\0' && end != buf;
}

static bool
kv_get_hex(const char *body, const char *key, uint64 *value)
{
	char		buf[64];
	char	   *end;

	if (!kv_get(body, key, buf, sizeof(buf)))
		return false;
	errno = 0;
	*value = strtou64(buf, &end, 16);
	return errno == 0 && *end == '\0' && end != buf;
}

static bool
kv_get_lsn(const char *body, const char *key, XLogRecPtr *lsn)
{
	char		buf[64];
	uint32		hi,
				lo;

	if (!kv_get(body, key, buf, sizeof(buf)) ||
		sscanf(buf, "%X/%X", &hi, &lo) != 2)
		return false;
	*lsn = ((uint64) hi << 32) | lo;
	return true;
}

static void
render_writer(StringInfo buf, WriterBody *w)
{
	resetStringInfo(buf);
	appendStringInfo(buf, "format %d\nbucket %016llx\ntoken " UINT64_FORMAT
					 "\nattempt %u\nowner %s\nexpires_at " INT64_FORMAT "\n",
					 LEASE_FORMAT, (unsigned long long) w->bucket, w->token,
					 w->attempt, w->owner, w->expiresAt);
}

static bool
parse_writer(const char *body, WriterBody *w)
{
	uint64		v;

	memset(w, 0, sizeof(*w));
	if (!kv_get_hex(body, "bucket", &w->bucket) ||
		!kv_get_u64(body, "token", &w->token) ||
		!kv_get_u64(body, "attempt", &v) ||
		!kv_get(body, "owner", w->owner, sizeof(w->owner)))
		return false;
	w->attempt = (uint32) v;
	if (!kv_get_u64(body, "expires_at", &v))
		return false;
	w->expiresAt = (int64) v;
	return true;
}

static void
render_root(StringInfo buf, RootBody *r, S3LeaseState *state)
{
	resetStringInfo(buf);
	appendStringInfo(buf, "format %d\nbucket %016llx\ntoken " UINT64_FORMAT
					 "\nattempt %u\nowner %s\ncheckpoint %u\nredo_lsn %X/%08X\n"
					 "checkpoint_lsn %X/%08X\n",
					 LEASE_FORMAT, (unsigned long long) r->bucket, r->token,
					 r->attempt, r->owner,
					 r->checkpoint, LSN_FORMAT_ARGS(r->redoLsn),
					 LSN_FORMAT_ARGS(r->checkpointLsn));
	if (r->controlId != 0)
		appendStringInfo(buf, "control_id %016llx\n",
						 (unsigned long long) r->controlId);
	for (int i = 0; i < state->numEpochs; i++)
		appendStringInfo(buf, "epoch %u " UINT64_FORMAT "\n",
						 state->epochs[i].firstCheckpoint,
						 state->epochs[i].token);
}

static bool
parse_root(const char *body, RootBody *r)
{
	uint64		v;

	memset(r, 0, sizeof(*r));
	if (!kv_get_hex(body, "bucket", &r->bucket) ||
		!kv_get_u64(body, "token", &r->token) ||
		!kv_get_u64(body, "attempt", &v) ||
		!kv_get(body, "owner", r->owner, sizeof(r->owner)))
		return false;
	r->attempt = (uint32) v;
	if (!kv_get_u64(body, "checkpoint", &v))
		return false;
	r->checkpoint = (uint32) v;
	if (!kv_get_hex(body, "control_id", &r->controlId))
		r->controlId = 0;
	return kv_get_lsn(body, "redo_lsn", &r->redoLsn) &&
		kv_get_lsn(body, "checkpoint_lsn", &r->checkpointLsn);
}

/*
 * Seconds since 1970 of an HTTP date ("Thu, 09 Oct 2026 08:16:23 GMT"), or -1.
 */
static int64
http_date_epoch(const char *s)
{
	static const char months[] = "JanFebMarAprMayJunJulAugSepOctNovDec";
	char		mon[4];
	int			d,
				y,
				H,
				M,
				S,
				m;
	const char *p;
	int64		era,
				yoe,
				doy,
				doe;

	if (s == NULL ||
		sscanf(s, "%*[^,], %d %3s %d %d:%d:%d", &d, mon, &y, &H, &M, &S) != 6 ||
		(p = strstr(months, mon)) == NULL || (p - months) % 3 != 0)
		return -1;
	m = (p - months) / 3 + 1;

	/* days from the civil date (H. Hinnant) */
	y -= m <= 2;
	era = (y >= 0 ? y : y - 399) / 400;
	yoe = y - era * 400;
	doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
	doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	return (era * 146097 + doe - 719468) * 86400 + H * 3600 + M * 60 + S;
}

static bool
is_transient(long code)
{
	return code == 0 || code == 429 || code >= 500;
}

/*
 * The lease is judged on the store's clock, read from the Date header of its
 * answers.  An answer without one that reads leaves nothing to judge the
 * lease by: a writer refuses to take or wait for a lease then, rather than
 * judge it on its own clock, which another instance does not share.
 */
static void
require_store_clock(int64 storageNow, const char *key)
{
	if (storageNow >= 0)
		return;
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("the S3 store sent no usable Date header"),
			 errdetail("The answer to GET \"%s\" carries no Date header that "
					   "reads as an HTTP date; the writer lease is judged on "
					   "the store's clock.", key)));
}

/*
 * The same clock where only a reader pin is at stake: the local clock stands
 * in, said once.  A pin judged a little off stays a pin.
 */
static int64
store_clock_or_local(int64 storageNow, const char *key)
{
	static bool warned = false;

	if (storageNow >= 0)
		return storageNow;
	if (!warned)
	{
		warned = true;
		ereport(WARNING,
				(errmsg("the S3 store sent no usable Date header, judging the "
						"reader pins on the local clock"),
				 errdetail("The answer to GET \"%s\" carries no Date header that "
						   "reads as an HTTP date.", key)));
	}
	return (int64) time(NULL);
}

/*
 * Read an object: 200 with its body and ETag, 404, or a transient code.
 */
static long
lease_get(const char *key, StringInfo body, char *etag, int64 *storageNow)
{
	char	   *et = NULL,
			   *date = NULL;
	long		code;

	code = s3_request_once("GET", (char *) key, NULL, 0, NULL, false,
						   LEASE_REQUEST_TIMEOUT, body, &et, &date);
	if (code == S3_RESPONSE_OK)
	{
		if (et == NULL || strlen(et) >= S3_ETAG_LEN)
			code = 0;
		else
			strlcpy(etag, et, S3_ETAG_LEN);
	}
	if (storageNow)
		*storageNow = http_date_epoch(date);
	if (et)
		pfree(et);
	if (date)
		pfree(date);
	return code;
}

/*
 * Put body under ifMatch (or If-None-Match when ifMatch is NULL) and judge an
 * unclear answer by reading the object back.  On CAS_DONE and CAS_STALE,
 * etag is the object's ETag; on CAS_STALE and CAS_LOST, current holds the
 * object's body.
 */
static CasResult
lease_cas(const char *key, StringInfo body, const char *ifMatch,
		  uint64 token, const char *owner, uint32 attempt, bool isRoot,
		  char *etag, int64 *storageNow, StringInfo current)
{
	char	   *et = NULL,
			   *date = NULL;
	StringInfoData resp;
	long		code;
	uint64		curToken;
	uint32		curAttempt;
	char		curOwner[OWNER_LEN],
				curEtag[S3_ETAG_LEN];
	bool		parsed;

	initStringInfo(&resp);
	code = s3_request_once("PUT", (char *) key, body->data, body->len,
						   ifMatch, ifMatch == NULL, LEASE_REQUEST_TIMEOUT,
						   &resp, &et, &date);
	if (storageNow)
		*storageNow = http_date_epoch(date);
	if (date)
		pfree(date);
	pfree(resp.data);

	if (code == S3_RESPONSE_OK && et != NULL && strlen(et) < S3_ETAG_LEN)
	{
		strlcpy(etag, et, S3_ETAG_LEN);
		pfree(et);
		return CAS_DONE;
	}
	if (et)
		pfree(et);
	if (code >= 400 && code < 500 && code != 429 &&
		code != S3_RESPONSE_NOT_FOUND &&
		code != S3_RESPONSE_CONDITION_CONFLICT &&
		code != S3_RESPONSE_CONDITION_FAILED)
		return CAS_ERROR;

	/*
	 * The ETag read back is ours only while the object is: handed to a writer
	 * that lost it, a later If-Match (the release at stop) would overwrite
	 * the new writer's object.
	 */
	code = lease_get(key, current, curEtag, NULL);
	if (code == S3_RESPONSE_NOT_FOUND)
		return ifMatch ? CAS_LOST : CAS_UNKNOWN;
	if (code != S3_RESPONSE_OK)
		return CAS_UNKNOWN;

	if (isRoot)
	{
		RootBody	r;

		parsed = parse_root(current->data, &r);
		curToken = r.token;
		curAttempt = r.attempt;
		strlcpy(curOwner, r.owner, sizeof(curOwner));
	}
	else
	{
		WriterBody	w;

		parsed = parse_writer(current->data, &w);
		curToken = w.token;
		curAttempt = w.attempt;
		strlcpy(curOwner, w.owner, sizeof(curOwner));
	}

	if (!parsed || curToken != token || strcmp(curOwner, owner) != 0)
		return CAS_LOST;
	strlcpy(etag, curEtag, S3_ETAG_LEN);
	return curAttempt == attempt ? CAS_DONE : CAS_STALE;
}

static void
make_owner(char *owner)
{
	char		host[64];
	uint64		nonce = 0;

	if (gethostname(host, sizeof(host)) != 0)
		strlcpy(host, "unknown", sizeof(host));
	host[sizeof(host) - 1] = '\0';
	if (!pg_strong_random(&nonce, sizeof(nonce)))
		nonce = (uint64) time(NULL) ^ ((uint64) getpid() << 32);
	snprintf(owner, OWNER_LEN, "%s:%d:%016llx", host, (int) getpid(),
			 (unsigned long long) nonce);
}

/*
 * The local token file: "<token> <bucket id>".  Absent: token 0, bucket 0.
 */
static void
read_token_file(uint64 *token, uint64 *bucket)
{
	char		buf[64];
	int			fd;
	ssize_t		len;

	*token = *bucket = 0;
	fd = BasicOpenFile(TOKEN_FILENAME, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", TOKEN_FILENAME)));
	}
	len = read(fd, buf, sizeof(buf) - 1);
	close(fd);
	if (len <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\"", TOKEN_FILENAME)));
	buf[len] = '\0';
	if (sscanf(buf, UINT64_FORMAT " %llx", token, (unsigned long long *) bucket) != 2 ||
		*token == 0 || *bucket == 0)
		ereport(FATAL,
				(errmsg("invalid S3 writer token in file \"%s\"", TOKEN_FILENAME)));
}

static void
write_token_file(uint64 token, uint64 bucket)
{
	char		tmp[MAXPGPATH];
	char		buf[64];
	int			fd,
				len;

	snprintf(tmp, sizeof(tmp), "%s.tmp", TOKEN_FILENAME);
	len = snprintf(buf, sizeof(buf), UINT64_FORMAT " %016llx\n", token,
				   (unsigned long long) bucket);

	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmp)));
	if (write(fd, buf, len) != len || pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, TOKEN_FILENAME, FATAL);
}

/*
 * The local epochs file: "<first checkpoint> <token>" lines, in order.
 */
static void
read_epochs_file(S3LeaseState *state)
{
	FILE	   *f;
	unsigned int first;
	unsigned long long token;

	state->numEpochs = 0;
	f = AllocateFile(EPOCHS_FILENAME, "r");
	if (f == NULL)
	{
		if (errno == ENOENT)
			return;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", EPOCHS_FILENAME)));
	}
	while (fscanf(f, "%u %llu", &first, &token) == 2)
	{
		if (state->numEpochs >= S3_MAX_EPOCHS ||
			(state->numEpochs > 0 &&
			 state->epochs[state->numEpochs - 1].firstCheckpoint > first))
			ereport(FATAL,
					(errmsg("invalid S3 epochs file \"%s\"", EPOCHS_FILENAME)));
		state->epochs[state->numEpochs].firstCheckpoint = first;
		state->epochs[state->numEpochs].token = token;
		state->numEpochs++;
	}
	if (!feof(f))
		ereport(FATAL,
				(errmsg("invalid S3 epochs file \"%s\"", EPOCHS_FILENAME)));
	FreeFile(f);
}

/*
 * The tokens of this data directory's runs, the newest last: the chunks and
 * terminators any of them put are its own.  A data directory the loader made
 * starts a lineage of its own.
 */
static void
read_lineage_file(S3LeaseState *state)
{
	FILE	   *f;
	unsigned long long token;

	state->numLineage = 0;
	f = AllocateFile(LINEAGE_FILENAME, "r");
	if (f == NULL)
		return;
	while (fscanf(f, "%llu", &token) == 1)
	{
		if (state->numLineage == S3_MAX_LINEAGE)
		{
			memmove(state->lineage, state->lineage + 1,
					sizeof(uint64) * (S3_MAX_LINEAGE - 1));
			state->numLineage--;
		}
		state->lineage[state->numLineage++] = token;
	}
	FreeFile(f);
}

static void
write_lineage_file(S3LeaseState *state)
{
	char		tmp[MAXPGPATH];
	StringInfoData buf;
	int			fd;

	initStringInfo(&buf);
	for (int i = 0; i < state->numLineage; i++)
		appendStringInfo(&buf, UINT64_FORMAT "\n", state->lineage[i]);
	snprintf(tmp, sizeof(tmp), "%s.tmp", LINEAGE_FILENAME);
	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0 || write(fd, buf.data, buf.len) != buf.len || pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, LINEAGE_FILENAME, FATAL);
	pfree(buf.data);
}

static void
write_epochs_file(S3LeaseState *state)
{
	char		tmp[MAXPGPATH];
	StringInfoData buf;
	int			fd;

	initStringInfo(&buf);
	for (int i = 0; i < state->numEpochs; i++)
		appendStringInfo(&buf, "%u " UINT64_FORMAT "\n",
						 state->epochs[i].firstCheckpoint,
						 state->epochs[i].token);

	snprintf(tmp, sizeof(tmp), "%s.tmp", EPOCHS_FILENAME);
	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmp)));
	if (write(fd, buf.data, buf.len) != buf.len || pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, EPOCHS_FILENAME, FATAL);
	pfree(buf.data);
}

/*
 * This run writes the checkpoints after the local one.  A run that started
 * at the same checkpoint and never completed one keeps its line, before this
 * run's: a build it did names its tree's map with the local checkpoint under
 * its token, and recovery here went on from that map.  Checkpoint numbers
 * are this run's; only s3_map_tokens() looks at the earlier lines.
 */
static void
start_epoch(S3LeaseState *state, uint32 localCheckpoint, uint64 token)
{
	uint32		first = localCheckpoint + 1;

	int			same = 0;

	while (state->numEpochs > 0 &&
		   state->epochs[state->numEpochs - 1].firstCheckpoint > first)
		state->numEpochs--;

	/*
	 * A run that crashes again and again before its first checkpoint would
	 * grow the table without end: keep the newest S3_MAX_SAME_START lines of
	 * one start.  A build is done by a run that served clients, which a
	 * crash loop at start does not.
	 */
	while (same < state->numEpochs &&
		   state->epochs[state->numEpochs - 1 - same].firstCheckpoint == first)
		same++;
	if (same >= S3_MAX_SAME_START)
	{
		int			drop = same - S3_MAX_SAME_START + 1;
		int			at = state->numEpochs - same;

		memmove(&state->epochs[at], &state->epochs[at + drop],
				sizeof(S3Epoch) * (same - drop));
		state->numEpochs -= drop;
	}
	if (state->numEpochs >= S3_MAX_EPOCHS)
		ereport(FATAL,
				(errmsg("too many S3 writer epochs in \"%s\"", EPOCHS_FILENAME)));
	state->epochs[state->numEpochs].firstCheckpoint = first;
	state->epochs[state->numEpochs].token = token;
	state->numEpochs++;
}

/*
 * The epoch table is read without the lock but for one case: the GC prunes
 * it in place (s3_lease_prune_epochs()), so a reader in another process
 * takes the lock shared.  The postmaster takes no LWLock, and a process
 * that holds the lock already reads under it.
 */
static bool
lock_epochs(void)
{
	if (!IsUnderPostmaster || LWLockHeldByMe(&lease->lock))
		return false;
	LWLockAcquire(&lease->lock, LW_SHARED);
	return true;
}

/*
 * The GC: the checkpoints from `from` on are retained, and of the ones
 * before only refs[] have objects a retained checkpoint refers to.  The other
 * epochs are no longer needed to name an object: they go, so the table does
 * not grow with every run for ever.  A map of checkpoint c may carry the
 * token of the run after c's (a build names it so), so that epoch stays too.
 */
void
s3_lease_prune_epochs(uint32 from, const uint32 *refs, int nrefs)
{
	bool		keep[S3_MAX_EPOCHS];
	int			n = 0;

	LWLockAcquire(&lease->lock, LW_EXCLUSIVE);
	for (int i = 0; i < lease->numEpochs; i++)
		keep[i] = i + 1 == lease->numEpochs ||
			lease->epochs[i + 1].firstCheckpoint > from;
	for (int r = 0; r < nrefs; r++)
	{
		int			i = lease->numEpochs - 1;

		while (i >= 0 && lease->epochs[i].firstCheckpoint > refs[r])
			i--;
		if (i >= 0)
			keep[i] = true;
		/* every run that started after refs[r]: s3_map_tokens() */
		for (int j = i + 1; j < lease->numEpochs &&
			 lease->epochs[j].firstCheckpoint == lease->epochs[i + 1].firstCheckpoint; j++)
			keep[j] = true;
	}
	for (int i = 0; i < lease->numEpochs; i++)
		if (keep[i])
			lease->epochs[n++] = lease->epochs[i];
	if (n < lease->numEpochs)
	{
		lease->numEpochs = n;
		write_epochs_file(lease);
	}
	LWLockRelease(&lease->lock);
}

/*
 * The token of the epoch chkpNum falls in, if any.
 */
bool
s3_checkpoint_token_known(uint32 chkpNum, uint64 *token)
{
	bool		locked = lock_epochs();
	bool		found = false;

	for (int i = lease->numEpochs - 1; i >= 0; i--)
		if (lease->epochs[i].firstCheckpoint <= chkpNum)
		{
			*token = lease->epochs[i].token;
			found = true;
			break;
		}
	if (locked)
		LWLockRelease(&lease->lock);
	return found;
}

/*
 * The tokens a tree's map of checkpoint chkpNum may be under, the likeliest
 * first: the runs that started right after chkpNum, newest first (a build
 * names its map with the last checkpoint done, and a run that crashed before
 * its first checkpoint keeps its line), then the run of chkpNum + 1 and the
 * run of chkpNum.  Returns their number; *tokens is palloc'd.
 */
int
s3_map_tokens(uint32 chkpNum, uint64 **tokens)
{
	bool		locked = lock_epochs();
	int			n = 0;
	uint64		token;

	*tokens = palloc(sizeof(uint64) * (lease->numEpochs + 2));
	for (int i = lease->numEpochs - 1; i >= 0; i--)
		if (lease->epochs[i].firstCheckpoint == chkpNum + 1)
			(*tokens)[n++] = lease->epochs[i].token;
	if (locked)
		LWLockRelease(&lease->lock);
	for (int pass = 0; pass < 2; pass++)
	{
		bool		dup = false;

		if (!s3_checkpoint_token_known(chkpNum + 1 - pass, &token))
			continue;
		for (int i = 0; i < n; i++)
			dup |= (*tokens)[i] == token;
		if (!dup)
			(*tokens)[n++] = token;
	}
	return n;
}

/*
 * The token that wrote checkpoint chkpNum's objects.
 */
uint64
s3_checkpoint_token(uint32 chkpNum)
{
	uint64		token;

	if (!s3_checkpoint_token_known(chkpNum, &token))
		elog(ERROR, "no S3 writer token is known for checkpoint %u", chkpNum);
	return token;
}

/*
 * Read an object at startup, waiting out a store that does not answer.
 */
static bool
startup_get(const char *key, StringInfo body, char *etag, int64 *storageNow)
{
	long		code = 0;

	for (int i = 0; i < STARTUP_ATTEMPTS; i++)
	{
		code = lease_get(key, body, etag, storageNow);
		if (code == S3_RESPONSE_OK)
			return true;
		if (code == S3_RESPONSE_NOT_FOUND)
			return false;
		if (!is_transient(code))
			break;
		pg_usleep(1000000L);
	}
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("could not read \"%s\" from the S3 bucket", key),
			 errdetail("http code = %ld", code)));
	return false;
}

static void
startup_cas_failed(CasResult res, const char *key)
{
	if (res == CAS_LOST)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("another writer replaced \"%s\" in the S3 bucket while "
						"this instance was starting", key)));
	ereport(FATAL,
			(errcode(ERRCODE_CONNECTION_EXCEPTION),
			 errmsg("could not put object to S3"),
			 errdetail("could not write \"%s\"", key)));
}

/*
 * Wait for the lease our own token holds to run out: the previous run of this
 * data directory, which stopped without giving it back, or a copy of this
 * data directory that is running now.  A copy renews it, which ends the wait.
 */
static void
wait_for_own_lease(WriterBody *writer, char *writerEtag, int64 storageNow)
{
	StringInfoData body;
	char		etag[S3_ETAG_LEN];

	ereport(LOG,
			(errmsg("waiting for the S3 writer lease of token " UINT64_FORMAT
					" to run out", writer->token),
			 errdetail("This data directory holds it, and it has " INT64_FORMAT
					   " seconds left.",
					   writer->expiresAt - storageNow + 1)));
	initStringInfo(&body);
	while (writer->expiresAt >= storageNow)
	{
		WriterBody	cur;

		pg_usleep(1000000L);
		if (!startup_get(WRITER_KEY, &body, etag, &storageNow) ||
			!parse_writer(body.data, &cur))
			ereport(FATAL,
					(errmsg("the \"%s\" object of the S3 bucket changed while "
							"waiting for it", WRITER_KEY)));
		require_store_clock(storageNow, WRITER_KEY);
		if (strcmp(etag, writerEtag) != 0)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the S3 bucket is in use by another writer"),
					 errdetail("Writer token " UINT64_FORMAT " (%s) renewed the lease "
							   "this data directory was waiting for: a copy of "
							   "this data directory is running.",
							   cur.token, cur.owner)));
	}
	pfree(body.data);
}

/*
 * Take the writer lease and write our token to root.  Runs in the postmaster
 * before anything is uploaded; refuses to start when another writer holds the
 * lease, or has written root since this data directory last did.
 */
/*
 * A copy of root under its writer's token and its checkpoint's number: root is
 * a single object, and a root lost or damaged is put back from the copy of the
 * largest token whose manifest is there (orioledb_s3_loader.py --repair-root).
 * Each commit leaves one, and so does a start, whose root carries the new
 * token and epoch under the same checkpoint: with the token in the key the
 * two do not share one, and a copy a fenced writer puts late never replaces
 * the new writer's.  The GC removes the copies with the manifests.
 */
static void
put_root_copy(uint64 token, uint32 checkpointNum, StringInfo body)
{
	char	   *key = psprintf(ROOT_COPY_FORMAT, token, checkpointNum);

	if (s3_put_object_conditional(key, body->data, body->len, NULL, false,
								  NULL) != S3_RESPONSE_OK)
		ereport(WARNING,
				(errmsg("could not put a copy of \"%s\" to S3 as \"%s\"",
						ROOT_KEY, key)));
	pfree(key);
}

void
s3_lease_acquire(void)
{
	StringInfoData body,
				current;
	char		writerEtag[S3_ETAG_LEN],
				rootEtag[S3_ETAG_LEN];
	WriterBody	writer,
				newWriter;
	RootBody	root,
				newRoot;
	bool		writerExists,
				rootExists;
	uint64		local,
				localBucket,
				bucket;
	CheckpointControl control;
	bool		haveControl;
	int64		storageNow = -1;
	TimestampTz sentAt = 0;
	CasResult	res = CAS_UNKNOWN;
	uint64		runToken = pm_lease.token;
	char		runOwner[OWNER_LEN];

	initStringInfo(&body);
	initStringInfo(&current);
	strlcpy(runOwner, pm_lease.owner, OWNER_LEN);

	/* a state older than the bucket's may be read, never written on */
	if (access(AS_OF_FILENAME, F_OK) == 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("this data directory was loaded as of an older checkpoint "
						"of the S3 bucket"),
				 errhint("Start it with orioledb.s3_reader = on, or load the "
						 "last checkpoint to write.")));

	read_token_file(&local, &localBucket);
	haveControl = get_checkpoint_control_data(&control);
	if (!haveControl)
	{
		control.lastCheckpointNumber = 0;
		control.controlIdentifier = 0;
	}
	start_checkpoint = control.lastCheckpointNumber;

	memset(&root, 0, sizeof(root));
	rootExists = startup_get(ROOT_KEY, &body, rootEtag, NULL);
	if (rootExists && !parse_root(body.data, &root))
		ereport(FATAL,
				(errmsg("invalid \"%s\" object in the S3 bucket", ROOT_KEY)));

	/* a versioned bucket keeps everything the GC deletes */
	s3_check_bucket_versioning();

	/* the data directory is the bucket's, and not behind it */
	if (rootExists)
	{
		const char *errmsg_text = NULL;
		const char *errdetail_text = NULL;

		if (!s3_check_control(haveControl ? &control : NULL, root.controlId,
							  root.checkpoint, &errmsg_text, &errdetail_text))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("%s", errmsg_text),
					 errdetail("%s", errdetail_text)));
	}

	memset(&writer, 0, sizeof(writer));
	writerExists = startup_get(WRITER_KEY, &body, writerEtag, &storageNow);
	if (writerExists && !parse_writer(body.data, &writer))
		ereport(FATAL,
				(errmsg("invalid \"%s\" object in the S3 bucket", WRITER_KEY)));
	require_store_clock(storageNow, WRITER_KEY);

	if (rootExists)
		bucket = root.bucket;
	else if (writerExists)
		bucket = writer.bucket;
	else
	{
		bucket = 0;
		while (bucket == 0)
			if (!pg_strong_random(&bucket, sizeof(bucket)))
				ereport(FATAL,
						(errmsg("could not generate a random S3 bucket id")));
	}

	/* a token of another bucket (or prefix) is not ours here */
	if (localBucket != bucket)
		local = 0;

	if (rootExists && root.token > local)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("another writer has committed to the S3 bucket since this "
						"data directory last did"),
				 errdetail("The bucket's root names writer token " UINT64_FORMAT
						   ", the local data directory token " UINT64_FORMAT ".",
						   root.token, local),
				 errhint("Load the data directory from the bucket.")));

	/*
	 * A restart after a crash: the lease is this postmaster's own, held under
	 * its owner, and no copy can have renewed it.
	 */
	if (writerExists && writer.expiresAt >= storageNow &&
		!(runToken != 0 && writer.token == runToken &&
		  strcmp(writer.owner, runOwner) == 0))
	{
		if (writer.token != local)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the S3 bucket is in use by another writer"),
					 errdetail("Writer token " UINT64_FORMAT " (%s) holds the lease "
							   "for " INT64_FORMAT " more seconds.",
							   writer.token, writer.owner,
							   writer.expiresAt - storageNow + 1)));
		wait_for_own_lease(&writer, writerEtag, storageNow);
	}

	memset(&pm_lease, 0, sizeof(pm_lease));
	pm_lease.clockOffset = storageNow - (int64) time(NULL);
	pm_lease.bucket = bucket;
	pm_lease.prevToken = local;
	/* a crash restart goes on from this postmaster's own run, loaded or not */
	pm_lease.loaded = runToken == 0 && access(LOADED_FILENAME, F_OK) == 0;
	make_owner(pm_lease.owner);
	pm_lease.token = Max(Max(local, writer.token), root.token) + 1;

	memset(&newWriter, 0, sizeof(newWriter));
	newWriter.token = pm_lease.token;
	newWriter.bucket = bucket;
	strlcpy(newWriter.owner, pm_lease.owner, OWNER_LEN);
	for (int i = 0; i < STARTUP_ATTEMPTS; i++)
	{
		newWriter.attempt++;
		newWriter.expiresAt = (int64) time(NULL) + pm_lease.clockOffset +
			s3_lease_duration;
		render_writer(&body, &newWriter);
		sentAt = GetCurrentTimestamp();
		res = lease_cas(WRITER_KEY, &body, writerExists ? writerEtag : NULL,
						pm_lease.token, pm_lease.owner, newWriter.attempt,
						false, writerEtag, NULL, &current);
		if (res == CAS_LOST && runToken != 0)
		{
			WriterBody	cur;

			/*
			 * A crash restart: a renewal the crashed run sent landed in
			 * between.  The lease is still this postmaster's: go again on it.
			 */
			if (parse_writer(current.data, &cur) && cur.token == runToken &&
				strcmp(cur.owner, runOwner) == 0)
			{
				StringInfoData again;

				initStringInfo(&again);
				writerExists = startup_get(WRITER_KEY, &again, writerEtag, NULL);
				pfree(again.data);
				continue;
			}
		}
		if (res == CAS_DONE || res == CAS_LOST || res == CAS_ERROR)
			break;
		if (res == CAS_STALE)
			writerExists = true;	/* retry on the ETag of our own attempt */
		else
			pg_usleep(1000000L);
	}
	if (res != CAS_DONE)
		startup_cas_failed(res, WRITER_KEY);

	pm_lease.writerAttempt = newWriter.attempt;
	strlcpy(pm_lease.writerEtag, writerEtag, S3_ETAG_LEN);
	pm_lease.renewedAt = sentAt;
	pm_lease.deadline = TimestampTzPlusSeconds(sentAt,
											   s3_lease_duration - LEASE_MARGIN_SECS);

	/* the token is ours before root says so: a crash in between keeps it ours */
	write_token_file(pm_lease.token, bucket);
	if (pm_lease.loaded)
		pm_lease.numLineage = 0;
	else
	{
		read_lineage_file(&pm_lease);
		/* a data directory from before the lineage file: its last run */
		if (pm_lease.numLineage == 0 && local != 0)
			pm_lease.lineage[pm_lease.numLineage++] = local;
	}
	if (pm_lease.numLineage == S3_MAX_LINEAGE)
	{
		memmove(pm_lease.lineage, pm_lease.lineage + 1,
				sizeof(uint64) * (S3_MAX_LINEAGE - 1));
		pm_lease.numLineage--;
	}
	pm_lease.lineage[pm_lease.numLineage++] = pm_lease.token;
	write_lineage_file(&pm_lease);
	read_epochs_file(&pm_lease);
	start_epoch(&pm_lease, control.lastCheckpointNumber, pm_lease.token);
	write_epochs_file(&pm_lease);

	newRoot = root;
	newRoot.token = pm_lease.token;
	newRoot.bucket = bucket;
	if (haveControl)
		newRoot.controlId = control.controlIdentifier;
	strlcpy(newRoot.owner, pm_lease.owner, OWNER_LEN);
	newRoot.attempt = 0;
	res = CAS_UNKNOWN;
	for (int i = 0; i < STARTUP_ATTEMPTS; i++)
	{
		newRoot.attempt++;
		render_root(&body, &newRoot, &pm_lease);
		res = lease_cas(ROOT_KEY, &body, rootExists ? rootEtag : NULL,
						pm_lease.token, pm_lease.owner, newRoot.attempt,
						true, rootEtag, NULL, &current);
		if (res == CAS_DONE || res == CAS_LOST || res == CAS_ERROR)
			break;
		if (res == CAS_STALE)
			rootExists = true;
		else
			pg_usleep(1000000L);
	}
	if (res != CAS_DONE)
		startup_cas_failed(res, ROOT_KEY);

	pm_lease.root = newRoot;
	strlcpy(pm_lease.rootEtag, rootEtag, S3_ETAG_LEN);
	/* a takeover's root, with its token and epoch, is the one to put back */
	if (newRoot.checkpoint > 0)
		put_root_copy(pm_lease.token, newRoot.checkpoint, &body);

	/*
	 * No chunk of an earlier writer after this: of the one this load takes
	 * over from, or of a copy of this data directory still going on with
	 * the previous run's token.
	 */
	s3_wal_place_terminator(pm_lease.token, pm_lease.loaded);

	/*
	 * The load is taken over: the next start goes on from this run, whether
	 * or not its shipper ever ran (with shipping off it never does).
	 */
	if (pm_lease.loaded)
		durable_unlink(LOADED_FILENAME, FATAL);

	ereport(LOG,
			(errmsg("took the S3 writer lease with token " UINT64_FORMAT,
					pm_lease.token),
			 errdetail("root checkpoint %u", root.checkpoint)));

	pfree(body.data);
	pfree(current.data);
}

/*
 * Give the lease back at a clean stop, so the next start (ours or another
 * instance's) does not wait for it to run out.  Only the postmaster calls
 * this, after every child is gone; a lease already lost is left alone, as
 * the If-Match fails.
 */
void
s3_lease_release(void)
{
	StringInfoData body,
				resp;
	WriterBody	w;

	if (lease == NULL || lease->token == 0)
		return;

	memset(&w, 0, sizeof(w));
	w.token = lease->token;
	w.bucket = lease->bucket;
	w.attempt = lease->writerAttempt + 1;
	strlcpy(w.owner, lease->owner, OWNER_LEN);
	w.expiresAt = 0;

	initStringInfo(&body);
	initStringInfo(&resp);
	render_writer(&body, &w);
	(void) s3_request_once("PUT", WRITER_KEY, body.data, body.len,
						   lease->writerEtag, false, LEASE_REQUEST_TIMEOUT,
						   &resp, NULL, NULL);
	pfree(body.data);
	pfree(resp.data);
}

/*
 * The checkpoint a reader was loaded at, or 0 when it is not known: the
 * loader puts that checkpoint's manifest in the data directory, and a reader
 * writes no manifest of its own.
 */
static uint32
loaded_checkpoint(void)
{
	FILE	   *f = fopen(ORIOLEDB_DATA_DIR "/s3_manifest", "r");
	char		line[256];
	uint32		chkp = 0;

	if (f == NULL)
		return 0;
	while (fgets(line, sizeof(line), f) != NULL)
		if (sscanf(line, "checkpoint %u", &chkp) == 1)
			break;
	fclose(f);
	return chkp;
}

/*
 * A reader's own pages go to local parts, which a start throws away (the
 * bucket holds other pages of the writer under their numbers).  A restartpoint
 * past the load names those local parts, so a reader that made one cannot
 * start again from it: recovery would read the writer's parts in their place.
 * It starts only from the checkpoint it was loaded at.
 */
static void
reader_check_restartable(uint32 lastCheckpoint)
{
	uint32		loaded = loaded_checkpoint();

	if (loaded != 0 && lastCheckpoint > loaded)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("this S3 reader cannot start again on its data directory"),
				 errdetail("It was loaded at checkpoint %u and made restartpoints up to checkpoint %u; "
						   "the pages it wrote since are local and were not kept.",
						   loaded, lastCheckpoint),
				 errhint("Load the data directory from the bucket again.")));
}

/*
 * The checkpoint the data directory held when the postmaster started: a build
 * the writer before this run did after it named its tree's map with it.
 */
uint32
s3_start_checkpoint(void)
{
	return start_checkpoint;
}

/*
 * A reader takes no lease and writes nothing to the bucket: it only needs
 * the epochs, to name the maps of the checkpoints it opens.
 */
void
s3_lease_reader_init(void)
{
	memset(&pm_lease, 0, sizeof(pm_lease));
	read_epochs_file(&pm_lease);
	if (pm_lease.numEpochs == 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("an S3 reader needs a data directory loaded from the bucket"),
				 errdetail("The file \"%s\" is missing.", EPOCHS_FILENAME)));
	{
		CheckpointControl control;

		if (!get_checkpoint_control_data(&control))
			control.lastCheckpointNumber = 0;
		start_checkpoint = control.lastCheckpointNumber;
		reader_check_restartable(control.lastCheckpointNumber);
		reader_pin_and_check(control.lastCheckpointNumber);
	}
	ereport(LOG,
			(errmsg("reading the S3 bucket without writing to it")));
}

Size
s3_lease_shmem_needs(void)
{
	return sizeof(S3LeaseState);
}

void
s3_lease_shmem_init(Pointer ptr, bool found)
{
	static bool made = false;

	/*
	 * The postmaster makes shared memory again after a crash: the run that
	 * follows takes a new token, so the checkpoint numbers it takes again
	 * never name the objects the crashed run left, nor one of its PUTs still
	 * on the way.  It is taken as at the first start, with no shared state.
	 */
	if (!found && made && !IsUnderPostmaster && pm_lease.token != 0 &&
		!orioledb_s3_reader)
	{
		lease = NULL;
		s3_postmaster_retries = true;
		s3_lease_acquire();
		s3_postmaster_retries = false;
	}
	/* a reader's crash restart is a start too: its local parts were dropped */
	if (!found && made && !IsUnderPostmaster && orioledb_s3_reader)
	{
		CheckpointControl control;

		if (get_checkpoint_control_data(&control))
			reader_check_restartable(control.lastCheckpointNumber);
	}
	made = true;
	lease = (S3LeaseState *) ptr;

	if (!found)
	{
		int			trancheId = LWLockNewTrancheId();

		memcpy(lease, &pm_lease, sizeof(S3LeaseState));
		lease->trancheId = trancheId;
		LWLockInitialize(&lease->lock, trancheId);
		LWLockInitialize(&lease->requestLock, trancheId);
	}
	LWLockRegisterTranche(lease->trancheId, "S3LeaseTranche");
}

uint64
s3_lease_token(void)
{
	return lease->token;
}

/*
 * Whether chunks of WAL a writer token put may be taken as this data
 * directory's own: this run's, or the previous run's of a directory that the
 * loader did not just make.
 */
bool
s3_lease_own_token(uint64 token)
{
	if (token == lease->token)
		return true;
	for (int i = 0; i < lease->numLineage; i++)
		if (lease->lineage[i] == token)
			return true;
	return false;
}

/* The data directory has gone on from what the loader made. */
void
s3_lease_loaded_done(void)
{
	lease->loaded = false;
}

/*
 * Stop the instance for a reason that is not a lost lease, but where going on
 * would put a history the bucket does not have.
 */
void
s3_stop_instance(const char *message, const char *detail)
{
	ereport(LOG, (errmsg("%s", message), errdetail("%s", detail)));
	(void) kill(PostmasterPid, SIGQUIT);
	ereport(FATAL,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("%s", message)));
}

/*
 * The lease is gone, or may be: stop the whole instance at once.  A backend
 * waiting for a commit would otherwise be answered "committed locally".
 */
void
s3_lease_lost(const char *detail)
{
	ereport(LOG,
			(errmsg("lost the S3 writer lease, shutting down"),
			 errdetail("%s", detail)));
	(void) kill(PostmasterPid, SIGQUIT);
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("lost the S3 writer lease")));
}

/*
 * The requests to the store (a renewal, a root commit) run under requestLock
 * alone, so there is one renewer at a time; lease->lock guards the state and
 * is held for a copy in or out, never across a request.  A status read or an
 * epoch lookup (every S3 worker's key) then never waits on a store that does
 * not answer.
 */
static TimestampTz
lease_deadline(void)
{
	TimestampTz deadline;

	LWLockAcquire(&lease->lock, LW_SHARED);
	deadline = lease->deadline;
	LWLockRelease(&lease->lock);
	return deadline;
}

/*
 * One renewal; requestLock is held.  Returns false when the store gave no
 * answer to judge by.
 */
static bool
lease_renew(void)
{
	StringInfoData body,
				current;
	WriterBody	w;
	CasResult	res;
	int64		storageNow,
				clockOffset;
	TimestampTz sentAt;
	uint32		attempt;
	char		etag[S3_ETAG_LEN];
	bool		done = false;

	initStringInfo(&body);
	initStringInfo(&current);
	memset(&w, 0, sizeof(w));

	LWLockAcquire(&lease->lock, LW_SHARED);
	w.token = lease->token;
	w.bucket = lease->bucket;
	strlcpy(w.owner, lease->owner, OWNER_LEN);
	attempt = lease->writerAttempt;
	strlcpy(etag, lease->writerEtag, S3_ETAG_LEN);
	clockOffset = lease->clockOffset;
	LWLockRelease(&lease->lock);

	for (int i = 0; i < 3; i++)
	{
		w.attempt = ++attempt;
		w.expiresAt = (int64) time(NULL) + clockOffset + s3_lease_duration;
		render_writer(&body, &w);
		sentAt = GetCurrentTimestamp();
		res = lease_cas(WRITER_KEY, &body, etag, w.token, w.owner, w.attempt,
						false, etag, &storageNow, &current);
		if (res == CAS_DONE)
		{
			if (storageNow >= 0)
				clockOffset = storageNow - (int64) time(NULL);
			LWLockAcquire(&lease->lock, LW_EXCLUSIVE);
			lease->writerAttempt = attempt;
			strlcpy(lease->writerEtag, etag, S3_ETAG_LEN);
			lease->renewedAt = sentAt;
			lease->deadline = TimestampTzPlusSeconds(sentAt,
													 s3_lease_duration - LEASE_MARGIN_SECS);
			lease->clockOffset = clockOffset;
			LWLockRelease(&lease->lock);
			done = true;
			break;
		}
		if (res == CAS_STALE)
		{
			WriterBody	cur;

			/* an earlier renewal of ours: it moved the ETag (a restart) */
			if (parse_writer(current.data, &cur))
				attempt = Max(attempt, cur.attempt);
			continue;
		}
		if (res == CAS_LOST)
			s3_lease_lost(psprintf("The \"%s\" object now holds: %s",
								   WRITER_KEY, current.data));
		break;
	}
	if (!done)
	{
		/* the attempts spent are not sent again */
		LWLockAcquire(&lease->lock, LW_EXCLUSIVE);
		lease->writerAttempt = Max(lease->writerAttempt, attempt);
		LWLockRelease(&lease->lock);
	}
	pfree(body.data);
	pfree(current.data);
	return done;
}

/*
 * Make sure the lease is ours for at least minSecs more; requestLock is held.
 */
static void
lease_ensure(int minSecs)
{
	for (;;)
	{
		TimestampTz now = GetCurrentTimestamp();

		if (now < TimestampTzPlusSeconds(lease_deadline(), -minSecs))
			return;
		if (lease_renew())
			return;
		if (GetCurrentTimestamp() >= lease_deadline())
			s3_lease_lost("The store did not answer before the lease ran out.");
		pg_usleep(1000000L);
	}
}

/*
 * Before an object a fenced writer must not put (a WAL segment of the
 * archive): the lease is ours for a while yet, or the instance stops.
 * Returns until when: the put is given up then (s3_put_object_fenced_until)
 * and done again under a renewed lease.  A writer taken over waits for the
 * lease to run out on the store's clock, which is past that.
 */
TimestampTz
s3_lease_hold(void)
{
	TimestampTz deadline;

	LWLockAcquire(&lease->requestLock, LW_EXCLUSIVE);
	lease_ensure(s3_lease_duration / 2);
	deadline = lease_deadline();
	LWLockRelease(&lease->requestLock);
	return deadline;
}

/*
 * Commit checkpoint K: point root at it.  Everything K needs is uploaded.
 */
void
s3_root_commit(uint32 checkpointNum, XLogRecPtr redoLsn,
			   XLogRecPtr checkpointLsn)
{
	StringInfoData body,
				current;
	RootBody	r;
	uint32		attempt;
	char		etag[S3_ETAG_LEN];

	initStringInfo(&body);
	initStringInfo(&current);

	LWLockAcquire(&lease->requestLock, LW_EXCLUSIVE);
	LWLockAcquire(&lease->lock, LW_SHARED);
	r = lease->root;
	r.token = lease->token;
	r.bucket = lease->bucket;
	strlcpy(r.owner, lease->owner, OWNER_LEN);
	attempt = lease->root.attempt;
	strlcpy(etag, lease->rootEtag, S3_ETAG_LEN);
	LWLockRelease(&lease->lock);
	r.checkpoint = checkpointNum;
	r.redoLsn = redoLsn;
	r.checkpointLsn = checkpointLsn;
	{
		CheckpointControl control;

		if (get_checkpoint_control_data(&control))
			r.controlId = control.controlIdentifier;
	}

	for (;;)
	{
		CasResult	res;

		lease_ensure(s3_lease_duration / 2);
		r.attempt = ++attempt;
		/* the epoch table: the GC prunes it under the lock */
		LWLockAcquire(&lease->lock, LW_SHARED);
		render_root(&body, &r, lease);
		LWLockRelease(&lease->lock);
		res = lease_cas(ROOT_KEY, &body, etag, r.token, r.owner, r.attempt,
						true, etag, NULL, &current);
		if (res == CAS_DONE)
			break;
		if (res == CAS_STALE)
		{
			RootBody	cur;

			if (parse_root(current.data, &cur))
				attempt = Max(attempt, cur.attempt);
			continue;
		}
		if (res == CAS_LOST)
			s3_lease_lost(psprintf("The \"%s\" object now holds: %s",
								   ROOT_KEY, current.data));
		pg_usleep(1000000L);
	}
	LWLockAcquire(&lease->lock, LW_EXCLUSIVE);
	lease->root = r;
	strlcpy(lease->rootEtag, etag, S3_ETAG_LEN);
	LWLockRelease(&lease->lock);
	LWLockRelease(&lease->requestLock);

	put_root_copy(r.token, checkpointNum, &body);

	elog(DEBUG1, "S3 root now points at checkpoint %u", checkpointNum);
	pfree(body.data);
	pfree(current.data);
}

uint32
s3_lease_root_checkpoint(void)
{
	uint32		chkp;

	LWLockAcquire(&lease->lock, LW_SHARED);
	chkp = lease->root.checkpoint;
	LWLockRelease(&lease->lock);
	return chkp;
}

/*
 * The GC's progress: retain, its retention point R, published before the
 * pins are read (a reader that pins after that read sees R and does not start
 * below it); folded, the checkpoints whose lists of dead extents are in the
 * trees' GC states; collected, the checkpoints whose manifests and the
 * objects only they referred to are gone.  Another writer that takes the
 * bucket over goes on from there.  `skipped` is lines "skipped <db> <rel>"
 * (NULL: none): the trees whose lists the GC left because it could not read
 * their state, which --sweep leaves too.
 */
void
s3_gc_publish(uint32 retain, uint32 folded, uint32 collected,
			  uint32 pinFloor, uint32 pinWarn, const char *skipped)
{
	StringInfoData body;

	StringInfoData current;

	LWLockAcquire(&lease->lock, LW_EXCLUSIVE);
	lease->gcPublished[0] = retain;
	lease->gcPublished[1] = folded;
	lease->gcPublished[2] = collected;
	LWLockRelease(&lease->lock);

	initStringInfo(&body);
	appendStringInfo(&body, "format %d\nretain %u\nfolded %u\ncollected %u\n"
					 "pin_floor %u\npin_warn %u\ntoken " UINT64_FORMAT "\n",
					 LEASE_FORMAT, retain, folded, collected, pinFloor, pinWarn,
					 lease->token);
	if (skipped)
		appendStringInfoString(&body, skipped);

	/*
	 * Put over what is there only while it is ours or an earlier writer's,
	 * and only if it is still what was read: a writer this one was fenced
	 * off by publishes its own progress.
	 */
	initStringInfo(&current);
	for (int attempt = 0;; attempt++)
	{
		char		etag[S3_ETAG_LEN];
		char		value[32];
		long		code;

		resetStringInfo(&current);
		code = lease_get(GC_KEY, &current, etag, NULL);
		if (code != S3_RESPONSE_OK && code != S3_RESPONSE_NOT_FOUND)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("could not read \"%s\" from S3", GC_KEY)));
		if (code == S3_RESPONSE_OK &&
			kv_get(current.data, "token", value, sizeof(value)) &&
			strtou64(value, NULL, 10) > lease->token)
			s3_lease_lost(psprintf("The \"%s\" object now holds: %s",
								   GC_KEY, current.data));
		code = s3_put_object_conditional(GC_KEY, body.data, body.len,
										 code == S3_RESPONSE_OK ? etag : NULL,
										 code != S3_RESPONSE_OK, NULL);
		if (code == S3_RESPONSE_OK)
			break;
		if (code != S3_RESPONSE_CONDITION_FAILED &&
			code != S3_RESPONSE_CONDITION_CONFLICT &&
			code != S3_RESPONSE_NOT_FOUND)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("could not put \"%s\" to S3", GC_KEY),
					 errdetail("http code = %ld", code)));
		pg_usleep(Min(1000000L, 10000L << Min(attempt, 7)));
	}
	pfree(current.data);
	pfree(body.data);
}

/*
 * What the GC published last.  False when the bucket cannot tell.  The
 * "skipped" lines go to `skipped` when it is not NULL.
 */
bool
s3_gc_progress(uint32 *retain, uint32 *folded, uint32 *collected,
			   StringInfo skipped)
{
	StringInfoData body;
	char		etag[S3_ETAG_LEN];
	char		value[32];
	long		code;

	*retain = *folded = *collected = 0;
	initStringInfo(&body);
	code = lease_get(GC_KEY, &body, etag, NULL);
	if (code == S3_RESPONSE_OK)
	{
		if (kv_get(body.data, "retain", value, sizeof(value)))
			*retain = (uint32) strtoul(value, NULL, 10);
		if (kv_get(body.data, "folded", value, sizeof(value)))
			*folded = (uint32) strtoul(value, NULL, 10);
		if (kv_get(body.data, "collected", value, sizeof(value)))
			*collected = (uint32) strtoul(value, NULL, 10);
		if (skipped)
		{
			const char *line = body.data;

			while (line && *line)
			{
				const char *end = strchr(line, '\n');

				if (strncmp(line, "skipped ", 8) == 0)
					appendBinaryStringInfo(skipped, line,
										   end ? end - line + 1 : strlen(line));
				line = end ? end + 1 : NULL;
			}
		}
	}
	pfree(body.data);
	return code == S3_RESPONSE_OK || code == S3_RESPONSE_NOT_FOUND;
}

/*
 * Pins are lines "<id> <checkpoint> <expires_at> [<ttl>]" of the readers
 * object, the store's clock.  Returns the lowest checkpoint of a pin not
 * expired, or upper when there is none.  A pin below `floor` is not counted;
 * one below `pinFloor` is not counted once its reader has had a renewal in
 * which to see pin_warn and stop -- the pin is older than a renewal interval,
 * a third of its time to live (the line's; a line without one, a pin from
 * before the field, is taken for this writer's s3_reader_pin_ttl).  A pin
 * written since was renewed before the floor passed it: its reader does not
 * know yet, and may be reading what is below.
 */
static uint32
pins_min(const char *body, int64 storageNow, uint32 upper, uint32 floor,
		 uint32 pinFloor)
{
	const char *line = body;
	uint32		result = upper;

	while (line && *line)
	{
		char		id[64];
		unsigned int chkp;
		long long	expires,
					ttl = s3_reader_pin_ttl;
		int			n = sscanf(line, "%63s %u %lld %lld", id, &chkp, &expires, &ttl);

		if (n >= 3 && expires >= storageNow && chkp >= floor && chkp < result)
		{
			int64		age = (int64) ttl - (expires - storageNow);

			if (chkp < pinFloor && age >= ttl / 3)
				;				/* passed over: its reader was told */
			else
				result = chkp;
		}
		line = strchr(line, '\n');
		if (line)
			line++;
	}
	return result;
}

uint32
s3_reader_pins_min(uint32 upper, uint32 floor, uint32 pinFloor)
{
	StringInfoData body;
	char		etag[S3_ETAG_LEN];
	int64		storageNow = -1;
	long		code;
	uint32		result;

	initStringInfo(&body);
	code = lease_get(READERS_KEY, &body, etag, &storageNow);
	if (code == S3_RESPONSE_NOT_FOUND)
		result = upper;
	else if (code != S3_RESPONSE_OK)
		result = 0;				/* not known: collect nothing */
	else
		result = pins_min(body.data, store_clock_or_local(storageNow, READERS_KEY),
						  upper, floor, pinFloor);
	pfree(body.data);
	return result;
}

/*
 * Put this reader's pin in the readers object: a conditional replace, since
 * every reader edits it.  Expired pins of others are dropped on the way.
 */
static void
reader_pin(const char *id, uint32 chkp)
{
	for (int attempt = 0;; attempt++)
	{
		StringInfoData body,
					next,
					resp;
		char		etag[S3_ETAG_LEN];
		int64		storageNow = -1;
		long		code;
		bool		exists;
		const char *line;

		initStringInfo(&body);
		initStringInfo(&next);
		initStringInfo(&resp);
		code = lease_get(READERS_KEY, &body, etag, &storageNow);
		if (code != S3_RESPONSE_OK && code != S3_RESPONSE_NOT_FOUND)
		{
			if (attempt >= STARTUP_ATTEMPTS)
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("could not read \"%s\" from the S3 bucket", READERS_KEY)));
			pg_usleep(1000000L);
			continue;
		}
		exists = code == S3_RESPONSE_OK;
		storageNow = store_clock_or_local(storageNow, READERS_KEY);

		/* the others' lines as they are: a field this reader does not know stays */
		for (line = exists ? body.data : ""; line && *line;)
		{
			char		other[64];
			unsigned int otherChkp;
			long long	expires;
			const char *end = strchr(line, '\n');

			if (sscanf(line, "%63s %u %lld", other, &otherChkp, &expires) == 3 &&
				strcmp(other, id) != 0 && expires >= storageNow)
			{
				appendBinaryStringInfo(&next, line, end ? end - line : strlen(line));
				appendStringInfoChar(&next, '\n');
			}
			line = end ? end + 1 : NULL;
		}
		appendStringInfo(&next, "%s %u " INT64_FORMAT " %d\n", id, chkp,
						 storageNow + s3_reader_pin_ttl, s3_reader_pin_ttl);

		code = s3_request_once("PUT", READERS_KEY, next.data, next.len,
							   exists ? etag : NULL, !exists,
							   LEASE_REQUEST_TIMEOUT, &resp, NULL, NULL);
		pfree(body.data);
		pfree(next.data);
		pfree(resp.data);
		if (code == S3_RESPONSE_OK)
			return;
		if (attempt >= 100)
			ereport(FATAL,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("could not write the reader pin to the S3 bucket")));
		pg_usleep(100000L * (1 + attempt % 10));
	}
}

/*
 * The oldest checkpoint the GC still keeps whole: R, and the checkpoints it
 * has folded and collected up to, which a GC that kept more again (a setting
 * raised) does not bring back.
 */
static uint32
gc_kept(void)
{
	uint32		retain,
				folded,
				collected;

	if (!s3_gc_progress(&retain, &folded, &collected, NULL))
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("could not read \"%s\" from the S3 bucket", GC_KEY)));
	return Max(retain, Max(folded, collected));
}

static char reader_id[64];
static uint32 reader_checkpoint;

/*
 * A reader pins the checkpoint it opens, then checks the GC has not passed it
 * already: pinned before the GC reads the pins, it is counted; pinned after,
 * it sees the R the GC published first.
 */
static void
reader_pin_and_check(uint32 chkp)
{
	uint64		nonce = 0;
	uint32		retain;

	if (!pg_strong_random(&nonce, sizeof(nonce)))
		nonce = (uint64) time(NULL) ^ ((uint64) getpid() << 32);
	snprintf(reader_id, sizeof(reader_id), "%016llx", (unsigned long long) nonce);
	reader_checkpoint = chkp;
	reader_pin(reader_id, chkp);
	retain = gc_kept();
	if (chkp < retain)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the S3 bucket no longer keeps checkpoint %u", chkp),
				 errdetail("Its GC keeps checkpoints from %u on.", retain),
				 errhint("Load the data directory from the bucket again.")));
}

/*
 * Keep the pin, and stop before the GC stops honouring it: a pin further
 * behind the committed checkpoint than orioledb.s3_gc_max_pin_lag is passed
 * over, and the parts this reader reads may go.  The writer publishes where
 * it starts warning (pin_warn), a margin above where it starts ignoring.
 */
void
s3_reader_pin_renew(void)
{
	StringInfoData body;
	char		etag[S3_ETAG_LEN];
	char		value[32];

	if (!reader_id[0])
		return;
	reader_pin(reader_id, reader_checkpoint);

	initStringInfo(&body);
	if (lease_get(GC_KEY, &body, etag, NULL) == S3_RESPONSE_OK)
	{
		/*
		 * The GC passed the pin: it lapsed (renewals failed for longer than
		 * its time to live), or was below the floor.  What the GC folds and
		 * collects up to never passes a pin it counted.
		 */
		for (int i = 0; i < 2; i++)
		{
			const char *name = i == 0 ? "folded" : "collected";

			if (kv_get(body.data, name, value, sizeof(value)) &&
				reader_checkpoint < (uint32) strtoul(value, NULL, 10))
				s3_stop_instance("the S3 GC passed this reader's checkpoint",
								 psprintf("It opened checkpoint %u; the GC has %s "
										  "checkpoints up to %s. Load the data "
										  "directory from the bucket again.",
										  reader_checkpoint, name, value));
		}
		if (kv_get(body.data, "pin_warn", value, sizeof(value)) &&
			reader_checkpoint < (uint32) strtoul(value, NULL, 10))
			s3_stop_instance("this S3 reader is too far behind the writer",
							 psprintf("It opened checkpoint %u; the GC is about to "
									  "pass over pins below %s. Load the data "
									  "directory from the bucket again.",
									  reader_checkpoint, value));
	}
	pfree(body.data);
}

void
register_s3_lease_worker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	/*
	 * A system worker, as the S3 workers are: a regular one is stopped before
	 * the shutdown checkpoint, which uploads and commits for as long as that
	 * takes, and the lease ran out in it.  It gets its SIGTERM once the
	 * checkpointer is gone, and the postmaster gives the lease back after it.
	 */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_SYSTEM;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 1;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3_lease_worker_main");
	strcpy(worker.bgw_name, "orioledb s3 lease");
	strcpy(worker.bgw_type, "orioledb s3 lease");
	RegisterBackgroundWorker(&worker);
}

/*
 * Renew the lease every half of its duration.  The first renewal is at once:
 * after a crash restart the shared state holds what the postmaster saw at
 * startup, and only a renewal tells whether the lease is still ours.
 */
void
s3_lease_worker_main(Datum main_arg)
{
	MemoryContext cxt;
	bool		renewed = false;

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	cxt = AllocSetContextCreate(TopMemoryContext, "orioledb s3 lease",
								ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(cxt);

	/* a reader holds no lease: it keeps its pin from expiring */
	while (orioledb_s3_reader && !ShutdownRequestPending)
	{
		s3_take_config_reload();
		s3_reader_pin_renew();
		MemoryContextReset(cxt);
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) s3_reader_pin_ttl * 1000L / 3, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}

	while (!ShutdownRequestPending)
	{
		TimestampTz now,
					renewedAt;

		s3_take_config_reload();
		now = GetCurrentTimestamp();

		LWLockAcquire(&lease->requestLock, LW_EXCLUSIVE);
		LWLockAcquire(&lease->lock, LW_SHARED);
		renewedAt = lease->renewedAt;
		LWLockRelease(&lease->lock);
		if (!renewed ||
			now >= TimestampTzPlusSeconds(renewedAt, s3_lease_duration / 2))
		{
			if (lease_renew())
				renewed = true;
			else if (GetCurrentTimestamp() >= lease_deadline())
				s3_lease_lost("The store did not answer before the lease ran out.");
		}
		LWLockRelease(&lease->requestLock);
		MemoryContextReset(cxt);

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	proc_exit(0);
}

/*
 * One row about this instance and the bucket, from shared memory alone (no
 * request is made): for monitoring.
 */
Datum
orioledb_s3_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[12];
	bool		nulls[12] = {0};
	XLogRecPtr	flush = RecoveryInProgress() ? GetXLogReplayRecPtr(NULL) :
		GetFlushRecPtr(NULL);
	XLogRecPtr	shipped;
	bool		running;
	uint64		token;
	TimestampTz deadline;
	uint32		root;
	uint32		gc[3];

	if (!orioledb_s3_mode)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("orioledb.s3_mode is off")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	LWLockAcquire(&lease->lock, LW_SHARED);
	token = lease->token;
	deadline = lease->deadline;
	root = lease->root.checkpoint;
	memcpy(gc, lease->gcPublished, sizeof(gc));
	LWLockRelease(&lease->lock);
	s3_wal_shipper_state(&shipped, &running);

	values[0] = CStringGetTextDatum(orioledb_s3_reader ? "reader" : "writer");
	values[1] = Int64GetDatum((int64) token);
	if (orioledb_s3_reader || deadline == 0)
		nulls[2] = true;
	else
		values[2] = Float8GetDatum((double) (deadline - GetCurrentTimestamp()) / 1000000.0);
	values[3] = Int64GetDatum(root);
	values[4] = LSNGetDatum(flush);
	if (shipped == InvalidXLogRecPtr)
	{
		nulls[5] = nulls[6] = true;
	}
	else
	{
		values[5] = LSNGetDatum(shipped);
		values[6] = Int64GetDatum(flush > shipped ? (int64) (flush - shipped) : 0);
	}
	values[7] = BoolGetDatum(running);
	values[8] = Int64GetDatum(gc[0]);
	values[9] = Int64GetDatum(gc[1]);
	values[10] = Int64GetDatum(gc[2]);
	values[11] = Int64GetDatum((int64) s3_headers_unsealed_parts());
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
