/*-------------------------------------------------------------------------
 *
 * headers.c
 * 		Routines for handling of S3-specific data file headers.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "checkpoint/checkpoint.h"
#include "s3/headers.h"
#include "s3/requests.h"
#include "s3/worker.h"

#include "catalog/pg_tablespace.h"
#include "common/file_utils.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/condition_variable.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define S3_HEADER_BUFFERS_PER_GROUP 4
#define S3_HEADER_BUFFERS_PER_GROUP_NUM_BITS 2
#define S3_HEADER_NUM_VALUES (ORIOLEDB_SEGMENT_SIZE / ORIOLEDB_S3_PART_SIZE)

/*
 * Hash an S3HeaderTag using only the fields that participate in
 * S3HeaderTagsIsEqual: datoid, relnode, tablespace, segNum.
 * reloid and ixType must be excluded so that every code path that
 * constructs a tag (including iterate_tablespace_files, which cannot know
 * reloid/ixType from the filename) always lands in the same hash group.
 */
static inline uint32
s3_header_tag_hash(S3HeaderTag tag)
{
	struct
	{
		Oid			datoid;
		Oid			relnode;
		Oid			tablespace;
		int			segNum;
	}			hashKey;

	hashKey.datoid = tag.key.oids.datoid;
	hashKey.relnode = tag.key.oids.relnode;
	hashKey.tablespace = tag.key.oids.spcoid;
	hashKey.segNum = tag.segNum;
	return hash_any((unsigned char *) &hashKey, sizeof(hashKey));
}

typedef struct
{
	int			groupCtlTrancheId;
	int			bufferCtlTrancheId;
	pg_atomic_uint64 numberOfLoadedParts;
	/* parts opened by a stream and not yet sealed: local only, not evictable */
	pg_atomic_uint64 numberOfUnsealedParts;

	/*
	 * Broadcast when a part leaves Loading or Evicting: whoever waits for a
	 * part waits for its status, not for the queue around its task.
	 */
	ConditionVariable partSettledCV;
} S3HeadersMeta;

typedef struct
{
	LWLock		bufferCtlLock;
	S3HeaderTag tag;
	S3HeaderTag shadowTag;
	uint32		changeCount;
	uint32		usageCount;
	pg_atomic_uint64 data[S3_HEADER_NUM_VALUES];
} S3HeaderBuffer;

typedef struct
{
	LWLock		groupCtlLock;
	S3HeaderBuffer buffers[S3_HEADER_BUFFERS_PER_GROUP];
} S3HeadersBuffersGroup;

int			s3_headers_buffers_size;
static int	buffersCount = 0;
static int	groupsCount = 0;
static S3HeadersMeta *meta;
static S3HeadersBuffersGroup *groups;

/* whether this process reads for a sequential scan (s3_headers_scan_read()) */
static bool scanRead = false;

/* whether an iterator steps to its next leaf (s3_headers_range_step()) */
static bool rangeStep = false;

#define S3_HEADER_MAX_CHANGE_COUNT (0x7FFFFFFF)

#define S3_PART_DIRTY_BIT		   UINT64CONST(0x8000000000000000)

#define S3_PART_CHANGE_COUNT_MASK  UINT64CONST(0x7FFFFFFF00000000)
#define S3_PART_CHANGE_COUNT_SHIFT (32)
#define S3_PART_GET_CHANGE_COUNT(p) (((p) & S3_PART_CHANGE_COUNT_MASK) >> S3_PART_CHANGE_COUNT_SHIFT)

#define S3_PART_LOWER_MASK		   UINT64CONST(0x00000000FFFFFFFF)
#define S3_PART_GET_LOWER(p)	   ((p) & S3_PART_LOWER_MASK)

#define S3_PART_MAKE(lower, changeCount, dirty) \
	((uint64) (lower) | \
	 ((uint64) (changeCount) << S3_PART_CHANGE_COUNT_SHIFT) | \
	 (uint64) ((dirty) ? S3_PART_DIRTY_BIT : 0))

/*
 * The lower 32 bits of a part's status word:
 *   0-15  number of locks held on the part
 *   16    the stream (checkpoint parity) that writes the part
 *   17    sealed: the stream's checkpoint synced the tree, the part does
 *         not change any more and is uploaded as it is
 *   18-20 S3PartStatus
 *   21    dirty: written locally since it was last uploaded
 *   22    an upload is in flight
 *   23    an upload is queued
 *   24    local only: a temporary tree's part, never sealed, uploaded or
 *         evicted
 *   25-31 usage count
 */
#define S3_PART_LOCKS_NUM_MASK	   UINT64CONST(0x000000000000FFFF)
#define S3_PART_LOCKS_ONE		   (1)
#define S3_PART_LOCKS_NUM_SHIFT	   (0)
#define S3_PART_GET_LOCKS_NUM(p) (((p) & S3_PART_LOCKS_NUM_MASK) >> S3_PART_LOCKS_NUM_SHIFT)
#define S3_PART_STREAM_FLAG		   UINT64CONST(0x0000000000010000)
#define S3_PART_GET_STREAM(p)	   (((p) & S3_PART_STREAM_FLAG) ? 1 : 0)
#define S3_PART_SET_STREAM(p, s)   (((p) & (~S3_PART_STREAM_FLAG)) | ((s) ? S3_PART_STREAM_FLAG : 0))
#define S3_PART_SEALED_FLAG		   UINT64CONST(0x0000000000020000)
#define S3_PART_LOCAL_FLAG		   UINT64CONST(0x0000000001000000)
#define S3_PART_STATUS_MASK		   UINT64CONST(0x00000000001C0000)
#define S3_PART_STATUS_SHIFT	   (18)
#define S3_PART_GET_STATUS(p) ((S3PartStatus) (((p) & S3_PART_STATUS_MASK) >> S3_PART_STATUS_SHIFT))
#define S3_PART_SET_STATUS(p, s) (((p) & (~S3_PART_STATUS_MASK)) | ((uint64) (s) << S3_PART_STATUS_SHIFT))
#define S3_PART_DIRTY_FLAG		   UINT64CONST(0x0000000000200000)
#define S3_PART_WRITING_FLAG	   UINT64CONST(0x0000000000400000)
#define S3_PART_SCHEDULED_FOR_WRITE_FLAG UINT64CONST(0x0000000000800000)
#define S3_PART_USAGE_COUNT_MASK   UINT64CONST(0x00000000FE000000)
#define S3_PART_USAGE_COUNT_MAX    (0x7F)
#define S3_PART_USAGE_COUNT_SHIFT  (25)
#define S3_PART_GET_USAGE_COUNT(p) (((p) & S3_PART_USAGE_COUNT_MASK) >> S3_PART_USAGE_COUNT_SHIFT)
#define S3_PART_SET_USAGE_COUNT(p, u) (((p) & (~S3_PART_USAGE_COUNT_MASK)) | ((uint64) (u) << S3_PART_USAGE_COUNT_SHIFT))

static void initial_parts_conting(void);
static void sync_buffer(S3HeaderBuffer *buffer);

/* header buffers this process loaded from their files (change_buffer) */
static uint64 headerLoads = 0;

Size
s3_headers_shmem_needs(void)
{
	buffersCount = (int) (((uint64) s3_headers_buffers_size * BLCKSZ) / ORIOLEDB_BLCKSZ);
	groupsCount = (buffersCount + S3_HEADER_BUFFERS_PER_GROUP - 1) / S3_HEADER_BUFFERS_PER_GROUP;

	return add_size(CACHELINEALIGN(sizeof(S3HeadersMeta)),
					CACHELINEALIGN(mul_size(sizeof(S3HeadersBuffersGroup), groupsCount)));
}

void
s3_headers_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;

	meta = (S3HeadersMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3HeadersMeta));

	groups = (S3HeadersBuffersGroup *) ptr;

	if (!found)
	{
		uint32		i,
					j;

		meta->groupCtlTrancheId = LWLockNewTrancheId();
		meta->bufferCtlTrancheId = LWLockNewTrancheId();
		pg_atomic_init_u64(&meta->numberOfLoadedParts, 0);
		pg_atomic_init_u64(&meta->numberOfUnsealedParts, 0);
		ConditionVariableInit(&meta->partSettledCV);

		for (i = 0; i < groupsCount; i++)
		{
			S3HeadersBuffersGroup *group = &groups[i];

			LWLockInitialize(&group->groupCtlLock,
							 meta->groupCtlTrancheId);
			for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			{
				S3HeaderBuffer *buffer = &group->buffers[j];

				LWLockInitialize(&buffer->bufferCtlLock,
								 meta->bufferCtlTrancheId);
				buffer->tag.key.oids.datoid = InvalidOid;
				buffer->tag.key.oids.relnode = InvalidOid;
				buffer->tag.segNum = 0;
				buffer->usageCount = 0;
				buffer->changeCount = 0;
			}
		}
	}
	LWLockRegisterTranche(meta->groupCtlTrancheId,
						  "S3HeadersGroupTranche");
	LWLockRegisterTranche(meta->bufferCtlTrancheId,
						  "S3HeadersBufferTranche");

	if (orioledb_s3_mode)
		initial_parts_conting();
}

void
s3_headers_increase_loaded_parts(uint64 inc)
{
	uint64		result;

	result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, inc);
	elog(DEBUG1, "s3_headers_increase_loaded_parts(%llu %llu)",
		 (unsigned long long) result, (unsigned long long) inc);
}

uint64
s3_headers_unsealed_parts(void)
{
	return pg_atomic_read_u64(&meta->numberOfUnsealedParts);
}

static void
read_from_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES],
			   bool *dirty)
{
	char	   *filename;
	int			fd;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES,
				rc;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.key, tag.segNum, 0);
	fd = BasicOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	rc = pg_pread(fd, (char *) values, headerSize, 0);
	pgstat_report_wait_end();

	/*
	 * A file with no header yet holds no part: every part a tree writes is
	 * opened first (s3_header_open_part), which marks it loaded, and a part
	 * that is not local is in S3.
	 */
	*dirty = false;
	if (rc == 0)
		MemSet(values, 0, headerSize);
	else if (rc != headerSize)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read header from data file %s: %m", filename)));

	close(fd);
}

static void
write_to_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES])
{
	char	   *filename;
	int			fd;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.key, tag.segNum, 0);
	fd = BasicOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	if (pg_pwrite(fd, (char *) values, headerSize, 0) != headerSize)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));
	pg_flush_data(fd, 0, headerSize);
	pgstat_report_wait_end();

	close(fd);
}

static void
change_buffer(S3HeadersBuffersGroup *group, int index, S3HeaderTag tag)
{
	S3HeaderTag prevTag;
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	uint32		newValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false,
				newDirty = false;
	uint32		prevChangeCount PG_USED_FOR_ASSERTS_ONLY;
	S3HeaderBuffer *buffer = NULL;
	int			i;

	buffer = &group->buffers[index];
	headerLoads++;

	prevTag = buffer->tag;
	prevChangeCount = buffer->changeCount;

	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;

	pg_write_barrier();

	buffer->tag = tag;
	buffer->shadowTag = prevTag;

	pg_write_barrier();

	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;

	LWLockRelease(&group->groupCtlLock);

	if (OidIsValid(tag.key.oids.datoid) && OidIsValid(tag.key.oids.relnode))
		read_from_file(tag, newValues, &newDirty);
	else
		memset(newValues, 0, sizeof(uint32) * S3_HEADER_NUM_VALUES);

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint64		oldValue;

		oldValue = pg_atomic_exchange_u64(&buffer->data[i],
										  S3_PART_MAKE(newValues[i],
													   buffer->changeCount,
													   newDirty));
		Assert(S3_PART_GET_CHANGE_COUNT(oldValue) == prevChangeCount);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	/*
	 * The file keeps its header whatever happened to its parts: a part that
	 * is not local is in S3 and the header says so.  The file goes only with
	 * its tree (cleanup_btree_files).
	 */
	if (OidIsValid(prevTag.key.oids.datoid) && OidIsValid(prevTag.key.oids.relnode) && dirty)
		write_to_file(prevTag, oldValues);

	pg_write_barrier();

	buffer->shadowTag.key.oids.datoid = InvalidOid;
	buffer->shadowTag.key.oids.relnode = InvalidOid;
	buffer->shadowTag.segNum = 0;

	LWLockRelease(&buffer->bufferCtlLock);
}

static void
load_header_buffer(S3HeaderTag tag)
{
	uint32		hash = s3_header_tag_hash(tag);
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i,
				victim = 0;
	uint32		victimUsageCount = 0;

	/* First check if required buffer is already loaded */
	LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);

	/* Search for victim buffer */
	victim = 0;
	victimUsageCount = group->buffers[0].usageCount;
	for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
	{
		S3HeaderBuffer *buffer = &group->buffers[i];

		if (S3HeaderTagsIsEqual(buffer->tag, tag))
		{
			buffer->usageCount++;
			LWLockRelease(&group->groupCtlLock);
			return;
		}

		if (S3HeaderTagsIsEqual(buffer->shadowTag, tag))
		{
			/*
			 * There is an in-progress operation with required tag.  We must
			 * wait till it's completed.
			 */
			if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
				LWLockRelease(&buffer->bufferCtlLock);
		}

		if (i == 0 || buffer->usageCount < victimUsageCount)
		{
			victim = i;
			victimUsageCount = buffer->usageCount;
		}
		buffer->usageCount /= 2;
	}

	LWLockAcquire(&group->buffers[victim].bufferCtlLock, LW_EXCLUSIVE);

	change_buffer(group, victim, tag);

}

static uint32
s3_header_read_value(S3HeaderTag tag, int index)
{
	uint32		hash = s3_header_tag_hash(tag);
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i;

	while (true)
	{
		bool		tagMatched = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			S3HeaderBuffer *buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				/* check there is no read collision */
				uint32		changeCount = buffer->changeCount;
				uint64		value;

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				value = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(value) != changeCount)
				{
					tagMatched = true;

					/*
					 * Change count mismatch, wait new page to be loaded.
					 */
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				buffer->usageCount++;
				return S3_PART_GET_LOWER(value);
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

static bool
s3_header_compare_and_swap_extended(S3HeaderTag tag, int index,
									uint32 *oldValue, uint32 newValue,
									uint32 *bufferLoadId)
{
	uint32		hash = s3_header_tag_hash(tag);
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i;

	while (true)
	{
		bool		tagMatched = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			S3HeaderBuffer *buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				/* check there is no read collision */
				uint32		changeCount = buffer->changeCount;
				uint64		fullValue;
				uint64		newFullValue;

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				fullValue = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(fullValue) != changeCount)
				{
					tagMatched = true;

					/*
					 * Change count mismatch, wait new page to be loaded.
					 */
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				if (S3_PART_GET_LOWER(fullValue) != *oldValue)
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					return false;
				}

				newFullValue = S3_PART_MAKE(newValue, changeCount, true);

				if (pg_atomic_compare_exchange_u64(&buffer->data[index],
												   &fullValue, newFullValue))
				{
					if (bufferLoadId)
						*bufferLoadId = (changeCount << S3_HEADER_BUFFERS_PER_GROUP_NUM_BITS) + (uint32) i;
					buffer->usageCount++;
					if (S3_PART_GET_STATUS(fullValue) == S3PartStatusLoaded &&
						S3_PART_GET_STATUS(newFullValue) == S3PartStatusEvicting)
						sync_buffer(buffer);
					return true;
				}
				else
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					return false;
				}
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

static bool
s3_header_compare_and_swap(S3HeaderTag tag, int index,
						   uint32 *oldValue, uint32 newValue)
{
	return s3_header_compare_and_swap_extended(tag, index, oldValue,
											   newValue, NULL);
}


/*
 * We allow only one part to be locked simultaneosly.
 */
static S3HeaderTag curLockedTag = {{{InvalidOid, InvalidOid, InvalidOid,
InvalidOid}}, 0};
static int	curLockedIndex = 0;

/*
 * Wait for a part to leave Loading or Evicting, and return its status word
 * then.  The worker that loads a part and the bgwriter that evicts it
 * broadcast partSettledCV when they are done; the timeout only bounds a
 * missed broadcast.  A reader's wait can be cancelled, as its wait for the
 * download task could; a writer opening a part waits with interrupts held,
 * as it did.
 */
static uint32
s3_header_wait_part_settled(S3HeaderTag tag, int index, bool interruptible)
{
	uint32		value;
	bool		slept = false;

	if (!interruptible)
		HOLD_INTERRUPTS();
	value = s3_header_read_value(tag, index);
	while (S3_PART_GET_STATUS(value) == S3PartStatusLoading ||
		   S3_PART_GET_STATUS(value) == S3PartStatusEvicting)
	{
		if (!slept)
			ConditionVariablePrepareToSleep(&meta->partSettledCV);
		else
			(void) ConditionVariableTimedSleep(&meta->partSettledCV, 100,
											   PG_WAIT_EXTENSION);
		slept = true;
		value = s3_header_read_value(tag, index);
	}
	if (slept)
		ConditionVariableCancelSleep();
	if (!interruptible)
		RESUME_INTERRUPTS();
	return value;
}

/* the part this process is downloading itself (s3_header_load_part()) */
static S3HeaderTag curLoadingTag = {{{InvalidOid, InvalidOid, InvalidOid,
InvalidOid}}, 0};
static int	curLoadingIndex = 0;

S3PartStatus
s3_header_part_status(S3HeaderTag tag, int index)
{
	return S3_PART_GET_STATUS(s3_header_read_value(tag, index));
}

/*
 * Give back a part this process marked loading and did not download: it is
 * not loaded again, for whoever needs it next.
 */
void
s3_header_release_part_load(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusLoading);
		newValue = S3_PART_SET_STATUS(newValue, S3PartStatusNotLoaded);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			pg_atomic_fetch_sub_u64(&meta->numberOfLoadedParts, 1);
			ConditionVariableBroadcast(&meta->partSettledCV);
			return;
		}
	}
}

/*
 * Download a part that is not here.  The process that needs it downloads it
 * itself: queued, the task waited for a free S3 worker behind every task put
 * before it, and a GC's deletes of WAL chunks kept all of them busy for 45 s
 * after a start (SNB SF1, local data evicted).  Inside a critical section, in
 * an S3 worker, or when the store does not give the part, the S3 workers
 * download it, and the caller waits for its status.
 *
 * A part the bucket does not hold (404), or one the store refuses for good
 * (403 past orioledb.s3_auth_retry_time), is the query's error, naming the
 * object: handed to the workers, it was replayed for ever by a worker dying
 * on it, a restart a second, while the query waited for the part's status
 * with no message.
 */
static void
s3_header_load_part(S3HeaderTag tag, int index)
{
	if (CritSectionCount == 0 && !s3_worker_process)
	{
		uint32		value = s3_header_read_value(tag, index);

		if (S3_PART_GET_STATUS(value) == S3PartStatusEvicting)
			value = s3_header_wait_part_settled(tag, index, true);

		/*
		 * The part is recorded as this process's download before it is
		 * claimed, so a claim it does not live to see through is given back
		 * (s3_headers_error_cleanup releases a recorded part that is
		 * Loading); recorded after the claim, a death in between left the
		 * part Loading for good.
		 */
		curLoadingTag = tag;
		curLoadingIndex = index;
		while (S3_PART_GET_STATUS(value) == S3PartStatusNotLoaded ||
			   S3_PART_GET_STATUS(value) == S3PartStatusFailed)
		{
			uint32		newValue = S3_PART_SET_STATUS(value, S3PartStatusLoading);

			if (s3_header_compare_and_swap(tag, index, &value, newValue))
			{
				int64		part = (int64) tag.segNum * S3_PARTS_PER_SEGMENT + index;
				long		code;

				pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				code = s3_load_file_part_here(tag.key, tag.segNum, index);
				if (code == S3_RESPONSE_OK)
					s3_header_mark_part_loaded(tag, index);
				else
					s3_header_release_part_load(tag, index);
				curLoadingTag.key.oids.datoid = InvalidOid;
				curLoadingTag.key.oids.relnode = InvalidOid;
				if (code == S3_RESPONSE_OK)
					return;
				if (code == S3_RESPONSE_NOT_FOUND)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("part " INT64_FORMAT " of tree %u/%u is not in the S3 bucket",
									part, tag.key.oids.datoid, tag.key.oids.relnode),
							 errdetail("Object \"%s\" does not exist.",
									   s3_part_object_name(tag.key, part))));
				if (code == 403)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("the S3 store refused part " INT64_FORMAT " of tree %u/%u",
									part, tag.key.oids.datoid, tag.key.oids.relnode),
							 errdetail("Object \"%s\", http code 403, past orioledb.s3_auth_retry_time.",
									   s3_part_object_name(tag.key, part))));
				/* any other answer: the S3 workers' turn */
				break;
			}
		}
		curLoadingTag.key.oids.datoid = InvalidOid;
		curLoadingTag.key.oids.relnode = InvalidOid;
	}
	s3_load_file_part(tag.key, tag.segNum, index);
}

/*
 * A run of misses on nearby parts of one tree is read on ahead, as a file
 * system reads a file read in order: the parts after the one missed are
 * hinted to the S3 workers, S3_READ_AHEAD_PARTS of them.  It goes by part
 * number, not the tree: a range scan's leaves follow each other in the file,
 * and a read ahead by the tree stopped at every internal page (the internal
 * page's part was downloaded alone, then its leaves': two downloads' wait
 * each few megabytes).  A tree writes its parts in two streams (the two
 * checkpoint parities), so a scan's misses can alternate between two runs of
 * parts: two runs are followed.  A run is misses at most two parts apart: a
 * range scan's are next to each other, and lookups in key order a few parts
 * apart read ahead 16 parts each when a run was eight parts wide (20 lookups
 * downloaded 96 parts; 29 now).  The third miss of a run starts reading
 * ahead, and an iterator stepping to its next leaf, a range scan, the first.
 * Measured with 100 ms downloads, a 30 MB range scan whose internal pages
 * were in memory went from 2.6 s to 1.1 s.
 */
#define S3_READ_AHEAD_PARTS 16
#define S3_READ_AHEAD_NEAR 2
#define S3_READ_AHEAD_RUNS 2

typedef struct
{
	ORelOids	oids;
	int64		last;
	int64		hinted;
	int			misses;
} S3ReadAheadRun;

static S3ReadAheadRun readAheadRuns[S3_READ_AHEAD_RUNS];
static int	readAheadOldest = 0;

static void
s3_header_note_miss(S3HeaderTag tag, int index)
{
	int64		part = (int64) tag.segNum * S3_PARTS_PER_SEGMENT + index;
	S3ReadAheadRun *run = NULL;
	int64		from,
				to;

	if (s3_worker_process || CritSectionCount > 0)
		return;

	for (int i = 0; i < S3_READ_AHEAD_RUNS; i++)
	{
		S3ReadAheadRun *r = &readAheadRuns[i];

		if (r->misses > 0 && ORelOidsIsEqual(r->oids, tag.key.oids) &&
			part >= r->last - S3_READ_AHEAD_NEAR &&
			part <= r->last + S3_READ_AHEAD_NEAR)
		{
			run = r;
			break;
		}
	}
	if (run == NULL)
	{
		/* a new run in place of the oldest */
		run = &readAheadRuns[readAheadOldest];
		readAheadOldest = (readAheadOldest + 1) % S3_READ_AHEAD_RUNS;
		run->oids = tag.key.oids;
		run->hinted = -1;
		run->misses = 0;
	}
	run->misses++;
	run->last = part;

	/*
	 * An iterator stepping to its next leaf is a range scan: its first miss
	 * reads ahead.
	 */
	if (run->misses < (rangeStep ? 1 : 3))
		return;

	from = Max(part + 1, run->hinted + 1);
	to = part + S3_READ_AHEAD_PARTS;
	for (int64 p = from; p <= to; p++)
		s3_schedule_part_read_ahead(tag.key, p);
	run->hinted = Max(run->hinted, to);
}

/*
 * Lock file part in S3 header.  This shouldn't let anybody to concurrently
 * evict the same file part.
 */
bool
s3_header_lock_part(S3HeaderTag tag, int index, uint32 *loadId)
{
	uint32		value;

	Assert(!OidIsValid(curLockedTag.key.oids.datoid) &&
		   !OidIsValid(curLockedTag.key.oids.relnode));

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value,
					usageCount = S3_PART_GET_USAGE_COUNT(value);
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusNotLoaded)
		{
			s3_header_note_miss(tag, index);
			s3_header_load_part(tag, index);
			value = s3_header_read_value(tag, index);
			continue;
		}
		else if (status == S3PartStatusLoading ||
				 status == S3PartStatusEvicting)
		{
			s3_header_note_miss(tag, index);
			value = s3_header_wait_part_settled(tag, index, true);
			continue;
		}
		else if (status == S3PartStatusFailed)
		{
			int64		part = (int64) tag.segNum * S3_PARTS_PER_SEGMENT + index;

			/*
			 * An S3 worker could not download it (s3_header_mark_part_failed).
			 * The part goes back to NotLoaded, for the next reader to try the
			 * store again, and this one reports it.
			 */
			newValue = S3_PART_SET_STATUS(value, S3PartStatusNotLoaded);
			if (s3_header_compare_and_swap(tag, index, &value, newValue))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("could not read part " INT64_FORMAT " of tree %u/%u from the S3 bucket",
								part, tag.key.oids.datoid, tag.key.oids.relnode),
						 errdetail("An S3 worker could not get object \"%s\"; its log says why.",
								   s3_part_object_name(tag.key, part))));
			continue;
		}
		else
		{
			Assert(status == S3PartStatusLoaded);
			newValue += S3_PART_LOCKS_ONE;

			/*
			 * A sequential scan's read does not count, and takes a part only
			 * the load counted (a read ahead) back to cold: eviction takes
			 * such parts before the ones queries use.
			 */
			if (scanRead)
				usageCount = usageCount <= 1 ? 0 : usageCount;
			else if (usageCount < S3_PART_USAGE_COUNT_MAX)
				usageCount++;
			newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount);
		}

		if (s3_header_compare_and_swap_extended(tag, index, &value,
												newValue, loadId))
		{
			curLockedTag = tag;
			curLockedIndex = index;
			return (value & S3_PART_DIRTY_FLAG);
		}
	}
}

S3PartStatus
s3_header_mark_part_loading(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue;
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);
		/* nothing is here after a failed download, as for NotLoaded */
		if (status == S3PartStatusFailed)
			status = S3PartStatusNotLoaded;

		if (status == S3PartStatusLoaded ||
			status == S3PartStatusLoading)
		{
			return status;
		}
		else if (status == S3PartStatusEvicting)
		{
			value = s3_header_wait_part_settled(tag, index, true);
			continue;
		}
		else
		{
			Assert(status == S3PartStatusNotLoaded);
			newValue = S3_PART_SET_STATUS(value, S3PartStatusLoading);
		}

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			if (status == S3PartStatusNotLoaded)
			{
				uint64		result;

				result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				elog(DEBUG1, "s3_header_mark_part_loading(%u %u %d %d) - %llu",
					 tag.key.oids.datoid, tag.key.oids.relnode,
					 tag.segNum, index,
					 (unsigned long long) result);
			}
			return status;
		}
	}
}

void
s3_header_mark_part_loaded(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue;
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);

		/*
		 * Two downloads of one part: a worker replaying its task, or a hint
		 * the process that needed the part claimed first.  Both put the same
		 * object's bytes; the first to finish marked it.
		 */
		if (status == S3PartStatusLoaded)
			return;

		/*
		 * Only whoever moved the part to Loading may declare it loaded.  Say
		 * which part and which status when that does not hold: the bare
		 * assertion left nothing to go on, and the caller that broke it (an
		 * s3 worker finishing a download) is not the one that changed the
		 * status out from under it.
		 */
		if (status != S3PartStatusLoading)
			elog(WARNING, "part %d of file %u/%u (segment %d) "
				 "is in status %d, expected loading",
				 index, tag.key.oids.datoid, tag.key.oids.relnode,
				 tag.segNum, (int) status);
		Assert(status == S3PartStatusLoading);
		newValue = S3_PART_SET_STATUS(value, S3PartStatusLoaded);
		newValue = S3_PART_SET_USAGE_COUNT(newValue, 1);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			ConditionVariableBroadcast(&meta->partSettledCV);
			return;
		}
	}
}

/*
 * An S3 worker could not download a part it was to load: the bucket does not
 * hold it, or the store refused it for good.  Marked failed, the part counts
 * as not loaded, whoever waits for it reports it instead of waiting on
 * (s3_header_lock_part), and the next reader tries the store again.  Only a
 * part still Loading is marked: one a waiter already reported was given back.
 */
void
s3_header_mark_part_failed(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue;

		if (S3_PART_GET_STATUS(value) != S3PartStatusLoading)
			return;
		newValue = S3_PART_SET_STATUS(value, S3PartStatusFailed);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			pg_atomic_fetch_sub_u64(&meta->numberOfLoadedParts, 1);
			ConditionVariableBroadcast(&meta->partSettledCV);
			return;
		}
	}
}

/*
 * Release the lock taken by s3_header_lock_part().  A write (setDirty) also
 * records the stream it belongs to: a part is written by one stream only,
 * and the mark is what a seal looks for, so writing it at every write keeps
 * it right after a restart stripped the flags.
 */
void
s3_header_unlock_part(S3HeaderTag tag, int index, bool setDirty, int stream)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusLoaded);
		Assert(S3_PART_GET_LOCKS_NUM(value) >= 0);

		/*
		 * A sealed part is uploaded as it is and never changes: a write to
		 * one would be lost to every load from the bucket, silently.  Stop
		 * here, in every build.
		 */
		if (setDirty && (value & S3_PART_SEALED_FLAG))
			elog(PANIC, "part " INT64_FORMAT " of tree %u/%u (segment %d, part %d) was written by stream %d after it was sealed, last checkpoint %u",
				 (int64) tag.segNum * S3_PARTS_PER_SEGMENT + index,
				 tag.key.oids.datoid, tag.key.oids.relnode, tag.segNum, index,
				 stream, checkpoint_state->lastCheckpointNumber);

		newValue -= S3_PART_LOCKS_ONE;
		if (setDirty && !(value & S3_PART_LOCAL_FLAG))
		{
			newValue |= S3_PART_DIRTY_FLAG;
			newValue = S3_PART_SET_STREAM(newValue, stream);
		}

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			/*
			 * A part written again after a restart stripped its flags is
			 * unsealed again: count it, or its seal would take the count
			 * below zero.
			 */
			if ((newValue & S3_PART_DIRTY_FLAG) && !(value & S3_PART_DIRTY_FLAG))
				pg_atomic_fetch_add_u64(&meta->numberOfUnsealedParts, 1);
			curLockedTag.key.oids.datoid = InvalidOid;
			curLockedTag.key.oids.relnode = InvalidOid;
			curLockedTag.segNum = 0;
			curLockedIndex = 0;
			return;
		}
	}
}

/*
 * A stream opens a part it is about to write for the first time: the part is
 * local (loaded), dirty and unsealed.  Opening a part twice is harmless; a
 * part reused after a crash reads the same way.  A temporary tree's part is
 * local only and stays out of the unsealed count.
 */
void
s3_header_open_part(S3HeaderTag tag, int index, int stream, bool localOnly)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;
		S3PartStatus status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusLoading || status == S3PartStatusEvicting)
		{
			value = s3_header_wait_part_settled(tag, index, false);
			continue;
		}

		newValue = S3_PART_SET_STATUS(newValue, S3PartStatusLoaded);
		newValue &= ~(S3_PART_SEALED_FLAG | S3_PART_LOCAL_FLAG);
		newValue |= localOnly ? S3_PART_LOCAL_FLAG : S3_PART_DIRTY_FLAG;
		newValue = S3_PART_SET_STREAM(newValue, stream);
		newValue = S3_PART_SET_USAGE_COUNT(newValue, 1);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			bool		wasUnsealed = (value & S3_PART_DIRTY_FLAG) &&
				!(value & (S3_PART_SEALED_FLAG | S3_PART_LOCAL_FLAG));

			if (status == S3PartStatusNotLoaded || status == S3PartStatusFailed)
				pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
			if (!localOnly && !wasUnsealed)
				pg_atomic_fetch_add_u64(&meta->numberOfUnsealedParts, 1);
			else if (localOnly && wasUnsealed)
				pg_atomic_fetch_sub_u64(&meta->numberOfUnsealedParts, 1);
			elog(DEBUG1, "s3_header_open_part(%u %u %d %d) stream %d",
				 tag.key.oids.datoid, tag.key.oids.relnode,
				 tag.segNum, index, stream);
			return;
		}
	}
}

/*
 * Seal a part of the given stream after its checkpoint synced the tree.
 * Returns whether the part holds data not yet uploaded; the caller then
 * schedules the upload.  A part of the other stream, or one not written
 * since it was last uploaded, is left alone.
 */
bool
s3_header_seal_part(S3HeaderTag tag, int index, int stream)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		if (!(value & S3_PART_DIRTY_FLAG) || S3_PART_GET_STREAM(value) != stream)
			return false;
		if (value & S3_PART_SEALED_FLAG)
			return true;

		newValue |= S3_PART_SEALED_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			pg_atomic_fetch_sub_u64(&meta->numberOfUnsealedParts, 1);
			return true;
		}
	}
}

bool
s3_header_mark_part_scheduled_for_write(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		if (!(value & S3_PART_DIRTY_FLAG))
			return false;

		if (value & S3_PART_SCHEDULED_FOR_WRITE_FLAG)
			return false;

		newValue |= S3_PART_SCHEDULED_FOR_WRITE_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return true;
		}
	}
}

void
s3_header_mark_part_writing(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		/*
		 * The task is done again after a worker exited in the middle of it:
		 * an exit (FATAL) skips the cleanup that marks the part not written.
		 */
		if ((value & S3_PART_WRITING_FLAG) &&
			!(value & S3_PART_SCHEDULED_FOR_WRITE_FLAG))
			return;

		Assert(value & S3_PART_DIRTY_FLAG);
		Assert(value & S3_PART_SCHEDULED_FOR_WRITE_FLAG);

		newValue &= ~(S3_PART_DIRTY_FLAG | S3_PART_SCHEDULED_FOR_WRITE_FLAG);
		newValue |= S3_PART_WRITING_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

static void
s3_header_mark_not_loaded(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusEvicting);
		Assert(S3_PART_GET_LOCKS_NUM(value) == 0);
		newValue = S3_PART_SET_STATUS(newValue, S3PartStatusNotLoaded);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			ConditionVariableBroadcast(&meta->partSettledCV);
			return;
		}
	}
}

void
s3_header_mark_part_written(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		newValue &= ~S3_PART_WRITING_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

/*
 * Called on write failure
 */
void
s3_header_mark_part_not_written(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		newValue &= ~S3_PART_WRITING_FLAG;
		newValue |= S3_PART_DIRTY_FLAG | S3_PART_SCHEDULED_FOR_WRITE_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

static void
sync_buffer(S3HeaderBuffer *buffer)
{
	S3HeaderTag tag;
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false;
	int			i;

	LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint64		oldValue;

		oldValue = pg_atomic_fetch_and_u64(&buffer->data[i],
										   ~S3_PART_DIRTY_BIT);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	tag = buffer->tag;
	if (OidIsValid(tag.key.oids.datoid) && OidIsValid(tag.key.oids.relnode) && dirty)
		write_to_file(tag, oldValues);

	LWLockRelease(&buffer->bufferCtlLock);
}

void
s3_headers_sync(void)
{
	int			i,
				j;

	for (i = 0; i < groupsCount; i++)
	{
		for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			sync_buffer(&groups[i].buffers[j]);
	}
}

void
s3_headers_error_cleanup(void)
{
	scanRead = false;
	rangeStep = false;

	/* a download of this process's own cut short: give the part back */
	if (OidIsValid(curLoadingTag.key.oids.datoid) &&
		OidIsValid(curLoadingTag.key.oids.relnode))
	{
		S3HeaderTag tag = curLoadingTag;

		curLoadingTag.key.oids.datoid = InvalidOid;
		curLoadingTag.key.oids.relnode = InvalidOid;
		if (s3_header_part_status(tag, curLoadingIndex) == S3PartStatusLoading)
			s3_header_release_part_load(tag, curLoadingIndex);
	}

	if (!OidIsValid(curLockedTag.key.oids.datoid) ||
		!OidIsValid(curLockedTag.key.oids.relnode))
		return;

	s3_header_unlock_part(curLockedTag, curLockedIndex, false, 0);
}

typedef void (*IterateFilesCallback) (S3HeaderTag tag);

static void
iterate_tablespace_files(Oid tablespace, char *path, IterateFilesCallback callback)
{
	DIR		   *dir,
			   *dbDir;
	struct dirent *file,
			   *dbFile;

	dir = opendir(path);
	if (dir == NULL)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open orioledb data directory: %s: %m",
							   path)));

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		Oid			dbOid;
		char	   *dbDirName;

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf(ORIOLEDB_DATA_DIR "/%u", dbOid);
		dbDir = opendir(dbDirName);
		pfree(dbDirName);
		if (dbDir == NULL)
			continue;

		while (errno = 0, (dbFile = readdir(dbDir)) != NULL)
		{
			uint32		file_relnode,
						file_segno;
			S3HeaderTag tag = {0};
			int			pos,
						len = strlen(dbFile->d_name);

			if (sscanf(dbFile->d_name, "%10u%n",
					   &file_relnode, &pos) == 1 &&
				pos == len)
			{
				tag.key.oids.datoid = dbOid;
				tag.key.oids.relnode = file_relnode;
				tag.key.oids.spcoid = tablespace;
				tag.segNum = 0;
				callback(tag);
			}
			else if (sscanf(dbFile->d_name, "%10u.%10u%n",
							&file_relnode, &file_segno, &pos) == 2 &&
					 pos == len)
			{
				tag.key.oids.datoid = dbOid;
				tag.key.oids.relnode = file_relnode;
				tag.key.oids.spcoid = tablespace;
				tag.segNum = file_segno;
				callback(tag);
			}
		}
		closedir(dbDir);
	}

	closedir(dir);

}

static void
iterate_files(IterateFilesCallback callback)
{
	DIR		   *dir;
	char		path[MAXPGPATH];
	char		targetpath[MAXPGPATH];
	struct dirent *file;

#define PG_TBLSPC "pg_tblspc"

	path[0] = '\0';
	strlcat(path, ORIOLEDB_DATA_DIR, MAXPGPATH);
	iterate_tablespace_files(DEFAULTTABLESPACE_OID, path, callback);

	dir = opendir(PG_TBLSPC);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", PG_TBLSPC)));
	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		struct stat st;
		int			rllen;
		Oid			tablespace;

		/* Skip special stuff */
		if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;

		tablespace = pg_strtoint64(file->d_name);
		Assert(OidIsValid(tablespace));
		path[0] = '\0';
		pg_snprintf(path, MAXPGPATH,
					PG_TBLSPC "/%s/" TABLESPACE_VERSION_DIRECTORY,
					file->d_name);
		if (lstat(path, &st) < 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							file->d_name)));
		}

		if (!S_ISLNK(st.st_mode))
		{
			strlcat(path, "/" ORIOLEDB_DATA_DIR, MAXPGPATH);
			iterate_tablespace_files(tablespace, path, callback);
		}
		else
		{
			rllen = readlink(path, targetpath, sizeof(targetpath));
			if (rllen < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								path)));
			if (rllen >= sizeof(targetpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								path)));
			targetpath[rllen] = '\0';

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						"%s/" ORIOLEDB_DATA_DIR,
						targetpath);
			iterate_tablespace_files(tablespace, path, callback);
		}
	}
	closedir(dir);
#undef PG_TBLSPC
}

static off_t totalFilesSize;
static off_t totalOccupiedSize;
static uint64 totalFilesCount;

static void
initial_parts_counting_callback(S3HeaderTag tag)
{
	uint32		values[S3_HEADER_NUM_VALUES];
	bool		dirty;
	off_t		fileSize;
	int			i;
	int			fd;
	char	   *filename;

	read_from_file(tag, values, &dirty);

	filename = btree_filename(tag.key, tag.segNum, 0);
	fd = BasicOpenFile(filename, O_RDWR | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));

	fileSize = lseek(fd, 0, SEEK_END);
	totalFilesCount++;
	totalFilesSize += fileSize;

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		S3PartStatus status = S3_PART_GET_STATUS(values[i]);
		uint32		usageCount = S3_PART_GET_USAGE_COUNT(values[i]);
		off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;

		/*
		 * A local part (a temporary tree's, or one a reader wrote) is not in
		 * the bucket under its number: what the bucket has there is another
		 * writer's, to be fetched again.
		 */
		if (status == S3PartStatusNotLoaded ||
			status == S3PartStatusLoading ||
			status == S3PartStatusEvicting ||
			status == S3PartStatusFailed ||
			(values[i] & S3_PART_LOCAL_FLAG))
		{

			status = S3PartStatusNotLoaded;
			if (fileSize > offset)
				punch_fd_hole(fd, offset, Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset, filename);
		}
		else
		{
			Assert(status == S3PartStatusLoaded);
			if (fileSize > offset)
			{
				uint64		result;

				result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				elog(DEBUG1, "initial_parts_counting_callback(%u %u %d %d) - %llu",
					 tag.key.oids.datoid, tag.key.oids.relnode,
					 tag.segNum, i,
					 (unsigned long long) result);
				totalOccupiedSize += Min(ORIOLEDB_S3_PART_SIZE, fileSize - offset);
			}
		}
		/*
		 * Dirty, sealed, stream and upload flags do not survive a restart: a
		 * part not uploaded before the crash holds nothing a committed
		 * checkpoint refers to, and it is written again before it matters.
		 */
		values[i] = S3_PART_SET_STATUS(usageCount << S3_PART_USAGE_COUNT_SHIFT, status);
	}

	close(fd);
	pfree(filename);

	write_to_file(tag, values);
}

static void
initial_parts_conting(void)
{
	totalFilesSize = 0;
	totalOccupiedSize = 0;
	totalFilesCount = 0;
	elog(LOG, "OrioleDB initial files scan started");
	iterate_files(initial_parts_counting_callback);
	elog(LOG, "OrioleDB initial files scan finished (num files %llu, size %llu, occupied size %llu)",
		 (unsigned long long) totalFilesCount,
		 (unsigned long long) totalFilesSize,
		 (unsigned long long) totalOccupiedSize);

}

static uint64 unsealedCount;

static void
unsealed_counting_callback(S3HeaderTag tag)
{
	for (int i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint32		value = s3_header_read_value(tag, i);

		if ((value & S3_PART_DIRTY_FLAG) &&
			!(value & (S3_PART_SEALED_FLAG | S3_PART_LOCAL_FLAG)))
			unsealedCount++;
	}
}

/*
 * Count the unsealed parts again from the headers: the count kept as parts
 * open and seal still holds the parts of a tree dropped, or a build aborted,
 * before its parts were sealed.  A part opened or sealed during the count
 * may be missed; the next count has it.
 */
static uint64
recount_unsealed_parts(void)
{
	unsealedCount = 0;
	iterate_files(unsealed_counting_callback);
	pg_atomic_write_u64(&meta->numberOfUnsealedParts, unsealedCount);
	return unsealedCount;
}

/*
 * Eviction keeps the local tree data under orioledb.s3_desired_size, measured
 * in bytes on disk: a part counts as its blocks, so a file's last, part-full
 * part does not count whole (counted in parts, the data stayed far under the
 * bound with many small files).  Once the loaded parts' count reaches the
 * bound, the files are measured (at most once a second) and a pass evicts
 * down to nine tenths of it.
 *
 * A pass first evicts the cold parts only: parts with no usage count, which a
 * sequential scan or a read ahead leaves (s3_header_lock_part() does not count
 * a scan's read, and takes one that only the read ahead counted back to
 * zero), then parts counted once.  Only when they are not enough does it go
 * on as a clock sweep over the files' parts, from where the last one stopped
 * (the hand): a part with a usage count is spared and its count halved, one
 * without is evicted.  A sweep halving every part a scan passed over took the
 * parts queries used with them.
 */
typedef enum
{
	EvictionSeekingHand,		/* skipping the files before the hand */
	EvictionSweeping,			/* evicting */
	EvictionWrapped,			/* evicting, from the first file to the hand */
	EvictionDone
} EvictionPhase;

static EvictionPhase evictionPhase;
static S3HeaderTag evictionHand;
static bool evictionHandValid = false;
/* a cold pass evicts parts counted up to this, the clock sweep is -1 */
static int	evictionColdLimit = -1;
static uint64 evictionBytes;
static uint64 evictionTargetBytes;
static TimestampTz lastMeasure = 0;


void
s3_headers_scan_read(bool on)
{
	scanRead = on;
}

void
s3_headers_range_step(bool on)
{
	rangeStep = on;
}

bool
s3_headers_in_range_step(void)
{
	return rangeStep;
}

static uint64 occupiedBytes;

/*
 * Each file's size on disk, kept between cycles: a stat() of every file every
 * cycle, with thousands of small trees, was most of a cycle that evicted
 * nothing.  A file is measured again every S3_OCCUPIED_REFRESH_CYCLES cycles
 * (downloads by other processes grow it meanwhile), and what a cycle evicts
 * from a file comes off its entry at once.  Entries of files gone are dropped
 * at the refresh.
 */
#define S3_OCCUPIED_REFRESH_CYCLES 16

typedef struct
{
	Oid			datoid;
	Oid			relnode;
	Oid			spcoid;
	int			segNum;
} OccupiedKey;

typedef struct
{
	OccupiedKey key;
	uint64		blocks;			/* st_blocks */
	uint64		measuredCycle;
	uint64		seenCycle;
} OccupiedEntry;

static HTAB *occupiedSizes = NULL;
static uint64 evictionCycle = 0;

/* what one cycle did, for its DEBUG1 line */
static uint32 filesMeasured;
static uint32 partsEvicted;

static OccupiedEntry *
occupied_entry(S3HeaderTag tag, HASHACTION action, bool *found)
{
	OccupiedKey key;

	if (occupiedSizes == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(OccupiedKey);
		ctl.entrysize = sizeof(OccupiedEntry);
		ctl.hcxt = TopMemoryContext;
		occupiedSizes = hash_create("S3 file sizes", 1024, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	memset(&key, 0, sizeof(key));
	key.datoid = tag.key.oids.datoid;
	key.relnode = tag.key.oids.relnode;
	key.spcoid = tag.key.oids.spcoid;
	key.segNum = tag.segNum;
	return hash_search(occupiedSizes, &key, action, found);
}

static void
occupied_callback(S3HeaderTag tag)
{
	bool		found;
	OccupiedEntry *entry = occupied_entry(tag, HASH_ENTER, &found);

	if (!found ||
		evictionCycle - entry->measuredCycle >= S3_OCCUPIED_REFRESH_CYCLES)
	{
		char	   *filename = btree_filename(tag.key, tag.segNum, 0);
		struct stat st;

		entry->blocks = stat(filename, &st) == 0 ? (uint64) st.st_blocks : 0;
		entry->measuredCycle = evictionCycle;
		filesMeasured++;
		pfree(filename);
	}
	entry->seenCycle = evictionCycle;
	occupiedBytes += entry->blocks * 512;
}

/* the bytes a cycle punched out of a file come off its entry */
static void
occupied_evicted(S3HeaderTag tag, uint64 bytes)
{
	bool		found;
	OccupiedEntry *entry = occupied_entry(tag, HASH_FIND, &found);

	if (found)
		entry->blocks -= Min(entry->blocks, bytes / 512);
}

static void
measure_occupied(void)
{
	occupiedBytes = 0;
	filesMeasured = 0;
	iterate_files(occupied_callback);

	if (evictionCycle % S3_OCCUPIED_REFRESH_CYCLES == 0)
	{
		HASH_SEQ_STATUS seq;
		OccupiedEntry *entry;

		hash_seq_init(&seq, occupiedSizes);
		while ((entry = hash_seq_search(&seq)) != NULL)
		{
			if (entry->seenCycle != evictionCycle)
				(void) hash_search(occupiedSizes, &entry->key, HASH_REMOVE,
								   NULL);
		}
	}
}

static void
eviction_done_at(S3HeaderTag tag)
{
	evictionHand = tag;
	evictionHandValid = true;
	evictionPhase = EvictionDone;
}

/*
 * What the first cold pass finds for the passes after it, so they read only
 * the headers that hold something for them: the files with a part the next
 * cold pass may evict, and whether any part is left for the clock sweep
 * (one evictable but for its usage count).  Every pass over every file
 * reloaded every header into the small header cache: three times a cycle,
 * once a second, for a cycle that evicted nothing.
 */
static S3HeaderTag *coldNextFiles = NULL;
static int	coldNextCount = 0;
static int	coldNextCapacity = 0;
static uint64 clockCandidates = 0;

static void
cold_next_file(S3HeaderTag tag)
{
	if (coldNextCount == coldNextCapacity)
	{
		coldNextCapacity = Max(coldNextCapacity * 2, 1024);
		if (coldNextFiles == NULL)
			coldNextFiles = MemoryContextAlloc(TopMemoryContext,
											   sizeof(S3HeaderTag) * coldNextCapacity);
		else
			coldNextFiles = repalloc(coldNextFiles,
									 sizeof(S3HeaderTag) * coldNextCapacity);
	}
	coldNextFiles[coldNextCount++] = tag;
}

static void
eviction_callback(S3HeaderTag tag)
{
	int			fd;
	char	   *filename;
	off_t		fileSize,
				dataSize;
	int			i;
	int			numParts;
	bool		coldNext = false;

	if (evictionPhase == EvictionSeekingHand)
	{
		if (!S3HeaderTagsIsEqual(tag, evictionHand))
			return;
		evictionPhase = EvictionSweeping;
	}
	else if (evictionPhase == EvictionWrapped && evictionHandValid &&
			 S3HeaderTagsIsEqual(tag, evictionHand))
		evictionPhase = EvictionDone;

	if (evictionPhase == EvictionDone)
		return;

	if (evictionBytes <= evictionTargetBytes)
	{
		if (evictionColdLimit < 0)
			eviction_done_at(tag);
		else
			evictionPhase = EvictionDone;
		return;
	}

	filename = btree_filename(tag.key, tag.segNum, 0);
	fd = BasicOpenFile(filename, O_RDWR | PG_BINARY);

	if (fd <= 0)
	{
		pfree(filename);
		return;
	}

	fileSize = lseek(fd, 0, SEEK_END);

	/* Only the header is here: nothing was written past it */
	if (fileSize <= ORIOLEDB_BLCKSZ)
	{
		close(fd);
		pfree(filename);
		return;
	}

	/*
	 * Parts follow the header block: a full segment is S3_HEADER_NUM_VALUES
	 * parts and one block.  Counted from the file size, it was one part more,
	 * and the header value past the last one never reads (its change count
	 * never matches), so this looped for ever and no part was evicted again.
	 */
	dataSize = fileSize - ORIOLEDB_BLCKSZ;
	numParts = Min((dataSize + ORIOLEDB_S3_PART_SIZE - 1) / ORIOLEDB_S3_PART_SIZE,
				   S3_HEADER_NUM_VALUES);
	for (i = 0; i < numParts; i++)
	{
		uint32		value;

		if (evictionBytes <= evictionTargetBytes)
		{
			if (evictionColdLimit < 0)
				eviction_done_at(tag);
			else
				evictionPhase = EvictionDone;
			break;
		}

		/*
		 * A file's last, part-full part is evicted with the probability of
		 * its fill: an emptier one frees less for the GET it costs later.
		 * Every pass draws again.  (The draw was a signed int32 compared
		 * with the fill as unsigned: a negative draw, half of them, counted
		 * as above any fill, and the part was skipped whatever it held.)
		 */
		if (i == numParts - 1 && dataSize < (uint64) numParts * (uint64) ORIOLEDB_S3_PART_SIZE)
		{
			static pg_prng_state random_state;
			static bool seed_initialized = false;
			uint64		filled = dataSize - (uint64) (numParts - 1) * (uint64) ORIOLEDB_S3_PART_SIZE;

			if (!seed_initialized)
			{
				pg_prng_seed(&random_state, 0);
				seed_initialized = true;
			}

			if (pg_prng_uint64_range(&random_state, 0,
									 ORIOLEDB_S3_PART_SIZE - 1) > filled)
			{
				/* skipped this pass: the next passes draw again */
				if (evictionColdLimit == 0)
				{
					coldNext = true;
					clockCandidates++;
				}
				continue;
			}
		}

		value = s3_header_read_value(tag, i);

		while (true)
		{
			uint32		newValue = value,
						usageCount = S3_PART_GET_USAGE_COUNT(value);
			bool		evictable;

			evictable = S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
				S3_PART_GET_LOCKS_NUM(value) == 0 &&
				(value & (S3_PART_DIRTY_FLAG | S3_PART_WRITING_FLAG | S3_PART_LOCAL_FLAG)) == 0;

			if (evictable && usageCount <= (uint32) Max(evictionColdLimit, 0))
			{
				newValue = S3_PART_SET_STATUS(newValue, S3PartStatusEvicting);
			}
			else if (evictionColdLimit >= 0)
			{
				/* the cold pass leaves the others as they are */
				if (evictable && evictionColdLimit == 0)
				{
					if (usageCount == 1)
						coldNext = true;
					else
						clockCandidates++;
				}
				break;
			}
			else
			{
				newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount / 2);
			}

			if (s3_header_compare_and_swap(tag, i, &value, newValue))
			{
				if (S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
					S3_PART_GET_STATUS(newValue) == S3PartStatusEvicting)
				{
					off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;
					uint64		punched = Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset;
					uint64		result;

					elog(DEBUG1, "S3 evict %u %u %d %d",
						 tag.key.oids.datoid, tag.key.oids.relnode,
						 tag.segNum, i);
					punch_fd_hole(fd, offset, punched, filename);
					evictionBytes -= Min(evictionBytes, punched);
					occupied_evicted(tag, punched);
					partsEvicted++;

					result = pg_atomic_fetch_sub_u64(&meta->numberOfLoadedParts, 1);
					elog(DEBUG1, "eviction_callback(%llu 1)",
						 (unsigned long long) result);

					s3_header_mark_not_loaded(tag, i);
				}
				break;
			}
		}
	}

	if (coldNext && evictionColdLimit == 0)
		cold_next_file(tag);

	close(fd);
	pfree(filename);
}

void
s3_headers_try_eviction_cycle(void)
{
	uint64		desiredNumParts = (uint64) s3_desired_size * (uint64) (1024 * 1024) / (uint64) ORIOLEDB_S3_PART_SIZE;
	uint64		maxUnsealedParts = (uint64) s3_max_unsealed_size * (uint64) (1024 * 1024) / (uint64) ORIOLEDB_S3_PART_SIZE;
	static TimestampTz lastRequest = 0;
	uint64		headerLoadsBefore;

	Assert(orioledb_s3_mode);

	/*
	 * Unsealed parts cannot be evicted: only the checkpoint of the stream
	 * that writes them seals them.  Past the bound, count them again and ask
	 * for one, at most once a minute so a load that outruns the checkpointer
	 * does not queue requests.  A spread checkpoint takes about
	 * checkpoint_timeout while the parts go on piling up: past twice the
	 * bound the request is immediate, which hurries one already going.
	 */
	if (pg_atomic_read_u64(&meta->numberOfUnsealedParts) > maxUnsealedParts)
	{
		TimestampTz now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(lastRequest, now, 60 * 1000))
		{
			uint64		unsealed;

			lastRequest = now;
			unsealed = recount_unsealed_parts();
			if (unsealed > maxUnsealedParts)
			{
				bool		immediate = unsealed > 2 * maxUnsealedParts;

				elog(LOG, "requesting %s checkpoint: %llu MB of local S3 parts are not sealed yet (orioledb.s3_max_unsealed_size = %d MB)",
					 immediate ? "an immediate" : "a",
					 (unsigned long long) (unsealed * ORIOLEDB_S3_PART_SIZE / (1024 * 1024)),
					 s3_max_unsealed_size);
				RequestCheckpoint(CHECKPOINT_FORCE |
								  (immediate ? CHECKPOINT_IMMEDIATE : 0));
			}
		}
	}

	/* no more bytes than parts: under the bound in parts, under it in bytes */
	if (pg_atomic_read_u64(&meta->numberOfLoadedParts) < desiredNumParts)
		return;

	{
		TimestampTz now = GetCurrentTimestamp();
		uint64		desiredBytes = (uint64) s3_desired_size * 1024 * 1024;

		if (!TimestampDifferenceExceeds(lastMeasure, now, 1000))
			return;
		lastMeasure = now;
		evictionCycle++;
		headerLoadsBefore = headerLoads;
		partsEvicted = 0;
		measure_occupied();
		if (occupiedBytes < desiredBytes)
		{
			elog(DEBUG1, "S3 eviction cycle: %u files measured, %llu header loads, %u parts evicted",
				 filesMeasured, (unsigned long long) (headerLoads - headerLoadsBefore),
				 partsEvicted);
			return;
		}
		evictionBytes = occupiedBytes;
		evictionTargetBytes = desiredBytes - desiredBytes / 10;
	}

	/*
	 * The cold parts first, whatever the hand: those nothing counted, then
	 * those counted once (read ahead, or read once).  A part queries keep
	 * using is swept only when they are not enough: on a slow machine a long
	 * scan made many sweeps, each halving every count, and took the parts
	 * queries used.  The first pass notes what the later ones have to look
	 * at; they skip the rest of the files.
	 */
	coldNextCount = 0;
	clockCandidates = 0;
	evictionColdLimit = 0;
	evictionPhase = EvictionSweeping;
	iterate_files(eviction_callback);
	if (evictionBytes > evictionTargetBytes && coldNextCount > 0)
	{
		evictionColdLimit = 1;
		evictionPhase = EvictionSweeping;
		for (int i = 0; i < coldNextCount && evictionPhase != EvictionDone; i++)
			eviction_callback(coldNextFiles[i]);
	}
	evictionColdLimit = -1;

	if (evictionBytes > evictionTargetBytes && clockCandidates > 0)
	{
		evictionPhase = EvictionSeekingHand;
		if (evictionHandValid)
		{
			/* from the hand to the last file */
			iterate_files(eviction_callback);
		}
		if (evictionPhase != EvictionDone)
		{
			/*
			 * From the first file to the hand: the rest of the circle, or
			 * all of it when the hand's file is gone or there is no hand yet.
			 */
			evictionPhase = EvictionWrapped;
			iterate_files(eviction_callback);
		}
	}

	/*
	 * Measure again at the next cycle if this one evicted; if nothing could
	 * go (the parts are unsealed or in use), not before a second: the passes
	 * over every file each cycle slowed a checkpoint on a slow machine.
	 */
	if (evictionBytes < occupiedBytes)
		lastMeasure = 0;

	elog(DEBUG1, "S3 eviction cycle: %u files measured, %llu header loads, %u parts evicted",
		 filesMeasured, (unsigned long long) (headerLoads - headerLoadsBefore),
		 partsEvicted);
}
