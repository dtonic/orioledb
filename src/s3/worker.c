/*-------------------------------------------------------------------------
 *
 * worker.c
 *		Routines for S3 worker process.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "catalog/o_sys_cache.h"
#include "s3/checksum.h"
#include "s3/headers.h"
#include "s3/gc.h"
#include "s3/lease.h"
#include "s3/manifest.h"
#include "s3/queue.h"
#include "s3/requests.h"
#include "s3/wal_shipper.h"
#include "s3/worker.h"

#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "transam/undo.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "pgstat.h"

#include "openssl/sha.h"
#include <fcntl.h>
#include <unistd.h>


static S3TaskLocation s3_schedule_file_part_read(uint32 chkpNum, OIndexKey key,
												 int64 partNum, bool claim);

#define WORKERS_FILE_CHECKSUMS_MAX_LEN 100

typedef struct S3WorkerCtl
{
	pg_atomic_uint32 fileChecksumsCnt;
	ConditionVariable fileChecksumsFlushedCV;

	/* S3 workers are in progress of putting PostgreSQL files into S3 bucket */
	pg_atomic_flag workersInProgress[FLEXIBLE_ARRAY_MEMBER];
} S3WorkerCtl;

static volatile S3TaskLocation *workers_locations = NULL;
static S3FileChecksum *workers_file_checksums = NULL;

static S3WorkerCtl *workers_ctl = NULL;
static S3ChecksumState *checksum_state = NULL;

static int	worker_num;

/* this process is an S3 worker; the task it runs is one it replays */
bool		s3_worker_process = false;
static bool s3_worker_replaying = false;

Size
s3_workers_shmem_needs(void)
{
	Size		size;

	size = CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers)));
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3FileChecksum),
											s3_num_workers *
											WORKERS_FILE_CHECKSUMS_MAX_LEN)));

	return size;
}

void
s3_workers_init_shmem(Pointer ptr, bool found)
{
	int			i;

	workers_ctl = (S3WorkerCtl *) ptr;
	ptr += CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);

	workers_locations = (S3TaskLocation *) ptr;
	ptr += CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers));

	workers_file_checksums = (S3FileChecksum *) ptr;

	if (!found)
	{
		pg_atomic_init_u32(&workers_ctl->fileChecksumsCnt, 0);

		ConditionVariableInit(&workers_ctl->fileChecksumsFlushedCV);

		for (i = 0; i < s3_num_workers; i++)
		{
			workers_locations[i] = InvalidS3TaskLocation;
			pg_atomic_init_flag(&workers_ctl->workersInProgress[i]);
		}
	}
}

void
register_s3worker(int num)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_SYSTEM;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	worker.bgw_main_arg = Int32GetDatum(num);
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3worker_main");
	pg_snprintf(worker.bgw_name, sizeof(worker.bgw_name),
				"orioledb s3 worker %d", num);
	strcpy(worker.bgw_type, "orioledb s3 worker");
	RegisterBackgroundWorker(&worker);
}

/*
 * Wait until all S3 workers flushed their checksum files.
 */
static void
s3_workers_wait_for_flush(void)
{
	Assert(workers_ctl != NULL);

	for (;;)
	{
		int			all_flushed = pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0;

		for (int i = 0; (i < s3_num_workers) && all_flushed; i++)
		{
			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[i]))
				all_flushed = false;
		}
		if (all_flushed)
			break;

		ConditionVariableTimedSleep(&workers_ctl->fileChecksumsFlushedCV, BgWriterDelay,
									WAIT_EVENT_BGWRITER_MAIN);
	}

	ConditionVariableCancelSleep();
}

/*
 * Prepare S3 workers to checkpoint database files.
 */
void
s3_workers_checkpoint_init(void)
{
	/* Just in case delete any leftover files */
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		pg_atomic_clear_flag(&workers_ctl->workersInProgress[i]);

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

/*
 * Compact all S3 workers checksum files into one file.
 */
void
s3_workers_checkpoint_finish(void)
{
	int			file;

	s3_workers_wait_for_flush();

	file = BasicOpenFile(FILE_CHECKSUMS_FILENAME, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	for (int i = 0; i < s3_num_workers; i++)
	{
		int			worker_file;
		char		worker_filename[MAXPGPATH];
		Size		readBytes;
		char		buffer[8192];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		worker_file = BasicOpenFile(worker_filename, O_RDONLY | PG_BINARY);
		if (worker_file < 0)
		{
			/*
			 * In case if this worker didn't manage to process any
			 * S3TaskTypeWritePGFile just skip it.
			 */
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", worker_filename)));
		}

		while ((readBytes = read(worker_file, buffer, sizeof(buffer))) > 0)
		{
			if (write(file, buffer, readBytes) != readBytes)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));
		}

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", worker_filename)));

		close(worker_file);
	}

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	close(file);

	/* The compaction is completed, now we can remove worker files */
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

static S3FileChecksum *
get_worker_file_checksums(void)
{
	return workers_file_checksums + WORKERS_FILE_CHECKSUMS_MAX_LEN * worker_num;
}

static void
flush_worker_checksum_state(void)
{
	char		filename[MAXPGPATH];

	Assert(checksum_state != NULL);
	Assert(checksum_state->fileChecksumsLen > 0);

	snprintf(filename, MAXPGPATH, "%s.%d", FILE_CHECKSUMS_FILENAME, worker_num);

	flushS3ChecksumState(checksum_state, filename);
}

/*
 * Object names of a tree's data.  A part is named by its number in the
 * tree's own address space, nothing else: it is written once and read back
 * by that name for as long as any checkpoint refers to it.  A map is the
 * tree's state at one checkpoint.
 */
char *
s3_part_object_name(OIndexKey key, int64 partNum)
{
	return psprintf("trees/%u/%u/parts/" INT64_FORMAT,
					key.oids.datoid, key.oids.relnode, partNum);
}

static char *
s3_map_object_name(OIndexKey key, uint64 token, uint32 chkpNum)
{
	return psprintf("trees/%u/%u/maps/" UINT64_FORMAT "-%u",
					key.oids.datoid, key.oids.relnode, token, chkpNum);
}

/*
 * Fetch the map of checkpoint chkpNum.  It was written by the run that did
 * that checkpoint, or by a run that started after it (a build names its tree's
 * map with the last checkpoint done): s3_map_tokens().
 */
static void
s3_get_map(OIndexKey key, uint32 chkpNum, char *filename)
{
	uint64	   *tokens;
	int			n = s3_map_tokens(chkpNum, &tokens);
	StringInfoData buf;
	char	   *objectname;

	if (n == 0)
		elog(ERROR, "no S3 writer token is known for checkpoint %u", chkpNum);
	initStringInfo(&buf);
	for (int i = 0; i < n - 1; i++)
	{
		objectname = s3_map_object_name(key, tokens[i], chkpNum);
		resetStringInfo(&buf);
		if (s3_get_object(objectname, &buf, true) == S3_RESPONSE_OK)
		{
			elog(DEBUG1, "S3 map get %s %s", objectname, filename);
			s3_write_file(filename, buf.data, buf.len);
			pfree(objectname);
			pfree(buf.data);
			pfree(tokens);
			return;
		}
		pfree(objectname);
	}
	objectname = s3_map_object_name(key, tokens[n - 1], chkpNum);
	elog(DEBUG1, "S3 map get %s %s", objectname, filename);
	s3_get_file(objectname, filename);
	pfree(objectname);
	pfree(buf.data);
	pfree(tokens);
}

/*
 * Process the task at given location.
 */
static void
s3process_task(uint64 taskLocation)
{
	S3Task	   *task = (S3Task *) s3_queue_get_task(taskLocation);
	char	   *objectname;

	Assert(workers_ctl != NULL);

	if (task->type == S3TaskTypeWriteFile)
	{
		char	   *filename = task->typeSpecific.writeFile.filename;
		long		result;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/" UINT64_FORMAT "-%u/%s",
							  s3_checkpoint_token(task->typeSpecific.writeFile.chkpNum),
							  task->typeSpecific.writeFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWriteEmptyDir)
	{
		char	   *dirname = task->typeSpecific.writeEmptyDir.dirname;

		if (dirname[0] == '.' && dirname[1] == '/')
			dirname += 2;

		objectname = psprintf("data/" UINT64_FORMAT "-%u/%s/",
							  s3_checkpoint_token(task->typeSpecific.writeEmptyDir.chkpNum),
							  task->typeSpecific.writeEmptyDir.chkpNum,
							  dirname);

		elog(DEBUG1, "S3 dir put %s %s", objectname, dirname);

		s3_put_empty_dir(objectname);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart &&
			 task->typeSpecific.filePart.partNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.key = task->typeSpecific.filePart.key;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);
		s3_get_map(task->typeSpecific.filePart.key,
				   task->typeSpecific.filePart.chkpNum, filename);
		pfree(filename);
	}
	else if (task->type == S3TaskTypeReadFilePart)
	{
		char	   *filename;
		S3HeaderTag tag;
		int64		partNum = task->typeSpecific.filePart.partNum;
		int32		segNum = partNum / S3_PARTS_PER_SEGMENT,
					index = partNum % S3_PARTS_PER_SEGMENT;
		long		code;

		tag.key = task->typeSpecific.filePart.key;
		tag.segNum = segNum;

		/*
		 * A hint read ahead: claim the part now, and leave it if it is here
		 * or somebody is downloading it (the process that needs it does that
		 * itself).  A replayed hint the dead attempt had claimed is still
		 * Loading: it is this task's to finish.
		 */
		if (!task->typeSpecific.filePart.claimed)
		{
			S3PartStatus status = s3_header_mark_part_loading(tag, index);

			if (status == S3PartStatusLoaded ||
				(status == S3PartStatusLoading && !s3_worker_replaying))
				goto part_done;
		}

		filename = btree_filename(task->typeSpecific.filePart.key, segNum, 0);
		objectname = s3_part_object_name(task->typeSpecific.filePart.key, partNum);

		elog(DEBUG1, "S3 part get %s %s", objectname, filename);

		code = s3_try_get_file_part(objectname, filename, index,
									!task->typeSpecific.filePart.claimed);
		if (code != S3_RESPONSE_OK)
		{
			if (task->typeSpecific.filePart.claimed)
			{
				/*
				 * A part somebody waits for that the store did not give
				 * (not there, or refused for good: a transient failure is
				 * retried inside the request).  Dying on it replayed the
				 * task for ever, a restart a second, while the waiters
				 * waited with no message.  The part is marked failed: the
				 * waiter reports it and gives it back for the next try.
				 */
				ereport(WARNING,
						(errmsg("could not get S3 object \"%s\" for part " INT64_FORMAT " of tree %u/%u",
								objectname, partNum,
								tag.key.oids.datoid, tag.key.oids.relnode),
						 errdetail("http code = %ld", code)));
				s3_header_mark_part_failed(tag, index);
			}
			else
			{
				/*
				 * A hint for a part the store does not have (a read ahead
				 * past the tree's last part) or refuses: give it back
				 * rather than end the worker.  A part a query needs is
				 * downloaded by the process that needs it, which reports a
				 * part that is missing.
				 */
				s3_header_release_part_load(tag, index);
			}
			pfree(filename);
			pfree(objectname);
			goto part_done;
		}

		/*
		 * A worker that errors out is restarted with bgw_restart_time 0 and
		 * replays the task it left in workers_locations[], so this download
		 * can be the second one of a part the first attempt already marked
		 * loaded -- the attempt only has to have died between that mark and
		 * clearing its slot.  Claiming the part again keeps the replay
		 * idempotent; without it the restarted worker fails Assert(status ==
		 * S3PartStatusLoading) the moment it starts, and dies again, on the
		 * same task.
		 *
		 * s3_header_mark_part_loading() returns the status it found: Loading
		 * means the mark is still ours to make, NotLoaded means the part was
		 * evicted in between and it just claimed it back for us, and Loaded
		 * means the first attempt already finished this.
		 */
		if (s3_header_mark_part_loading(tag, index) != S3PartStatusLoaded)
			s3_header_mark_part_loaded(tag, index);

		pfree(filename);
		pfree(objectname);
part_done:
		;
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.partNum >= 0)
	{
		char	   *filename;
		S3HeaderTag tag;
		int64		partNum = task->typeSpecific.filePart.partNum;
		int32		segNum = partNum / S3_PARTS_PER_SEGMENT,
					index = partNum % S3_PARTS_PER_SEGMENT;

		filename = btree_filename(task->typeSpecific.filePart.key, segNum, 0);
		objectname = s3_part_object_name(task->typeSpecific.filePart.key, partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		tag.key = task->typeSpecific.filePart.key;
		tag.segNum = segNum;

		s3_header_mark_part_writing(tag, index);

		PG_TRY();
		{
			(void) s3_put_file_part(objectname, filename, index);
		}
		PG_CATCH();
		{
			s3_header_mark_part_not_written(tag, index);
			PG_RE_THROW();
		}
		PG_END_TRY();

		s3_header_mark_part_written(tag, index);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.partNum == S3_DEAD_FILE_PART)
	{
		char	   *filename;
		SeqBufTag	dead_tag;
		OIndexKey	key = task->typeSpecific.filePart.key;

		memset(&dead_tag, 0, sizeof(dead_tag));
		dead_tag.key = key;
		dead_tag.num = task->typeSpecific.filePart.chkpNum;
		dead_tag.type = 'd';

		filename = get_seq_buf_filename(&dead_tag);
		objectname = psprintf("trees/%u/%u/dead/" UINT64_FORMAT "-%u",
							  key.oids.datoid, key.oids.relnode,
							  s3_checkpoint_token(dead_tag.num), dead_tag.num);

		elog(DEBUG1, "S3 dead extents put %s %s", objectname, filename);

		/* the GC folds the local copy; without a GC nothing reads it here */
		if (s3_put_file(objectname, filename, false) == S3_RESPONSE_OK &&
			s3_gc_interval == 0)
			(void) unlink(filename);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.partNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.key = task->typeSpecific.filePart.key;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);
		/* the run that writes a map is the one whose key it is */
		objectname = s3_map_object_name(task->typeSpecific.filePart.key,
										s3_lease_token(),
										task->typeSpecific.filePart.chkpNum);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

		/*
		 * The key names the writer: no other writer's map is ever here, and
		 * ours may be written again (a build, then the checkpoint).
		 */
		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteWALFile)
	{
		char	   *filename;
		Pointer		data;
		uint64		size;

		/*
		 * The WAL a checkpoint without the shipper leaves unarchived, the
		 * segment it ends in named "<segment>.partial": tagged and fenced
		 * like an archived one, so a writer fenced off cannot put its
		 * version over the bucket's.
		 */
		filename = psprintf(XLOGDIR "/%.*s", XLOG_FNAME_LEN,
							task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);
		data = read_file(filename, &size);
		if (data == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\"", filename)));
		while (s3_put_object_fenced_until(objectname, data, size, NULL,
										  s3_lease_hold()) != S3_RESPONSE_OK)
			;
		pfree(data);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteArchiveFile)
	{
		char	   *filename;
		Pointer		data;
		uint64		size;

		/*
		 * A completed segment or a timeline history is written once: a
		 * writer that was fenced off can neither put its version of a
		 * segment over ours nor start a timeline another writer started.
		 */
		filename = psprintf(XLOGDIR "/%s", task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);
		data = read_file(filename, &size);
		if (data == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\"", filename)));
		/* a key not taken yet is no fence: the lease is, while it lasts */
		while (s3_put_object_fenced_until(objectname, data, size, NULL,
										  s3_lease_hold()) != S3_RESPONSE_OK)
			;
		pfree(data);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteUndoFile)
	{
		uint64		fileNum = task->typeSpecific.writeUndoFile.fileNum;
		uint32		chkpNum = task->typeSpecific.writeUndoFile.chkpNum;
		char	   *filename;

		if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegular)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xrow/" UINT64_FORMAT "-%u",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum,
								  s3_checkpoint_token(chkpNum), chkpNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegularPageLevel)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xpage/" UINT64_FORMAT "-%u",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum,
								  s3_checkpoint_token(chkpNum), chkpNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogSystem)
		{
			filename = psprintf(ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xsystem/" UINT64_FORMAT "-%u",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum,
								  s3_checkpoint_token(chkpNum), chkpNum);
		}
		else
		{
			Assert(false);
			filename = NULL;
			objectname = NULL;
		}

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeDeleteWalChunks)
	{
		uint64		from = task->typeSpecific.deleteWalChunks.fromSeq,
					to = task->typeSpecific.deleteWalChunks.toSeq;
		int			count = (int) (to - from);
		char	  **keys = palloc(sizeof(char *) * Max(count, 1));

		for (int i = 0; i < count; i++)
			keys[i] = s3_wal_chunk_key(task->typeSpecific.deleteWalChunks.timeline,
									   from + i);
		s3_delete_objects(keys, count);
		for (int i = 0; i < count; i++)
			pfree(keys[i]);
		pfree(keys);
	}
	else if (task->type == S3TaskTypeWritePGFile)
	{
		char	   *filename = task->typeSpecific.writePGFile.filename;
		Pointer		data;
		uint64		size;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/" UINT64_FORMAT "-%u/%s",
							  s3_checkpoint_token(task->typeSpecific.writePGFile.chkpNum),
							  task->typeSpecific.writePGFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 PG file put %s %s", objectname, filename);

		data = read_file(filename, &size);

		if (data != NULL)
		{
			S3FileChecksum *entry;

			pg_atomic_test_set_flag(&workers_ctl->workersInProgress[worker_num]);

			if (checksum_state == NULL)
				checksum_state = makeS3ChecksumState(task->typeSpecific.writePGFile.chkpNum,
													 get_worker_file_checksums(),
													 WORKERS_FILE_CHECKSUMS_MAX_LEN,
													 FILE_CHECKSUMS_FILENAME);

			Assert(checksum_state->checkpointNumber == task->typeSpecific.writePGFile.chkpNum);

			if (checksum_state->fileChecksumsLen == WORKERS_FILE_CHECKSUMS_MAX_LEN)
				flush_worker_checksum_state();

			entry = getS3FileChecksum(checksum_state, filename, data, size);

			if (entry->changed)
				(void) s3_put_object_with_contents(objectname, data, size,
												   entry->checksum, false);

			pfree(data);
		}

		pfree(objectname);

		/* Mark this task as processed */
		pg_atomic_fetch_sub_u32(&workers_ctl->fileChecksumsCnt, 1);
	}

	pfree(task);
	s3_queue_erase_task(taskLocation);
}

/*
 * Schedule a synchronization of given data file to S3.
 */
S3TaskLocation
s3_schedule_file_write(uint32 chkpNum, char *filename, bool delete)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteFile;
	task->typeSpecific.writeFile.chkpNum = chkpNum;
	task->typeSpecific.writeFile.delete = delete;
	memcpy(task->typeSpecific.writeFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);
	s3_manifest_note(chkpNum, "data %s",
					 (filename[0] == '.' && filename[1] == '/') ? filename + 2 : filename);

	elog(DEBUG1, "S3 schedule file write: %s %u %u (%llu)",
		 filename, chkpNum, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given empty directory to S3.
 */
S3TaskLocation
s3_schedule_empty_dir_write(uint32 chkpNum, char *dirname)
{
	S3Task	   *task;
	int			dirnameLen,
				taskLen;
	S3TaskLocation location;

	dirnameLen = strlen(dirname);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeEmptyDir.dirname) +
					   dirnameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteEmptyDir;
	task->typeSpecific.writeEmptyDir.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writeEmptyDir.dirname, dirname, dirnameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);
	s3_manifest_note(chkpNum, "data %s/",
					 (dirname[0] == '.' && dirname[1] == '/') ? dirname + 2 : dirname);

	elog(DEBUG1, "S3 schedule empty dir write: %s %u (%llu)",
		 dirname, chkpNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the upload of a sealed part.  The caller sealed it
 * (s3_header_seal_part); a part already queued or clean is left alone.
 */
S3TaskLocation
s3_schedule_file_part_write(OIndexKey key, int64 partNum)
{
	S3Task	   *task;
	S3TaskLocation location;
	S3HeaderTag tag = {.key = key,.segNum = partNum / S3_PARTS_PER_SEGMENT};

	if (!s3_header_mark_part_scheduled_for_write(tag, partNum % S3_PARTS_PER_SEGMENT))
		return (S3TaskLocation) 0;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = 0;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part write: %u %u " INT64_FORMAT " (%llu)",
		 key.oids.datoid, key.oids.relnode, partNum,
		 (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the upload of a tree's map of the given checkpoint.
 */
S3TaskLocation
s3_schedule_map_file_write(uint32 chkpNum, OIndexKey key)
{
	S3Task	   *task;
	S3TaskLocation location;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.partNum = -1;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));
	s3_manifest_note(chkpNum, "map %u %u %u", key.oids.datoid, key.oids.relnode,
					 key.oids.spcoid);

	elog(DEBUG1, "S3 schedule map write: %u %u %u (%llu)",
		 key.oids.datoid, key.oids.relnode, chkpNum,
		 (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the upload of the extents of a tree that died at chkpNum.
 */
S3TaskLocation
s3_schedule_dead_file_write(uint32 chkpNum, OIndexKey key)
{
	S3Task	   *task;
	S3TaskLocation location;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.partNum = S3_DEAD_FILE_PART;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));
	s3_manifest_note(chkpNum, "dead %u %u %u", key.oids.datoid, key.oids.relnode,
					 key.oids.spcoid);
	pfree(task);
	return location;
}

/*
 * Schedule the read of a tree's part from S3, or of its map of checkpoint
 * chkpNum when partNum < 0.
 */
static S3TaskLocation
s3_schedule_file_part_read(uint32 chkpNum, OIndexKey key, int64 partNum,
						   bool claim)
{
	S3Task	   *task;
	S3TaskLocation location;
	char	   *prefix;
	char	   *db_prefix;

	o_get_prefixes_for_tablespace(key.oids.datoid, key.oids.spcoid,
								  &prefix, &db_prefix);
	o_verify_dir_exists_or_create(prefix, NULL, NULL);
	o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
	pfree(db_prefix);

	if (partNum >= 0 && !claim)
	{
		S3HeaderTag tag = {.key = key,.segNum = partNum / S3_PARTS_PER_SEGMENT};
		S3PartStatus status;

		/* a hint: the worker claims the part when it gets to the task */
		status = s3_header_part_status(tag, partNum % S3_PARTS_PER_SEGMENT);
		if (status != S3PartStatusNotLoaded && status != S3PartStatusFailed)
			return 0;
	}
	else if (partNum >= 0)
	{
		S3PartStatus status;
		S3HeaderTag tag = {.key = key,.segNum = partNum / S3_PARTS_PER_SEGMENT};

		status = s3_header_mark_part_loading(tag, partNum % S3_PARTS_PER_SEGMENT);
		if (status == S3PartStatusLoading)
		{
			S3TaskLocation insertLocation = s3_queue_get_insert_location();

			/*
			 * The task is already scheduled.  We don't know the location, but
			 * we know it's lower than current insert location, so it is done
			 * once everything up to there is erased.  s3_queue_wait_for_location()
			 * waits for the given location itself to be erased: given the
			 * insert location, it waited for a task nobody has scheduled yet,
			 * and with the queue idle that took until the next unrelated task
			 * (a WAL file, a checkpoint) came and went.
			 *
			 * The process marking the part loading may not have put its task
			 * yet, and then this returns early; s3_header_lock_part() looks
			 * at the part's status again and waits for it to be loaded.
			 */
			return insertLocation > 0 ? insertLocation - 1 : 0;
		}
		else if (status == S3PartStatusLoaded)
		{
			return 0;
		}
		Assert(status == S3PartStatusNotLoaded);
	}

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeReadFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.partNum = partNum;
	task->typeSpecific.filePart.claimed = claim;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part read: %u %u %u " INT64_FORMAT " (%llu)",
		 key.oids.datoid, key.oids.relnode, chkpNum, partNum,
		 (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given WAL file to S3.
 */
S3TaskLocation
s3_schedule_wal_file_write(char *filename, bool archived)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.walFilename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = archived ? S3TaskTypeWriteArchiveFile : S3TaskTypeWriteWALFile;
	memcpy(task->typeSpecific.walFilename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule WAL file write: %s (%llu)",
		 filename, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the removal of WAL chunks [fromSeq, toSeq) of a timeline: the
 * shipper hands it over rather than waiting on the deletes itself, as every
 * commit waits on the shipper.
 */
void
s3_schedule_wal_chunks_delete(uint32 timeline, uint64 fromSeq, uint64 toSeq)
{
	S3Task	   *task;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeDeleteWalChunks;
	task->typeSpecific.deleteWalChunks.timeline = timeline;
	task->typeSpecific.deleteWalChunks.fromSeq = fromSeq;
	task->typeSpecific.deleteWalChunks.toSeq = toSeq;
	/* in the background lane: a start's backlog held every worker */
	s3_queue_put_background_task((Pointer) task, sizeof(S3Task));
	pfree(task);
}

/*
 * Schedule a synchronization of given UNDO file to S3.
 */
S3TaskLocation
s3_schedule_undo_file_write(UndoLogType undoType, uint64 fileNum,
							uint32 chkpNum)
{
	S3Task	   *task;
	S3TaskLocation location;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteUndoFile;
	task->typeSpecific.writeUndoFile.undoType = undoType;
	task->typeSpecific.writeUndoFile.fileNum = fileNum;
	task->typeSpecific.writeUndoFile.chkpNum = chkpNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));
	s3_manifest_note(chkpNum, "undo %02X%08X%s",
					 (uint32) (fileNum >> 32), (uint32) fileNum,
					 undoType == UndoLogRegular ? "row" :
					 undoType == UndoLogSystem ? "system" : "page");

	elog(DEBUG1, "S3 schedule UNDO file write: %llu (%llu)",
		 (unsigned long long) fileNum, (unsigned long long) location);

	pfree(task);

	return location;
}


/*
 * The number of the part the page of the given downlink starts in.
 */
int64
s3_downlink_part(BTreeDescr *desc, uint64 downlink)
{
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	off_t		byte_offset;

	if (!OCompressIsValid(desc->compress))
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
	else
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;

	return byte_offset / ORIOLEDB_S3_PART_SIZE;
}

/*
 * Schedule the load of given downlink from S3 to local storage.
 */
S3TaskLocation
s3_schedule_downlink_load(BTreeDescr *desc, uint64 downlink)
{
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	off_t		byte_offset,
				read_size;
	S3TaskLocation result = 0;

	if (!OCompressIsValid(desc->compress))
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;
	}
	else
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		read_size = len * ORIOLEDB_COMP_BLCKSZ;
	}

	while (true)
	{
		OIndexKey	key = {.oids = desc->oids};
		S3TaskLocation location;

		location = s3_schedule_file_part_read(0, key,
											  byte_offset / ORIOLEDB_S3_PART_SIZE,
											  false);
		result = Max(result, location);
		if (byte_offset % ORIOLEDB_S3_PART_SIZE + read_size > ORIOLEDB_S3_PART_SIZE)
		{
			uint64		shift = ORIOLEDB_S3_PART_SIZE - byte_offset % ORIOLEDB_S3_PART_SIZE;

			byte_offset += shift;
			read_size -= shift;
		}
		else
		{
			break;
		}
	}

	return result;
}

/*
 * Schedule a synchronization of given PGDATA file to S3.
 */
S3TaskLocation
s3_schedule_pg_file_write(uint32 chkpNum, char *filename)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	Assert(workers_ctl != NULL);

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writePGFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWritePGFile;
	task->typeSpecific.writePGFile.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writePGFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule PGDATA file write: %s %u (%llu)",
		 filename, chkpNum, (unsigned long long) location);

	pfree(task);

	pg_atomic_fetch_add_u32(&workers_ctl->fileChecksumsCnt, 1);

	return location;
}

/*
 * Schedule the load of a part and return: the caller, s3_header_lock_part(),
 * finds the part Loading and waits for its status.  Waiting for the task's
 * queue location instead waited for every task queued before it too (the
 * queue is erased in order), a checkpoint's uploads included.
 */
void
s3_load_file_part(OIndexKey key, int32 segNum, int32 partNum)
{
	(void) s3_schedule_file_part_read(0, key,
									  (int64) segNum * S3_PARTS_PER_SEGMENT + partNum,
									  true);
}

/*
 * Hint the S3 workers to download a part, if it is not here: a read ahead.
 */
void
s3_schedule_part_read_ahead(OIndexKey key, int64 partNum)
{
	(void) s3_schedule_file_part_read(0, key, partNum, false);
}

/*
 * Download a part the caller marked loading, in the calling process: the HTTP
 * status, S3_RESPONSE_OK when the part is here (the caller judges the rest:
 * a part that is not in the bucket, one refused, or one left to the workers).
 */
long
s3_load_file_part_here(OIndexKey key, int32 segNum, int32 partNum)
{
	int64		part = (int64) segNum * S3_PARTS_PER_SEGMENT + partNum;
	char	   *filename = btree_filename(key, segNum, 0);
	char	   *objectname = s3_part_object_name(key, part);
	long		code;

	elog(DEBUG1, "S3 part get here %s %s", objectname, filename);
	code = s3_try_get_file_part(objectname, filename, partNum, false);
	pfree(objectname);
	pfree(filename);
	return code;
}

void
s3_load_map_file(uint32 chkpNum, OIndexKey key)
{
	S3TaskLocation location;

	location = s3_schedule_file_part_read(chkpNum, key, -1, true);

	if (location > 0)
		s3_queue_wait_for_location(location);
}

/*
 * The map of checkpoint chkpNum, when the bucket has one: a build done after
 * that checkpoint names its tree's map with it (s3_get_map()), while a tree
 * created since has no map at all.  Fetched in the calling process; true when
 * the map was found and written to the local map file.
 */
bool
s3_try_load_map_file(uint32 chkpNum, OIndexKey key)
{
	uint64	   *tokens;
	int			n = s3_map_tokens(chkpNum, &tokens);
	SeqBufTag	chkp_tag;
	char	   *filename;
	StringInfoData buf;
	bool		found = false;

	memset(&chkp_tag, 0, sizeof(chkp_tag));
	chkp_tag.key = key;
	chkp_tag.num = chkpNum;
	chkp_tag.type = 'm';
	filename = get_seq_buf_filename(&chkp_tag);
	initStringInfo(&buf);
	for (int i = 0; i < n && !found; i++)
	{
		char	   *objectname = s3_map_object_name(key, tokens[i], chkpNum);

		resetStringInfo(&buf);
		if (s3_get_object(objectname, &buf, true) == S3_RESPONSE_OK)
		{
			elog(DEBUG1, "S3 map get %s %s", objectname, filename);
			s3_write_file(filename, buf.data, buf.len);
			found = true;
		}
		pfree(objectname);
	}
	pfree(buf.data);
	pfree(filename);
	pfree(tokens);
	return found;
}

void
s3worker_main(Datum main_arg)
{
	worker_num = Int32GetDatum(main_arg);
	s3_worker_process = true;

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the s3 worker in pg_stat_activity, */
	InitializeSessionUserIdStandalone();

	/* catch SIGTERM signal for reason to not interrupt background writing */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb s3 worker %d started", worker_num);

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);

		/*
		 * There might be task to process saved into shared memory.  If so,
		 * pick and process it.  The location is recorded before the pick
		 * (s3_queue_try_pick_task), so one the pick did not take, or one
		 * another worker erased since, is not a task of ours.
		 */
		if (workers_locations[worker_num] != InvalidS3TaskLocation &&
			!s3_queue_task_is_live(workers_locations[worker_num]))
			workers_locations[worker_num] = InvalidS3TaskLocation;
		if (workers_locations[worker_num] != InvalidS3TaskLocation)
		{
			/*
			 * The task the worker before this one died on.  A failure that
			 * persists (a refused key) would restart the worker at once, over
			 * and over: a pause bounds that to an attempt a second.
			 */
			TimestampTz until = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 1000);
			long		left;

			/* a latch set (a reload, a new task) does not cut it short */
			while ((left = TimestampDifferenceMilliseconds(GetCurrentTimestamp(),
														   until)) > 0)
			{
				(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 left, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
			s3_worker_replaying = true;
			s3process_task(workers_locations[worker_num]);
			s3_worker_replaying = false;
			workers_locations[worker_num] = InvalidS3TaskLocation;
		}

		while (true)
		{
			uint64		taskLocation;

			if (ShutdownRequestPending)
				break;
			s3_take_config_reload();

			/*
			 * Sleep until a task is put, or bgwriter_delay passes for the
			 * checksum flush below and the shutdown and reload flags.  The
			 * sleep exits the process if the postmaster dies.
			 */
			s3_queue_wait_for_task(BgWriterDelay, WAIT_EVENT_BGWRITER_MAIN);

			CHECK_FOR_INTERRUPTS();

			/*
			 * Task processing loop.  It might happen that error occurs and
			 * worker restarts.  The task location is saved to shared memory
			 * before the pick takes it, to be able to process it after a
			 * restart: recorded after, a death in between left the task
			 * picked by nobody, and the erased location never passed it.
			 */
			while ((taskLocation = s3_queue_try_pick_task(&workers_locations[worker_num])) != InvalidS3TaskLocation)
			{
				s3process_task(taskLocation);
				workers_locations[worker_num] = InvalidS3TaskLocation;
			}

			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[worker_num]) &&
				pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0)
			{
				/* checksum_state might be NULL if the worker restarted */
				if (checksum_state != NULL)
				{
					if (checksum_state->fileChecksumsLen > 0)
						flush_worker_checksum_state();

					freeS3ChecksumState(checksum_state);
					checksum_state = NULL;
				}

				pg_atomic_clear_flag(&workers_ctl->workersInProgress[worker_num]);
				ConditionVariableBroadcast(&workers_ctl->fileChecksumsFlushedCV);
			}

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb s3 worker %d is shut down", worker_num);
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
