/*-------------------------------------------------------------------------
 *
 * worker.h
 *		Declarations for S3 worker process.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_WORKER_H__
#define __S3_WORKER_H__

#include "orioledb.h"

#include "btree/undo.h"
#include "s3/queue.h"

typedef enum
{
	S3TaskTypeWriteFile,
	S3TaskTypeWriteFilePart,
	S3TaskTypeReadFilePart,
	S3TaskTypeWriteWALFile,
	S3TaskTypeWriteUndoFile,
	S3TaskTypeWriteEmptyDir,
	S3TaskTypeWriteRootFile,
	S3TaskTypeWritePGFile,
	S3TaskTypeWriteArchiveFile,	/* a complete WAL file: put once, fenced */
	S3TaskTypeDeleteWalChunks	/* WAL chunks an archived segment holds */
} S3TaskType;

/*
 * The data structure representing the task for S3 worker.
 */
#define S3_DEAD_FILE_PART (-2)

typedef struct
{
	S3TaskType	type;
	union
	{
		struct
		{
			uint32		chkpNum;
			bool		delete;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writeFile;
		struct
		{
			UndoLogType undoType;
			uint64		fileNum;
			uint32		chkpNum;
		}			writeUndoFile;
		/*
		 * A tree's part (partNum >= 0, the part's number in the tree's own
		 * address space; chkpNum unused) or its map of checkpoint chkpNum
		 * (partNum < 0).  A part's task either downloads a part its scheduler
		 * marked loading (claimed), or is a hint read ahead: the worker then
		 * claims the part when it gets to the task, and leaves it if somebody
		 * did first.
		 */
		struct
		{
			OIndexKey	key;
			uint32		chkpNum;
			int64		partNum;
			bool		claimed;
		}			filePart;
		char		walFilename[1];
		struct
		{
			uint32		chkpNum;
			char		dirname[FLEXIBLE_ARRAY_MEMBER];
		}			writeEmptyDir;
		struct
		{
			bool		delete;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writeRootFile;
		struct
		{
			uint32		chkpNum;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writePGFile;
		struct
		{
			uint32		timeline;
			uint64		fromSeq;	/* first chunk */
			uint64		toSeq;		/* past the last */
		}			deleteWalChunks;
	}			typeSpecific;
} S3Task;

#define FILE_CHECKSUMS_FILENAME		ORIOLEDB_DATA_DIR "/file_checksums"

extern Size s3_workers_shmem_needs(void);
extern void s3_workers_init_shmem(Pointer ptr, bool found);
extern void register_s3worker(int num);
extern void s3_workers_checkpoint_init(void);
extern void s3_workers_checkpoint_finish(void);
PGDLLEXPORT void s3worker_main(Datum);

extern S3TaskLocation s3_schedule_file_write(uint32 chkpNum, char *filename,
											 bool delete);
extern S3TaskLocation s3_schedule_empty_dir_write(uint32 chkpNum,
												  char *dirname);
extern S3TaskLocation s3_schedule_file_part_write(OIndexKey key, int64 partNum);
extern S3TaskLocation s3_schedule_map_file_write(uint32 chkpNum, OIndexKey key);
extern S3TaskLocation s3_schedule_dead_file_write(uint32 chkpNum, OIndexKey key);
extern S3TaskLocation s3_schedule_wal_file_write(char *filename, bool archived);
extern S3TaskLocation s3_schedule_undo_file_write(UndoLogType undoType,
												  uint64 fileNum,
												  uint32 chkpNum);
extern S3TaskLocation s3_schedule_downlink_load(struct BTreeDescr *desc,
												uint64 downlink);
extern int64 s3_downlink_part(struct BTreeDescr *desc, uint64 downlink);
extern S3TaskLocation s3_schedule_root_file_write(char *filename, bool delete);
extern S3TaskLocation s3_schedule_pg_file_write(uint32 chkpNum, char *filename);
extern void s3_schedule_wal_chunks_delete(uint32 timeline,
										  uint64 fromSeq,
										  uint64 toSeq);
extern void s3_load_file_part(OIndexKey key, int32 segNum, int32 partNum);
extern bool s3_load_file_part_here(OIndexKey key, int32 segNum, int32 partNum);
extern void s3_schedule_part_read_ahead(OIndexKey key, int64 partNum);

extern bool s3_worker_process;
extern void s3_load_map_file(uint32 chkpNum, OIndexKey key);


#endif							/* __S3_WORKER_H__ */
