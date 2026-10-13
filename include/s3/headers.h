/*-------------------------------------------------------------------------
 *
 * headers.h
 * 		Declarations for handling of S3-specific data file headers.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/headers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_HEADERS_H__
#define __S3_HEADERS_H__

typedef struct
{
	OIndexKey	key;
	int			segNum;
} S3HeaderTag;

typedef enum
{
	S3PartStatusNotLoaded = 0,
	S3PartStatusLoading = 1,
	S3PartStatusLoaded = 2,
	S3PartStatusEvicting = 3,
	/*
	 * An S3 worker could not download the part (s3_header_mark_part_failed):
	 * a reader finding it reports the part and puts it back to NotLoaded.
	 * Nothing is here, as for NotLoaded.
	 */
	S3PartStatusFailed = 4
} S3PartStatus;

/*
 * Two S3HeaderTags identify the same on-disk file when they share the same
 * datoid, relnode, tablespace and segNum.  reloid and ixType are tree-level
 * metadata that do not affect the file path and must not participate in the
 * hash/equality check.
 */
#define S3HeaderTagsIsEqual(t1, t2) \
	((t1).key.oids.datoid == (t2).key.oids.datoid && \
	 (t1).key.oids.relnode == (t2).key.oids.relnode && \
	 (t1).key.oids.spcoid == (t2).key.oids.spcoid && \
	 (t1).segNum == (t2).segNum)

extern int	s3_headers_buffers_size;

extern Size s3_headers_shmem_needs(void);
extern void s3_headers_shmem_init(Pointer buf, bool found);
extern void s3_headers_increase_loaded_parts(uint64 inc);
extern bool s3_header_lock_part(S3HeaderTag tag, int index,
								uint32 *loadId);
extern void s3_header_open_part(S3HeaderTag tag, int index, int stream,
								bool localOnly);
extern bool s3_header_seal_part(S3HeaderTag tag, int index, int stream);
extern uint64 s3_headers_unsealed_parts(void);
extern S3PartStatus s3_header_part_status(S3HeaderTag tag, int index);
extern S3PartStatus s3_header_mark_part_loading(S3HeaderTag tag, int index);
extern void s3_header_release_part_load(S3HeaderTag tag, int index);
extern void s3_header_mark_part_failed(S3HeaderTag tag, int index);
extern void s3_header_mark_part_loaded(S3HeaderTag tag, int index);
extern void s3_header_unlock_part(S3HeaderTag tag, int index, bool setDirty,
								  int stream);
extern bool s3_header_mark_part_scheduled_for_write(S3HeaderTag tag, int index);
extern void s3_header_mark_part_writing(S3HeaderTag tag, int index);
extern void s3_header_mark_part_written(S3HeaderTag tag, int index);
extern void s3_header_mark_part_not_written(S3HeaderTag tag, int index);
extern void s3_headers_sync(void);
extern void s3_headers_error_cleanup(void);
extern void s3_headers_try_eviction_cycle(void);
extern void s3_headers_scan_read(bool on);
extern void s3_headers_range_step(bool on);
extern bool s3_headers_in_range_step(void);

#endif							/* __S3_HEADERS_H__ */
