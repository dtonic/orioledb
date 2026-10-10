/*-------------------------------------------------------------------------
 *
 * gc.h
 *		Declarations for the S3 GC.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/gc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_GC_H__
#define __S3_GC_H__

#include "lib/stringinfo.h"

extern int	s3_gc_interval;
extern int	s3_gc_max_pin_lag;
extern char *s3_compact_database;
extern int	s3_compact_interval;
extern double s3_compact_threshold;

extern int64 s3_compact_relation(Oid relid, double threshold);
extern void register_s3_compact_worker(void);
extern PGDLLEXPORT void s3_compact_worker_main(Datum main_arg);
extern int	s3_gc_keep_checkpoints;

extern int	s3_gc_cycle(void);
extern Size s3_gc_shmem_needs(void);
extern void s3_gc_shmem_init(Pointer ptr, bool found);
extern void s3_gc_note_scans(uint32 oldest);
extern uint64 *s3_gc_sparse_parts(Oid datoid, Oid relnode, double threshold,
								  int *count);
extern void register_s3_gc_worker(void);
extern PGDLLEXPORT void s3_gc_worker_main(Datum main_arg);

#endif							/* __S3_GC_H__ */
