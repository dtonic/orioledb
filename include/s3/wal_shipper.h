/*-------------------------------------------------------------------------
 *
 * wal_shipper.h
 *		Declarations for shipping WAL to S3 as it is written.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/wal_shipper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_WAL_SHIPPER_H__
#define __S3_WAL_SHIPPER_H__

#include "access/xlogdefs.h"

extern bool s3_wal_shipping;
extern int	s3_wal_chunk_delay;
extern char *s3_wal_conninfo;

extern Size s3_wal_shipper_shmem_needs(void);
extern void s3_wal_shipper_shmem_init(Pointer ptr, bool found);
extern void s3_wal_shipper_init(void);
extern void register_s3_wal_shipper(void);
extern void s3_wal_shipper_set_catch_up(XLogRecPtr lsn);
extern bool s3_wal_ship_rest(TimeLineID tli);
extern char *s3_wal_chunk_key(TimeLineID tli, uint64 seq);
extern bool s3_wal_chunk_end(TimeLineID tli, uint64 seq, bool *terminator,
							 XLogRecPtr *end);
extern bool s3_wal_shipper_wait(XLogRecPtr lsn);
extern void s3_wal_place_terminator(uint64 token);
extern PGDLLEXPORT void s3_wal_shipper_main(Datum main_arg);

#endif							/* __S3_WAL_SHIPPER_H__ */
