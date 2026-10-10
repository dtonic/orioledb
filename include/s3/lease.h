/*-------------------------------------------------------------------------
 *
 * lease.h
 *		Declarations for the S3 writer lease and the root commit pointer.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/lease.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_LEASE_H__
#define __S3_LEASE_H__

#include "access/xlogdefs.h"

extern int	s3_lease_duration;
extern int	s3_reader_pin_ttl;

extern Size s3_lease_shmem_needs(void);
extern void s3_lease_shmem_init(Pointer ptr, bool found);

extern void s3_lease_acquire(void);
extern void s3_lease_reader_init(void);
extern void s3_lease_release(void);
extern void register_s3_lease_worker(void);
extern PGDLLEXPORT void s3_lease_worker_main(Datum main_arg);

extern uint64 s3_lease_token(void);
extern uint32 s3_lease_root_checkpoint(void);
extern void s3_gc_publish(uint32 retain, uint32 folded, uint32 collected,
						  uint32 pinFloor, uint32 pinWarn);
extern bool s3_gc_progress(uint32 *retain, uint32 *folded, uint32 *collected);
extern uint32 s3_reader_pins_min(uint32 upper, uint32 floor);
extern void s3_reader_pin_renew(void);
extern bool s3_lease_own_token(uint64 token);
extern void s3_lease_loaded_done(void);
extern void s3_stop_instance(const char *message, const char *detail);
extern uint64 s3_checkpoint_token(uint32 chkpNum);
extern bool s3_checkpoint_token_known(uint32 chkpNum, uint64 *token);
extern void s3_lease_lost(const char *detail);
extern void s3_lease_hold(void);
extern void s3_root_commit(uint32 checkpointNum, XLogRecPtr redoLsn,
						   XLogRecPtr checkpointLsn);

#endif							/* __S3_LEASE_H__ */
