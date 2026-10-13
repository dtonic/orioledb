/*-------------------------------------------------------------------------
 *
 * queue.h
 * 		Declarations for queue of tasks for S3 workers.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/queue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_QUEUE_H__
#define __S3_QUEUE_H__

#include "postgres.h"

#include "orioledb.h"

#define InvalidS3TaskLocation (UINT64_MAX)

extern Size s3_queue_shmem_needs(void);
extern void s3_queue_init_shmem(Pointer ptr, bool found);
extern S3TaskLocation s3_queue_get_insert_location(void);
extern S3TaskLocation s3_queue_put_task(Pointer data, uint32 len);
extern void s3_queue_put_background_task(Pointer data, uint32 len);
extern S3TaskLocation s3_queue_try_pick_task(volatile S3TaskLocation *record);
extern bool s3_queue_task_is_live(S3TaskLocation taskLocation);
extern void s3_queue_wait_for_task(long timeout, uint32 wait_event_info);
Pointer		s3_queue_get_task(S3TaskLocation taskLocation);
extern void s3_queue_erase_task(S3TaskLocation taskLocation);
extern void s3_queue_wait_for_location(S3TaskLocation location);

#endif							/* __S3_QUEUE_H__ */
