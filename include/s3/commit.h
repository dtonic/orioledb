/*-------------------------------------------------------------------------
 *
 * commit.h
 *		Declarations for committing a checkpoint to the S3 bucket.
 *
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/commit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_COMMIT_H__
#define __S3_COMMIT_H__

extern void s3_checkpoint_commit(uint32 chkpNum);

#endif							/* __S3_COMMIT_H__ */
