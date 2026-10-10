/*-------------------------------------------------------------------------
 *
 * control.h
 *		Declarations for S3 control and lock files.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_CONTROL_H__
#define __S3_CONTROL_H__

#include "checkpoint/control.h"

extern bool s3_check_control(CheckpointControl *control, uint64 rootControlId,
							 uint32 rootCheckpoint,
							 const char **errmsgp, const char **errdetailp);

#endif							/* __S3_CONTROL_H__ */
