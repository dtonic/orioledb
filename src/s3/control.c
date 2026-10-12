/*-------------------------------------------------------------------------
 *
 * control.c
 *		Functions to work with the S3 control file.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/control.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include "orioledb.h"

#include "checkpoint/control.h"
#include "s3/control.h"
#include "s3/requests.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

/*
 * Check the local data directory belongs to the S3 bucket and is not behind
 * it: root names the control identifier and the last checkpoint committed.
 * control is NULL when the data directory has no control file yet.
 */
bool
s3_check_control(CheckpointControl *control, uint64 rootControlId,
				 uint32 rootCheckpoint,
				 const char **errmsgp, const char **errdetailp)
{
	/* a bucket nothing committed to is free to use */
	if (rootControlId == 0 && rootCheckpoint == 0)
		return true;

	if (control == NULL)
	{
		*errmsgp = psprintf("OrioleDB can be incompatible with the S3 bucket "
							"because the control file exists on the S3 bucket");
		*errdetailp = psprintf("OrioleDB control file \"%s\" is absent", CONTROL_FILENAME);
		return false;
	}

	if (rootControlId != 0 && control->controlIdentifier != rootControlId)
	{
		*errmsgp = psprintf("OrioleDB and the S3 bucket have files from "
							"different instances and they are incompatible with "
							"each other");
		*errdetailp = psprintf("OrioleDB control identifier " UINT64_FORMAT
							   " differs from the S3 bucket identifier " UINT64_FORMAT,
							   control->controlIdentifier, rootControlId);
		return false;
	}

	if (control->lastCheckpointNumber < rootCheckpoint)
	{
		*errmsgp = psprintf("OrioleDB misses new changes and checkpoints from "
							"the S3 bucket and they are incompatible with "
							"each other");
		*errdetailp = psprintf("OrioleDB last checkpoint number %u is behind "
							   "the S3 bucket last checkpoint number %u",
							   control->lastCheckpointNumber, rootCheckpoint);
		return false;
	}
	else if (control->lastCheckpointNumber > rootCheckpoint)
		ereport(LOG,
				(errmsg("OrioleDB has more changes and checkpoints than "
						"the S3 bucket but they are still compatible with each "
						"other"),
				 errdetail("OrioleDB last checkpoint number %u is ahead of "
						   "the S3 bucket last checkpoint number %u",
						   control->lastCheckpointNumber, rootCheckpoint)));
	return true;
}
