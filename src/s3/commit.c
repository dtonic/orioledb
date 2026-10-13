/*-------------------------------------------------------------------------
 *
 * commit.c
 *		Committing a checkpoint to the S3 bucket.
 *
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/commit.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "s3/commit.h"
#include "s3/lease.h"
#include "s3/manifest.h"
#include "s3/wal_shipper.h"

#include "access/xlog.h"
#include "common/controldata_utils.h"
#include "miscadmin.h"

/*
 * Name checkpoint chkpNum in the bucket: publish its manifest, then point
 * root at it (s3_root_commit) and move the manifest from pending to
 * committed.  Called from o_after_checkpoint_cleanup_hook() once every file
 * of the checkpoint, pg_control included, is in the bucket.
 *
 * Root names a checkpoint only with its manifest up: a checkpoint whose PG
 * file backup did not finish (a crash in it, then the hook at the end of
 * recovery) is not committed, the next one is.  Nor before its checkpoint
 * record is in the bucket: a restore from root starts by reading it.  A
 * checkpoint the shipper cannot vouch for (it stopped) is left for the next
 * one.
 */
void
s3_checkpoint_commit(uint32 chkpNum)
{
	bool		crc_ok;
	ControlFileData *cf = get_controlfile(DataDir, &crc_ok);

	if (!crc_ok)
		elog(ERROR, "pg_control checksum mismatch");

	if (XLogInsertAllowed() && s3_wal_shipping &&
		!s3_wal_shipper_wait(GetFlushRecPtr(NULL)))
		ereport(LOG,
				(errmsg("S3 checkpoint %u is not committed: the WAL shipper is not running",
						chkpNum)));
	else if (s3_manifest_publish(chkpNum, cf->checkPointCopy.redo, cf->checkPoint,
								 cf->checkPointCopy.ThisTimeLineID))
	{
		s3_root_commit(chkpNum, cf->checkPointCopy.redo, cf->checkPoint);
		s3_manifest_committed(chkpNum);
	}
	pfree(cf);
}
