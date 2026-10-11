/*-------------------------------------------------------------------------
 *
 * manifest.h
 *		Declarations for the per-checkpoint S3 manifests.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/manifest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_MANIFEST_H__
#define __S3_MANIFEST_H__

#include "access/xlogdefs.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

#define S3_MANIFEST_FORMAT		1

typedef struct
{
	Oid			datoid;
	Oid			relnode;
} S3ManifestTreeKey;

typedef struct
{
	S3ManifestTreeKey key;
	Oid			spcoid;
	uint32		map;			/* the checkpoint number of its map */
} S3ManifestTree;

typedef struct
{
	char		name[MAXPGPATH];
	uint32		chkp;			/* the checkpoint that uploaded it */
} S3ManifestRef;

typedef struct
{
	uint32		chkp;
	int			count;
} S3ManifestBundles;

/* a timeline left behind: its chunks up to the terminator */
typedef struct
{
	TimeLineID	timeline;
	uint64		lastSeq;
} S3ManifestRetired;

/*
 * What checkpoint K uploaded, and which uploaded objects -- its own and
 * earlier checkpoints' -- restoring K needs.
 */
typedef struct
{
	uint32		checkpoint;
	uint64		token;
	bool		complete;		/* every reference is known */
	XLogRecPtr	redoLsn;
	XLogRecPtr	checkpointLsn;
	TimeLineID	timeline;
	int64		time;			/* Unix time it was published at, or 0 */

	/* uploaded by this checkpoint */
	List	   *data;			/* paths under data/<T>-<K>/ */
	List	   *undoUp;			/* undo file names */
	List	   *mapUp;			/* S3ManifestTree */
	List	   *deadUp;			/* S3ManifestTree */
	List	   *buildDeadUp;	/* S3ManifestTree */
	List	   *retired;		/* S3ManifestRetired */

	/* what restoring it reads */
	HTAB	   *trees;			/* S3ManifestTree, the catalog's trees */
	HTAB	   *files;			/* S3ManifestRef: big PG files */
	HTAB	   *small;			/* S3ManifestRef: small PG files */
	HTAB	   *bundles;		/* S3ManifestBundles by checkpoint */
	HTAB	   *undo;			/* S3ManifestRef: undo files */
} S3Manifest;

extern void s3_manifest_begin(uint32 chkpNum);
extern void s3_manifest_note(uint32 chkpNum, const char *fmt,...) pg_attribute_printf(2, 3);
extern void s3_manifest_note_tree(Oid datoid, Oid relnode, Oid spcoid);
extern void s3_manifest_prepare(uint32 chkpNum, int bundles);
extern bool s3_manifest_publish(uint32 chkpNum, XLogRecPtr redoLsn,
								XLogRecPtr checkpointLsn, TimeLineID timeline);
extern void s3_manifest_committed(uint32 chkpNum);

extern char *s3_manifest_key(uint32 chkpNum, uint64 token);
extern S3Manifest *s3_manifest_parse(const char *body);
extern S3Manifest *s3_manifest_fetch(uint32 chkpNum);
extern void s3_manifest_free(S3Manifest *m);

/* trees dropped since the GC last looked, and the checkpoint then going */
#define S3_DROPPED_TREES_FILE	ORIOLEDB_DATA_DIR "/s3_dropped_trees"

typedef struct
{
	uint32		chkp;
	Oid			datoid;
	Oid			relnode;
} S3DroppedTree;

extern void s3_note_dropped_tree(Oid datoid, Oid relnode);
extern void s3_tree_parts_gone(Oid datoid, Oid relnode, uint64 floor);
extern void s3_build_part_end(Oid datoid, Oid relnode, Oid spcoid,
							  uint64 blocksPerPart, uint64 *ends, int nends);
extern char *s3_build_dead_filename(Oid datoid, Oid relnode);
extern int	s3_local_lists_lock(void);
extern void s3_local_lists_unlock(int fd);
extern void s3_tree_lists_unlink(Oid datoid, Oid relnode);

#endif							/* __S3_MANIFEST_H__ */
