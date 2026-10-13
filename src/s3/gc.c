/*-------------------------------------------------------------------------
 *
 * gc.c
 *		Remove what no retained checkpoint of the S3 bucket refers to.
 *
 * R is the oldest retained checkpoint: the committed one minus
 * orioledb.s3_gc_keep_checkpoints, or the oldest committed within
 * orioledb.s3_gc_keep_time if that is older, and no later than the checkpoint any
 * reader still has pinned (readers object).  The GC publishes R in the `gc`
 * object before it reads the pins; a reader writes its pin before it reads
 * `gc`, and refuses to start on a checkpoint older than R.  Whichever comes
 * first, a reader's checkpoint is not collected under it.
 *
 * Parts.  Every checkpoint N lists, per tree, the extents that died at N: no
 * checkpoint from N on refers to them (free_extent_for_checkpoint()).  The
 * same list says where a part ends once the tree is done writing to it; a
 * tree build leaves the ends of its parts in a list of its own.  The GC keeps,
 * per tree, a bitmap of the dead blocks of each part it has not removed yet
 * (local orioledb_data/<db>/<rel>.gcstate, copied to trees/<db>/<rel>/gc),
 * folds the lists of the checkpoints up to R into it, and removes a part once
 * its end is known and every block below the end is dead.  A block recorded
 * twice is counted once, so folding a list again is harmless.
 *
 * Everything else.  Each checkpoint's manifest says what it uploaded and what
 * restoring it reads (manifest.c).  References only move forward, so an
 * object of a checkpoint before R that R's manifest does not refer to is not
 * referred to from R on: the GC removes it, then the manifest.  A tree in an
 * older manifest and not in R's was dropped by R: its parts, maps and state
 * go.  The WAL segments before R's REDO go too, and the chunks of every
 * timeline that end before the segment of R's REDO.
 *
 * Progress is published in `gc` (folded, collected), so a writer that takes
 * the bucket over goes on where the last one stopped; a tree's state is the
 * union of this data directory's copy and the bucket's.  A relnode a
 * manifest names again after R dropped its tree is a new tree under the old
 * keys: its state starts over, so the dropped tree's dead bits do not mark
 * the new tree's parts dead.
 *
 * Every delete, and every write of a tree's state, is fenced: the lease is
 * ours for a while yet (s3_lease_hold()), and a state is put over the one in
 * the bucket only while that one is ours or an earlier writer's.  A pass
 * frozen past its lease would otherwise delete what the writer that took
 * over has its readers pinned on.  A delete the store refuses for good is
 * logged and left for a later pass; the part stays in the tree's state.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/gc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "orioledb.h"

#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "s3/gc.h"
#include "s3/lease.h"
#include "s3/manifest.h"
#include "s3/requests.h"
#include "s3/wal_shipper.h"
#include "utils/seq_buf.h"

#include "access/twophase.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define GC_STATE_MAGIC		0x34434753	/* "SGC4" */

int			s3_gc_interval = 60;
int			s3_gc_keep_checkpoints = 2;
int			s3_gc_keep_time = 0;
int			s3_gc_max_pin_lag = 1000;

#define PART_GONE	UINT64_MAX

typedef struct
{
	uint64		part;
	uint64		end;			/* 0: the part is not complete yet;
								 * PART_GONE: every block of it is dead */
	uint8	   *dead;			/* one bit per block of the part, sized by the
								 * tree's blocks per part; NULL: none dead */
} GcPart;

typedef struct
{
	S3ManifestTreeKey key;
	Oid			spcoid;
	uint64		blocksPerPart;	/* 0: not known yet */
	bool		skip;			/* a list could not be read: collect nothing */
	bool		changed;
	HTAB	   *parts;
	MemoryContext cxt;			/* the parts and their bitmaps */
} GcTree;

static inline Size
bitmap_bytes(GcTree *tree)
{
	return (tree->blocksPerPart + 7) / 8;
}

static int
pg_cmp_u64_wrapper(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a,
				y = *(const uint64 *) b;

	return x < y ? -1 : (x > y ? 1 : 0);
}

/*
 * Deletions are queued and sent many at a time (s3_delete_objects_try()).  A
 * caller flushes before it records that what it deleted is gone: a tree's
 * state without its parts, a published progress, a manifest.  The keys the
 * store refused are kept for the pass (gc_failed()): what they name is left
 * for a later pass, and nothing records it as gone.
 */
#define GC_DELETE_BATCH		1000

typedef struct
{
	char		key[MAXPGPATH + 64];
} DeletedKey;

static List *gc_pending = NIL;
static HTAB *gcFailed = NULL;

static void
gc_note_failed(const char *key)
{
	DeletedKey	k = {{0}};

	if (gcFailed == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(DeletedKey);
		ctl.entrysize = sizeof(DeletedKey);
		ctl.hcxt = CurrentMemoryContext;
		gcFailed = hash_create("orioledb s3 gc failed deletes", 16, &ctl,
							   HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
	strlcpy(k.key, key, sizeof(k.key));
	(void) hash_search(gcFailed, &k, HASH_ENTER, NULL);
}

static bool
gc_failed(const char *key)
{
	DeletedKey	k = {{0}};

	if (gcFailed == NULL)
		return false;
	strlcpy(k.key, key, sizeof(k.key));
	return hash_search(gcFailed, &k, HASH_FIND, NULL) != NULL;
}

static void
gc_flush(void)
{
	int			n = list_length(gc_pending);
	char	  **keys;
	ListCell   *lc;
	List	   *failed;
	int			i = 0;

	if (n == 0)
		return;

	/*
	 * The lease is ours for a while yet, or the instance stops here: a pass
	 * frozen past its lease and thawed would delete what the writer that
	 * took over keeps for its readers.
	 */
	(void) s3_lease_hold();
	keys = palloc(sizeof(char *) * n);
	foreach(lc, gc_pending)
		keys[i++] = lfirst(lc);
	failed = s3_delete_objects_try(keys, n);
	foreach(lc, failed)
	{
		const char *key = keys[lfirst_int(lc)];

		ereport(LOG,
				(errmsg("S3 GC could not delete \"%s\", the store refused; left for a later pass",
						key)));
		gc_note_failed(key);
	}
	list_free(failed);
	pfree(keys);
	list_free_deep(gc_pending);
	gc_pending = NIL;
}

static void
gc_delete(const char *key)
{
	gc_pending = lappend(gc_pending, pstrdup(key));
	if (list_length(gc_pending) >= GC_DELETE_BATCH)
		gc_flush();
}

/*
 * Per process, the oldest checkpoint whose maps its sequential scans read (0:
 * none).  Such a scan reads the extents of that checkpoint's map, which a
 * later checkpoint may list as dead: R stays at or before it.
 */
static pg_atomic_uint32 *scanCheckpoints = NULL;

static int
scan_slots(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;
}

Size
s3_gc_shmem_needs(void)
{
	return mul_size(scan_slots(), sizeof(pg_atomic_uint32));
}

void
s3_gc_shmem_init(Pointer ptr, bool found)
{
	scanCheckpoints = (pg_atomic_uint32 *) ptr;
	if (!found)
		for (int i = 0; i < scan_slots(); i++)
			pg_atomic_init_u32(&scanCheckpoints[i], 0);
}

static void
clear_scan_slot(int code, Datum arg)
{
	pg_atomic_write_u32(&scanCheckpoints[DatumGetInt32(arg)], 0);
}

void
s3_gc_note_scans(uint32 oldest)
{
	static bool exitHook = false;
	int			slot;

	if (scanCheckpoints == NULL || MyProc == NULL)
		return;
	slot = MyProc - ProcGlobal->allProcs;
	/* an exit that skips the scans' cleanup must not leave the slot set */
	if (!exitHook)
	{
		on_shmem_exit(clear_scan_slot, Int32GetDatum(slot));
		exitHook = true;
	}
	pg_atomic_write_u32(&scanCheckpoints[slot], oldest);
}

static uint32
oldest_scan_checkpoint(void)
{
	uint32		oldest = 0;

	for (int i = 0; i < scan_slots(); i++)
	{
		uint32		c = pg_atomic_read_u32(&scanCheckpoints[i]);

		if (c != 0 && (oldest == 0 || c < oldest))
			oldest = c;
	}
	return oldest;
}

/*
 * A shutdown does not wait for a long pass: every step can be done again,
 * and progress is published only after the steps it covers.
 */
static inline void
gc_check_stop(void)
{
	if (ShutdownRequestPending)
		proc_exit(0);
}

static char *
state_filename(Oid datoid, Oid relnode)
{
	return psprintf(ORIOLEDB_DATA_DIR "/%u/%u.gcstate", datoid, relnode);
}

static char *
state_key(Oid datoid, Oid relnode)
{
	return psprintf("trees/%u/%u/gc", datoid, relnode);
}

static char *
part_key(Oid datoid, Oid relnode, uint64 part)
{
	return psprintf("trees/%u/%u/parts/" INT64_FORMAT, datoid, relnode,
					(int64) part);
}

static GcPart *
tree_part(GcTree *tree, uint64 part)
{
	bool		found;
	GcPart	   *entry = hash_search(tree->parts, &part, HASH_ENTER, &found);

	if (!found)
	{
		entry->end = 0;
		entry->dead = NULL;
	}
	return entry;
}

/* the part's bitmap, made once the blocks per part are known */
static uint8 *
part_bitmap(GcTree *tree, GcPart *p)
{
	Assert(tree->blocksPerPart != 0);
	if (p->dead == NULL)
		p->dead = MemoryContextAllocZero(tree->cxt, bitmap_bytes(tree));
	return p->dead;
}

static bool
block_dead(GcPart *p, uint64 i)
{
	return p->dead != NULL && (p->dead[i / 8] & (1 << (i % 8))) != 0;
}

/*
 * A state's serialized form: magic, the token of the writer that wrote it,
 * blocks per part, count, then per part its number, end and bitmap of
 * (blocks per part + 7) / 8 bytes.  It is merged into what the tree has.
 */
#define STATE_HEADER_SIZE	(sizeof(uint32) + 3 * sizeof(uint64))

static bool
state_token(const char *data, uint64 size, uint64 *token)
{
	uint32		magic;

	if (size < STATE_HEADER_SIZE)
		return false;
	memcpy(&magic, data, sizeof(magic));
	if (magic != GC_STATE_MAGIC)
		return false;
	memcpy(token, data + sizeof(magic), sizeof(uint64));
	return true;
}

static bool
state_decode(GcTree *tree, const char *data, uint64 size)
{
	uint64		token,
				count,
				bpp,
				bytes;
	const char *p = data;

	if (!state_token(data, size, &token))
		return false;
	p += sizeof(uint32) + sizeof(uint64);
	memcpy(&bpp, p, sizeof(uint64));
	p += sizeof(uint64);
	memcpy(&count, p, sizeof(count));
	p += sizeof(count);
	if (bpp > ORIOLEDB_S3_PART_SIZE)
		return false;
	bytes = (bpp + 7) / 8;
	if (size != STATE_HEADER_SIZE + count * (2 * sizeof(uint64) + bytes))
		return false;
	if (bpp != 0)
	{
		if (tree->blocksPerPart != 0 && tree->blocksPerPart != bpp)
			return false;
		tree->blocksPerPart = bpp;
	}
	for (uint64 i = 0; i < count; i++)
	{
		uint64		part,
					end;
		GcPart	   *entry;

		memcpy(&part, p, sizeof(part));
		p += sizeof(part);
		memcpy(&end, p, sizeof(end));
		p += sizeof(end);
		entry = tree_part(tree, part);
		entry->end = Max(entry->end, end);
		if (bytes > 0)
		{
			uint8	   *dead = part_bitmap(tree, entry);

			for (uint64 b = 0; b < bytes; b++)
				dead[b] |= (uint8) p[b];
		}
		p += bytes;
	}
	return true;
}

static void
state_encode(GcTree *tree, StringInfo out, uint64 token)
{
	uint32		magic = GC_STATE_MAGIC;
	uint64		count = hash_get_num_entries(tree->parts);
	Size		bytes = bitmap_bytes(tree);
	HASH_SEQ_STATUS seq;
	GcPart	   *entry;

	appendBinaryStringInfo(out, (char *) &magic, sizeof(magic));
	appendBinaryStringInfo(out, (char *) &token, sizeof(token));
	appendBinaryStringInfo(out, (char *) &tree->blocksPerPart, sizeof(uint64));
	appendBinaryStringInfo(out, (char *) &count, sizeof(count));
	hash_seq_init(&seq, tree->parts);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		appendBinaryStringInfo(out, (char *) &entry->part, sizeof(uint64));
		appendBinaryStringInfo(out, (char *) &entry->end, sizeof(uint64));
		if (entry->dead != NULL)
			appendBinaryStringInfo(out, (char *) entry->dead, bytes);
		else
			for (Size b = 0; b < bytes; b++)
				appendStringInfoChar(out, 0);
	}
}

/*
 * The tree's state: this data directory's copy and the bucket's (a writer
 * before this one may have gone further), merged.
 */
static void
state_load(GcTree *tree)
{
	char	   *filename = state_filename(tree->key.datoid, tree->key.relnode);
	char	   *key = state_key(tree->key.datoid, tree->key.relnode);
	struct stat st;
	bool		ok = true;
	StringInfoData body;

	if (stat(filename, &st) == 0)
	{
		uint64		size;
		Pointer		data = read_file(filename, &size);

		ok = data != NULL && state_decode(tree, data, size);
		if (data)
			pfree(data);
	}
	initStringInfo(&body);
	if (ok && s3_get_object(key, &body, true) == S3_RESPONSE_OK)
		ok = state_decode(tree, body.data, body.len);
	pfree(body.data);
	if (!ok)
	{
		ereport(LOG,
				(errmsg("S3 GC skips tree %u/%u: its state is not readable",
						tree->key.datoid, tree->key.relnode)));
		tree->skip = true;
	}
	pfree(filename);
	pfree(key);
}

/*
 * Save the tree's state locally and in the bucket.  The bucket's copy is put
 * over what is there only while that is ours or an earlier writer's, and only
 * if it is still what was read (the same fence as s3_gc_publish()): a state
 * a later writer wrote is theirs, and this instance stops.
 */
static void
state_save(GcTree *tree)
{
	char	   *filename = state_filename(tree->key.datoid, tree->key.relnode);
	char	   *tmp = psprintf("%s.tmp", filename);
	char	   *dir = psprintf(ORIOLEDB_DATA_DIR "/%u", tree->key.datoid);
	char	   *key = state_key(tree->key.datoid, tree->key.relnode);
	uint64		mine = s3_lease_token();
	StringInfoData body;
	int			fd;

	(void) s3_lease_hold();
	initStringInfo(&body);
	state_encode(tree, &body, mine);
	(void) MakePGDirectory(dir);
	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0 || write(fd, body.data, body.len) != body.len ||
		pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, filename, ERROR);

	for (int attempt = 0;; attempt++)
	{
		StringInfoData current;
		char	   *etag = NULL;
		uint64		theirs;
		long		code;

		initStringInfo(&current);
		code = s3_get_object_etag(key, &current, true, &etag);
		if (code == S3_RESPONSE_OK &&
			state_token(current.data, current.len, &theirs) && theirs > mine)
			s3_lease_lost(psprintf("The \"%s\" object was written by writer token " UINT64_FORMAT,
								   key, theirs));
		code = s3_put_object_conditional(key, body.data, body.len,
										 code == S3_RESPONSE_OK ? etag : NULL,
										 code != S3_RESPONSE_OK, NULL);
		pfree(current.data);
		if (etag)
			pfree(etag);
		if (code == S3_RESPONSE_OK)
			break;
		/* the condition failed: another writer of the key, read it again */
		pg_usleep(Min(1000000L, 10000L << Min(attempt, 7)));
	}
	pfree(body.data);
	pfree(filename);
	pfree(tmp);
	pfree(dir);
	pfree(key);
}

static void
state_drop(Oid datoid, Oid relnode)
{
	char	   *filename = state_filename(datoid, relnode);
	char	   *key = state_key(datoid, relnode);

	gc_delete(key);
	(void) unlink(filename);
	pfree(filename);
	pfree(key);
}

/*
 * Fold one list of dead extents into the tree's state.
 */
static void
fold_list(GcTree *tree, const char *data, uint64 size, const char *name)
{
	FileExtent	extent;

	for (uint64 pos = 0; pos + sizeof(extent) <= size; pos += sizeof(extent))
	{
		uint64		bpp;

		memcpy(&extent, data + pos, sizeof(extent));
		if (extent.len == S3_DEAD_UNIT_LEN)
		{
			if (extent.off == 0 || extent.off > ORIOLEDB_S3_PART_SIZE ||
				(tree->blocksPerPart != 0 && tree->blocksPerPart != extent.off))
			{
				ereport(LOG,
						(errmsg("S3 GC skips tree %u/%u: \"%s\" gives %llu blocks per part",
								tree->key.datoid, tree->key.relnode, name,
								(unsigned long long) extent.off)));
				tree->skip = true;
				return;
			}
			tree->blocksPerPart = extent.off;
			continue;
		}
		if (extent.len == S3_DEAD_PARTS_BELOW_LEN)
		{
			/* the tree was emptied in place: its parts below go whole */
			for (uint64 part = 0; part < extent.off; part++)
				tree_part(tree, part)->end = PART_GONE;
			tree->changed = true;
			continue;
		}
		bpp = tree->blocksPerPart;
		if (bpp == 0)
		{
			/*
			 * Every list opens with the blocks per part.  Without it the
			 * offsets cannot be put in parts: give up on the tree rather than
			 * guess, which could make a live part look dead.
			 */
			ereport(LOG,
					(errmsg("S3 GC skips tree %u/%u: \"%s\" does not give its blocks per part",
							tree->key.datoid, tree->key.relnode, name)));
			tree->skip = true;
			return;
		}
		tree->changed = true;
		if (extent.len == 0)
		{
			if (extent.off > 0)
			{
				/* lists fold in any order: an end only moves on */
				GcPart	   *p = tree_part(tree, (extent.off - 1) / bpp);

				p->end = Max(p->end, extent.off);
			}
			continue;
		}
		for (uint64 b = extent.off; b < extent.off + extent.len; b++)
		{
			GcPart	   *p = tree_part(tree, b / bpp);
			uint64		i = b % bpp;

			part_bitmap(tree, p)[i / 8] |= (uint8) (1 << (i % 8));
		}
	}
}

static bool
part_all_dead(GcTree *tree, GcPart *p)
{
	uint64		start = p->part * tree->blocksPerPart;

	if (p->end == PART_GONE)
		return true;
	if (p->end == 0 || p->end <= start)
		return false;
	for (uint64 i = 0; i < p->end - start; i++)
		if (!block_dead(p, i))
			return false;
	return true;
}

/*
 * The parts queued for deletion this pass.  A part leaves its tree's state
 * only once the flush shows its delete went through (forget_removed_parts()):
 * one the store refused is tried again by the pass that next folds the tree.
 */
typedef struct
{
	GcTree	   *tree;
	uint64		part;
	char	   *key;
} PendingPart;

static List *gc_parts = NIL;

static void
collect_tree(GcTree *tree)
{
	HASH_SEQ_STATUS seq;
	GcPart	   *entry;

	if (tree->skip)
		return;
	hash_seq_init(&seq, tree->parts);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		if (part_all_dead(tree, entry))
		{
			PendingPart *pending = palloc(sizeof(PendingPart));

			gc_check_stop();
			pending->tree = tree;
			pending->part = entry->part;
			pending->key = part_key(tree->key.datoid, tree->key.relnode,
									entry->part);
			gc_parts = lappend(gc_parts, pending);
			gc_delete(pending->key);
		}
	}
}

/* after the flush: the parts whose delete went through are gone */
static int
forget_removed_parts(void)
{
	ListCell   *lc;
	int			removed = 0;

	foreach(lc, gc_parts)
	{
		PendingPart *pending = lfirst(lc);

		if (gc_failed(pending->key))
			continue;
		(void) hash_search(pending->tree->parts, &pending->part, HASH_REMOVE,
						   NULL);
		pending->tree->changed = true;
		removed++;
	}
	list_free_deep(gc_parts);
	gc_parts = NIL;
	return removed;
}

typedef struct
{
	char	   *object;			/* the list in the bucket */
	char	   *local;			/* its local copy, or NULL */
	GcTree	   *tree;
} FoldedList;

/*
 * A checkpoint's manifest, or NULL when the bucket has none.  One that does
 * not parse, or is not its checkpoint's, is passed over like one never
 * written, with a LOG: its lists are folded as an unpublished checkpoint's
 * (fold_unpublished()), what only it referred to is for --sweep, and it goes
 * with the manifests below R.  An ERROR here would end the worker, which
 * would start again and meet the same object: no pass would ever finish.
 */
static S3Manifest *
gc_manifest_fetch(uint32 chkpNum)
{
	S3Manifest *volatile m = NULL;
	MemoryContext cxt = CurrentMemoryContext;

	PG_TRY();
	{
		m = s3_manifest_fetch(chkpNum);
	}
	PG_CATCH();
	{
		ErrorData  *e;

		MemoryContextSwitchTo(cxt);
		e = CopyErrorData();
		FlushErrorState();
		ereport(LOG,
				(errmsg("S3 GC skips the manifest of checkpoint %u: %s",
						chkpNum, e->message)));
		FreeErrorData(e);
		m = NULL;
	}
	PG_END_TRY();
	return m;
}

static GcTree *
get_tree(HTAB *trees, S3ManifestTree *t)
{
	bool		found;
	GcTree	   *tree = hash_search(trees, &t->key, HASH_ENTER, &found);

	if (!found)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(GcPart);
		ctl.hcxt = CurrentMemoryContext;
		tree->spcoid = t->spcoid;
		tree->blocksPerPart = 0;
		tree->skip = false;
		tree->changed = false;
		tree->cxt = CurrentMemoryContext;
		tree->parts = hash_create("orioledb s3 gc parts", 64, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		state_load(tree);
	}
	return tree;
}

/*
 * Fold the lists of one checkpoint: each from its local copy when this data
 * directory wrote it, else from the bucket.
 */
static List *
fold_checkpoint(HTAB *trees, S3Manifest *m, List *folded)
{
	ListCell   *lc;

	for (int build = 0; build < 2; build++)
	{
		foreach(lc, build ? m->buildDeadUp : m->deadUp)
		{
			S3ManifestTree *t = lfirst(lc);
			GcTree	   *tree = get_tree(trees, t);
			FoldedList *f = palloc0(sizeof(FoldedList));
			struct stat st;
			uint64		size = 0;
			Pointer		data = NULL;

			f->tree = tree;

			f->object = psprintf("trees/%u/%u/dead/" UINT64_FORMAT "-%u%s",
								 t->key.datoid, t->key.relnode, m->token,
								 m->checkpoint, build ? ".build" : "");
			if (!build)
			{
				SeqBufTag	tag;

				memset(&tag, 0, sizeof(tag));
				tag.key.oids.datoid = t->key.datoid;
				tag.key.oids.relnode = t->key.relnode;
				tag.key.oids.spcoid = t->spcoid;
				tag.num = m->checkpoint;
				tag.type = 'd';
				f->local = get_seq_buf_filename(&tag);
				if (stat(f->local, &st) == 0)
					data = read_file(f->local, &size);
			}
			if (data == NULL)
			{
				StringInfoData body;

				initStringInfo(&body);
				if (s3_get_object(f->object, &body, true) != S3_RESPONSE_OK)
				{
					/* folded and removed already: the state has it */
					pfree(body.data);
					folded = lappend(folded, f);
					continue;
				}
				data = body.data;
				size = body.len;
			}
			if (!tree->skip)
				fold_list(tree, data, size, f->object);
			pfree(data);
			folded = lappend(folded, f);
		}
	}
	return folded;
}

/*
 * A checkpoint with no manifest: it finished locally and its trees uploaded
 * their dead lists, but the instance stopped before the manifest went up (or
 * the shipper could not vouch for it), and the next checkpoint does not name
 * its lists.  They are looked for under every tree the next manifest knows,
 * as the trees that could have had them.  Rare, so one GET per tree is fine;
 * a list not found is one that never was.
 */
static List *
fold_unpublished(HTAB *trees, uint32 k, uint32 committed, List *folded)
{
	uint64		token;
	S3Manifest *later = NULL;
	S3Manifest	m;
	HASH_SEQ_STATUS seq;
	S3ManifestTree *t;

	if (!s3_checkpoint_token_known(k, &token))
		return folded;
	for (uint32 n = k + 1; n <= committed && later == NULL; n++)
		later = gc_manifest_fetch(n);
	if (later == NULL)
		return folded;

	memset(&m, 0, sizeof(m));
	m.checkpoint = k;
	m.token = token;
	hash_seq_init(&seq, later->trees);
	while ((t = hash_seq_search(&seq)) != NULL)
	{
		S3ManifestTree *copy = palloc(sizeof(S3ManifestTree));

		*copy = *t;
		m.deadUp = lappend(m.deadUp, copy);
	}
	m.buildDeadUp = list_copy(m.deadUp);
	folded = fold_checkpoint(trees, &m, folded);
	list_free(m.buildDeadUp);
	list_free_deep(m.deadUp);
	s3_manifest_free(later);
	return folded;
}

/*
 * A tree's map of checkpoint chkp is under the token of the run that did
 * chkp, or of a run after it (a build names its map with the last
 * checkpoint done): s3_map_tokens().
 */
static void
delete_map(Oid datoid, Oid relnode, uint32 chkp)
{
	uint64	   *tokens;
	int			n;

	if (chkp == 0)
		return;
	n = s3_map_tokens(chkp, &tokens);

	for (int i = 0; i < n; i++)
	{
		char	   *key = psprintf("trees/%u/%u/maps/" UINT64_FORMAT "-%u",
								   datoid, relnode, tokens[i], chkp);

		gc_delete(key);
		pfree(key);
	}
	pfree(tokens);
}

typedef struct
{
	S3ManifestTreeKey key;
} DroppedTree;

static void
delete_once(HTAB *deleted, const char *key)
{
	DeletedKey	k = {{0}};
	bool		found;

	gc_check_stop();
	strlcpy(k.key, key, sizeof(k.key));
	(void) hash_search(deleted, &k, HASH_ENTER, &found);
	if (!found)
		gc_delete(k.key);
}

static void
delete_map_once(HTAB *deleted, Oid datoid, Oid relnode, uint32 chkp)
{
	DeletedKey	k = {{0}};
	bool		found;

	snprintf(k.key, sizeof(k.key), "map %u %u %u", datoid, relnode, chkp);
	(void) hash_search(deleted, &k, HASH_ENTER, &found);
	if (!found)
		delete_map(datoid, relnode, chkp);
}

static char *
data_key(uint64 token, uint32 chkp, const char *path)
{
	return psprintf("data/" UINT64_FORMAT "-%u/%s", token, chkp, path);
}

static void
note_dropped(HTAB *dropped, S3ManifestTreeKey *key)
{
	(void) hash_search(dropped, key, HASH_ENTER, NULL);
}

/*
 * Remove what checkpoint K uploaded or referred to that R's manifest does
 * not refer to, and note the trees R no longer has.
 */
static void
collect_checkpoint(S3Manifest *m, S3Manifest *r, HTAB *dropped, HTAB *deleted)
{
	ListCell   *lc;
	HASH_SEQ_STATUS seq;
	S3ManifestRef *ref;
	S3ManifestTree *tree;
	S3ManifestBundles *bundles;

	/* PG files */
	foreach(lc, m->data)
	{
		char	   *path = lfirst(lc);
		char	   *key;
		unsigned int n;

		if (sscanf(path, ORIOLEDB_DATA_DIR "/small_files_%u", &n) == 1)
		{
			if (hash_search(r->bundles, &m->checkpoint, HASH_FIND, NULL))
				continue;
		}
		else
		{
			S3ManifestRef *rref = hash_search(r->files, path, HASH_FIND, NULL);

			if (rref && rref->chkp == m->checkpoint)
				continue;
		}
		key = data_key(m->token, m->checkpoint, path);
		delete_once(deleted, key);
		pfree(key);
	}
	hash_seq_init(&seq, m->files);
	while ((ref = hash_seq_search(&seq)) != NULL)
	{
		S3ManifestRef *rref = hash_search(r->files, ref->name, HASH_FIND, NULL);
		uint64		token;
		char	   *key;

		if ((rref && rref->chkp == ref->chkp) ||
			!s3_checkpoint_token_known(ref->chkp, &token))
			continue;
		key = data_key(token, ref->chkp, ref->name);
		delete_once(deleted, key);
		pfree(key);
	}
	hash_seq_init(&seq, m->bundles);
	while ((bundles = hash_seq_search(&seq)) != NULL)
	{
		uint64		token;

		if (hash_search(r->bundles, &bundles->chkp, HASH_FIND, NULL) ||
			!s3_checkpoint_token_known(bundles->chkp, &token))
			continue;
		for (int i = 0; i < bundles->count; i++)
		{
			char	   *path = psprintf(ORIOLEDB_DATA_DIR "/small_files_%d", i);
			char	   *key = data_key(token, bundles->chkp, path);

			delete_once(deleted, key);
			pfree(key);
			pfree(path);
		}
	}

	/* undo: every checkpoint uploads what it retains */
	foreach(lc, m->undoUp)
	{
		char	   *name = lfirst(lc);
		S3ManifestRef *rref = hash_search(r->undo, name, HASH_FIND, NULL);
		char	   *key;

		if (rref && rref->chkp == m->checkpoint)
			continue;
		key = psprintf("orioledb_undo/%s/" UINT64_FORMAT "-%u", name, m->token,
					   m->checkpoint);
		delete_once(deleted, key);
		pfree(key);
	}

	/*
	 * Maps.  A dropped tree's are left for after its last map is read: it
	 * says how many parts to remove.
	 */
	foreach(lc, m->mapUp)
	{
		S3ManifestTree *t = lfirst(lc);
		S3ManifestTree *rt = hash_search(r->trees, &t->key, HASH_FIND, NULL);

		if (rt == NULL)
		{
			if (t->key.datoid != SYS_TREES_DATOID)
				note_dropped(dropped, &t->key);
			continue;
		}
		if (rt->map != m->checkpoint)
			delete_map_once(deleted, t->key.datoid, t->key.relnode, m->checkpoint);
	}
	hash_seq_init(&seq, m->trees);
	while ((tree = hash_seq_search(&seq)) != NULL)
	{
		S3ManifestTree *rt = hash_search(r->trees, &tree->key, HASH_FIND, NULL);

		if (rt == NULL)
		{
			if (tree->key.datoid != SYS_TREES_DATOID)
				note_dropped(dropped, &tree->key);
			continue;
		}
		if (rt->map != tree->map)
			delete_map_once(deleted, tree->key.datoid, tree->key.relnode,
							tree->map);
	}
}

static void delete_chunks_below(TimeLineID tli, XLogRecPtr below);

/*
 * The checkpoints whose objects m refers to: a PG file, a bundle, an undo
 * file or a tree's map uploaded then.  Their keys carry the token of that
 * checkpoint's run, so its epoch stays (s3_lease_prune_epochs()).
 */
static void
prune_epochs_for(S3Manifest *m)
{
	HASH_SEQ_STATUS seq;
	S3ManifestRef *ref;
	S3ManifestBundles *b;
	S3ManifestTree *t;
	HTAB	   *refs[] = {m->files, m->small, m->undo};
	List	   *chkps = NIL;
	uint32	   *arr;
	int			n = 0;
	ListCell   *lc;

	for (int i = 0; i < lengthof(refs); i++)
	{
		hash_seq_init(&seq, refs[i]);
		while ((ref = hash_seq_search(&seq)) != NULL)
			chkps = list_append_unique_int(chkps, (int) ref->chkp);
	}
	hash_seq_init(&seq, m->bundles);
	while ((b = hash_seq_search(&seq)) != NULL)
		chkps = list_append_unique_int(chkps, (int) b->chkp);
	hash_seq_init(&seq, m->trees);
	while ((t = hash_seq_search(&seq)) != NULL)
		if (t->map != 0)
			chkps = list_append_unique_int(chkps, (int) t->map);
	arr = palloc(sizeof(uint32) * Max(1, list_length(chkps)));
	foreach(lc, chkps)
		arr[n++] = (uint32) lfirst_int(lc);
	s3_lease_prune_epochs(m->checkpoint, arr, n);
	pfree(arr);
	list_free(chkps);
}

static void
delete_wal(S3Manifest *from, S3Manifest *to)
{
	XLogSegNo	first,
				last;

	if (to->redoLsn != InvalidXLogRecPtr && to->timeline != 0)
	{
		XLogRecPtr	below;

		XLByteToSeg(to->redoLsn, last, wal_segment_size);
		XLogSegNoOffsetToRecPtr(last, 0, wal_segment_size, below);

		/*
		 * The timelines a takeover left too: the first segment of the next
		 * timeline is assembled from its parent's chunks up to the switch,
		 * so those stay while R's REDO is in that segment.
		 */
		for (TimeLineID tli = 1; tli <= to->timeline; tli++)
			delete_chunks_below(tli, below);
	}

	if (from->redoLsn == InvalidXLogRecPtr || to->redoLsn == InvalidXLogRecPtr ||
		from->timeline == 0 || to->timeline == 0)
		return;
	XLByteToSeg(from->redoLsn, first, wal_segment_size);
	XLByteToSeg(to->redoLsn, last, wal_segment_size);
	for (XLogSegNo seg = first; seg < last; seg++)
	{
		gc_check_stop();
		/* no timeline needs a segment below R's REDO */
		for (TimeLineID tli = 1; tli <= to->timeline; tli++)
		{
			char		name[MAXFNAMELEN];
			char	   *key;

			XLogFileName(name, tli, seg, wal_segment_size);
			key = psprintf("wal/%s", name);
			gc_delete(key);
			pfree(key);
			key = psprintf("wal/%s.partial", name);
			gc_delete(key);
			pfree(key);
		}
	}
}

/*
 * The chunks of a timeline that end before the segment R's REDO is in:
 * restoring R, or anything after it, starts at that segment, and a segment
 * is assembled whole, so its first chunk stays (of the parent timeline too,
 * up to the switch, when the switch is in that segment).  The shipper removes
 * chunks once their segments are archived; this is for a bucket whose
 * segments never are (archiving off, or another archive_library).  Chunks go
 * in their order, up to the first that cannot go; a terminator stays, or its
 * key would be free for the writer it fenced.  Chunks do not change, so the
 * ends read are kept for the next passes, and so is where a pass stopped: the
 * next lists from the last chunk removed, and not at all while the first
 * chunk kept still ends past `below`.
 */
typedef struct
{
	uint64		tli;
	uint64		lastRemoved;	/* 0: none yet */
	XLogRecPtr	keptEnd;		/* the first chunk kept ends here; 0: not
								 * known */
} ChunkCursor;

static void
delete_chunks_below(TimeLineID tli, XLogRecPtr below)
{
	static HTAB *ends = NULL;
	static HTAB *cursors = NULL;
	char	   *prefix = psprintf("wal/%08X/chunks/", tli);
	char	   *after = NULL;
	uint64		tliKey = tli;
	ChunkCursor *cursor;
	bool		known;
	List	   *keys;
	List	   *removed = NIL;
	ListCell   *lc;

	if (ends == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64) * 2;
		ctl.entrysize = sizeof(uint64) * 3;
		ctl.hcxt = TopMemoryContext;
		ends = hash_create("orioledb s3 gc chunk ends", 256, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(ChunkCursor);
		ctl.hcxt = TopMemoryContext;
		cursors = hash_create("orioledb s3 gc chunk cursors", 16, &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	cursor = hash_search(cursors, &tliKey, HASH_ENTER, &known);
	if (!known)
	{
		cursor->lastRemoved = 0;
		cursor->keptEnd = InvalidXLogRecPtr;
	}
	if (cursor->keptEnd != InvalidXLogRecPtr && cursor->keptEnd > below)
	{
		pfree(prefix);
		return;
	}
	if (cursor->lastRemoved != 0)
		after = psprintf("%s%020llu", prefix,
						 (unsigned long long) cursor->lastRemoved);
	keys = s3_list_objects(prefix, after);
	if (after)
		pfree(after);
	cursor->keptEnd = InvalidXLogRecPtr;

	/*
	 * The chunks before the first one listed are gone (the shipper removes
	 * archived ones too): their ends are not kept.
	 */
	{
		uint64		first = keys != NIL ?
			strtou64((char *) linitial(keys) + strlen(prefix), NULL, 10) :
			PG_UINT64_MAX;
		HASH_SEQ_STATUS seq;
		uint64	   *cached;

		hash_seq_init(&seq, ends);
		while ((cached = hash_seq_search(&seq)) != NULL)
			if (cached[0] == tli && cached[1] < first)
				(void) hash_search(ends, cached, HASH_REMOVE, NULL);
	}
	foreach(lc, keys)
	{
		char	   *key = lfirst(lc);
		uint64		entry[3] = {tli, strtou64(key + strlen(prefix), NULL, 10), 0};
		bool		found;
		uint64	   *cached;

		gc_check_stop();
		cached = hash_search(ends, entry, HASH_ENTER, &found);
		if (!found)
		{
			bool		terminator;
			XLogRecPtr	end;

			if (!s3_wal_chunk_end(tli, entry[1], &terminator, &end))
			{
				/* removed meanwhile */
				(void) hash_search(ends, entry, HASH_REMOVE, NULL);
				continue;
			}
			cached[2] = terminator ? InvalidXLogRecPtr : end;
		}
		if (cached[2] == InvalidXLogRecPtr)
			continue;
		if (cached[2] > below)
		{
			cursor->keptEnd = cached[2];
			break;
		}
		gc_delete(key);
		removed = lappend(removed, key);
	}

	/*
	 * The cursor moves past the chunks that went; one the store refused is
	 * listed again next pass, and so is everything after it.
	 */
	gc_flush();
	foreach(lc, removed)
	{
		char	   *key = lfirst(lc);
		uint64		entry[3] = {tli, strtou64(key + strlen(prefix), NULL, 10), 0};

		if (gc_failed(key))
		{
			cursor->keptEnd = InvalidXLogRecPtr;
			break;
		}
		cursor->lastRemoved = entry[1];
		(void) hash_search(ends, entry, HASH_REMOVE, NULL);
	}
	list_free(removed);
	list_free_deep(keys);
	pfree(prefix);
}

/*
 * The trees a checkpoint's manifest names, kept across passes: a manifest
 * does not change.  One the bucket does not have is asked for again.
 */
typedef struct
{
	uint32		chkp;
	int			count;
	S3ManifestTreeKey *keys;	/* in TopMemoryContext */
} ManifestTrees;

static HTAB *manifestTrees = NULL;

static ManifestTrees *
manifest_trees(uint32 chkp)
{
	ManifestTrees *entry;
	bool		found;
	S3Manifest *m;
	HASH_SEQ_STATUS seq;
	S3ManifestTree *t;
	int			n = 0;

	if (manifestTrees == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(ManifestTrees);
		ctl.hcxt = TopMemoryContext;
		manifestTrees = hash_create("orioledb s3 gc manifest trees", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	entry = hash_search(manifestTrees, &chkp, HASH_FIND, NULL);
	if (entry != NULL)
		return entry;
	m = gc_manifest_fetch(chkp);
	if (m == NULL)
		return NULL;
	entry = hash_search(manifestTrees, &chkp, HASH_ENTER, &found);
	entry->count = hash_get_num_entries(m->trees);
	entry->keys = MemoryContextAlloc(TopMemoryContext,
									 sizeof(S3ManifestTreeKey) * Max(1, entry->count));
	hash_seq_init(&seq, m->trees);
	while ((t = hash_seq_search(&seq)) != NULL)
		entry->keys[n++] = t->key;
	s3_manifest_free(m);
	return entry;
}

/* the manifests below `collected` are gone: so are their entries */
static void
forget_manifest_trees_below(uint32 collected)
{
	HASH_SEQ_STATUS seq;
	ManifestTrees *entry;

	if (manifestTrees == NULL)
		return;
	hash_seq_init(&seq, manifestTrees);
	while ((entry = hash_seq_search(&seq)) != NULL)
		if (entry->chkp < collected)
		{
			pfree(entry->keys);
			(void) hash_search(manifestTrees, &entry->chkp, HASH_REMOVE, NULL);
		}
}

/*
 * Whether a checkpoint from `from` through `committed` names the tree: a tree
 * that has the relnode again (OIDs wrapped around) has the dropped one's
 * keys, and is left, with whatever the dropped one left under them.
 */
static bool
tree_named(S3ManifestTreeKey *key, uint32 from, uint32 committed)
{
	for (uint32 k = Max(from, 1); k <= committed; k++)
	{
		ManifestTrees *mt = manifest_trees(k);

		if (mt == NULL)
			continue;
		for (int i = 0; i < mt->count; i++)
			if (mt->keys[i].datoid == key->datoid &&
				mt->keys[i].relnode == key->relnode)
				return true;
	}
	return false;
}

/*
 * The local lists of dead extents of a tree that is gone: those of the
 * checkpoints after the drop were never named by a manifest and never
 * folded.  Left, a tree that has the relnode again would fold them as its
 * own, marking its blocks dead.  Under the default tablespace, as the GC
 * state file is.
 */
static void
unlink_local_dead_lists(Oid datoid, Oid relnode)
{
	char	   *dir = psprintf(ORIOLEDB_DATA_DIR "/%u", datoid);
	char		prefix[32];
	size_t		plen = snprintf(prefix, sizeof(prefix), "%u-", relnode);
	DIR		   *d = AllocateDir(dir);
	struct dirent *de;

	while (d != NULL && (de = ReadDirExtended(d, dir, LOG)) != NULL)
	{
		size_t		len = strlen(de->d_name);

		if (len > plen + 5 && strncmp(de->d_name, prefix, plen) == 0 &&
			strcmp(de->d_name + len - 5, ".dead") == 0)
		{
			char	   *path = psprintf("%s/%s", dir, de->d_name);

			(void) unlink(path);
			pfree(path);
		}
	}
	if (d != NULL)
		FreeDir(d);
	pfree(dir);
}

/*
 * Remove a dropped tree: every object under its prefix (its parts, maps and
 * GC state, those of a run that crashed before naming them too), and its
 * local GC files and dead lists.
 */
static int
collect_dropped_tree(S3ManifestTreeKey *key)
{
	char	   *prefix = psprintf("trees/%u/%u/", key->datoid, key->relnode);
	List	   *keys = s3_list_objects(prefix, NULL);
	ListCell   *lc;
	int			removed = 0;

	foreach(lc, keys)
	{
		gc_check_stop();
		gc_delete((char *) lfirst(lc));
		if (strstr((char *) lfirst(lc), "/parts/") != NULL)
			removed++;
	}
	list_free_deep(keys);
	pfree(prefix);
	state_drop(key->datoid, key->relnode);
	s3_tree_lists_unlink(key->datoid, key->relnode);
	unlink_local_dead_lists(key->datoid, key->relnode);
	return removed;
}

/*
 * The trees dropped since the last pass: each goes whole once a checkpoint
 * that began after the drop is committed and no checkpoint from R on names
 * the tree.  One named there is a tree the manifests knew, which goes when
 * the manifests stop naming it (collect_dropped_tree()).  The list is taken
 * aside first, so drops noted meanwhile go to a new one; what cannot go yet
 * is noted again.
 */
static int
collect_abandoned_trees(uint32 r, uint32 committed)
{
	char	   *taken = psprintf("%s.gc", S3_DROPPED_TREES_FILE);
	int			fd;
	struct stat st;
	char	   *data;
	int			removed = 0;

	if (stat(taken, &st) != 0)
	{
		int			lock = s3_local_lists_lock();
		bool		renamed = rename(S3_DROPPED_TREES_FILE, taken) == 0;

		s3_local_lists_unlock(lock);
		if (!renamed)
		{
			pfree(taken);
			return 0;
		}
	}
	fd = BasicOpenFile(taken, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0)
	{
		if (fd >= 0)
			close(fd);
		pfree(taken);
		return 0;
	}
	data = palloc(st.st_size + 1);
	if (read(fd, data, st.st_size) != st.st_size)
	{
		close(fd);
		pfree(data);
		pfree(taken);
		return 0;
	}
	close(fd);

	for (off_t pos = 0; pos + (off_t) sizeof(S3DroppedTree) <= st.st_size;
		 pos += sizeof(S3DroppedTree))
	{
		S3DroppedTree rec;
		S3ManifestTreeKey key;

		memcpy(&rec, data + pos, sizeof(rec));
		gc_check_stop();
		if (committed <= rec.chkp)
		{
			/* the checkpoint going at the drop may still name it */
			int			lock = s3_local_lists_lock();

			fd = BasicOpenFile(S3_DROPPED_TREES_FILE,
							   O_WRONLY | O_CREAT | O_APPEND | PG_BINARY);
			if (fd < 0 || write(fd, &rec, sizeof(rec)) != sizeof(rec) ||
				pg_fsync(fd) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m",
								S3_DROPPED_TREES_FILE)));
			close(fd);
			s3_local_lists_unlock(lock);
			continue;
		}
		memset(&key, 0, sizeof(key));
		key.datoid = rec.datoid;
		key.relnode = rec.relnode;
		if (tree_named(&key, r, committed))
			continue;
		removed += collect_dropped_tree(&key);
	}
	gc_flush();
	(void) unlink(taken);
	pfree(data);
	pfree(taken);
	return removed;
}

/*
 * The oldest checkpoint from `retain` back that was published within
 * orioledb.s3_gc_keep_time.  A manifest does not change, so the times read
 * are kept for the next passes.  One without a time (published before
 * manifests had it) is taken for an old one.
 */
static uint32
retain_by_time(uint32 retain, uint32 collected)
{
	static HTAB *times = NULL;
	int64		now = (int64) time(NULL);

	if (times == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(uint32) + sizeof(int64);
		ctl.hcxt = TopMemoryContext;
		times = hash_create("orioledb s3 gc manifest times", 64, &ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	else
	{
		/* the manifests below collected are gone: so are their times */
		HASH_SEQ_STATUS seq;
		uint32	   *k;

		hash_seq_init(&seq, times);
		while ((k = hash_seq_search(&seq)) != NULL)
			if (*k < collected)
				(void) hash_search(times, k, HASH_REMOVE, NULL);
	}
	/* the checkpoints before collected are gone already */
	while (retain > Max(collected, 1))
	{
		uint32		k = retain - 1;
		bool		found;
		char	   *entry = hash_search(times, &k, HASH_ENTER, &found);
		int64		t;

		if (!found)
		{
			S3Manifest *m = gc_manifest_fetch(k);

			t = m != NULL ? m->time : -1;
			if (m != NULL)
				s3_manifest_free(m);
			memcpy(entry + sizeof(uint32), &t, sizeof(t));
		}
		memcpy(&t, entry + sizeof(uint32), sizeof(t));
		if (t == -1)
		{
			/* no manifest: a checkpoint never committed, look past it */
			retain = k;
			continue;
		}
		if (t == 0 || now - t >= s3_gc_keep_time)
			break;
		retain = k;
	}
	return retain;
}

/*
 * The trees the GC skips (their state or a list could not be read): their
 * lists stay in the bucket for whoever repairs the state, and the `gc`
 * object names them, so --sweep leaves those lists too.  The set is the
 * union of what the object named and what this pass skipped: a tree skipped
 * once keeps lists no later pass folds again.
 */
static HTAB *skippedTrees = NULL;

static void
note_skipped(Oid datoid, Oid relnode)
{
	S3ManifestTreeKey key = {datoid, relnode};

	if (skippedTrees == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(S3ManifestTreeKey);
		ctl.entrysize = sizeof(S3ManifestTreeKey);
		ctl.hcxt = TopMemoryContext;
		skippedTrees = hash_create("orioledb s3 gc skipped trees", 16, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	(void) hash_search(skippedTrees, &key, HASH_ENTER, NULL);
}

static void
note_skipped_lines(const char *lines)
{
	const char *line = lines;

	while (line && *line)
	{
		unsigned int a,
					b;

		if (sscanf(line, "skipped %u %u", &a, &b) == 2)
			note_skipped(a, b);
		line = strchr(line, '\n');
		if (line)
			line++;
	}
}

static char *
skipped_lines(void)
{
	StringInfoData out;
	HASH_SEQ_STATUS seq;
	S3ManifestTreeKey *key;

	initStringInfo(&out);
	if (skippedTrees != NULL)
	{
		hash_seq_init(&seq, skippedTrees);
		while ((key = hash_seq_search(&seq)) != NULL)
			appendStringInfo(&out, "skipped %u %u\n", key->datoid, key->relnode);
	}
	return out.data;
}

/*
 * One pass.  Returns the number of parts removed.
 */
int
s3_gc_cycle(void)
{
	uint32		committed = s3_lease_root_checkpoint();
	uint32		pinFloor = 0,
				pinWarn = 0;
	uint32		published,
				folded,
				collected,
				retain,
				r;
	HTAB	   *trees;
	HASHCTL		ctl;
	List	   *lists = NIL;
	ListCell   *lc;
	HASH_SEQ_STATUS seq;
	GcTree	   *tree;
	StringInfoData skippedBefore;
	int			removed = 0;

	/* what a pass left is in the pass's context, reset with it */
	gcFailed = NULL;
	gc_parts = NIL;
	gc_pending = NIL;
	initStringInfo(&skippedBefore);
	if (committed < (uint32) s3_gc_keep_checkpoints ||
		!s3_gc_progress(&published, &folded, &collected, &skippedBefore))
		return 0;
	note_skipped_lines(skippedBefore.data);
	pfree(skippedBefore.data);

	/*
	 * The retention point: the committed checkpoint minus what is kept,
	 * published before the pins are read, and no later than any of them.
	 */
	retain = committed - s3_gc_keep_checkpoints + 1;
	if (s3_gc_keep_time > 0)
		retain = retain_by_time(retain, collected);
	/* a setting raised keeps more from now on; what is gone stays gone */
	retain = Max(retain, Max(folded, collected));
	if (s3_gc_max_pin_lag > 0 && committed > (uint32) s3_gc_max_pin_lag)
	{
		pinFloor = committed - s3_gc_max_pin_lag;
		pinWarn = pinFloor + Max(1, s3_gc_max_pin_lag / 4);
	}
	s3_gc_publish(retain, folded, collected, pinFloor, pinWarn, skipped_lines());
	/*
	 * A pin below what is folded or collected already holds nothing: its
	 * reader stops at its next renewal.  Counted, a stopped reader's pin
	 * would hold the GC for the pin's lifetime.  One below the pin floor is
	 * passed over only once its reader has had a renewal in which to see
	 * pin_warn (s3_reader_pins_min()).
	 */
	r = Min(retain, s3_reader_pins_min(retain, Max(folded, collected), pinFloor));
	{
		uint32		scans = oldest_scan_checkpoint();

		if (scans != 0)
			r = Min(r, scans);
	}
	if (r == 0)
		return 0;

	/* fold the dead extents of the checkpoints up to R */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(S3ManifestTreeKey);
	ctl.entrysize = sizeof(GcTree);
	ctl.hcxt = CurrentMemoryContext;
	trees = hash_create("orioledb s3 gc trees", 64, &ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	for (uint32 k = folded + 1; k <= r; k++)
	{
		S3Manifest *m;

		gc_check_stop();
		m = gc_manifest_fetch(k);

		if (m == NULL)
		{
			lists = fold_unpublished(trees, k, committed, lists);
			continue;
		}
		lists = fold_checkpoint(trees, m, lists);
		s3_manifest_free(m);
	}
	hash_seq_init(&seq, trees);
	while ((tree = hash_seq_search(&seq)) != NULL)
	{
		if (tree->skip)
			note_skipped(tree->key.datoid, tree->key.relnode);
		collect_tree(tree);
	}
	gc_flush();
	removed += forget_removed_parts();
	hash_seq_init(&seq, trees);
	while ((tree = hash_seq_search(&seq)) != NULL)
		if (tree->changed)
			state_save(tree);
	/*
	 * The states have them now.  A skipped tree's state does not: its lists
	 * stay, for whoever repairs the state to fold again.
	 */
	foreach(lc, lists)
	{
		FoldedList *f = lfirst(lc);

		if (f->tree->skip)
			continue;
		gc_delete(f->object);
		if (f->local)
			(void) unlink(f->local);
	}
	gc_flush();
	if (r > folded)
	{
		folded = r;
		s3_gc_publish(retain, folded, collected, pinFloor, pinWarn,
					  skipped_lines());
	}

	/* remove what only the checkpoints before R referred to */
	if (collected < r)
	{
		S3Manifest *rm = NULL;
		uint32		rk;

		/*
		 * R's manifest has to know every reference of its checkpoint, or an
		 * object R reads would look unreferenced; an older complete one does
		 * as well, as references only move forward.
		 */
		for (rk = r; rk > collected && rm == NULL; rk--)
		{
			rm = gc_manifest_fetch(rk);
			if (rm != NULL && !rm->complete)
			{
				s3_manifest_free(rm);
				rm = NULL;
			}
		}
		if (rm != NULL && rm->checkpoint > Max(collected, 1))
		{
			HTAB	   *dropped,
					   *deleted;
			S3Manifest *first = NULL;
			DroppedTree *d;

			rk = rm->checkpoint;
			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(S3ManifestTreeKey);
			ctl.entrysize = sizeof(DroppedTree);
			ctl.hcxt = CurrentMemoryContext;
			dropped = hash_create("orioledb s3 gc dropped", 16, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(DeletedKey);
			ctl.entrysize = sizeof(DeletedKey);
			ctl.hcxt = CurrentMemoryContext;
			deleted = hash_create("orioledb s3 gc deleted", 256, &ctl,
								  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

			for (uint32 k = Max(collected, 1); k < rk; k++)
			{
				S3Manifest *m;

				gc_check_stop();
				m = gc_manifest_fetch(k);
				if (m == NULL)
					continue;
				collect_checkpoint(m, rm, dropped, deleted);
				if (first == NULL)
					first = m;
				else
					s3_manifest_free(m);
			}

			hash_seq_init(&seq, dropped);
			while ((d = hash_seq_search(&seq)) != NULL)
			{
				if (tree_named(&d->key, rk + 1, committed))
				{
					/*
					 * A new tree under the dropped one's keys.  The dropped
					 * tree's dead bits would OR into the new tree's parts,
					 * and a live part would go: the state starts over.  Its
					 * lists up to r are folded already, so nothing of the
					 * dropped tree comes back; a list of the new tree folded
					 * this pass (r past R') is lost with it, a leak only.
					 */
					ereport(LOG,
							(errmsg("S3 GC leaves dropped tree %u/%u: a tree has its relnode again, its GC state starts over",
									d->key.datoid, d->key.relnode)));
					state_drop(d->key.datoid, d->key.relnode);
					continue;
				}
				removed += collect_dropped_tree(&d->key);
			}

			if (first != NULL)
			{
				delete_wal(first, rm);
				s3_manifest_free(first);
			}
			/* what the manifests name goes before the manifests */
			gc_flush();

			for (uint32 k = Max(collected, 1); k < rk; k++)
			{
				uint64		token;
				uint64	   *tokens;
				int			ntokens;

				if (s3_checkpoint_token_known(k, &token))
				{
					char	   *key = s3_manifest_key(k, token);

					gc_delete(key);
					pfree(key);
				}

				/*
				 * The copies of root under k: its commit's, under the token
				 * of k, and a start's at the same number, under the token of
				 * a run that started at k -- the tokens a map of k may be
				 * under.  A copy from before the token was in the key goes
				 * too.
				 */
				ntokens = s3_map_tokens(k, &tokens);
				for (int i = 0; i < ntokens; i++)
				{
					char	   *key = psprintf(ROOT_COPY_FORMAT, tokens[i], k);

					gc_delete(key);
					pfree(key);
				}
				pfree(tokens);
				{
					char	   *key = psprintf("roots/%u", k);

					gc_delete(key);
					pfree(key);
				}
			}
			gc_flush();
			collected = rk;
			prune_epochs_for(rm);
			s3_gc_publish(retain, folded, collected, pinFloor, pinWarn,
						  skipped_lines());
			forget_manifest_trees_below(collected);
			hash_destroy(dropped);
			hash_destroy(deleted);
		}
		s3_manifest_free(rm);
	}

	removed += collect_abandoned_trees(r, committed);
	gc_flush();
	if (removed > 0)
		ereport(LOG,
				(errmsg("S3 GC removed %d parts dead through checkpoint %u", removed, r)));
	return removed;
}

/*
 * The parts of a tree to compact: complete, not all dead, and with fewer
 * live blocks than threshold of a part.  Sorted; *count gets their number.
 */
uint64 *
s3_gc_sparse_parts(Oid datoid, Oid relnode, double threshold, int *count)
{
	GcTree		tree;
	S3ManifestTree t;
	HASHCTL		ctl;
	HTAB	   *trees;
	HASH_SEQ_STATUS seq;
	GcPart	   *p;
	uint64	   *result;
	int			n = 0;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(S3ManifestTreeKey);
	ctl.entrysize = sizeof(GcTree);
	ctl.hcxt = CurrentMemoryContext;
	trees = hash_create("orioledb s3 compact trees", 4, &ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	memset(&t, 0, sizeof(t));
	t.key.datoid = datoid;
	t.key.relnode = relnode;
	tree = *get_tree(trees, &t);
	result = palloc(sizeof(uint64) * Max(1, hash_get_num_entries(tree.parts)));
	if (!tree.skip && tree.blocksPerPart > 0)
	{
		hash_seq_init(&seq, tree.parts);
		while ((p = hash_seq_search(&seq)) != NULL)
		{
			uint64		start = p->part * tree.blocksPerPart;
			uint64		live = 0;

			if (p->end == 0 || p->end <= start || p->end == PART_GONE)
				continue;
			for (uint64 i = 0; i < p->end - start; i++)
				if (!block_dead(p, i))
					live++;
			if (live > 0 && live < threshold * tree.blocksPerPart)
				result[n++] = p->part;
		}
	}
	qsort(result, n, sizeof(uint64), pg_cmp_u64_wrapper);
	*count = n;
	return result;
}

void
register_s3_gc_worker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3_gc_worker_main");
	strcpy(worker.bgw_name, "orioledb s3 gc");
	strcpy(worker.bgw_type, "orioledb s3 gc");
	RegisterBackgroundWorker(&worker);
}

void
s3_gc_worker_main(Datum main_arg)
{
	MemoryContext cxt;

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	cxt = AllocSetContextCreate(TopMemoryContext, "orioledb s3 gc",
								ALLOCSET_DEFAULT_SIZES);

	while (!ShutdownRequestPending)
	{
		MemoryContext old;

		s3_take_config_reload();
		old = MemoryContextSwitchTo(cxt);
		(void) s3_gc_cycle();
		MemoryContextSwitchTo(old);
		MemoryContextReset(cxt);

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) s3_gc_interval * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	proc_exit(0);
}
