/*-------------------------------------------------------------------------
 *
 * gc.c
 *		Remove what no retained checkpoint of the S3 bucket refers to.
 *
 * R is the oldest retained checkpoint: the committed one minus
 * orioledb.s3_gc_keep_checkpoints, and no later than the checkpoint any
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
 * go.  The WAL segments before R's REDO go too.
 *
 * Progress is published in `gc` (folded, collected), so a writer that takes
 * the bucket over goes on where the last one stopped; a tree's state is the
 * union of this data directory's copy and the bucket's.
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

#define GC_STATE_MAGIC		0x33434753	/* "SGC3" */
#define MAX_PART_BLOCKS		2048

int			s3_gc_interval = 60;
int			s3_gc_keep_checkpoints = 2;
int			s3_gc_max_pin_lag = 1000;

typedef struct
{
	uint64		part;
	uint64		end;			/* 0: the part is not complete yet */
	uint8		dead[MAX_PART_BLOCKS / 8];
} GcPart;

typedef struct
{
	S3ManifestTreeKey key;
	Oid			spcoid;
	uint64		blocksPerPart;	/* 0: not known yet */
	bool		skip;			/* a list could not be read: collect nothing */
	bool		changed;
	HTAB	   *parts;
} GcTree;

static int
pg_cmp_u64_wrapper(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a,
				y = *(const uint64 *) b;

	return x < y ? -1 : (x > y ? 1 : 0);
}

/*
 * Deletions are queued and sent many at a time (s3_delete_objects()).  A
 * caller flushes before it records that what it deleted is gone: a tree's
 * state without its parts, a published progress, a manifest.
 */
#define GC_DELETE_BATCH		1000

static List *gc_pending = NIL;

static void
gc_flush(void)
{
	int			n = list_length(gc_pending);
	char	  **keys;
	ListCell   *lc;
	int			i = 0;

	if (n == 0)
		return;
	keys = palloc(sizeof(char *) * n);
	foreach(lc, gc_pending)
		keys[i++] = lfirst(lc);
	s3_delete_objects(keys, n);
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
		memset(entry->dead, 0, sizeof(entry->dead));
	}
	return entry;
}

/*
 * A state's serialized form: magic, blocks per part, count, the parts.  It is
 * merged into what the tree has.
 */
static bool
state_decode(GcTree *tree, const char *data, uint64 size)
{
	uint32		magic;
	uint64		count,
				bpp;
	const char *p = data;

	if (size < sizeof(uint32) + 2 * sizeof(uint64))
		return false;
	memcpy(&magic, p, sizeof(magic));
	p += sizeof(magic);
	if (magic != GC_STATE_MAGIC)
		return false;
	memcpy(&bpp, p, sizeof(uint64));
	p += sizeof(uint64);
	memcpy(&count, p, sizeof(count));
	p += sizeof(count);
	if (size != sizeof(uint32) + 2 * sizeof(uint64) + count * sizeof(GcPart))
		return false;
	if (bpp != 0)
	{
		if (tree->blocksPerPart != 0 && tree->blocksPerPart != bpp)
			return false;
		tree->blocksPerPart = bpp;
	}
	for (uint64 i = 0; i < count; i++)
	{
		GcPart		rec;
		GcPart	   *entry;

		memcpy(&rec, p, sizeof(rec));
		p += sizeof(rec);
		entry = tree_part(tree, rec.part);
		entry->end = Max(entry->end, rec.end);
		for (int b = 0; b < MAX_PART_BLOCKS / 8; b++)
			entry->dead[b] |= rec.dead[b];
	}
	return true;
}

static void
state_encode(GcTree *tree, StringInfo out)
{
	uint32		magic = GC_STATE_MAGIC;
	uint64		count = hash_get_num_entries(tree->parts);
	HASH_SEQ_STATUS seq;
	GcPart	   *entry;

	appendBinaryStringInfo(out, (char *) &magic, sizeof(magic));
	appendBinaryStringInfo(out, (char *) &tree->blocksPerPart, sizeof(uint64));
	appendBinaryStringInfo(out, (char *) &count, sizeof(count));
	hash_seq_init(&seq, tree->parts);
	while ((entry = hash_seq_search(&seq)) != NULL)
		appendBinaryStringInfo(out, (char *) entry, sizeof(GcPart));
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

static void
state_save(GcTree *tree)
{
	char	   *filename = state_filename(tree->key.datoid, tree->key.relnode);
	char	   *tmp = psprintf("%s.tmp", filename);
	char	   *dir = psprintf(ORIOLEDB_DATA_DIR "/%u", tree->key.datoid);
	char	   *key = state_key(tree->key.datoid, tree->key.relnode);
	StringInfoData body;
	int			fd;

	initStringInfo(&body);
	state_encode(tree, &body);
	(void) MakePGDirectory(dir);
	fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0 || write(fd, body.data, body.len) != body.len ||
		pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, filename, ERROR);
	(void) s3_put_object_with_contents(key, body.data, body.len, NULL, false);
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
			if (extent.off == 0 || extent.off > MAX_PART_BLOCKS ||
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

			p->dead[i / 8] |= (uint8) (1 << (i % 8));
		}
	}
}

static bool
part_all_dead(GcTree *tree, GcPart *p)
{
	uint64		start = p->part * tree->blocksPerPart;

	if (p->end == 0 || p->end <= start)
		return false;
	for (uint64 i = 0; i < p->end - start; i++)
		if (!(p->dead[i / 8] & (1 << (i % 8))))
			return false;
	return true;
}

static int
collect_tree(GcTree *tree)
{
	HASH_SEQ_STATUS seq;
	GcPart	   *entry;
	List	   *done = NIL;
	ListCell   *lc;
	int			removed = 0;

	if (tree->skip)
		return 0;
	hash_seq_init(&seq, tree->parts);
	while ((entry = hash_seq_search(&seq)) != NULL)
		if (part_all_dead(tree, entry))
			done = lappend(done, entry);

	foreach(lc, done)
	{
		GcPart	   *p = (GcPart *) lfirst(lc);
		char	   *key = part_key(tree->key.datoid, tree->key.relnode, p->part);
		uint64		part = p->part;

		gc_check_stop();

		gc_delete(key);
		pfree(key);
		(void) hash_search(tree->parts, &part, HASH_REMOVE, NULL);
		removed++;
	}
	list_free(done);
	if (removed > 0)
		tree->changed = true;
	return removed;
}

typedef struct
{
	char	   *object;			/* the list in the bucket */
	char	   *local;			/* its local copy, or NULL */
	GcTree	   *tree;
} FoldedList;

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
 * A tree's map of checkpoint chkp is under the token of the run that did
 * chkp, or of the run after it (a build names its map with the last
 * checkpoint done).
 */
static int
map_tokens(uint32 chkp, uint64 *tokens)
{
	int			n = 0;

	if (s3_checkpoint_token_known(chkp + 1, &tokens[n]))
		n++;
	if (s3_checkpoint_token_known(chkp, &tokens[n]) &&
		(n == 0 || tokens[n] != tokens[0]))
		n++;
	return n;
}

static void
delete_map(Oid datoid, Oid relnode, uint32 chkp)
{
	uint64		tokens[2];
	int			n;

	if (chkp == 0)
		return;
	n = map_tokens(chkp, tokens);

	for (int i = 0; i < n; i++)
	{
		char	   *key = psprintf("trees/%u/%u/maps/" UINT64_FORMAT "-%u",
								   datoid, relnode, tokens[i], chkp);

		gc_delete(key);
		pfree(key);
	}
}

/*
 * The number of parts a tree has handed out, from its map of checkpoint
 * chkp: the parts of a dropped tree are 0 up to it.
 */
static bool
tree_next_part(Oid datoid, Oid relnode, uint32 chkp, uint64 *nextPart)
{
	uint64		tokens[2];
	int			n = map_tokens(chkp, tokens);

	for (int i = 0; i < n; i++)
	{
		char	   *key = psprintf("trees/%u/%u/maps/" UINT64_FORMAT "-%u",
								   datoid, relnode, tokens[i], chkp);
		StringInfoData body;
		bool		found = false;

		initStringInfo(&body);
		if (s3_get_object(key, &body, true) == S3_RESPONSE_OK &&
			body.len >= sizeof(CheckpointFileHeader))
		{
			CheckpointFileHeader header;

			memcpy(&header, body.data, sizeof(header));
			*nextPart = header.datafileLength;
			found = true;
		}
		pfree(body.data);
		pfree(key);
		if (found)
			return true;
	}
	return false;
}

typedef struct
{
	S3ManifestTreeKey key;
	uint32		lastMap;
	List	   *maps;			/* every map number seen */
} DroppedTree;

typedef struct
{
	char		key[MAXPGPATH + 64];
} DeletedKey;

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
note_dropped(HTAB *dropped, S3ManifestTreeKey *key, uint32 map)
{
	bool		found;
	DroppedTree *d = hash_search(dropped, key, HASH_ENTER, &found);

	if (!found)
	{
		d->lastMap = map;
		d->maps = NIL;
	}
	d->lastMap = Max(d->lastMap, map);
	if (!list_member_int(d->maps, (int) map))
		d->maps = lappend_int(d->maps, (int) map);
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
	 * The chunks of a timeline a takeover left: no retained checkpoint is
	 * before the switch any more.
	 */
	foreach(lc, m->retired)
	{
		S3ManifestRetired *t = lfirst(lc);

		for (uint64 seq = 0; seq <= t->lastSeq; seq++)
		{
			char	   *key = psprintf("wal/%08X/chunks/%020llu", t->timeline,
									   (unsigned long long) seq);

			gc_check_stop();
			gc_delete(key);
			pfree(key);
		}
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
				note_dropped(dropped, &t->key, m->checkpoint);
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
				note_dropped(dropped, &tree->key, tree->map);
			continue;
		}
		if (rt->map != tree->map)
			delete_map_once(deleted, tree->key.datoid, tree->key.relnode,
							tree->map);
	}
}

static void
delete_wal(S3Manifest *from, S3Manifest *to)
{
	XLogSegNo	first,
				last;

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
		}
	}
}

/*
 * Remove a dropped tree: its parts (as many as its last map handed out), its
 * maps and its GC state.
 */
static int
collect_dropped_tree(DroppedTree *d)
{
	uint64		nextPart = 0;
	int			removed = 0;
	ListCell   *lc;
	char	   *bend;

	if (tree_next_part(d->key.datoid, d->key.relnode, d->lastMap, &nextPart))
	{
		for (uint64 p = 0; p < nextPart; p++)
		{
			char	   *key = part_key(d->key.datoid, d->key.relnode, p);

			gc_check_stop();

			gc_delete(key);
			pfree(key);
			removed++;
		}
	}
	else
		ereport(LOG,
				(errmsg("S3 GC could not read the last map of dropped tree %u/%u",
						d->key.datoid, d->key.relnode)));
	foreach(lc, d->maps)
		delete_map(d->key.datoid, d->key.relnode, (uint32) lfirst_int(lc));
	state_drop(d->key.datoid, d->key.relnode);
	bend = s3_build_dead_filename(d->key.datoid, d->key.relnode);
	(void) unlink(bend);
	pfree(bend);
	return removed;
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
	int			removed = 0;

	if (committed < (uint32) s3_gc_keep_checkpoints ||
		!s3_gc_progress(&published, &folded, &collected))
		return 0;

	/*
	 * The retention point: the committed checkpoint minus what is kept,
	 * published before the pins are read, and no later than any of them.
	 */
	retain = committed - s3_gc_keep_checkpoints + 1;
	if (s3_gc_max_pin_lag > 0 && committed > (uint32) s3_gc_max_pin_lag)
	{
		pinFloor = committed - s3_gc_max_pin_lag;
		pinWarn = pinFloor + Max(1, s3_gc_max_pin_lag / 4);
	}
	s3_gc_publish(retain, folded, collected, pinFloor, pinWarn);
	r = Min(retain, s3_reader_pins_min(retain, pinFloor));
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
		m = s3_manifest_fetch(k);

		if (m == NULL)
			continue;
		lists = fold_checkpoint(trees, m, lists);
		s3_manifest_free(m);
	}
	hash_seq_init(&seq, trees);
	while ((tree = hash_seq_search(&seq)) != NULL)
		removed += collect_tree(tree);
	gc_flush();
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
		s3_gc_publish(retain, folded, collected, pinFloor, pinWarn);
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
			rm = s3_manifest_fetch(rk);
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
				m = s3_manifest_fetch(k);
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
				removed += collect_dropped_tree(d);

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

				if (s3_checkpoint_token_known(k, &token))
				{
					char	   *key = s3_manifest_key(k, token);

					gc_delete(key);
					pfree(key);
				}
			}
			gc_flush();
			collected = rk;
			s3_gc_publish(retain, folded, collected, pinFloor, pinWarn);
			hash_destroy(dropped);
			hash_destroy(deleted);
		}
		s3_manifest_free(rm);
	}

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

			if (p->end == 0 || p->end <= start)
				continue;
			for (uint64 i = 0; i < p->end - start; i++)
				if (!(p->dead[i / 8] & (1 << (i % 8))))
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
	BackgroundWorkerUnblockSignals();

	cxt = AllocSetContextCreate(TopMemoryContext, "orioledb s3 gc",
								ALLOCSET_DEFAULT_SIZES);

	while (!ShutdownRequestPending)
	{
		MemoryContext old = MemoryContextSwitchTo(cxt);

		(void) s3_gc_cycle();
		MemoryContextSwitchTo(old);
		MemoryContextReset(cxt);

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) s3_gc_interval * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	proc_exit(0);
}
