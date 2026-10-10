/*-------------------------------------------------------------------------
 *
 * manifest.c
 *		The manifest of each S3 checkpoint.
 *
 * Checkpoint K writes manifests/<T>-<K>: what it uploaded, and every object
 * restoring K reads -- the map of each tree of the catalog, the PG files (with
 * the checkpoint that last uploaded each), the small file bundles, the undo.
 * A checkpoint's references only move forward: a file's latest upload, a
 * tree's map, an undo file's version are never older at a later checkpoint.
 * So an object an older checkpoint uploaded that the oldest retained
 * checkpoint R does not refer to is not referred to from R on, and the GC
 * removes it (gc.c).
 *
 * The checkpointer collects the uploads while it checkpoints, writes the
 * manifest at the end of the PG file backup into a local pending file and the
 * bucket, and the after-hook adds the WAL position and uploads it again just
 * before it commits root.  The committed manifest's local copy carries the
 * small file bundle counts forward.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/manifest.c
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
#include "checkpoint/control.h"
#include "s3/lease.h"
#include "s3/manifest.h"
#include "s3/requests.h"
#include "transam/undo.h"

#include "storage/fd.h"
#include "utils/memutils.h"

#define MANIFEST_PENDING		ORIOLEDB_DATA_DIR "/s3_manifest.pending"
#define MANIFEST_COMMITTED		ORIOLEDB_DATA_DIR "/s3_manifest"
#define FILE_CHECKSUMS			ORIOLEDB_DATA_DIR "/file_checksums"
#define SMALL_FILE_CHECKSUMS	ORIOLEDB_DATA_DIR "/small_file_checksums"
#define RETIRED_FILENAME		ORIOLEDB_DATA_DIR "/s3_retired"

static StringInfo collected = NULL;
static StringInfo catalogTrees = NULL;
static uint32 collectChkp = 0;

void
s3_manifest_begin(uint32 chkpNum)
{
	if (collected == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		collected = makeStringInfo();
		catalogTrees = makeStringInfo();
		MemoryContextSwitchTo(old);
	}
	resetStringInfo(collected);
	resetStringInfo(catalogTrees);
	collectChkp = chkpNum;
}

/*
 * Record an upload of checkpoint chkpNum: a line of the manifest.  Only the
 * checkpointer collects; the uploads of other processes (a tree build's map)
 * are not the checkpoint's.
 */
void
s3_manifest_note(uint32 chkpNum, const char *fmt,...)
{
	va_list		args;

	if (collected == NULL || chkpNum != collectChkp)
		return;
	for (;;)
	{
		int			needed;

		va_start(args, fmt);
		needed = appendStringInfoVA(collected, fmt, args);
		va_end(args);
		if (needed == 0)
			break;
		enlargeStringInfo(collected, needed);
	}
	appendStringInfoChar(collected, '\n');
}

void
s3_manifest_note_tree(Oid datoid, Oid relnode, Oid spcoid)
{
	if (catalogTrees != NULL)
		appendStringInfo(catalogTrees, "%u %u %u\n", datoid, relnode, spcoid);
}

char *
s3_manifest_key(uint32 chkpNum, uint64 token)
{
	return psprintf("manifests/" UINT64_FORMAT "-%u", token, chkpNum);
}

char *
s3_build_dead_filename(Oid datoid, Oid relnode)
{
	return psprintf(ORIOLEDB_DATA_DIR "/%u/%u.bend", datoid, relnode);
}

/*
 * A tree build is done: the ends of the parts it filled, for the GC.  The
 * build's meta page is private, so the checkpoint lists have none of them;
 * the next checkpoint that sees the tree in the catalog uploads this list
 * with its manifest.
 */
void
s3_build_part_end(Oid datoid, Oid relnode, Oid spcoid, uint64 blocksPerPart,
				  uint64 *ends, int nends)
{
	char	   *dir = psprintf(ORIOLEDB_DATA_DIR "/%u", datoid);
	char	   *path = s3_build_dead_filename(datoid, relnode);
	int			fd;
	FileExtent	unit = {.off = blocksPerPart,.len = S3_DEAD_UNIT_LEN};

	(void) MakePGDirectory(dir);
	fd = BasicOpenFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0 ||
		write(fd, &unit, sizeof(unit)) != sizeof(unit))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", path)));
	for (int i = 0; i < nends; i++)
	{
		FileExtent	mark = {.off = ends[i],.len = 0};

		if (write(fd, &mark, sizeof(mark)) != sizeof(mark))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m", path)));
	}
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", path)));
	close(fd);
	pfree(dir);
	pfree(path);
}

static HTAB *
make_hash(const char *name, Size keysize, Size entrysize)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = keysize;
	ctl.entrysize = entrysize;
	ctl.hcxt = CurrentMemoryContext;
	return hash_create(name, 64, &ctl,
					   HASH_ELEM | HASH_CONTEXT |
					   (keysize == MAXPGPATH ? HASH_STRINGS : HASH_BLOBS));
}

static S3ManifestTree *
tree_key(Oid datoid, Oid relnode, Oid spcoid)
{
	S3ManifestTree *t = palloc0(sizeof(S3ManifestTree));

	t->key.datoid = datoid;
	t->key.relnode = relnode;
	t->spcoid = spcoid;
	return t;
}

/*
 * Lines of a body, each to the end of the line: paths may hold spaces.
 */
S3Manifest *
s3_manifest_parse(const char *body)
{
	S3Manifest *m = palloc0(sizeof(S3Manifest));
	const char *line = body;

	m->complete = true;
	m->timeline = 0;
	m->trees = make_hash("s3 manifest trees", sizeof(S3ManifestTreeKey),
						 sizeof(S3ManifestTree));
	m->files = make_hash("s3 manifest files", MAXPGPATH, sizeof(S3ManifestRef));
	m->small = make_hash("s3 manifest small", MAXPGPATH, sizeof(S3ManifestRef));
	m->bundles = make_hash("s3 manifest bundles", sizeof(uint32),
						   sizeof(S3ManifestBundles));
	m->undo = make_hash("s3 manifest undo", MAXPGPATH, sizeof(S3ManifestRef));

	while (line && *line)
	{
		const char *end = strchr(line, '\n');
		int			len = end ? end - line : strlen(line);
		char	   *l = pnstrdup(line, len);
		unsigned int a,
					b,
					c,
					d;
		unsigned long long t;
		unsigned int hi,
					lo;
		int			n = 0;

		if (sscanf(l, "format %u", &a) == 1)
		{
			if (a != S3_MANIFEST_FORMAT)
				ereport(ERROR,
						(errmsg("unknown S3 manifest format %u", a)));
		}
		else if (sscanf(l, "checkpoint %u", &a) == 1)
			m->checkpoint = a;
		else if (sscanf(l, "token %llu", &t) == 1)
			m->token = t;
		else if (sscanf(l, "complete %u", &a) == 1)
			m->complete = m->complete && a == 1;
		else if (sscanf(l, "redo %X/%X", &hi, &lo) == 2)
			m->redoLsn = ((uint64) hi << 32) | lo;
		else if (sscanf(l, "lsn %X/%X", &hi, &lo) == 2)
			m->checkpointLsn = ((uint64) hi << 32) | lo;
		else if (sscanf(l, "timeline %u", &a) == 1)
			m->timeline = a;
		else if (strncmp(l, "data ", 5) == 0)
			m->data = lappend(m->data, pstrdup(l + 5));
		else if (strncmp(l, "undo ", 5) == 0)
			m->undoUp = lappend(m->undoUp, pstrdup(l + 5));
		else if (sscanf(l, "map %u %u %u", &a, &b, &c) == 3)
			m->mapUp = lappend(m->mapUp, tree_key(a, b, c));
		else if (sscanf(l, "dead %u %u %u", &a, &b, &c) == 3)
			m->deadUp = lappend(m->deadUp, tree_key(a, b, c));
		else if (sscanf(l, "bdead %u %u %u", &a, &b, &c) == 3)
			m->buildDeadUp = lappend(m->buildDeadUp, tree_key(a, b, c));
		else if (sscanf(l, "tree %u %u %u %u", &a, &b, &c, &d) == 4)
		{
			S3ManifestTreeKey key = {a, b};
			S3ManifestTree *tree = hash_search(m->trees, &key, HASH_ENTER, NULL);

			tree->spcoid = c;
			tree->map = d;
		}
		else if ((sscanf(l, "file %u %n", &a, &n) == 1 ||
				  sscanf(l, "small %u %n", &a, &n) == 1) && n > 0 &&
				 strlen(l + n) < MAXPGPATH)
		{
			HTAB	   *h = l[0] == 'f' ? m->files : m->small;
			char		name[MAXPGPATH] = {0};
			S3ManifestRef *ref;

			strlcpy(name, l + n, MAXPGPATH);
			ref = hash_search(h, name, HASH_ENTER, NULL);
			ref->chkp = a;
		}
		else if (sscanf(l, "retired %u %llu", &a, &t) == 2)
		{
			S3ManifestRetired *r = palloc(sizeof(S3ManifestRetired));

			r->timeline = a;
			r->lastSeq = t;
			m->retired = lappend(m->retired, r);
		}
		else if (sscanf(l, "bundles %u %u", &a, &b) == 2)
		{
			S3ManifestBundles *e = hash_search(m->bundles, &a, HASH_ENTER, NULL);

			e->count = b;
		}
		else if (strncmp(l, "undoref ", 8) == 0 &&
				 sscanf(l, "undoref %u %n", &a, &n) == 1 && n > 0)
		{
			char		name[MAXPGPATH] = {0};
			S3ManifestRef *ref;

			strlcpy(name, l + n, MAXPGPATH);
			ref = hash_search(m->undo, name, HASH_ENTER, NULL);
			ref->chkp = a;
		}
		pfree(l);
		line = end ? end + 1 : NULL;
	}
	return m;
}

void
s3_manifest_free(S3Manifest *m)
{
	if (m == NULL)
		return;
	list_free_deep(m->data);
	list_free_deep(m->undoUp);
	list_free_deep(m->mapUp);
	list_free_deep(m->deadUp);
	list_free_deep(m->buildDeadUp);
	list_free_deep(m->retired);
	hash_destroy(m->trees);
	hash_destroy(m->files);
	hash_destroy(m->small);
	hash_destroy(m->bundles);
	hash_destroy(m->undo);
	pfree(m);
}

/*
 * The manifest of checkpoint chkpNum from the bucket, or NULL when it has
 * none (a checkpoint that crashed before it wrote one, or one collected).
 */
S3Manifest *
s3_manifest_fetch(uint32 chkpNum)
{
	uint64		token;
	char	   *key;
	StringInfoData body;
	S3Manifest *m = NULL;

	if (!s3_checkpoint_token_known(chkpNum, &token))
		return NULL;
	key = s3_manifest_key(chkpNum, token);
	initStringInfo(&body);
	if (s3_get_object(key, &body, true) == S3_RESPONSE_OK)
	{
		m = s3_manifest_parse(body.data);
		if (m->checkpoint != chkpNum)
			ereport(ERROR,
					(errmsg("S3 manifest \"%s\" is of checkpoint %u", key,
							m->checkpoint)));
	}
	pfree(body.data);
	pfree(key);
	return m;
}

static S3Manifest *
read_local(const char *path)
{
	uint64		size;
	Pointer		data;
	S3Manifest *m;
	struct stat st;

	if (stat(path, &st) != 0)
		return NULL;
	data = read_file(path, &size);
	if (data == NULL)
		return NULL;
	data = repalloc(data, size + 1);
	data[size] = '\0';
	m = s3_manifest_parse(data);
	pfree(data);
	return m;
}

/*
 * "FILE: <path>, CHECKSUM: <checksum>, CHECKPOINT: <n>" lines of a checksum
 * file as manifest lines "<kind> <n> <path>".
 */
static void
add_checksum_refs(StringInfo out, const char *path, const char *kind,
				  HTAB *chkps)
{
	FILE	   *f = AllocateFile(path, "r");
	char		line[MAXPGPATH + 128];

	if (f == NULL)
	{
		if (errno == ENOENT)
			return;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}
	while (fgets(line, sizeof(line), f) != NULL)
	{
		char		name[MAXPGPATH];
		char		checksum[65];
		unsigned int chkp;

		if (sscanf(line, "FILE: %1023[^,], CHECKSUM: %64[^,], CHECKPOINT: %u",
				   name, checksum, &chkp) != 3)
			continue;
		appendStringInfo(out, "%s %u %s\n", kind, chkp, name);
		if (chkps)
			(void) hash_search(chkps, &chkp, HASH_ENTER, NULL);
	}
	FreeFile(f);
}

static void
write_durable(const char *path, StringInfo body)
{
	char	   *tmp = psprintf("%s.tmp", path);
	int			fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);

	if (fd < 0 || write(fd, body->data, body->len) != body->len ||
		pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmp)));
	close(fd);
	durable_rename(tmp, path, ERROR);
	pfree(tmp);
}

static void
upload(uint32 chkpNum, StringInfo body)
{
	char	   *key = s3_manifest_key(chkpNum, s3_checkpoint_token(chkpNum));

	(void) s3_put_object_with_contents(key, body->data, body->len, NULL, false);
	pfree(key);
}

static const char *
undo_suffix(UndoLogType type)
{
	switch (type)
	{
		case UndoLogRegular:
			return "row";
		case UndoLogRegularPageLevel:
			return "page";
		case UndoLogSystem:
			return "system";
		default:
			return NULL;
	}
}

/*
 * The checkpoint's PG file backup is done: write its manifest.
 */
void
s3_manifest_prepare(uint32 chkpNum, int bundles)
{
	StringInfoData body;
	S3Manifest *prev = read_local(MANIFEST_COMMITTED);
	S3Manifest *pending = read_local(MANIFEST_PENDING);
	HTAB	   *trees;
	HTAB	   *smallChkps;
	HASHCTL		ctl;
	HASH_SEQ_STATUS seq;
	uint32	   *chkp;
	bool		complete = true;
	const char *line;
	CheckpointControl control;
	S3ManifestTree *tree;

	if (collected == NULL || collectChkp != chkpNum)
		return;

	initStringInfo(&body);
	appendStringInfo(&body, "format %d\ncheckpoint %u\ntoken " UINT64_FORMAT "\n",
					 S3_MANIFEST_FORMAT, chkpNum, s3_checkpoint_token(chkpNum));
	appendBinaryStringInfo(&body, collected->data, collected->len);
	/* the after-hook puts these up */
	appendStringInfoString(&body, "data global/pg_control\n");
	appendStringInfoString(&body, "data " ORIOLEDB_DATA_DIR "/control\n");
	appendStringInfo(&body, "bundles %u %d\n", chkpNum, bundles);

	/* the trees: catalog trees, and the system trees whose maps went up */
	trees = make_hash("s3 manifest trees", sizeof(S3ManifestTreeKey),
					  sizeof(S3ManifestTree));
	for (line = collected->data; line && *line;)
	{
		unsigned int a,
					b,
					c;

		if (sscanf(line, "map %u %u %u", &a, &b, &c) == 3)
		{
			S3ManifestTreeKey key = {a, b};

			tree = hash_search(trees, &key, HASH_ENTER, NULL);
			tree->spcoid = c;
			tree->map = chkpNum;
		}
		line = strchr(line, '\n');
		if (line)
			line++;
	}
	for (line = catalogTrees->data; line && *line;)
	{
		unsigned int a,
					b,
					c;

		if (sscanf(line, "%u %u %u", &a, &b, &c) == 3)
		{
			S3ManifestTreeKey key = {a, b};
			bool		found;
			char	   *bend = s3_build_dead_filename(a, b);
			struct stat st;

			tree = hash_search(trees, &key, HASH_ENTER, &found);
			tree->spcoid = c;
			if (!found)
			{
				bool		known;

				/* 0: no map yet, the tree is still listed as alive */
				tree->map = o_get_latest_chkp_num(a, b, c, chkpNum, &known);
				if (!known)
					tree->map = 0;
			}

			/* the part ends of a build, once its tree is in the catalog */
			if (stat(bend, &st) == 0)
			{
				char	   *object = psprintf("trees/%u/%u/dead/" UINT64_FORMAT "-%u.build",
											  a, b, s3_checkpoint_token(chkpNum),
											  chkpNum);

				/* the local copy goes once the checkpoint is committed */
				if (s3_put_file(object, bend, false) == S3_RESPONSE_OK)
					appendStringInfo(&body, "bdead %u %u %u\n", a, b, c);
				pfree(object);
			}
			pfree(bend);
		}
		line = strchr(line, '\n');
		if (line)
			line++;
	}
	hash_seq_init(&seq, trees);
	while ((tree = hash_seq_search(&seq)) != NULL)
		appendStringInfo(&body, "tree %u %u %u %u\n", tree->key.datoid,
						 tree->key.relnode, tree->spcoid, tree->map);
	hash_destroy(trees);

	/* PG files, and the checkpoints whose small file bundles are read */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(uint32);
	ctl.hcxt = CurrentMemoryContext;
	smallChkps = hash_create("s3 manifest small chkps", 16, &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	add_checksum_refs(&body, FILE_CHECKSUMS, "file", NULL);
	add_checksum_refs(&body, SMALL_FILE_CHECKSUMS, "small", smallChkps);
	hash_seq_init(&seq, smallChkps);
	while ((chkp = hash_seq_search(&seq)) != NULL)
	{
		S3ManifestBundles *b;

		if (*chkp == chkpNum)
			continue;
		b = prev ? hash_search(prev->bundles, chkp, HASH_FIND, NULL) : NULL;

		/*
		 * A checkpoint that finished its uploads and crashed before root
		 * named it: its bundles are up, and the local checksums already list
		 * its small files.
		 */
		if (b == NULL && pending != NULL && pending->checkpoint < chkpNum)
			b = hash_search(pending->bundles, chkp, HASH_FIND, NULL);
		if (b)
			appendStringInfo(&body, "bundles %u %d\n", *chkp, b->count);
		else
			complete = false;
	}
	hash_destroy(smallChkps);

	/*
	 * Undo: every checkpoint uploads the whole range it retains, so its
	 * references are its own uploads.
	 */
	if (get_checkpoint_control_data(&control))
	{
		for (int i = 0; i < NUM_CHECKPOINTABLE_UNDO_LOGS; i++)
		{
			CheckpointUndoInfo *info = &control.undoInfo[i];
			const char *suffix = undo_suffix(GetCheckpointableUndoLog(i));

			if (info->checkpointRetainEndLocation <= info->checkpointRetainStartLocation)
				continue;
			for (uint64 f = info->checkpointRetainStartLocation / UNDO_FILE_SIZE;
				 f <= (info->checkpointRetainEndLocation - 1) / UNDO_FILE_SIZE; f++)
				appendStringInfo(&body, "undoref %u %02X%08X%s\n", chkpNum,
								 (uint32) (f >> 32), (uint32) f, suffix);
		}
	}
	else
		complete = false;

	/* a timeline a takeover left behind */
	{
		FILE	   *f = AllocateFile(RETIRED_FILENAME, "r");
		unsigned int tli;
		unsigned long long seq;

		if (f != NULL)
		{
			while (fscanf(f, "%u %llu", &tli, &seq) == 2)
				appendStringInfo(&body, "retired %u %llu\n", tli, seq);
			FreeFile(f);
		}
	}

	appendStringInfo(&body, "complete %d\n", complete ? 1 : 0);

	write_durable(MANIFEST_PENDING, &body);
	upload(chkpNum, &body);
	s3_manifest_free(prev);
	s3_manifest_free(pending);
	pfree(body.data);
	collectChkp = 0;
}

/*
 * Before root names checkpoint chkpNum: put its manifest up with the WAL
 * position.  False when this run has no manifest of chkpNum (its backup did
 * not finish): root must not name it.
 */
bool
s3_manifest_publish(uint32 chkpNum, XLogRecPtr redoLsn,
					XLogRecPtr checkpointLsn, TimeLineID timeline)
{
	S3Manifest *m = read_local(MANIFEST_PENDING);
	StringInfoData body;
	uint64		size;
	Pointer		data;

	if (m == NULL || m->checkpoint != chkpNum)
	{
		s3_manifest_free(m);
		return false;
	}
	s3_manifest_free(m);

	data = read_file(MANIFEST_PENDING, &size);
	initStringInfo(&body);
	appendBinaryStringInfo(&body, data, size);
	pfree(data);
	appendStringInfo(&body, "redo %X/%X\nlsn %X/%X\ntimeline %u\n",
					 LSN_FORMAT_ARGS(redoLsn), LSN_FORMAT_ARGS(checkpointLsn),
					 timeline);
	write_durable(MANIFEST_PENDING, &body);
	upload(chkpNum, &body);
	pfree(body.data);
	return true;
}

/*
 * Root names chkpNum: its manifest is the committed one.
 */
void
s3_manifest_committed(uint32 chkpNum)
{
	S3Manifest *m = read_local(MANIFEST_PENDING);

	if (m != NULL && m->checkpoint == chkpNum)
	{
		ListCell   *lc;

		durable_rename(MANIFEST_PENDING, MANIFEST_COMMITTED, ERROR);
		if (m->retired != NIL)
			(void) durable_unlink(RETIRED_FILENAME, LOG);

		/*
		 * A build's part ends are in the bucket under a committed checkpoint
		 * now; until then the next checkpoint would put them up again.
		 */
		foreach(lc, m->buildDeadUp)
		{
			S3ManifestTree *t = lfirst(lc);
			char	   *bend = s3_build_dead_filename(t->key.datoid,
													  t->key.relnode);

			(void) unlink(bend);
			pfree(bend);
		}
	}
	s3_manifest_free(m);
}
