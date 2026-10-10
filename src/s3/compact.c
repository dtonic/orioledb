/*-------------------------------------------------------------------------
 *
 * compact.c
 *		Move the live pages out of sparse S3 parts.
 *
 * Random updates leave parts mostly dead; such a part is removed only once
 * every block of it is dead.  orioledb_s3_compact() walks a relation's trees
 * and marks dirty every page that lives in a part whose live blocks are below
 * the threshold (from the GC's state of the tree): the next checkpoint writes
 * each to a new extent and lists the old one as dead, and the GC removes the
 * part once that checkpoint is no longer retained.  Pages not in memory are
 * loaded only when they are in such a part, or are inner pages whose children
 * may be.
 *
 * The walk locates each page with the engine's descent (find_page), which
 * holds one page lock at a time, so it waits on no lock while holding one.
 *
 * With orioledb.s3_compact_database set, a background worker connected to
 * that database compacts all its orioledb relations every
 * orioledb.s3_compact_interval.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/compact.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/page_contents.h"
#include "btree/page_state.h"
#include "s3/gc.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "utils/page_pool.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/objectaddress.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(orioledb_s3_compact);

typedef struct
{
	BTreeDescr *desc;
	uint64		blocksPerPart;
	uint64	   *parts;
	int			nparts;
	int64		marked;
} CompactState;

static bool
in_sparse_part(CompactState *state, uint64 off)
{
	uint64		part = off / state->blocksPerPart;
	int			lo = 0,
				hi = state->nparts - 1;

	while (lo <= hi)
	{
		int			mid = (lo + hi) / 2;

		if (state->parts[mid] == part)
			return true;
		if (state->parts[mid] < part)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return false;
}

/*
 * Lock the page of the level that holds key (the leftmost one when key is
 * NULL), loading it and the pages above it as needed: the engine's own
 * descent, which holds one page lock at a time.
 */
static OInMemoryBlkno
compact_find(CompactState *state, OBTreeFindPageContext *context,
			 OFixedKey *key, uint16 level)
{
	OFindPageResult result;

	init_page_find_context(context, state->desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);
	if (key == NULL)
		result = find_page(context, NULL, BTreeKeyNone, level);
	else
		result = find_page(context, &key->tuple, BTreeKeyNonLeafKey, level);
	Assert(result == OFindPageResultSuccess);
	(void) result;
	return context->items[context->index].blkno;
}

/* Mark the locked page to be moved when it lives in a sparse part. */
static void
compact_mark(CompactState *state, OInMemoryBlkno blkno)
{
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

	if (FileExtentIsValid(page_desc->fileExtent) &&
		in_sparse_part(state, page_desc->fileExtent.off))
	{
		MARK_DIRTY(state->desc, blkno);
		state->marked++;
	}
}

/*
 * The children of a locked level-1 page that may live in a sparse part: those
 * on disk in one, and those in memory whose extent is in one (read without
 * the child's lock: a hint, judged again under it).  Each is named by a key
 * it holds -- its separator, or for the first child the key the parent was
 * found by.
 */
static List *
compact_leaf_keys(CompactState *state, Page p, OFixedKey *parentKey)
{
	BTreePageItemLocator loc;
	List	   *keys = NIL;
	bool		first = true;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
		uint64		downlink = tuphdr->downlink;
		bool		wanted = false;

		if (DOWNLINK_IS_ON_DISK(downlink))
			wanted = in_sparse_part(state, DOWNLINK_GET_DISK_OFF(downlink));
		else if (DOWNLINK_IS_IN_MEMORY(downlink))
		{
			OrioleDBPageDesc *child = O_GET_IN_MEMORY_PAGEDESC(DOWNLINK_GET_IN_MEMORY_BLKNO(downlink));
			FileExtent	extent = child->fileExtent;

			wanted = FileExtentIsValid(extent) &&
				in_sparse_part(state, extent.off);
		}

		if (wanted)
		{
			OFixedKey  *key = NULL;

			if (!first)
			{
				key = palloc(sizeof(OFixedKey));
				copy_fixed_page_key(state->desc, key, p, &loc);
			}
			else if (parentKey)
			{
				key = palloc(sizeof(OFixedKey));
				*key = *parentKey;
				key->tuple.data = key->fixedData;
			}
			keys = lappend(keys, key);
		}
		first = false;
	}
	return keys;
}

/*
 * Visit every page of each level from the root's down to level 1, and the
 * leaves the level-1 pages say may live in a sparse part.  A level is walked
 * left to right by its pages' high keys; a page split or merged meanwhile is
 * at worst visited twice or missed, and a missed page is moved by a later
 * pass.
 */
static int64
compact_tree(BTreeDescr *desc, double threshold)
{
	CompactState state;
	OBTreeFindPageContext context;
	int			rootLevel;

	memset(&state, 0, sizeof(state));
	state.desc = desc;
	state.blocksPerPart = S3_PART_BLOCKS(OCompressIsValid(desc->compress) ?
										 ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);
	state.parts = s3_gc_sparse_parts(desc->oids.datoid, desc->oids.relnode,
									 threshold, &state.nparts);
	if (state.nparts == 0)
	{
		pfree(state.parts);
		return 0;
	}

	o_btree_load_shmem(desc);
	rootLevel = PAGE_GET_LEVEL(O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno));

	for (int level = rootLevel; level >= 1; level--)
	{
		OFixedKey	key;
		bool		leftmost = true;

		for (;;)
		{
			OInMemoryBlkno blkno;
			Page		p;
			List	   *leafKeys = NIL;
			bool		rightmost;
			ListCell   *lc;

			CHECK_FOR_INTERRUPTS();
			blkno = compact_find(&state, &context, leftmost ? NULL : &key, level);
			p = O_GET_IN_MEMORY_PAGE(blkno);
			compact_mark(&state, blkno);
			if (level == 1)
				leafKeys = compact_leaf_keys(&state, p, leftmost ? NULL : &key);
			rightmost = O_PAGE_IS(p, RIGHTMOST);
			if (!rightmost)
				copy_fixed_hikey(desc, &key, p);
			unlock_page(blkno);

			foreach(lc, leafKeys)
			{
				OFixedKey  *leafKey = (OFixedKey *) lfirst(lc);

				CHECK_FOR_INTERRUPTS();
				blkno = compact_find(&state, &context, leafKey, 0);
				compact_mark(&state, blkno);
				unlock_page(blkno);
				if (leafKey)
					pfree(leafKey);
			}
			list_free(leafKeys);

			if (rightmost)
				break;
			leftmost = false;
		}
	}
	if (rootLevel == 0)
	{
		OInMemoryBlkno blkno = compact_find(&state, &context, NULL, 0);

		compact_mark(&state, blkno);
		unlock_page(blkno);
	}

	elog(DEBUG1, "S3 compaction marked " INT64_FORMAT " pages of tree %u/%u "
		 "in %d sparse parts", state.marked, desc->oids.datoid,
		 desc->oids.relnode, state.nparts);
	pfree(state.parts);
	return state.marked;
}

/*
 * Compact every tree of an orioledb relation: the number of pages marked.
 * Errors when the relation is not an orioledb one.
 */
int64
s3_compact_relation(Oid relid, double threshold)
{
	Relation	rel;
	OTableDescr *descr;
	int64		marked = 0;

	orioledb_check_shmem();
	rel = relation_open(relid, AccessShareLock);
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(rel->rd_rel->relkind),
					   RelationGetRelationName(rel));
	descr = relation_get_descr(rel);
	if (descr == NULL)
	{
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an orioledb table",
						RelationGetRelationName(rel))));
	}

	for (int i = 0; i <= descr->nIndices; i++)
	{
		BTreeDescr *td = i < descr->nIndices ? &descr->indices[i]->desc :
			&descr->toast->desc;

		marked += compact_tree(td, threshold);
	}
	relation_close(rel, AccessShareLock);
	return marked;
}

/*
 * orioledb_s3_compact(rel regclass, threshold float8): the number of pages
 * marked to be moved by the next checkpoint.
 */
Datum
orioledb_s3_compact(PG_FUNCTION_ARGS)
{
	double		threshold = PG_GETARG_FLOAT8(1);

	if (!orioledb_s3_mode || orioledb_s3_reader)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("S3 compaction runs on the S3 writer only")));
	if (threshold <= 0.0 || threshold > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("threshold must be in (0, 1]")));
	PG_RETURN_INT64(s3_compact_relation(PG_GETARG_OID(0), threshold));
}

char	   *s3_compact_database = NULL;
int			s3_compact_interval = 3600;
double		s3_compact_threshold = 0.3;

void
register_s3_compact_worker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 60;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3_compact_worker_main");
	strcpy(worker.bgw_name, "orioledb s3 compaction");
	strcpy(worker.bgw_type, "orioledb s3 compaction");
	RegisterBackgroundWorker(&worker);
}

/*
 * The orioledb relations of the database, each compacted in a transaction
 * of its own.  A relation dropped in between is skipped.
 */
static void
compact_database(void)
{
	List	   *relids = NIL;
	ListCell   *lc;
	int64		marked = 0;

	StartTransactionCommand();
	{
		Oid			am = get_am_oid("orioledb", true);
		Relation	pg_class;
		SysScanDesc scan;
		HeapTuple	tuple;

		if (!OidIsValid(am))
		{
			CommitTransactionCommand();
			return;
		}
		pg_class = table_open(RelationRelationId, AccessShareLock);
		scan = systable_beginscan(pg_class, InvalidOid, false, NULL, 0, NULL);
		while ((tuple = systable_getnext(scan)) != NULL)
		{
			Form_pg_class form = (Form_pg_class) GETSTRUCT(tuple);

			if (form->relam == am &&
				(form->relkind == RELKIND_RELATION ||
				 form->relkind == RELKIND_MATVIEW))
			{
				/* outlives the transaction */
				MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

				relids = lappend_oid(relids, form->oid);
				MemoryContextSwitchTo(old);
			}
		}
		systable_endscan(scan);
		table_close(pg_class, AccessShareLock);
	}
	CommitTransactionCommand();

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);

		CHECK_FOR_INTERRUPTS();
		if (ShutdownRequestPending)
			break;
		StartTransactionCommand();
		if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
			marked += s3_compact_relation(relid, s3_compact_threshold);
		CommitTransactionCommand();
	}
	list_free(relids);
	if (marked > 0)
		ereport(LOG,
				(errmsg("S3 compaction marked " INT64_FORMAT " pages to move out of sparse parts",
						marked)));
}

void
s3_compact_worker_main(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(s3_compact_database, NULL, 0);

	while (!ShutdownRequestPending)
	{
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) s3_compact_interval * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (!ShutdownRequestPending)
			compact_database();
	}
	proc_exit(0);
}
