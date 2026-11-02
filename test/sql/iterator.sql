-- Whitebox tests for the orioledb B-tree iterator.  The C helpers exercised
-- here live at the bottom of src/btree/iterator.c and are registered in the
-- 1.8->1.9 dev migration.

CREATE SCHEMA iterator;
SET SESSION search_path = 'iterator';
CREATE EXTENSION orioledb;

-- it->curKeyReturned set BEFORE the end-key bound check in
-- btree_iterate_raw_internal().  orioledb_test_endkey_returned_skip()
-- drives an iterator into the buggy state via an end-bound rejection,
-- forces iterator_refind_partial_leaf() (the partial-read failure path),
-- then drains the iterator with a wide end and returns the PKs as int4[].
-- The bug drops the rejected row from the resumed scan, so the resulting
-- query has one fewer row than expected.
CREATE TABLE o_endkey_skip (id int4 PRIMARY KEY) USING orioledb;
INSERT INTO o_endkey_skip SELECT g FROM generate_series(1, 10) g;

SELECT unnest(orioledb_test_endkey_returned_skip('o_endkey_skip'::regclass, 5)) AS id;

DROP TABLE o_endkey_skip;

-- Backward refind: silent invalid locator at items_count in
-- iterator_refind_partial_leaf().  orioledb_test_back_refind_skip_tail()
-- opens a backward iterator, forges curKey to a value past the leaf's max,
-- then forces a refind.  With the bug the locator stays invalid; with the
-- fix it decrements to the leaf's tail (mirroring the seed path) and the
-- backward drain sees every row, descending.
CREATE TABLE o_back_refind (id int4 PRIMARY KEY) USING orioledb;
INSERT INTO o_back_refind SELECT g FROM generate_series(1, 10) g;

SELECT unnest(orioledb_test_back_refind_skip_tail('o_back_refind'::regclass, 1000)) AS id;

DROP TABLE o_back_refind;

-- An advance past the tuple the end bound rejected.
-- orioledb_test_advance_past_rejected() drains an iterator up to a tuple the
-- end bound rejects, which stays its resume key not yet returned, advances
-- it to a later key, then forces iterator_refind_partial_leaf().  The
-- re-find resumed at the rejected tuple and handed it out again, below the
-- range the scan had moved to: a skip scan pinned its leading value to it
-- and went round the same range for ever.
CREATE TABLE o_advance_past (id int4 PRIMARY KEY) USING orioledb;
INSERT INTO o_advance_past SELECT g FROM generate_series(1, 10) g;

SELECT unnest(orioledb_test_advance_past_rejected('o_advance_past'::regclass, 4, 7)) AS id;

DROP TABLE o_advance_past;

-- A bound exclusive in a leading column.  The descent's fast path searched
-- for the bound's values as if it were inclusive there, and landed inside the
-- run of the leading value it was to pass: a skip scan got a row of the
-- leading value it had just finished, pinned it again, and went round for
-- ever.  Enough rows that the run spans pages.
CREATE TABLE o_past_prefix (a int4, b int4, PRIMARY KEY (a, b)) USING orioledb;
INSERT INTO o_past_prefix SELECT g / 1000, g % 1000 FROM generate_series(0, 9999) g;

SELECT orioledb_test_past_exclusive_prefix('o_past_prefix'::regclass, 5);

DROP TABLE o_past_prefix;

-- A PG18 skip array is the one case where switch_to_next_range() recomputes
-- ostate->exact, so a single scan can hold both kinds of range: the first binds
-- the whole key and builds the iterator through o_btree_find_tuples_start(), a
-- later one is a range and reuses that iterator to step to a sibling page.  The
-- step reads the parent locator through parentImg, so the iterator has to have
-- asked find_page() to keep it there.
CREATE TABLE o_skip_step (
	a int,
	b int,
	pad text,
	PRIMARY KEY (a, b)
) USING orioledb;
INSERT INTO o_skip_step
	SELECT g % 10, g / 10, repeat('x', 400) FROM generate_series(1, 10000) g;
ANALYZE o_skip_step;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
SELECT count(pad) FROM o_skip_step WHERE b = 500;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;
DROP TABLE o_skip_step;

-- A backward index scan whose snapshot predates the page it reads takes the
-- page from undo.  An undo image leftmost on its base location got its low key
-- compared from nothing, and the backend crashed: here, MAX() over the target
-- table inside an INSERT ... SELECT, once earlier rows of that range had been
-- inserted and deleted.  (Crashed on release builds only.)
CREATE TABLE o_self_max (
	rt text NOT NULL,
	rid text NOT NULL,
	version bigint NOT NULL,
	PRIMARY KEY (rt, rid, version)
) USING orioledb;
INSERT INTO o_self_max
	SELECT 'a' || lpad(g::text, 6, '0'), 'resource.' || md5(g::text), 1
	FROM generate_series(1, 100) g;
\set ECHO none
SELECT stmt FROM generate_series(0, 29) i,
	LATERAL (VALUES
		(format($q$INSERT INTO o_self_max
			SELECT v.rt, v.rid,
				   COALESCE((SELECT max(h.version) FROM o_self_max h
							 WHERE h.rt = v.rt AND h.rid = v.rid), 0) + v.ord + 1
			FROM (VALUES (0, %1$L, 'a'), (1, %1$L, 'a'), (2, %1$L, 'a'),
						 (3, %1$L, 'a'), (4, %1$L, 'a'), (5, %1$L, 'a'),
						 (6, %1$L, 'a'), (7, %1$L, 'a'), (8, %1$L, 'a'),
						 (9, %1$L, 'a')) AS v(ord, rt, rid)$q$,
			't' || lpad(i::text, 8, '0')), 1),
		(format('DELETE FROM o_self_max WHERE rt = %L',
				't' || lpad(i::text, 8, '0')), 2)) AS s(stmt, n)
ORDER BY i, n \gexec
\set ECHO all
SELECT count(*) FROM o_self_max;
DROP TABLE o_self_max;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA iterator CASCADE;
RESET search_path;
