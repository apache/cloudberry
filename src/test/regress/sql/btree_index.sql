--
-- BTREE_INDEX
--

-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);

CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);

CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
);

\set filename :abs_srcdir '/data/desc.data'
COPY bt_i4_heap FROM :'filename';

\set filename :abs_srcdir '/data/hash.data'
COPY bt_name_heap FROM :'filename';

\set filename :abs_srcdir '/data/desc.data'
COPY bt_txt_heap FROM :'filename';

\set filename :abs_srcdir '/data/hash.data'
COPY bt_f8_heap FROM :'filename';

ANALYZE bt_i4_heap;
ANALYZE bt_name_heap;
ANALYZE bt_txt_heap;
ANALYZE bt_f8_heap;

--
-- BTREE ascending/descending cases
--
-- we load int4/text from pure descending data (each key is a new
-- low key) and name/f8 from pure ascending data (each key is a new
-- high key).  we had a bug where new low keys would sometimes be
-- "lost".
--
CREATE INDEX bt_i4_index ON bt_i4_heap USING btree (seqno int4_ops);

CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);

CREATE INDEX bt_txt_index ON bt_txt_heap USING btree (seqno text_ops);

CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops);

--
-- test retrieval of min/max keys for each index
--

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno < 1;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno >= 9999;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno = 4500;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno < '1'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno >= '9999'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno = '4500'::name;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno = '4500'::text;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno < '1'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno >= '9999'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno = '4500'::float8;

--
-- Check correct optimization of LIKE (special index operator support)
-- for both indexscan and bitmapscan cases
--

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
set enable_sort to false; -- GPDB needs more strong-arming to get same plans as upstream
explain (costs off)
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike '00%foo' order by 1;
select proname from pg_proc where proname ilike '00%foo' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike 'ri%foo' order by 1;

set enable_indexscan to false;
set enable_bitmapscan to true;
reset enable_sort;
explain (costs off)
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike '00%foo' order by 1;
select proname from pg_proc where proname ilike '00%foo' order by 1;
set enable_sort to false; -- GPDB needs more strong-arming to get same plans as upstream
set enable_bitmapscan to false;
explain (costs off)
select proname from pg_proc where proname ilike 'ri%foo' order by 1;

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;
reset enable_sort;

-- Also check LIKE optimization with binary-compatible cases

create temp table btree_bpchar (f1 text collate "C");
create index on btree_bpchar(f1 bpchar_ops) WITH (deduplicate_items=on);
insert into btree_bpchar values ('foo'), ('foo  '), ('fool'), ('bar'), ('quux');
-- doesn't match index:
explain (costs off)
select * from btree_bpchar where f1 like 'foo';
select * from btree_bpchar where f1 like 'foo';
explain (costs off)
select * from btree_bpchar where f1 like 'foo%';
select * from btree_bpchar where f1 like 'foo%';
-- these do match the index:
explain (costs off)
select * from btree_bpchar where f1::bpchar like 'foo';
select * from btree_bpchar where f1::bpchar like 'foo';
explain (costs off)
select * from btree_bpchar where f1::bpchar like 'foo%';
select * from btree_bpchar where f1::bpchar like 'foo%';

explain (costs off)
select * from btree_bpchar where f1::bpchar ='foo';
select * from btree_bpchar where f1::bpchar ='foo';

-- get test coverage for "single value" deduplication strategy:
insert into btree_bpchar select 'foo' from generate_series(1,1500);

--
-- Perform unique checking, with and without the use of deduplication
--
CREATE TABLE dedup_unique_test_table (a int) WITH (autovacuum_enabled=false);
CREATE UNIQUE INDEX dedup_unique ON dedup_unique_test_table (a) WITH (deduplicate_items=on);
CREATE UNIQUE INDEX plain_unique ON dedup_unique_test_table (a) WITH (deduplicate_items=off);
-- Generate enough garbage tuples in index to ensure that even the unique index
-- with deduplication enabled has to check multiple leaf pages during unique
-- checking (at least with a BLCKSZ of 8192 or less)
DO $$
BEGIN
    FOR r IN 1..1350 LOOP
        DELETE FROM dedup_unique_test_table;
        INSERT INTO dedup_unique_test_table SELECT 1;
    END LOOP;
END$$;

-- Exercise the LP_DEAD-bit-set tuple deletion code with a posting list tuple.
-- The implementation prefers deleting existing items to merging any duplicate
-- tuples into a posting list, so we need an explicit test to make sure we get
-- coverage (note that this test also assumes BLCKSZ is 8192 or less):
DROP INDEX plain_unique;
DELETE FROM dedup_unique_test_table WHERE a = 1;
INSERT INTO dedup_unique_test_table SELECT i FROM generate_series(0,450) i;

--
-- Test B-tree fast path (cache rightmost leaf page) optimization.
--

-- First create a tree that's at least three levels deep (i.e. has one level
-- between the root and leaf levels). The text inserted is long.  It won't be
-- TOAST compressed because we use plain storage in the table.  Only a few
-- index tuples fit on each internal page, allowing us to get a tall tree with
-- few pages.  (A tall tree is required to trigger caching.)
--
-- The text column must be the leading column in the index, since suffix
-- truncation would otherwise truncate tuples on internal pages, leaving us
-- with a short tree.
create table btree_tall_tbl(id int4, t text);
alter table btree_tall_tbl alter COLUMN t set storage plain;
create index btree_tall_idx on btree_tall_tbl (t, id) with (fillfactor = 10);
insert into btree_tall_tbl select g, repeat('x', 250)
from generate_series(1, 130) g;

--
-- Test for multilevel page deletion
--
CREATE TABLE delete_test_table (a bigint, b bigint, c bigint, d bigint);
INSERT INTO delete_test_table SELECT i, 1, 2, 3 FROM generate_series(1,80000) i;
ALTER TABLE delete_test_table ADD PRIMARY KEY (a,b,c,d);
-- Delete most entries, and vacuum, deleting internal pages and creating "fast
-- root"
DELETE FROM delete_test_table WHERE a < 79990;
VACUUM delete_test_table;

--
-- Test B-tree insertion with a metapage update (XLOG_BTREE_INSERT_META
-- WAL record type). This happens when a "fast root" page is split.  This
-- also creates coverage for nbtree FSM page recycling.
--
-- The vacuum above should've turned the leaf page into a fast root. We just
-- need to insert some rows to cause the fast root page to split.
INSERT INTO delete_test_table SELECT i, 1, 2, 3 FROM generate_series(1,1000) i;

--
-- GPDB: Test correctness of B-tree stats in consecutively VACUUM.
--
CREATE TABLE btree_stats_tbl(col_int int, col_text text, col_numeric numeric, col_unq int) DISTRIBUTED BY (col_int);
CREATE INDEX btree_stats_idx ON btree_stats_tbl(col_int);
INSERT INTO btree_stats_tbl VALUES (1, 'aa', 1001, 101), (2, 'bb', 1002, 102);
SELECT reltuples FROM pg_class WHERE relname='btree_stats_tbl';
-- inspect the state of the stats on segments
SELECT gp_segment_id, relname, reltuples FROM gp_dist_random('pg_class') WHERE relname = 'btree_stats_idx';
SELECT reltuples FROM pg_class WHERE relname='btree_stats_idx';
-- 1st VACUUM, expect reltuples = 2
vacuum btree_stats_tbl;
SELECT reltuples FROM pg_class WHERE relname='btree_stats_tbl';
-- inspect the state of the stats on segments
SELECT gp_segment_id, relname, reltuples FROM gp_dist_random('pg_class') WHERE relname = 'btree_stats_idx';
SELECT reltuples FROM pg_class WHERE relname='btree_stats_idx';
-- 2nd VACUUM, expect reltuples = 2
vacuum btree_stats_tbl;
SELECT reltuples FROM pg_class WHERE relname='btree_stats_tbl';
-- inspect the state of the stats on segments
SELECT gp_segment_id, relname, reltuples FROM gp_dist_random('pg_class') WHERE relname = 'btree_stats_idx';
SELECT reltuples FROM pg_class WHERE relname='btree_stats_idx';

-- Prior to this fix, the case would be failed here. Given the
-- scenario of updating stats during VACUUM:
-- 1) coordinator vacuums and updates stats of its own;
-- 2) then coordinator dispatches vacuum to segments;
-- 3) coordinator combines stats received from segments to overwrite the stats of its own.
-- Because upstream introduced a feature which could skip full index scan uring cleanup
-- of B-tree indexes when possible (refer to:
-- https://github.com/postgres/postgres/commit/857f9c36cda520030381bd8c2af20adf0ce0e1d4),
-- there was a case in QD-QEs distributed deployment that some QEs could skip full index scan and
-- stop updating statistics, result in QD being unable to collect all QEs' stats thus overwrote
-- a paritial accumulated value to index->reltuples. More interesting, it usually happened starting
-- from the 3rd time of consecutively VACUUM after fresh inserts due to above skipping index scan
-- criteria.
-- 3rd VACUUM, expect reltuples = 2
vacuum btree_stats_tbl;
SELECT reltuples FROM pg_class WHERE relname='btree_stats_tbl';
-- inspect the state of the stats on segments
SELECT gp_segment_id, relname, reltuples FROM gp_dist_random('pg_class') WHERE relname = 'btree_stats_idx';
SELECT reltuples FROM pg_class WHERE relname='btree_stats_idx';

-- Test unsupported btree opclass parameters
create index on btree_tall_tbl (id int4_ops(foo=1));

-- Test case of ALTER INDEX with abuse of column names for indexes.
-- This grammar is not officially supported, but the parser allows it.
CREATE INDEX btree_tall_idx2 ON btree_tall_tbl (id);
ALTER INDEX btree_tall_idx2 ALTER COLUMN id SET (n_distinct=100);
DROP INDEX btree_tall_idx2;
-- Partitioned index
CREATE TABLE btree_part (id int4) PARTITION BY RANGE (id);
CREATE INDEX btree_part_idx ON btree_part(id);
ALTER INDEX btree_part_idx ALTER COLUMN id SET (n_distinct=100);
DROP TABLE btree_part;
