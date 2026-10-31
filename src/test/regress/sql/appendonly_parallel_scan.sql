--
-- Parallel sequential scans of AOCS tables.
--
-- Each segment file is split into chunks of rows, and the parallel processes
-- of a segment take chunks from a shared counter (see
-- src/backend/access/aocs/README.parallel_scan.md).
--
-- Most cases run :check, which returns
--   rows        number of rows read
--   sum_a       sum of column a
--   misaligned  rows whose other columns do not belong to their column a,
--               i.e. the column readers were positioned at different rows;
--               always 0
--   sum_rownum  sum of the row numbers in the rows' ctids; it changes if a
--               row is returned twice or with another row's ctid
-- Column a holds 1..n, so rows and sum_a show whether every row was read
-- exactly once.  The check queries avoid aggregate FILTER and count(DISTINCT)
-- on a row value, which ORCA cannot plan.
--
-- Tables are distributed by a constant key, so all rows are on one segment
-- and the chunk layout of that segment is known.  A chunk holds at least
-- 512kB of rows (AOCS_PARALLEL_SCAN_MIN_CHUNK_BYTES), using the average row
-- size of the table.  The tables with columns (k int, a int, c text) have 41
-- bytes per row, so a chunk holds 524288 / 41 = 12787 rows.
--
set enable_parallel = on;
set optimizer = off;
set max_parallel_workers_per_gather = 4;

create schema appendonly_parallel_scan;
set search_path = appendonly_parallel_scan;

\set check 'select count(*) as rows, sum(a) as sum_a, sum(case when c <> md5(a::text) then 1 else 0 end) as misaligned, sum(((ctid::text::point)[0]::bigint % 33554432) * 32768 + (ctid::text::point)[1]::bigint - 1) as sum_rownum from'

-- Segment files that a scan reads on the segment that holds the table: those
-- with rows that are not awaiting drop (state 2).  Whether VACUUM can drop a
-- compacted segment file right away depends on other sessions' snapshots, so
-- empty and awaiting-drop segment files are left out to keep the output
-- stable.
create function segfile_rows(rel regclass)
returns table (segno int, tupcount bigint) language sql as $$
  select segno, tupcount
  from gp_toolkit.__gp_aocsseg(rel)
  where column_num = 0
    and tupcount > 0
    and state <> 2
    and segment_id in (select segment_id from gp_toolkit.__gp_aocsseg(rel)
                       where column_num = 0
                       group by segment_id having sum(tupcount) > 0)
  order by segno
$$;

--
-- Chunk layout
--

-- One segment file split into several chunks: 100000 rows are 8 chunks, the
-- last one 10491 rows long.
create table single_segfile (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into single_segfile select 0, i, md5(i::text) from generate_series(1, 100000) i;
select * from segfile_rows('single_segfile');
-- the average row size: uncompressed bytes of all columns per row
select div(sum(eof_uncompressed), 100000) as row_bytes
from gp_toolkit.__gp_aocsseg('single_segfile') where tupcount > 0;
explain (costs off) :check single_segfile;
:check single_segfile;

-- Several segment files of different sizes: an insert that spreads rows over
-- three segment files writes 20000, 20000 and 10000 rows, then a plain insert
-- appends 70000 more rows to the last one.
create table multi_segfile (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
set gp_appendonly_insert_files = 3;
set gp_appendonly_insert_files_tuples_range = 20000;
insert into multi_segfile select 0, i, md5(i::text) from generate_series(1, 50000) i;
reset gp_appendonly_insert_files;
reset gp_appendonly_insert_files_tuples_range;
insert into multi_segfile select 0, i, md5(i::text) from generate_series(50001, 120000) i;
select tupcount from segfile_rows('multi_segfile') order by 1;
explain (costs off) :check multi_segfile;
:check multi_segfile;

-- Segment files without rows to scan between, before and after the ones
-- with rows.  Three segment files of 30000 rows each; deleting all rows of one
-- and vacuuming leaves an empty segment file in its place, or one awaiting
-- drop if another session's snapshot can still see its rows.  The scan must
-- skip both.
create table empty_segfiles (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
set gp_appendonly_insert_files = 3;
set gp_appendonly_insert_files_tuples_range = 30000;
insert into empty_segfiles select 0, i, md5(i::text) from generate_series(1, 90000) i;
reset gp_appendonly_insert_files;
reset gp_appendonly_insert_files_tuples_range;
-- rows 30001-60000 are in the second segment file: empty it
delete from empty_segfiles where a between 30001 and 60000;
vacuum empty_segfiles;
select * from segfile_rows('empty_segfiles');
explain (costs off) :check empty_segfiles;
:check empty_segfiles;
-- rows 1-30000 are in the first segment file: empty it too
delete from empty_segfiles where a <= 30000;
vacuum empty_segfiles;
select * from segfile_rows('empty_segfiles');
explain (costs off) :check empty_segfiles;
:check empty_segfiles;

-- Row counts at a chunk boundary: exactly 3 chunks, one row less, and one
-- row more (a fourth chunk of a single row).
create table boundary_eq (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into boundary_eq select 0, i, md5(i::text) from generate_series(1, 3 * 12787) i;
explain (costs off) :check boundary_eq;
:check boundary_eq;
create table boundary_minus (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into boundary_minus select 0, i, md5(i::text) from generate_series(1, 3 * 12787 - 1) i;
explain (costs off) :check boundary_minus;
:check boundary_minus;
create table boundary_plus (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into boundary_plus select 0, i, md5(i::text) from generate_series(1, 3 * 12787 + 1) i;
explain (costs off) :check boundary_plus;
:check boundary_plus;

-- Fewer chunks than workers: a table of one short chunk, and an empty table.
create table tiny (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into tiny select 0, i, md5(i::text) from generate_series(1, 10) i;
explain (costs off) :check tiny;
:check tiny;
create table empty (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
explain (costs off) :check empty;
:check empty;

--
-- Positioning
--
-- At the first row of a chunk every column file is positioned separately, by
-- reading block headers.  Columns of different widths hold different numbers
-- of rows per block, so each column file stops at a different block.  All
-- tables below have the same columns, each derived from a, so misaligned
-- counts the rows where any column was read from another row than a.
--

-- b and n are fixed width, t is mostly 32 bytes, but every 1000th value is
-- 64000 bytes: larger than a block, so it is stored over several blocks.
create function t_for(a int) returns text language sql immutable parallel safe as $$
  select case when a % 1000 = 0 then repeat(md5(a::text), 2000)
              else md5(a::text) end
$$;

\set check_cols 'select count(*) as rows, sum(a) as sum_a, sum(case when b <> a::bigint * 7 or n <> a / 4.0 or t <> t_for(a) then 1 else 0 end) as misaligned, sum(((ctid::text::point)[0]::bigint % 33554432) * 32768 + (ctid::text::point)[1]::bigint - 1) as sum_rownum from'

-- Columns of different widths.
create table wide_cols (k int, a int, b bigint, n numeric, t text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into wide_cols select 0, i, i::bigint * 7, i / 4.0, t_for(i) from generate_series(1, 100000) i;
explain (costs off) :check_cols wide_cols;
:check_cols wide_cols;

-- Projections reading only some of the columns.
explain (costs off) select sum(n) from wide_cols;
select sum(n) from wide_cols;
explain (costs off) select sum(b), sum(length(t)) from wide_cols;
select sum(b), sum(length(t)) from wide_cols;

-- Small blocks: many blocks per chunk.
create table block_8k (k int, a int, b bigint, n numeric, t text)
  using ao_column with (parallel_workers = 4, blocksize = 8192) distributed by (k);
insert into block_8k select * from wide_cols;
explain (costs off) :check_cols block_8k;
:check_cols block_8k;

-- Large blocks: several chunks in one block.
create table block_2m (k int, a int, b bigint, n numeric, t text)
  using ao_column with (parallel_workers = 4, blocksize = 2097152) distributed by (k);
insert into block_2m select * from wide_cols;
explain (costs off) :check_cols block_2m;
:check_cols block_2m;

-- Compressed blocks.  Only the block holding the first row of a chunk is
-- decompressed.  rle_type packs the regular values of b and n densely, so a
-- block holds many more rows.
create table compress_zstd (k int, a int, b bigint, n numeric, t text)
  using ao_column with (parallel_workers = 4, compresstype = zstd, compresslevel = 1) distributed by (k);
insert into compress_zstd select * from wide_cols;
explain (costs off) :check_cols compress_zstd;
:check_cols compress_zstd;
create table compress_zlib (k int, a int, b bigint, n numeric, t text)
  using ao_column with (parallel_workers = 4, compresstype = zlib, compresslevel = 1) distributed by (k);
insert into compress_zlib select * from wide_cols;
explain (costs off) :check_cols compress_zlib;
:check_cols compress_zlib;
create table compress_rle (k int, a int,
                           b bigint encoding (compresstype = rle_type, compresslevel = 4),
                           n numeric encoding (compresstype = rle_type, compresslevel = 1),
                           t text encoding (compresstype = rle_type, compresslevel = 2))
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into compress_rle select * from wide_cols;
explain (costs off) :check_cols compress_rle;
:check_cols compress_rle;

-- A column added after rows were loaded: rows inserted before the ALTER get
-- the default, rows inserted after it their own value.
create table added_col (k int, a int, t text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into added_col select 0, i, md5(i::text) from generate_series(1, 60000) i;
alter table added_col add column b bigint default -1;
insert into added_col select 0, i, md5(i::text), i::bigint * 7 from generate_series(60001, 100000) i;
\set check_added 'select count(*) as rows, sum(a) as sum_a, sum(b) as sum_b, sum(case when t <> md5(a::text) or b <> case when a <= 60000 then -1 else a::bigint * 7 end then 1 else 0 end) as misaligned, sum(((ctid::text::point)[0]::bigint % 33554432) * 32768 + (ctid::text::point)[1]::bigint - 1) as sum_rownum from'
explain (costs off) :check_added added_col;
:check_added added_col;

--
-- Row visibility
--
-- Chunks cover every row stored in a segment file, including deleted ones.
-- The scan drops deleted rows with the visibility map, which it looks up by
-- the row number of each row, so sum_rownum also checks those row numbers.
--

-- Every 7th row deleted: every chunk has both visible and deleted rows.
create table vis_scattered (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into vis_scattered select 0, i, md5(i::text) from generate_series(1, 100000) i;
delete from vis_scattered where a % 7 = 0;
explain (costs off) :check vis_scattered;
:check vis_scattered;

-- Rows 12788-51148 deleted: the second, third and fourth chunks have no
-- visible rows.
create table vis_chunks (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into vis_chunks select 0, i, md5(i::text) from generate_series(1, 100000) i;
delete from vis_chunks where a between 12788 and 51148;
explain (costs off) :check vis_chunks;
:check vis_chunks;

-- Every 5th row updated: the old version is marked deleted and the new one is
-- appended, so the segment file holds both.
create table vis_updated (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into vis_updated select 0, i, md5(i::text) from generate_series(1, 100000) i;
update vis_updated set a = -a, c = md5((-a)::text) where a % 5 = 0;
explain (costs off) :check vis_updated;
:check vis_updated;

-- Gaps in row numbers: an aborted insert and many small inserts use up row
-- numbers that no stored row has, so row numbers no longer follow row
-- positions.
create table vis_gaps (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into vis_gaps select 0, i, md5(i::text) from generate_series(1, 30000) i;
begin;
insert into vis_gaps select 0, i, md5(i::text) from generate_series(1, 50000) i;
abort;
do $$
begin
  for j in 0..99 loop
    insert into vis_gaps
      select 0, i, md5(i::text) from generate_series(30001 + j * 500, 30500 + j * 500) i;
  end loop;
end $$;
insert into vis_gaps select 0, i, md5(i::text) from generate_series(80001, 100000) i;
select count(*) as rows,
       max(((ctid::text::point)[0]::bigint % 33554432) * 32768 + (ctid::text::point)[1]::bigint - 1) > count(*) as has_gaps
from vis_gaps;
explain (costs off) :check vis_gaps;
:check vis_gaps;

--
-- Filters
--
-- A filter on AOCS columns is evaluated inside the scan, by
-- aocs_getnext_withqual(); its first gp_predicate_pushdown_sample_rows rows go
-- through aocs_getnext_sample().  Both loops must stop at the end of a chunk.
--
create table filt (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (k);
insert into filt select 0, i, md5(i::text) from generate_series(1, 100000) i;

-- One column in the filter: 10000 rows match.
explain (costs off) :check filt where a % 10 = 3;
:check filt where a % 10 = 3;

-- Two columns in the filter.
explain (costs off) :check filt where a % 10 = 3 and c > '8';
:check filt where a % 10 = 3 and c > '8';

-- No row matches, and every row matches.
explain (costs off) :check filt where a < 0;
:check filt where a < 0;
explain (costs off) :check filt where a > 0;
:check filt where a > 0;

-- Sampling over more rows than a chunk, so the sampling loop reaches the end
-- of chunks; and no sampling at all.
set gp_predicate_pushdown_sample_rows = 50000;
explain (costs off) :check filt where a % 10 = 3;
:check filt where a % 10 = 3;
set gp_predicate_pushdown_sample_rows = 0;
explain (costs off) :check filt where a % 10 = 3;
:check filt where a % 10 = 3;
reset gp_predicate_pushdown_sample_rows;

--
-- Joins and aggregates
--

-- A parallel hash join of two parallel scans.
explain (costs off)
select count(*), sum(x.a) from filt x join filt y on x.a = y.a;
select count(*), sum(x.a) from filt x join filt y on x.a = y.a;

-- Partial and final aggregation.
explain (costs off)
select a % 7 as g, count(*), sum(a) from filt group by 1 order by 1;
select a % 7 as g, count(*), sum(a) from filt group by 1 order by 1;

--
-- Partitioned tables
--
-- Each AOCS partition is split into its own chunks.
--
create table part (k int, a int, c text) partition by range (a) distributed by (k);
create table part_1 partition of part for values from (1) to (30001)
  using ao_column with (parallel_workers = 4);
create table part_2 partition of part for values from (30001) to (60001)
  using ao_column with (parallel_workers = 4);
create table part_3 partition of part for values from (60001) to (90001)
  using ao_column with (parallel_workers = 4);
insert into part select 0, i, md5(i::text) from generate_series(1, 90000) i;
analyze part;
explain (costs off) :check part;
:check part;

--
-- Rescans
--
-- A parallel scan hands out each chunk once, so it cannot be scanned again.
-- Neither planner rescans one: it becomes the outer side of a nested loop, is
-- read once into a Materialize, or the plan is not parallel.  These queries
-- have shapes where a rescan could happen.
--
create table outer_rows (a int)
  using ao_column with (parallel_workers = 4) distributed by (a);
insert into outer_rows select i * 1000 from generate_series(1, 20) i;
create table inner_rows (k int, a int, c text)
  using ao_column with (parallel_workers = 4) distributed by (a);
insert into inner_rows select i % 7, i, md5(i::text) from generate_series(1, 200000) i;
analyze outer_rows;
analyze inner_rows;

set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_material = off;

explain (costs off)
select count(*), sum(i.a) from outer_rows o join inner_rows i on i.a < o.a and i.k = 3;
select count(*), sum(i.a) from outer_rows o join inner_rows i on i.a < o.a and i.k = 3;

explain (costs off)
select sum(n) from
  (select (select count(*) from inner_rows i where i.a < o.a and i.k = 3) as n
   from outer_rows o) s;
select sum(n) from
  (select (select count(*) from inner_rows i where i.a < o.a and i.k = 3) as n
   from outer_rows o) s;

explain (costs off)
select count(*) from outer_rows o
  where exists (select 1 from inner_rows i where i.a = o.a + 1);
select count(*) from outer_rows o
  where exists (select 1 from inner_rows i where i.a = o.a + 1);

explain (costs off)
select count(*) from outer_rows o
  where o.a in (select i.a from inner_rows i where i.k = o.a % 7);
select count(*) from outer_rows o
  where o.a in (select i.a from inner_rows i where i.k = o.a % 7);

explain (costs off)
select count(*), sum(l.n) from outer_rows o,
  lateral (select count(*) as n from inner_rows i where i.a < o.a) l;
select count(*), sum(l.n) from outer_rows o,
  lateral (select count(*) as n from inner_rows i where i.a < o.a) l;

reset enable_material;
reset enable_mergejoin;
reset enable_hashjoin;

-- The same plan executed several times.
prepare inner_rows_count(int) as select count(*), sum(a) from inner_rows where k = $1;
explain (costs off) execute inner_rows_count(3);
execute inner_rows_count(3);
execute inner_rows_count(4);
execute inner_rows_count(3);
deallocate inner_rows_count;

-- A cursor.
begin;
declare c cursor for select count(*), sum(a) from inner_rows where k = 3;
fetch all from c;
close c;
commit;

--
-- Planner
--
-- With the Postgres planner, an AOCS table without the parallel_workers
-- option gets workers according to its size, like a heap table, because its
-- segment files are split into chunks.  An AO row table still hands out whole
-- segment files, so it gets at most one worker per segment file.
--
create table plan_small (a int, c text) using ao_column distributed by (a);
insert into plan_small select i, md5(i::text) from generate_series(1, 1000) i;
create table plan_mid (a int, c text) using ao_column distributed by (a);
insert into plan_mid select i, md5(i::text) from generate_series(1, 100000) i;
create table plan_big (a int, c text) using ao_column distributed by (a);
insert into plan_big select i, md5(i::text) from generate_series(1, 1000000) i;
create table plan_option (a int, c text)
  using ao_column with (parallel_workers = 3) distributed by (a);
insert into plan_option select i, md5(i::text) from generate_series(1, 1000) i;
create table plan_row_one (a int, c text) using ao_row distributed by (a);
insert into plan_row_one select i, md5(i::text) from generate_series(1, 1000000) i;
create table plan_row_three (a int, c text) using ao_row distributed by (a);
set gp_appendonly_insert_files = 3;
set gp_appendonly_insert_files_tuples_range = 10000;
insert into plan_row_three select i, md5(i::text) from generate_series(1, 1000000) i;
reset gp_appendonly_insert_files;
reset gp_appendonly_insert_files_tuples_range;
analyze plan_small;
analyze plan_mid;
analyze plan_big;
analyze plan_option;
analyze plan_row_one;
analyze plan_row_three;
select c.relname, a.segfilecount
from pg_class c join pg_appendonly a on a.relid = c.oid
where c.relname like 'plan\_%' order by 1;

set min_parallel_table_scan_size = '8MB';

-- One segment file, and still a parallel plan.
explain (costs off) select count(*) from plan_big;

-- A table smaller than min_parallel_table_scan_size is scanned serially.
explain (costs off) select count(*) from plan_small;

-- The parallel_workers option overrides the size: 3 workers per segment.
explain (costs off) select count(*) from plan_option;

-- A larger table gets more workers, up to max_parallel_workers_per_gather.
set min_parallel_table_scan_size = '128kB';
set max_parallel_workers_per_gather = 8;
explain (costs off) select count(*) from plan_mid;
explain (costs off) select count(*) from plan_big;
set max_parallel_workers_per_gather = 2;
explain (costs off) select count(*) from plan_big;
set max_parallel_workers_per_gather = 8;

-- AO row tables: one segment file gives no parallel plan, three segment
-- files give 3 workers per segment.
explain (costs off) select count(*) from plan_row_one;
explain (costs off) select count(*) from plan_row_three;

-- No parallel index scans on AOCS tables.
create index plan_big_a on plan_big (a);
set enable_seqscan = off;
set min_parallel_index_scan_size = 0;
explain (costs off) select count(*) from plan_big where a < 100;
reset min_parallel_index_scan_size;
reset enable_seqscan;

reset min_parallel_table_scan_size;
set max_parallel_workers_per_gather = 4;

--
-- ORCA
--
-- ORCA plans a parallel scan only where it needs no rewinding, and puts a
-- Materialize above it on the inner side of a nested loop.
--
set optimizer = on;
analyze empty_segfiles;

explain (costs off) :check single_segfile;
:check single_segfile;
explain (costs off) :check empty_segfiles;
:check empty_segfiles;
explain (costs off) :check_cols compress_rle;
:check_cols compress_rle;
explain (costs off) :check vis_gaps;
:check vis_gaps;

-- LIMIT: the processes stop before all chunks are handed out.  The Postgres
-- planner does not choose a parallel scan for these.
explain (costs off) select count(*) from (select a from filt limit 10) s;
select count(*) from (select a from filt limit 10) s;
explain (costs off) select count(*) from (select a from filt limit 20000) s;
select count(*) from (select a from filt limit 20000) s;

set optimizer_enable_parallel_append = on;
explain (costs off) :check part;
:check part;
reset optimizer_enable_parallel_append;

set enable_hashjoin = off;
set enable_mergejoin = off;
explain (costs off)
select count(*), sum(i.a) from outer_rows o join inner_rows i on i.a < o.a and i.k = 3;
select count(*), sum(i.a) from outer_rows o join inner_rows i on i.a < o.a and i.k = 3;
reset enable_mergejoin;
reset enable_hashjoin;

reset optimizer;
reset max_parallel_workers_per_gather;
reset enable_parallel;
drop schema appendonly_parallel_scan cascade;
