--
-- ORCA Parallel Append
--
-- ORCA can implement a scan of a partitioned table as a parallel-aware
-- Append over parallel scans of the selected partitions, instead of a
-- serial Dynamic Seq Scan. The alternative is gated by
-- optimizer_enable_parallel_append and competes with the dynamic scan
-- on cost.
--
-- All GUCs are set locally in this file; the plans must stay stable with
-- 3 segments and max_parallel_workers_per_gather = 2.
--
create schema orca_parallel_append;
set search_path = orca_parallel_append, public;
set optimizer = on;
set optimizer_trace_fallback = on;

create table pa_ao (id int, d int, pad text)
with (appendonly = true)
distributed by (id)
partition by range (d) (start (1) end (6) every (1));

insert into pa_ao
select g, 1 + (g % 5), repeat('x', 20) from generate_series(1, 5000) g;
analyze pa_ao;

-- plain heap table, single level partitioning
create table pa_heap (id int, d int)
distributed by (id)
partition by list (d) (partition p1 values (1), partition p2 values (2), partition p3 values (3));
insert into pa_heap select g, 1 + (g % 3) from generate_series(1, 300) g;
analyze pa_heap;

-- empty partitioned table
create table pa_empty (id int, d int)
distributed by (id)
partition by range (d) (start (1) end (4) every (1));
analyze pa_empty;

-- non-partitioned side for joins and set operations
create table pa_dim (d int, name text) distributed by (d);
insert into pa_dim values (1, 'one'), (2, 'two'), (3, 'three');
analyze pa_dim;

set enable_parallel = on;
set max_parallel_workers_per_gather = 2;
set parallel_setup_cost = 0;
set min_parallel_table_scan_size = 0;
set min_parallel_index_scan_size = 0;

--
-- GUC off: serial dynamic scan
--
set optimizer_enable_parallel_append = off;
explain (costs off) select count(*) from pa_ao;
select count(*) from pa_ao;

--
-- GUC on: parallel append over parallel seq scans
--
set optimizer_enable_parallel_append = on;
explain (costs off) select count(*) from pa_ao;
select count(*) from pa_ao;
select d, count(*) from pa_ao group by d order by d;

-- heap partitions
explain (costs off) select count(*) from pa_heap;
select count(*) from pa_heap;

-- static partition pruning: multiple, one and zero partitions survive
explain (costs off) select count(*) from pa_ao where d >= 4;
select count(*) from pa_ao where d >= 4;
explain (costs off) select count(*) from pa_ao where d = 2;
select count(*) from pa_ao where d = 2;
explain (costs off) select count(*) from pa_ao where d = 42;
select count(*) from pa_ao where d = 42;

-- empty partitioned table
explain (costs off) select count(*) from pa_empty;
select count(*) from pa_empty;

-- dynamic partition elimination: the partition selector stays on the
-- hash side, the parallel append consumes its selector id
explain (costs off)
select count(*) from pa_ao join pa_dim using (d) where pa_dim.name = 'two';
select count(*) from pa_ao join pa_dim using (d) where pa_dim.name = 'two';

-- predicate and projection pushed into the per-partition scans
explain (costs off) select id, d from pa_ao where id < 10 order by id;
select id, d from pa_ao where id < 10 order by id;

-- set operations with a partitioned child are not parallelized: a serial
-- sibling under the same Append would be executed by every worker and
-- its rows duplicated
explain (costs off) select d from pa_dim union all select d from pa_heap;
select d, count(*) from (select d from pa_dim union all select d from pa_heap) s group by d order by d;
select count(*) from (select d from pa_dim union select d from pa_heap) s;

-- nested loop inner side requires rewindability, which the parallel append
-- cannot provide; the plan must still be correct
set optimizer_enable_hashjoin = off;
explain (costs off)
select count(*) from pa_dim join pa_heap on pa_dim.d = pa_heap.d;
select count(*) from pa_dim join pa_heap on pa_dim.d = pa_heap.d;
reset optimizer_enable_hashjoin;

-- results match the serial plan and the Postgres planner
set optimizer_enable_parallel_append = off;
select count(*) from pa_ao join pa_dim using (d) where pa_dim.name = 'two';
set optimizer = off;
select count(*) from pa_ao join pa_dim using (d) where pa_dim.name = 'two';
select count(*) from pa_ao;

reset optimizer;
reset optimizer_enable_parallel_append;
reset enable_parallel;
reset max_parallel_workers_per_gather;
reset parallel_setup_cost;
reset min_parallel_table_scan_size;
reset min_parallel_index_scan_size;
reset optimizer_trace_fallback;

-- start_ignore
drop schema orca_parallel_append cascade;
-- end_ignore
