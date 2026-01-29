--
-- CBDB PARALLEL
-- Test CBDB style parallel plan.
-- GUCs shoule be set with local, do not disturb other parallel plans.
-- Should not use force_parallel_mode as it will ignore plan and check results only.
-- We want to check plan in this file!
-- If there is need to do that, set it local inside a transaction.
-- Set optimizer off in this file, ORCA parallel is not supported.
--
-- Locus check expression:
-- This is just used to check locus codes in cdbpath_motion_for_parallel_join/cdbpathlocus_parallel_join
-- with corresponding examples quickly for parallel join.
-- Format:
--  1_2_3 means locus 1 join locus 2 generate locus 3.
--  1_P_2_3 means locus 1 Join(with shared hash table) locus 2 generate locus 3.
--  All this format represents for parallel join, while P implies it's a parallel_aware join.
--
-- The numbers steal from CdbLocusType enum.
--  0   CdbLocusType_Null
--  1   CdbLocusType_Entry
--  2   CdbLocusType_SingleQE
--  3   CdbLocusType_General
--  4   CdbLocusType_SegmentGeneral
--  5   CdbLocusType_SegmentGeneralWorkers
--  6   CdbLocusType_OuterQuery
--  7   CdbLocusType_Replicated
--  8   CdbLocusType_ReplicatedWorkers
--  9   CdbLocusType_Hashed
--  10  CdbLocusType_HashedOJ
--  11  CdbLocusType_Strewn
--  12  CdbLocusType_HashedWorkers
--
--
set force_parallel_mode = 0;
set optimizer = off;

create schema test_parallel;
set search_path to test_parallel;
-- set this to default in case regress change it by gpstop.
set gp_appendonly_insert_files = 4;

--
-- Parallel Parallel Join: DEDUP_SEMI and DEDUP_SEMI_REVERSE
--
begin;
create table foo(a int) with(parallel_workers=2) distributed randomly;
create table bar(b int) with(parallel_workers=2) distributed randomly;
insert into foo select i from generate_series(1, 1000)i;
insert into bar select i from generate_series(1, 20000)i;
analyze foo;
analyze bar;

-- non-parallel
set local enable_parallel = off;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
set local enable_parallel = on;

-- Parallel DEDUP_SEMI
set local enable_parallel_semi_join = off; 
set local enable_parallel_dedup_semi_reverse_join = off;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

-- Parallel DEDUP_SEMI_REVERSE
set local enable_parallel_semi_join = off; 
set local enable_parallel_dedup_semi_reverse_join = on;
set local enable_parallel_dedup_semi_join = on;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

-- Parallel oblivious
set local enable_parallel_hash = off;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

set local enable_parallel_dedup_semi_reverse_join = off;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

-- Parallel Nestloop DEDUP_SEMI
set local min_parallel_table_scan_size = 0;
set local enable_parallel_semi_join = off; 
set local enable_nestloop = on;
set local enable_hashjoin = off;
set local enable_mergejoin = off;
set local enable_parallel_semi_join = off; 
set local enable_parallel_dedup_semi_reverse_join = off;
set local enable_parallel_dedup_semi_join = on;
explain (costs off)
select sum(bar.b) from bar where exists (select 1 from foo where foo.a = bar.b);
select sum(bar.b) from bar where exists (select 1 from foo where foo.a = bar.b);

-- Parallel Nestloop DEDUP_SEMI_REVERSE
set local enable_parallel_dedup_semi_reverse_join = on;
set local enable_parallel_dedup_semi_join = off;
explain (costs off)
select sum(bar.b) from bar where exists (select 1 from foo where foo.a = bar.b);
select sum(bar.b) from bar where exists (select 1 from foo where foo.a = bar.b);

-- Parallel Mergejoin DEDUP_SEMI
set local enable_parallel_semi_join = off; 
set local enable_hashjoin = off;
set local enable_mergejoin = on;
set local enable_nestloop = off;
set local enable_parallel_semi_join = off; 
set local enable_parallel_dedup_semi_reverse_join = off;
set local enable_parallel_dedup_semi_join = on;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

-- Parallel Mergejoin DEDUP_SEMI_REVERSE
set local enable_parallel_dedup_semi_reverse_join = on;
set local enable_parallel_dedup_semi_join = off;
explain (costs off)
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);
select sum(foo.a) from foo where exists (select 1 from bar where foo.a = bar.b);

abort;


-- CBDB(#131): test parallel_workers during create AO/AOCO table take effect
begin;
set local enable_parallel = on;
create table test_131_ao1(x int, y int) using ao_row with(parallel_workers=2);
create table test_131_ao2(x int, y int) using ao_row with(parallel_workers=2);
create table test_131_ao3(x int, y int) using ao_row with(parallel_workers=0);
create table test_131_ao4(x int, y int) using ao_row with(parallel_workers=0);
create table test_131_aoco1(x int, y int) using ao_column with(parallel_workers=2);
create table test_131_aoco2(x int, y int) using ao_column with(parallel_workers=2);
create table test_131_aoco3(x int, y int) using ao_column with(parallel_workers=0);
create table test_131_aoco4(x int, y int) using ao_column with(parallel_workers=0);

select relname, reloptions from pg_catalog.pg_class where relname like 'test_131_ao%';
explain(locus, costs off) select count(*) from test_131_ao1, test_131_ao2 where test_131_ao1.x = test_131_ao2.x;
explain(locus, costs off) select count(*) from test_131_ao3, test_131_ao4 where test_131_ao3.x = test_131_ao4.x;
explain(locus, costs off) select count(*) from test_131_aoco1, test_131_aoco2 where test_131_aoco1.x = test_131_aoco2.x;
explain(locus, costs off) select count(*) from test_131_aoco3, test_131_aoco4 where test_131_aoco3.x = test_131_aoco4.x;
abort;

create table ao1(x int, y int) with(appendonly=true);
create table ao2(x int, y int) with(appendonly=true);
create table aocs1(x int, y int) with(appendonly=true, orientation=column);

begin;

-- encourage use of parallel plans
set local min_parallel_table_scan_size = 0;
set local max_parallel_workers_per_gather = 4;
-- test insert into multiple files even enable_parallel is off.
set local enable_parallel = off;

-- insert multiple segfiles for parallel
set local gp_appendonly_insert_files = 4;
set local gp_appendonly_insert_files_tuples_range = 50000;

-- test appendonly table parallel 
insert into ao1 select i, i from generate_series(1, 1200000) g(i);
analyze ao1;
insert into ao2 select i%10, i from generate_series(1, 1200000) g(i);
analyze ao2;
select segfilecount from pg_appendonly where relid = 'ao1'::regclass;
set local enable_parallel = on;
explain(costs off) select count(*) from ao1;
select count(*) from ao1;

-- test aocs table parallel 
set local enable_parallel = off;
insert into aocs1 select i, i from generate_series(1, 1200000) g(i);
analyze aocs1;
select segfilecount from pg_appendonly where relid = 'aocs1'::regclass;
set local enable_parallel = on;
explain(costs off) select count(*) from aocs1;
select count(*) from aocs1;

-- test locus of HashedWorkers can parallel join without motion
explain(locus, costs off) select count(*) from ao1, ao2 where ao1.x = ao2.x;
select count(*) from ao1, ao2 where ao1.x = ao2.x;

reset enable_parallel;
commit;

--
-- test parallel with indices
--
create index on ao1(y);
create index on aocs1(y);
analyze ao1;
analyze aocs1;

-- test AO/AOCS should not be IndexScan
begin;
set local enable_parallel = on;
set local enable_seqscan = off;
set local enable_indexscan = on;
set local enable_bitmapscan = on;

set local max_parallel_workers_per_gather=1;
explain(costs off) select y from ao1 where y > 1000000;
explain(costs off) select y from aocs1 where y > 1000000;
set local max_parallel_workers_per_gather=0;
explain(costs off) select y from ao1 where y > 1000000;
explain(costs off) select y from aocs1 where y > 1000000;
commit;

drop table ao1;
drop table ao2;
drop table aocs1;

-- test Parallel Bitmap Heap Scan
begin;
create table t1(c1 int, c2 int) with(parallel_workers=2) distributed by (c1);
set local enable_parallel = on;
create index on t1(c2);
insert into t1 select i, i from generate_series(1, 10000000) i;
analyze t1;
set local force_parallel_mode = 1;
set local enable_seqscan = off;
explain(locus, costs off) select c2 from t1;
-- results check
explain(locus, costs off) select count(c2) from t1;
select count(c2) from t1;
set local enable_parallel = off;
explain(locus, costs off) select count(c2) from t1;
select count(c2) from t1;
abort;

-- test segfilecount according to data volume.
create table ao_segfilecount(x int, y int) with(appendonly=true);
create table aocs_segfilecount(x int, y int) with(appendonly=true, orientation=column);
begin;
set local gp_appendonly_insert_files = 5;
set local gp_appendonly_insert_files_tuples_range = 10;

-- no enough data, open only one segment file.
insert into ao_segfilecount select i, i from generate_series(1, 29) g(i);
analyze ao_segfilecount;
select segfilecount from pg_appendonly where relid='ao_segfilecount'::regclass;

-- no enough data, open two segment files.
insert into ao_segfilecount select i, i from generate_series(1, 60) g(i);
analyze ao_segfilecount;
select segfilecount from pg_appendonly where relid='ao_segfilecount'::regclass;

-- proper data, open segment file according to GUC.
insert into ao_segfilecount select i, i from generate_series(1, 150) g(i);
analyze ao_segfilecount;
select segfilecount from pg_appendonly where relid='ao_segfilecount'::regclass;

-- excess data, open segment file according to GUC.
insert into ao_segfilecount select i, i from generate_series(1, 200) g(i);
analyze ao_segfilecount;
select segfilecount from pg_appendonly where relid='ao_segfilecount'::regclass;

-- no enough data, open only one segment file.
insert into aocs_segfilecount select i, i from generate_series(1, 29) g(i);
analyze aocs_segfilecount;
select segfilecount from pg_appendonly where relid='aocs_segfilecount'::regclass;

-- no enough data, open two segment files.
insert into aocs_segfilecount select i, i from generate_series(1, 60) g(i);
analyze aocs_segfilecount;
select segfilecount from pg_appendonly where relid='aocs_segfilecount'::regclass;

-- proper data, open segment file according to GUC.
insert into aocs_segfilecount select i, i from generate_series(1, 150) g(i);
analyze aocs_segfilecount;
select segfilecount from pg_appendonly where relid='aocs_segfilecount'::regclass;

-- excess data, open segment file according to GUC.
insert into aocs_segfilecount select i, i from generate_series(1, 200) g(i);
analyze aocs_segfilecount;
select segfilecount from pg_appendonly where relid='aocs_segfilecount'::regclass;
abort;
drop table ao_segfilecount;
drop table aocs_segfilecount;

-- test gp_appendonly_insert_files doesn't take effect
begin;

create table t (x int);
insert into t select i from generate_series(1, 1000) i;
set local gp_appendonly_insert_files=4;
set local gp_appendonly_insert_files_tuples_range = 10;

create table ao1 using ao_row as select * from t;
analyze ao1;
select segfilecount from pg_appendonly where relid='ao1'::regclass;

create table ao2 with(appendonly=true) as select * from t;
analyze ao2;
select segfilecount from pg_appendonly where relid='ao2'::regclass;

create table aocs1  using ao_column as select * from t;
analyze aocs1;
select segfilecount from pg_appendonly where relid='aocs1'::regclass;

create table aocs2 with(appendonly=true, orientation=column) as select * from t;
analyze aocs2;
select segfilecount from pg_appendonly where relid='aocs2'::regclass;

abort;

-- test replicated tables parallel
begin;
set local max_parallel_workers_per_gather = 2;
create table t1(a int, b int) with(parallel_workers=2);
create table rt1(a int, b int) with(parallel_workers=2) distributed replicated;
create table rt2(a int, b int) distributed replicated;
create table rt3(a int, b int) distributed replicated;
insert into t1 select i, i from generate_series(1, 100000) i;
insert into t1 select i, i+1 from generate_series(1, 10) i;
insert into rt1 select i, i+1 from generate_series(1, 10) i;
insert into rt2 select i, i+1 from generate_series(1, 10000) i;
insert into rt3 select i, i+1 from generate_series(1, 10) i;
analyze t1;
analyze rt1;
analyze rt2;
analyze rt3;

-- replica parallel select
set local enable_parallel = off;
explain(locus, costs off) select * from rt1;
select * from rt1;
set local enable_parallel = on;
explain(locus, costs off) select * from rt1;
select * from rt1;

-- replica join replica
set local enable_parallel = off;
select * from rt1 join rt2 on rt2.b = rt1.a;
set local enable_parallel = on;
explain(locus, costs off) select * from rt1 join rt2 on rt2.b = rt1.a;
select * from rt1 join rt2 on rt2.b = rt1.a;
--
-- ex 5_P_5_5
-- SegmentGeneralWorkers parallel join SegmentGeneralWorkers when parallel_aware generate SegmentGeneralWorerks locus.
--
set local min_parallel_table_scan_size = 0;
explain(locus, costs off) select * from rt1 join rt2 on rt2.b = rt1.a;
select * from rt1 join rt2 on rt2.b = rt1.a;

--
-- ex 5_4_5
-- SegmentGeneralWorkers parallel join SegmentGeneral generate SegmentGeneralWorkers locus. 
--
set local enable_parallel_hash = off;
explain(locus, costs off) select * from rt1 join rt2 on rt2.b = rt1.a;
select * from rt1 join rt2 on rt2.b = rt1.a;

--
--  t1 join rt1 join rt2
--
set local enable_parallel = off;
explain(locus, costs off) select * from rt1 join t1 on rt1.a = t1.b join rt2 on rt2.a = t1.b;
select * from rt1 join t1 on rt1.a = t1.b join rt2 on rt2.a = t1.b;
-- parallel hash join
set local enable_parallel = on;
set local enable_parallel_hash = on;
--
-- SegmentGeneralWorkers parallel join HashedWorkers when parallel_aware generate HashedWorkers.
-- ex 12_P_5_12
-- HashedWorkers parallel join SegmentGeneralWorkers when parallel_aware generate HashedWorkers.
--
explain(locus, costs off) select * from rt1 join t1 on rt1.a = t1.b join rt2 on rt2.a = t1.b;
select * from rt1 join t1 on rt1.a = t1.b join rt2 on rt2.a = t1.b;

--
--  t1 join rt1 join rt3
--
set local enable_parallel = off;
explain(locus, costs off) select * from rt1 join t1 on rt1.a = t1.b join rt3 on rt3.a = t1.b;
select * from rt1 join t1 on rt1.a = t1.b join rt3 on rt3.a = t1.b;
-- parallel join without parallel hash
set local enable_parallel = on;
set local enable_parallel_hash = off;
-- HashedWorkers parallel join SegmentGeneral generate HashedWorkers.
explain(locus, costs off) select * from rt1 join t1 on rt1.a = t1.b join rt3 on rt3.a = t1.b;
select * from rt1 join t1 on rt1.a = t1.b join rt3 on rt3.a = t1.b;

create table t2(a int, b int) with(parallel_workers=0);
create table rt4(a int, b int) with(parallel_workers=2) distributed replicated;
insert into t2 select i, i+1 from generate_series(1, 10) i;
insert into rt4 select i, i+1 from generate_series(1, 10000) i;
analyze t2;
analyze rt4;
set local enable_parallel = off;
select * from rt4 join t2 using(b);
set local enable_parallel = on;
set local enable_parallel_hash = off;
--
-- ex 5_9_12
-- SegmentGeneralWorkers(w=N) parallel join Hashed(W=0) generate HashedWorkers(w=N).
--
explain(locus, costs off) select * from rt4 join t2 using(b);
select * from rt4 join t2 using(b);

create table t3(a int, b int) with(parallel_workers=2);
insert into t3 select i, i+1 from generate_series(1, 9000) i;
analyze t3;
set local enable_parallel = off;
select count(*) from rt4 join t3 using(b);
set local enable_parallel = on;
set local enable_parallel_hash = on;
--
-- ex 5_P_12_12
-- SegmentGeneralWorkers parallel join HashedWorkers when parallel_aware generate HashedWorkers.
--
explain(locus, costs off) select * from rt4 join t3 using(b);
select count(*) from rt4 join t3 using(b);

abort;


--
-- ex 5_11_11
-- SegmentGeneralWorkers(workers=N) join Strewn(worker=0) without shared hash table.
-- Join locus: Strewn(worker=N).
--
begin;
create table t_replica_workers_2(a int, b int) with(parallel_workers=2) distributed replicated;
insert into t_replica_workers_2 select i, i+1 from generate_series(1, 10) i;
analyze t_replica_workers_2;
create table t_random_workers_0(a int, b int) with(parallel_workers=0) distributed randomly;
insert into t_random_workers_0 select i, i+1 from generate_series(1, 5) i;
analyze t_random_workers_0;
set local enable_parallel= true;
set local enable_parallel_hash= false;
explain(locus, costs off) select * from t_replica_workers_2 join t_random_workers_0 using(a);
select * from t_replica_workers_2 join t_random_workers_0 using(a);
-- non parallel results
set local enable_parallel=false;
select * from t_replica_workers_2 join t_random_workers_0 using(a);
abort;

--
-- ex 11_P_5_11
-- Strewn(worker=N) join SegmentGeneralWorkers(workers=N) with shared hash table.
-- Join locus: Strewn(worker=N).
--
begin;
create table t_replica_workers_2(a int, b int) with(parallel_workers=2) distributed replicated;
insert into t_replica_workers_2 select i, i+1 from generate_series(1, 10) i;
analyze t_replica_workers_2;
create table t_random_workers_2(a int, b int) with(parallel_workers=2) distributed randomly;
insert into t_random_workers_2 select i, i+1 from generate_series(1, 5) i;
analyze t_random_workers_2;
set local enable_parallel= true;
set local enable_parallel_hash= true;
explain(locus, costs off) select * from t_replica_workers_2 right join t_random_workers_2 using(a);
select * from t_replica_workers_2 right join t_random_workers_2 using(a);
-- non parallel results
set local enable_parallel=false;
select * from t_replica_workers_2 right join t_random_workers_2 using(a);
abort;


--
-- ex 5_P_11_11
-- SegmentGeneralWorkers(workers=N) join Strewn(workers=N) with shared hash table.
-- Join locus: Strewn(workers=N).
--
begin;
create table t_replica_workers_2(a int, b int) with(parallel_workers=2) distributed replicated;
insert into t_replica_workers_2 select i, i+1 from generate_series(1, 10) i;
analyze t_replica_workers_2;
create table t_random_workers_2(a int, b int) with(parallel_workers=2) distributed randomly;
insert into t_random_workers_2 select i, i+1 from generate_series(1, 5) i;
analyze t_random_workers_2;
set local enable_parallel= true;
set local enable_parallel_hash= true;
explain(locus, costs off) select * from t_replica_workers_2 join t_random_workers_2 using(a);
select * from t_replica_workers_2 join t_random_workers_2 using(a);
-- non parallel results
set local enable_parallel=false;
select * from t_replica_workers_2 join t_random_workers_2 using(a);
abort;

--
-- ex 2_P_5_2
-- SingleQE join SegmentGeneralWorkers.
-- Join locus: SingleQE(may be elided to Entry).
--
begin;
create table t1(a int, b int) with(parallel_workers=2);
create table rt1(a int, b int) with(parallel_workers=2) distributed replicated;
insert into t1 select i, i from generate_series(1, 100000) i;
insert into rt1 select i, i+1 from generate_series(1, 10000) i;
analyze t1;
analyze rt1;
set local enable_parallel = on;
explain(locus, costs off) select * from (select count(*) as a from t1) t1 left join rt1  on rt1.a = t1.a;
select * from (select count(*) as a from t1) t1 left join rt1  on rt1.a = t1.a;
set local enable_parallel = off;
select * from (select count(*) as a from t1) t1 left join rt1  on rt1.a = t1.a;
abort;

--
-- ex 5_P_2_2
-- SingleQE join SegmentGeneralWorkers.
-- Join locus: SingleQE(may be elided to Entry).
--
begin;
set local enable_parallel = on;
set local max_parallel_workers_per_gather = 4;
create table t1(a int, b int) with(parallel_workers=4);
create table t2(a int, b int) with(parallel_workers=4);
create table rt1(a int, b int) with(parallel_workers=4) distributed replicated;
insert into t1 select i, i from generate_series(1, 10000000) i;
insert into t2 select i, i from generate_series(1, 10000000) i;
insert into rt1 select i, i+1 from generate_series(1, 10000) i;
analyze t1;
analyze t2;
analyze rt1;
explain(costs off, locus) select * from rt1  join (select count(*) as c, sum(t1.a) as a  from t1 join t2 using(a)) t3 on t3.c = rt1.a;
select * from rt1  join (select count(*) as c, sum(t1.a) as a  from t1 join t2 using(a)) t3 on t3.c = rt1.a;
set local enable_parallel = off;
explain(costs off, locus) select * from rt1  join (select count(*) as c, sum(t1.a) as a  from t1 join t2 using(a)) t3 on t3.c = rt1.a;
select * from rt1  join (select count(*) as c, sum(t1.a) as a  from t1 join t2 using(a)) t3 on t3.c = rt1.a;
abort;

--
-- Test final join path's parallel_workers should be same with join_locus whose
-- parallel_workers is different from origin outer path(without motion).
--
begin;
create table t1(a int, b int) with(parallel_workers=3);
create table t2(b int, a int) with(parallel_workers=2);
insert into t1 select i, i+1 from generate_series(1, 10) i;
insert into t2 select i, i+1 from generate_series(1, 5) i;
analyze t1;
analyze t2;
set local optimizer=off;
set local enable_parallel=on;
set local max_parallel_workers_per_gather= 4;
explain(costs off) select * from t1 right join t2 on t1.b = t2.a;
abort;

--
-- Test SingleQE locus could particapte in parallel plan.
--
begin;
create table t1(a int, b int) with(parallel_workers=2);
create table t2(a int, b int) with(parallel_workers=2);
insert into t1 select i%10, i from generate_series(1, 5) i;
insert into t1 values (100000);
insert into t2 select i%10, i from generate_series(1, 100000) i;
analyze t1;
analyze t2;
set local enable_parallel = on;
-- parallel hash join with shared table, SinglQE as outer partial path.
explain(locus, costs off) select * from (select count(*) as a from t2) t2 left join t1 on t1.a = t2.a;
select * from (select count(*) as a from t2) t2 left join t1 on t1.a = t2.a;
set local enable_parallel = off;
select * from (select count(*) as a from t2) t2 left join t1 on t1.a = t2.a;
set local enable_parallel = on;
-- parallel hash join with shared table, SinglQE as inner partial path.
explain(locus, costs off) select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
set local enable_parallel = off;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
set local enable_parallel = on;
-- parallel hash join without shared table.
set local enable_parallel_hash = off;
explain(locus, costs off) select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
-- parallel merge join
set local enable_hashjoin = off;
explain(locus, costs off) select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
-- parallel nestloop join
set local enable_mergejoin = off;
set local enable_nestloop = on;
explain(locus, costs off) select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
-- non-parallel results
set local enable_parallel = off;
select * from t1 join (select count(*) as a from t2) t2 on t1.a = t2.a;
abort;

begin;
-- use rt1 to generate locus of SegmentGeneralWorkers
-- use rt2 to generate locus of SegmentGeneral
-- use t1 to generate locus of HashedWorkers
-- use t2 to generate locus of Hahsed
-- use pg_class to generate locus of Entry
-- use generate_series(1, 1000) to generate locus of General
-- use select count(*) as a from sq1 to generate locus of SingleQE
create table rt1(a int, b int) distributed replicated;
create table rt2(a int, b int) with (parallel_workers = 0) distributed replicated;
create table t1(a int, b int);
create table t2(a int, b int) with (parallel_workers = 0);
insert into t1 select i, i+1 from generate_series(1, 10000) i;
insert into t2 select i, i+1 from generate_series(1, 10000) i;
insert into rt1 select i, i+1 from generate_series(1, 10000) i;
insert into rt2 select i, i+1 from generate_series(1, 10000) i;
CREATE TABLE sq1 AS SELECT a, b FROM t1 WHERE gp_segment_id = 0;
set local optimizer=off;
set local enable_parallel=on;
set local min_parallel_table_scan_size to 0;
set local max_parallel_workers_per_gather= 4;
analyze rt1;
analyze rt2;
analyze t1;
analyze t2;
analyze sq1;

-- SegmentGeneralWorkers + SegmengGeneralWorkers = SegmentGeneralWorkers
explain (locus, costs off) select * from rt1 union all select * from rt1;
-- SegmentGeneralWorkers + SegmentGeneral = SegmentGeneralWorkers
explain (locus, costs off) select * from rt1 union all select * from rt2;
-- SegmentGeneralWorkers (Converted to Strewn, Limited on One Segment) + HashedWorkers = Strewn
explain (locus, costs off) select * from rt1 union all select * from t1;
-- SegmentGeneralWorkers (Converted to Strewn, Limited on One Segment) + Hashed = Strewn
explain (locus, costs off) select * from rt1 union all select * from t2;
-- partial subpath under UNION ALL
explain (locus, costs off) select a from rt1 union all select count(*) as a from sq1;
-- SegmentGeneralWorkers + General = SegmentGeneralWorkers
explain (locus, costs off) select a from rt1 union all select a from generate_series(1, 1000) a;
-- Entry as subquery seems cannot produce partial_pathlist and don't have chance to parallel append.
-- flaky case failed: expected use seqscan on pg_class but choose indexscan sometimes.
set local enable_indexscan = off;
set local enable_indexonlyscan = off;
explain (locus, costs off) select a from rt1 union all select oid as a from pg_class;
abort;

--
-- Test two-phase parallel Limit
--
begin;
create table t1(c1 int, c2 int) with(parallel_workers=2);
insert into t1 select i, i+1 from generate_series(1, 100000) i;
analyze t1;
set local optimizer = off;
set local enable_parallel = on;
explain(costs off, locus) select * from t1 order by c2 asc limit 3 offset 5;
select * from t1 order by c2 asc limit 3 offset 5;
-- non-parallel results
set local enable_parallel = off;
explain(costs off, locus) select * from t1 order by c2 asc limit 3 offset 5;
select * from t1 order by c2 asc limit 3 offset 5;
abort;

--
-- Test one-phase Limit with parallel subpath
--
begin;
create table t1(c1 int, c2 int) with(parallel_workers=2);
insert into t1 select i, i+1 from generate_series(1, 100000) i;
analyze t1;
set local optimizer = off;
set local gp_enable_multiphase_limit = off;
set local enable_parallel = on;
explain(costs off, locus) select * from t1 order by c2 asc limit 3 offset 5;
select * from t1 order by c2 asc limit 3 offset 5;
-- non-parallel results
set local enable_parallel = off;
explain(costs off, locus) select * from t1 order by c2 asc limit 3 offset 5;
select * from t1 order by c2 asc limit 3 offset 5;
abort;

--
-- Test Parallel Hash Left Anti Semi (Not-In) Join(parallel-oblivious).
--
create table t1(c1 int, c2 int) using ao_row distributed by (c1);
create table t2(c1 int, c2 int) using ao_row distributed by (c1);
create table t3_null(c1 int, c2 int) using ao_row distributed by (c1);
begin;
set local enable_parallel = on;
set local gp_appendonly_insert_files = 2;
set local gp_appendonly_insert_files_tuples_range = 100;
set local max_parallel_workers_per_gather = 2;
set local enable_parallel_hash = off;
insert into t1 select i, i from generate_series(1, 5000000) i;
insert into t2 select i+1, i from generate_series(1, 1200) i;
insert into t3_null select i+1, i from generate_series(1, 1200) i;
insert into t3_null values(NULL, NULL);
analyze t1;
analyze t2;
analyze t3_null;
explain(costs off) select sum(t1.c1) from t1 where c1 not in (select c1 from t2);
select sum(t1.c1) from t1 where c1 not in (select c1 from t2);
explain(costs off) select * from t1 where c1 not in (select c1 from t3_null);
select * from t1 where c1 not in (select c1 from t3_null);
-- non-parallel results.
set local enable_parallel = off;
select sum(t1.c1) from t1 where c1 not in (select c1 from t2);
select * from t1 where c1 not in (select c1 from t3_null);
end;
drop table t1;
drop table t2;
drop table t3_null;
--
-- End of Test Parallel Hash Left Anti Semi (Not-In) Join.
--

--
-- Test Parallel-aware Hash Left Anti Semi (Not-In) Join.
--
begin;
create table t1(c1 int, c2 int) with(parallel_workers=2) distributed by (c1);
create table t2(c1 int, c2 int) with(parallel_workers=2) distributed by (c1);
create table t3_null(c1 int, c2 int) with(parallel_workers=2) distributed by (c1);
set local enable_parallel = on;
set local max_parallel_workers_per_gather = 2;
insert into t1 select i, i from generate_series(1, 500000) i;
insert into t2 select i, i+1 from generate_series(1, 500000) i;
insert into t3_null select i, i+1 from generate_series(1, 500000) i;
insert into t3_null values(NULL, NULL);
analyze t1;
analyze t2;
analyze t3_null;
explain(costs off) select sum(t1.c1) from t1 where c1 not in (select c2 from t2);
select sum(t1.c1) from t1 where c1 not in (select c2 from t2);
explain(costs off) select * from t1 where c1 not in (select c2 from t3_null);
select * from t1 where c1 not in (select c2 from t3_null);
-- non-parallel results.
set local enable_parallel = off;
select sum(t1.c1) from t1 where c1 not in (select c2 from t2);
select * from t1 where c1 not in (select c2 from t3_null);
drop table t1;
drop table t2;
drop table t3_null;
end;
--
-- End of Test Parallel-aware Hash Left Anti Semi (Not-In) Join.
--

--
-- Test alter ao/aocs table parallel_workers options
--
begin;
set local optimizer = off;
set local enable_parallel = on;
-- ao table
create table ao (a INT, b INT) using ao_row;
insert into ao select i as a, i as b from generate_series(1, 100) AS i;
alter table ao set (parallel_workers = 2);
explain(costs off) select count(*) from ao;
select count(*) from ao;
alter table ao reset (parallel_workers);
-- aocs table
create table aocs (a INT, b INT) using ao_column;
insert into aocs select i as a, i as b from generate_series(1, 100) AS i;
alter table aocs set (parallel_workers = 2);
explain(costs off) select count(*) from aocs;
select count(*) from aocs;
alter table aocs reset (parallel_workers);
abort;

--
-- Test locus after eliding mtion node.
--
begin;
create table t1(c1 int) distributed by (c1);
insert into t1 values(11), (12);
analyze t1;
explain(costs off, locus) select distinct min(c1), max(c1) from t1;
abort;

begin;
create table t1(id int) distributed by (id);
create index on t1(id);
insert into t1 values(generate_series(1, 100));
analyze t1;
set enable_seqscan =off;
explain (locus, costs off)
select * from
  (select count(id) from t1 where id > 10) ss
  right join (values (1),(2),(3)) v(x) on true;
abort;

begin;
create table pagg_tab (a int, b int, c text, d int) partition by list(c);
create table pagg_tab_p1 partition of pagg_tab for values in ('0000', '0001', '0002', '0003', '0004');
create table pagg_tab_p2 partition of pagg_tab for values in ('0005', '0006', '0007', '0008');
create table pagg_tab_p3 partition of pagg_tab for values in ('0009', '0010', '0011');
insert into pagg_tab select i % 20, i % 30, to_char(i % 12, 'FM0000'), i % 30 from generate_series(0, 2999) i;
analyze pagg_tab;
set local enable_parallel to off;
set local enable_partitionwise_aggregate to true;
set local enable_partitionwise_join to true;
set local enable_incremental_sort to off;
set local enable_hashagg to false;
set local enable_parallel = off;
explain (costs off, locus)
select c, sum(a), avg(b), count(*) from pagg_tab group by 1 having avg(d) < 15 order by 1, 2, 3;
abort;
--
-- End of Test locus after eliding mtion node.
--

--
-- Test outer path has Motion of parallel plan. 
--
begin;
create table t1(a int, b int) with(parallel_workers=3);
create table t2(b int, a int) with(parallel_workers=2);
insert into t1 select i, i+1 from generate_series(1, 10) i;
insert into t2 select i, i+1 from generate_series(1, 5) i;
analyze t1;
analyze t2;
set local optimizer=off;
set local enable_parallel=on;
set local enable_parallel_hash=off;
set local max_parallel_workers_per_gather= 4;
explain(costs off) select * from t1 right join t2 on t1.b = t2.a;
abort;
--
-- Parallel Refresh AO Materialized View
--
create or replace function refresh_compare(ao_row bool, verbose bool, OUT parallel_is_better bool) as $$
declare
 t timestamptz;
 dur0 interval;
 dur1 interval;
 results0 RECORD;
 results1 RECORD;
begin
  create table t_p(c1 int, c2 int) with(parallel_workers=8) distributed by(c1);
  insert into t_p select i, i+1 from generate_series(1, 10000000)i;
  analyze t_p;
  if ao_row then
    create materialized view matv using ao_row as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p a join t_p b on a.c1 = b.c1 with no data distributed by(c2);
  else
    create materialized view matv using ao_column as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p a join t_p b on a.c1 = b.c1 with no data distributed by(c2);
  end if;
  -- refresh
  set enable_parallel=off;
  t = clock_timestamp();
  refresh materialized view matv;
  dur0 = age(clock_timestamp(), t);
  select * into results0 from matv;
  if refresh_compare.verbose then
    raise notice 'Non-parallel refresh duration=%', dur0;
    raise notice 'Non-parallel results=%', results0;
  end if;
  -- parallel refresh
  set enable_parallel=on;
  t = clock_timestamp();
  refresh materialized view matv;
  dur1 = age(clock_timestamp(), t);
  select * into results1 from matv;
  if refresh_compare.verbose then
    raise notice 'Parallel refresh duration=%', dur1;
    raise notice 'Parallel results=%', results1;
  end if;
  -- compare
  if results0 <> results1 then
    raise notice 'results of non-parallel % are not equal to parallel %', results0, results1;
  end if;
  parallel_is_better = dur0 > dur1;
  if NOT parallel_is_better then
    raise notice 'Non-parallel refresh duration=%', dur0;
    raise notice 'Parallel refresh duration=%', dur1;
  end if;
  drop materialized view  matv;
  drop table t_p;
  reset enable_parallel;
end
$$ language plpgsql;
begin;
set local max_parallel_workers_per_gather = 8;
-- start_ignore
-- FIXME: The test case is flaky, and the results are not always consistent.
select * from refresh_compare(true, false);
select * from refresh_compare(false, false);
-- end_ignore
drop function refresh_compare;
reset max_parallel_workers_per_gather;
end;

--
-- Parallel Create AO/AOCO Table AS
--
begin;
create table t_p2(c1 int, c2 int) with(parallel_workers=2) distributed by(c1);
insert into t_p2 select i, i+1 from generate_series(1, 10000000)i;
analyze t_p2;
set local enable_parallel = off;
explain(costs off) create table ctas_ao using ao_row as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p2 a join t_p2 b on a.c1 = b.c1 distributed by(c2);
explain(costs off) create table ctas_aoco using ao_column as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p2 a join t_p2 b on a.c1 = b.c1 distributed by(c2);
set local enable_parallel = on;
explain(costs off) create table ctas_ao using ao_row as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p2 a join t_p2 b on a.c1 = b.c1 distributed by(c2);
explain(costs off) create table ctas_aoco using ao_column as select sum(a.c2) as c2, avg(b.c1) as c1 from t_p2 a join t_p2 b on a.c1 = b.c1 distributed by(c2);
abort;

--
-- Parallel Semi Join
--
begin;
set local optimizer=off;
set local enable_parallel=on;
set local force_parallel_mode =1 ;
set local min_parallel_table_scan_size = 0;
create table semi_t1 (c1 integer) with(parallel_workers=2) distributed randomly;
create table semi_t2 (c2 integer) with(parallel_workers=2) distributed randomly;
insert into semi_t1 values (generate_series (1,20000));
insert into semi_t2 values (generate_series (1,10000));
analyze semi_t1;
analyze semi_t2;

-- Parallel-aware Hash Semi Join
explain(costs off) select c1 from semi_t1 where not c1 >=all (select c2 from semi_t2 where c2 = c1);
-- Parallel-oblivious Hash Semi Join
set local enable_parallel_hash = off;
explain(costs off) select c1 from semi_t1 where not c1 >=all (select c2 from semi_t2 where c2 = c1);
-- Parallel Merge Semi Join
set local enable_hashjoin = off;
explain(costs off) select c1 from semi_t1 where not c1 >=all (select c2 from semi_t2 where c2 = c1);
set local enable_mergejoin = off;
set local enable_nestloop = on;
-- Parallel Nested Loop Semi Join
explain(costs off) select c1 from semi_t1 where not c1 >=all (select c2 from semi_t2 where c2 = c1);
abort;

--
-- Test Materialize locus when enable_material is off.
--
begin;
create table t1(id int) distributed by (id);
create index on t1(id);
insert into t1 values(generate_series(1, 100));
analyze t1;
set enable_seqscan =off;
set enable_material =off;
explain (locus, costs off)
select * from
  (select count(id) from t1 where id > 10) ss
  right join (values (1),(2),(3)) v(x) on true;
abort;

-- Subplan locus, One-time flter locus is null
begin;
drop table if exists mrs_t1;
create table mrs_t1(x int) distributed by (x);
insert into mrs_t1 select generate_series(1,20);
analyze mrs_t1;
explain(locus, costs off) select * from mrs_t1 where exists (select x from mrs_t1 where x < -1);
explain(locus, costs off) select * from mrs_t1 where exists (select x from mrs_t1 where x = 1);
explain(locus, costs off) select * from mrs_t1 where x in (select x-95 from mrs_t1) or x < 5;
explain(locus, costs off) select * from pg_class where oid in (select x-95 from mrs_t1) or oid < 5;
drop table if exists mrs_t1;
abort;

-- prepare, execute locus is null
begin;
create table t1(c1 int, c2 int);
analyze t1;
prepare t1_count(integer) as select count(*) from t1;
explain(locus, costs off) execute t1_count(1);
abort;

-- Result locus is null
begin;
create table t1(id int) distributed by (id);
create index on t1(id);
insert into t1 values(generate_series(1, 10));
analyze t1;
explain(costs off, locus) select max(100) from t1;
abort;

-- test Parallel Anti Join
begin;
create table t1_anti(a int, b int) with(parallel_workers=2) distributed by (a);
insert into t1_anti values(generate_series(1, 10));
create table t2_anti(a int, b int) with(parallel_workers=2) distributed by (b);
insert into t2_anti values(generate_series(5, 10));
explain(costs off, verbose)
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
set local enable_parallel = on;
set local min_parallel_table_scan_size = 0;
explain(costs off, verbose)
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
set local enable_parallel_hash = off;
explain(costs off, verbose)
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
select t1_anti.a, t1_anti.b from t1_anti left join t2_anti on t1_anti.a = t2_anti.a where t2_anti.a is null;
abort;

--
-- Test Parallel DISTINCT
--
drop table if exists t_distinct_0;
create table t_distinct_0(a int, b int) using ao_column distributed randomly;
insert into t_distinct_0 select i, i+1 from generate_series(1, 1000) i;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
insert into t_distinct_0 select * from t_distinct_0;
analyze t_distinct_0;
explain(costs off)
select distinct a from t_distinct_0;
set enable_parallel = on;
-- first stage HashAgg, second stage GroupAgg
explain(costs off)
select distinct a from t_distinct_0;
set parallel_query_use_streaming_hashagg = off;
explain(costs off)
select distinct a from t_distinct_0;
-- GroupAgg
set enable_hashagg = off;
explain(costs off)
select distinct a from t_distinct_0;
-- HashAgg
set enable_hashagg = on;
set enable_groupagg = off;
explain(costs off)
select distinct a from t_distinct_0;
set parallel_query_use_streaming_hashagg = on;
explain(costs off)
select distinct a from t_distinct_0;
-- multi DISTINCT tlist
explain(costs off)
select distinct a, b from t_distinct_0;

-- DISTINCT on distribution key 
drop table if exists t_distinct_1;
create table t_distinct_1(a int, b int) using ao_column;
insert into t_distinct_1 select * from t_distinct_0;
analyze t_distinct_1;
set enable_parallel = off;
explain(costs off)
select distinct a from t_distinct_1;
set enable_parallel = on;
explain(costs off)
select distinct a from t_distinct_1;

--
-- End of test Parallel DISTINCT
--

--
-- Test Parallel UNION
--
set enable_parallel = off;
explain(costs off)
select distinct a from t_distinct_0 union select distinct b from t_distinct_0;
set enable_parallel = on;
set enable_groupagg = off;
set enable_hashagg = on;
explain(costs off)
select distinct a from t_distinct_0 union select distinct b from t_distinct_0;
reset enable_groupagg;
set enable_hashagg = off;
set enable_groupagg = on;
explain(costs off)
select distinct a from t_distinct_0 union select distinct b from t_distinct_0;
reset enable_groupagg;
reset enable_hashagg;
explain(costs off)
select distinct a from t_distinct_0 union select distinct b from t_distinct_0;
reset enable_parallel;
--
-- End of test Parallel UNION
--

--
-- Test Parallel Subquery.
--
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100)
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    salary NUMERIC,
    department_id INT
);

INSERT INTO departments VALUES 
(1, 'Sales'),
(2, 'IT'),
(3, 'HR');

INSERT INTO employees VALUES
(1, 'Alice', 5000, 1),
(2, 'Bob', 6000, 1),
(3, 'Charlie', 7000, 2),
(4, 'David', 8000, 2),
(5, 'Eve', 9000, 3);

set enable_parallel = off;
explain SELECT e.name
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id);

SELECT e.name
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id);

set enable_parallel = on;
set min_parallel_table_scan_size = 0;

explain SELECT e.name
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id);

SELECT e.name
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id);

--
-- Test https://github.com/apache/cloudberry/issues/1376
--
create table t1(a int, b int);
create table t2 (like t1);
set gp_cte_sharing = on;

explain(locus, costs off) with x as
  (select a, count(*) as b from t1 group by a union all
    select a, count(*) as b from t2 group by a)
  select count(*) from x a join x b on a.a = b.b;

reset gp_cte_sharing;
reset enable_parallel;
reset min_parallel_table_scan_size;

-- start_ignore
drop schema test_parallel cascade;
-- end_ignore

reset gp_appendonly_insert_files;
reset force_parallel_mode;
reset optimizer;
