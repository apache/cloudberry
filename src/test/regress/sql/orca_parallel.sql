create schema orca_parallel;
set search_path=orca_parallel, public;
set statement_mem = '256MB';
set optimizer=on;

create table t1(a int, b int) with(parallel_workers=2) distributed by (a);
create table t2(c int, d int ) with(parallel_workers=3) distributed by (c);
insert into t1 select i, i+1 from generate_series(1, 1000)i;
insert into t2 select i, i+2 from generate_series(1, 20000)i;
analyze t1;
analyze t2;

set parallel_setup_cost=0;
set max_parallel_workers_per_gather=4;
set enable_parallel = on;

explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.a = t2.d;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.c;
explain (verbose, costs off) select * from t1  join t2  on t1.b = t2.d;


reset enable_parallel;
reset max_parallel_workers_per_gather;
reset parallel_setup_cost;
reset statement_mem;
reset optimizer;

-- start_ignore
drop schema orca_parallel cascade;
-- end_ignore
