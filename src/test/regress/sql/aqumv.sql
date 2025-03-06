--
-- Tests of Answer Query Using Materialized Views.
--
set optimizer = off;
create schema aqumv;
set search_path to aqumv;
create table aqumv_t1(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t1 select i, i+1, i+2 from generate_series(1, 1000) i;
insert into aqumv_t1 select * from aqumv_t1;
analyze aqumv_t1;
set enable_answer_query_using_materialized_views = on;

-- drop views if there is no data populated
begin;
create incremental materialized view aqumv_mvt1_need_refresh as select * from aqumv_t1 where c1 = 2 with no data;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
refresh materialized view aqumv_mvt1_need_refresh;
analyze aqumv_mvt1_need_refresh;
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
abort;

begin;
create incremental materialized view aqumv_mvt1_0 as select * from aqumv_t1 where c1 = 2;
analyze aqumv_mvt1_0;

set local enable_answer_query_using_materialized_views = off;
select * from aqumv_t1 where c1 = 2;
select c1, c2, c3 from aqumv_t1 where c1 = 2;
select c2 from aqumv_t1 where c1 = 2;
select c3, c2 from aqumv_t1 where c1 = 2;

set local enable_answer_query_using_materialized_views = on;
select * from aqumv_t1 where c1 = 2;
select c1, c2, c3 from aqumv_t1 where c1 = 2;
select c2 from aqumv_t1 where c1 = 2;
select c3, c2 from aqumv_t1 where c1 = 2;
-- tlist matched.
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
explain(verbose, costs off) select c1, c2, c3 from aqumv_t1 where c1 = 2;
-- tlist partially matched.
explain(verbose, costs off) select c2 from aqumv_t1 where c1 = 2;
-- tlist disorder.
explain(verbose, costs off) select c3, c2 from aqumv_t1 where c1 = 2;
abort;

begin;
create incremental materialized view aqumv_mvt1_1 as select c2 as mc2, c3 as mc3, c1 as mc1, c2 as mc2_1 from aqumv_t1 where c1 = 3;
analyze aqumv_mvt1_1;
set local enable_answer_query_using_materialized_views = off;
select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
select c1, c1 from aqumv_t1 where c1 = 3;

set local enable_answer_query_using_materialized_views = on;
select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
select c1, c1 from aqumv_t1 where c1 = 3;

-- tlist alias.
explain(verbose, costs off) select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
-- duplicated projection.
explain(verbose, costs off) select c1, c1 from aqumv_t1 where c1 = 3;
abort;

begin;
create incremental materialized view aqumv_mvt1_nonvar_expr as
  select c2, 1 as mc_const_1, sqrt(100) as mc_sqrt_100
  from aqumv_t1 where c1 = 4;
analyze aqumv_mvt1_nonvar_expr;
set local enable_answer_query_using_materialized_views = off;
select c2, 200 from aqumv_t1 where c1 = 4;
select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;

set local enable_answer_query_using_materialized_views = on;
select c2, 200 from aqumv_t1 where c1 = 4;
select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;

-- Const are copied to output.
explain(verbose, costs off) select c2, 200 from aqumv_t1 where c1 = 4;
explain(verbose, costs off) select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;
abort;

begin;
CREATE OR REPLACE FUNCTION aqumv_func(i integer, j integer) RETURNS integer AS $$
    BEGIN
        RETURN i + j;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;
create incremental materialized view aqumv_mvt1_func_has_var as
  select c2, aqumv_func(c1, c3) as mc_func_res
  from aqumv_t1 where c1 = 5;
analyze aqumv_mvt1_func_has_var;

set local enable_answer_query_using_materialized_views = off; 
select c2, aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;

set local enable_answer_query_using_materialized_views = on;
select c2, aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;

-- Functions has Vars are replaced.
explain(verbose, costs off) select c2, aqumv_func(c1, c3), aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;
abort;

begin;
create incremental materialized view aqumv_mvt1_2 as select c2 as mc2, c1 as mc1 from aqumv_t1 where c1 > 1 and c1 < 5;
analyze aqumv_mvt1_2;
set local enable_answer_query_using_materialized_views = on;
-- shoud be unable to use mv, projection doesn't exit in mv's tlist 
explain(verbose, costs off) select c3 from aqumv_t1 where c1 < 5 and c1 > 1;
-- no post quals.
explain(verbose, costs off) select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1;

-- post quals added to mv.
set local enable_answer_query_using_materialized_views = off;
select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;
set local enable_answer_query_using_materialized_views = on;
select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;
explain(verbose, costs off) select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;

-- should be unable to use mv, post quals has column doesn't exit in mv's tlist.
explain(verbose, costs off) select * from aqumv_t1 where c1 < 5 and c1 > 1 and c3 > 1; 
abort;

begin;
create incremental materialized view aqumv_mvt1_3 as select c2 as mc2, c1 as mc1, c3+1 as mc3 from aqumv_t1 where c1 > 5 and c1 < 10;
analyze aqumv_mvt1_3;

set local enable_answer_query_using_materialized_views = on;
-- should be unable to use mv, column c3 doesn't exist in mv's tlist.
explain(verbose, costs off) select * from aqumv_t1 where c1 > 5 and c1 < 10; 
-- expr c3+1 is in mv's tlist
explain(verbose, costs off) select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
-- expr c1+1 could be derived from mv's tlist
explain(verbose, costs off) select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 

select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
set local enable_answer_query_using_materialized_views = off;
select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
abort;

begin;
create incremental materialized view aqumv_mvt1_4 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 10 and c1 < 15;

analyze aqumv_mvt1_4;

set local enable_answer_query_using_materialized_views = on;
-- complex exprs
explain(verbose, costs off) select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;

set local enable_answer_query_using_materialized_views = off;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;
abort;

-- post quals
begin;
create incremental materialized view aqumv_mvt1_post_quals as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 20 and c1 < 30;

analyze aqumv_mvt1_post_quals;

set local enable_answer_query_using_materialized_views = on;

explain(verbose, costs off)
  select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;

explain(verbose, costs off)
  select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;

explain(verbose, costs off)
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

set local enable_answer_query_using_materialized_views = off;

select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

abort;

-- choose the best one if there are multiple chooses based on cost.
begin;
set local enable_answer_query_using_materialized_views = on;

create incremental materialized view aqumv_mvt1_candidate_0 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3
  from aqumv_t1 where c1 > 30;
analyze aqumv_mvt1_candidate_0;
-- choose aqumv_mvt1_candidate_0
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;

create incremental materialized view aqumv_mvt1_candidate_1 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 30 and c1 < 40;
analyze aqumv_mvt1_candidate_1;
-- choose aqumv_mvt1_candidate_1 as it has lower cost(less rows).
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;

create incremental materialized view aqumv_mvt1_candidate_2 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 30 and c1 < 500;
analyze aqumv_mvt1_candidate_2;
-- still choose aqumv_mvt1_candidate_1 as it has lowest cost(less rows).
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;
abort;


--
-- Support origin query with aggregations.
-- Compute Aggregations from mv.
--
begin;
create table aqumv_t2(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t2 select i, i+1, i+2 from generate_series(1, 100) i;
insert into aqumv_t2 values (91, NULL, 95);
analyze aqumv_t2;

create incremental materialized view aqumv_mvt2_0 as
  select c1 as mc1, c2 as mc2, c3 as mc3
  from aqumv_t2 where c1 > 90;
analyze aqumv_mvt2_0;

-- test aggregation functions supported in IVM. 
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c1), sum(c2), avg(c3) from aqumv_t2 where c1 > 90;
select count(c1), sum(c2), avg(c3) from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c1), sum(c2), avg(c3) from aqumv_t2 where c1 > 90;
select count(c1), sum(c2), avg(c3) from aqumv_t2 where c1 > 90;

-- test complex expressions have AGG.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c1) + 1 from aqumv_t2 where c1 > 90;
select count(c1) + 1 from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c1) + 1 from aqumv_t2 where c1 > 90;
select count(c1) + 1 from aqumv_t2 where c1 > 90;

-- test AGG FILTER.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select sum(c2), sum(c2) filter (where c2 > 95) from aqumv_t2 where c1 > 90;
select sum(c2), sum(c2) filter (where c2 > 95) from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select sum(c2), sum(c2) filter (where c2 > 95) from aqumv_t2 where c1 > 90;
select sum(c2), sum(c2) filter (where c2 > 95) from aqumv_t2 where c1 > 90;

-- test AGG functions which are not supported in IVM now, but could work in AQUMV. 
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select max(c1), min(c3), stddev(c2) from aqumv_t2 where c1 > 90;
select max(c1), min(c3), stddev(c2) from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select max(c1), min(c3), stddev(c2) from aqumv_t2 where c1 > 90;
select max(c1), min(c3), stddev(c2) from aqumv_t2 where c1 > 90;

-- test count(*)
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c2), count(*) from aqumv_t2 where c1 > 90;
select count(c2), count(*) from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c2), count(*) from aqumv_t2 where c1 > 90;
select count(c2), count(*) from aqumv_t2 where c1 > 90;
abort;

-- Test use normal materialized views
create table t1(c1 int, c2 int, c3 int) distributed by (c1);
insert into t1 select i, i+1, i+2 from generate_series(1, 100) i;
insert into t1 values (91, NULL, 95);
analyze t1;

create materialized view normal_mv_t1 as
  select c3 as mc3, c1 as mc1
  from t1 where c1 > 90;
analyze normal_mv_t1;

set enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c3) from t1 where c1 > 90;
select count(c3) from t1 where c1 > 90;
set enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c3) from t1 where c1 > 90;
select count(c3) from t1 where c1 > 90;

vacuum full t1;
explain(costs off, verbose)
select count(c3) from t1 where c1 > 90;

explain(costs off, verbose)
select c3 from t1 where c1 > 90;

-- insert data after refresh
insert into t1 values (91, NULL, 95);
explain(costs off, verbose)
select count(c3) from t1 where c1 > 90;
select mvname, datastatus from gp_matview_aux where mvname = 'normal_mv_t1';

-- Test Agg on IMMV who has less columns than origin table.
begin;
create table aqumv_t2(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t2 select i, i+1, i+2 from generate_series(1, 100) i;
insert into aqumv_t2 values (91, NULL, 95);
analyze aqumv_t2;

create incremental materialized view aqumv_mvt2_1 as
  select c3 as mc3, c1 as mc1
  from aqumv_t2 where c1 > 90;
analyze aqumv_mvt2_1;

set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c3) from aqumv_t2 where c1 > 90;
select count(c3) from aqumv_t2 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c3) from aqumv_t2 where c1 > 90;
select count(c3) from aqumv_t2 where c1 > 90;
abort;

--
-- Test Group By clause of origin query.
-- GROUPING SETS
-- ROLLUP
-- CUBE
--
begin;
create table aqumv_t3(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t3 select i, i+1, i+2 from generate_series(1, 100) i;
insert into aqumv_t3 values (91, NULL, 95);
analyze aqumv_t3;

create incremental materialized view aqumv_mvt3_0 as
  select c1 as mc1, c2 as mc2, c3 as mc3
  from aqumv_t3 where c1 > 90;
analyze aqumv_mvt3_0;

-- Group By
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by c1, c3;
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by c1, c3;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by c1, c3;
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by c1, c3;

-- GROUPING SETS
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by grouping sets((c1), (c3));
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by grouping sets((c1), (c3));
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by grouping sets((c1), (c3));
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by grouping sets((c1), (c3));

-- ROLLUP
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by rollup(c1, c3);
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by rollup(c1, c3);
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by rollup(c1, c3);
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by rollup(c1, c3);

-- CUBE
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by cube(c1, c3);
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by cube(c1, c3);
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by cube(c1, c3);
select c1, c3, count(c2) from aqumv_t3 where c1 > 90 group by cube(c1, c3);
abort;

--
-- Test HAVING clause
--
begin;
create table aqumv_t4(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t4 select i, i+1, i+2 from generate_series(1, 100) i;
insert into aqumv_t4 values (91, NULL, 95);
analyze aqumv_t4;

create incremental materialized view aqumv_mvt4_0 as
  select c1 as mc1, c2 as mc2, c3 as mc3
  from aqumv_t4 where c1 > 90;
analyze aqumv_mvt4_0;

create incremental materialized view aqumv_mvt4_1 as
  select c2 as mc2, c1 as mc1
  from aqumv_t4 where c1 > 95;
analyze aqumv_mvt4_1;

-- HAVING clause pushed down to where quals.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3 from aqumv_t4 where c1 > 90 group by (c1, c3) having c3 > 97 ;
select c1, c3 from aqumv_t4 where c1 > 90 group by (c1, c3) having c3 > 97 ;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3 from aqumv_t4 where c1 > 90 group by (c1, c3) having c3 > 97 ;
select c1, c3 from aqumv_t4 where c1 > 90 group by (c1, c3) having c3 > 97 ;

-- quals kept in HAVING clause.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having avg(c2) > 95;
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having avg(c2) > 95;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having avg(c2) > 95;
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having avg(c2) > 95;

-- duplicated having quals with where quals.
explain(costs off, verbose)
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having c1 > 90;

-- having quals without column reference.
explain(costs off, verbose)
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having 2 > 1;

-- origin query have mutable functions can not use AQUMV.
explain(costs off, verbose)
select c1, c3, avg(c2) from aqumv_t4 where c1 > 90 group by (c1, c3) having random() > 0.5;
explain(costs off, verbose)
select c1, c3, avg(c2), random() from aqumv_t4 where c1 > 90 group by (c1, c3);

-- Test having quals have aggs.
-- Could not use AQUMV. 
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, avg(c2) from aqumv_t4 where c1 > 95 group by c1 having avg(c3) > 96;
select c1, avg(c2) from aqumv_t4 where c1 > 95 group by c1 having avg(c3) > 96;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, avg(c2) from aqumv_t4 where c1 > 95 group by c1 having avg(c3) > 96;
select c1, avg(c2) from aqumv_t4 where c1 > 95 group by c1 having avg(c3) > 96;
-- Can use AQUMV.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, avg(c2) from aqumv_t4 where c1 > 90 group by c1 having avg(c3) > 96;
select c1, avg(c2) from aqumv_t4 where c1 > 90 group by c1 having avg(c3) > 96;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, avg(c2) from aqumv_t4 where c1 > 90 group by c1 having avg(c3) > 96;
select c1, avg(c2) from aqumv_t4 where c1 > 90 group by c1 having avg(c3) > 96;
abort;

-- Test Order By of origin query.
begin;
create table aqumv_t5(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t5 select i, i+1, i+2 from generate_series(1, 100) i;
insert into aqumv_t5 values (91, NULL, 95);
analyze aqumv_t5;

create incremental materialized view aqumv_mvt5_0 as
  select c1 as mc1, c2 as mc2, c3 as mc3
  from aqumv_t5 where c1 > 90;
analyze aqumv_mvt5_0;

-- order by column.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by c2, c3 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by c2, c3 asc;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by c2, c3 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by c2, c3 asc;

-- order by expression.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by c2 - c1 - 1 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by c2 - c1 - 1 asc;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by c2 - c1 - 1 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by c2 - c1 - 1 asc;

-- order by number.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by 2, 1 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by 2, 1 asc;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c3 from aqumv_t5 where c1 > 90 order by 2, 1 asc;
select c1, c3 from aqumv_t5 where c1 > 90 order by 2, 1 asc;

-- order by result column.
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, sum(c3) as sum_c3 from aqumv_t5 where c1 > 90 group by c1 order by sum_c3 asc;
select c1, sum(c3) as sum_c3 from aqumv_t5 where c1 > 90 group by c1 order by sum_c3 asc;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, sum(c3) as sum_c3 from aqumv_t5 where c1 > 90 group by c1 order by sum_c3 asc;
select c1, sum(c3) as sum_c3 from aqumv_t5 where c1 > 90 group by c1 order by sum_c3 asc;
abort;

-- Test DISTINCT
begin;
create table aqumv_t6(c1 int, c2 int, c3 int, c4 int) distributed by (c1);
insert into aqumv_t6 select i, i+1, i+2, i+3 from generate_series(1, 100) i;
insert into aqumv_t6 select i, i+1, i+2, i+3 from generate_series(1, 100) i;
insert into aqumv_t6 values (91, NULL, 97, 98);
analyze aqumv_t6;

create incremental materialized view aqumv_mvt6_0 as
  select c1 as mc1, c2 as mc2
  from aqumv_t6 where c1 > 90;
analyze aqumv_mvt6_0;

-- DISTINCT
\pset null NULL
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select distinct c2, c1 from aqumv_t6 where c1 > 90;
select distinct c2, c1 from aqumv_t6 where c1 > 90 order by c2, c1;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select distinct c2, c1 from aqumv_t6 where c1 > 90;
select distinct c2, c1 from aqumv_t6 where c1 > 90 order by c2, c1;

-- Agg DISTINCT
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select count(c1) as count_c1, count(distinct c1) as count_distinct_c1,
sum(c2) as sum_c2, sum(distinct c2) as sum_distinct_c2 from aqumv_t6 where c1 > 90;
select count(c1) as count_c1, count(distinct c1) as count_distinct_c1,
sum(c2) as sum_c2, sum(distinct c2) as sum_distinct_c2 from aqumv_t6 where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select count(c1) as count_c1, count(distinct c1) as count_distinct_c1,
sum(c2) as sum_c2, sum(distinct c2) as sum_distinct_c2 from aqumv_t6 where c1 > 90;
select count(c1) as count_c1, count(distinct c1) as count_distinct_c1,
sum(c2) as sum_c2, sum(distinct c2) as sum_distinct_c2 from aqumv_t6 where c1 > 90;

-- Group DISTINCT
create incremental materialized view aqumv_mvt6_1 as
  select c3 as mc3, c4 as mc4, c1 as mc1, c2 as mc2
  from aqumv_t6 where c1 > 97;
analyze aqumv_mvt6_1;
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c1, c2, c3, sum(c4) from aqumv_t6 where c1 > 97 group by distinct rollup(c1, c2), rollup(c1, c3);
select c1, c2, c3, sum(c4) from aqumv_t6 where c1 > 97 group by distinct rollup(c1, c2), rollup(c1, c3);
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c1, c2, c3, sum(c4) from aqumv_t6 where c1 > 97 group by distinct rollup(c1, c2), rollup(c1, c3);
select c1, c2, c3, sum(c4) from aqumv_t6 where c1 > 97 group by distinct rollup(c1, c2), rollup(c1, c3);

-- DISTINCT ON
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select distinct on(c1 - 1) c1, c2 from aqumv_t6 where c1 > 90 order by c1 - 1, c2 nulls first;
select distinct on(c1 - 1) c1, c2 from aqumv_t6 where c1 > 90 order by c1 - 1, c2 nulls first;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select distinct on(c1 - 1) c1, c2 from aqumv_t6 where c1 > 90 order by c1 - 1, c2 nulls first;
select distinct on(c1 - 1) c1, c2 from aqumv_t6 where c1 > 90 order by c1 - 1, c2 nulls first;

\pset null ''
abort;

-- Test LIMIT
begin;
create table aqumv_t7(c1 int, c2 int, c3 int, c4 int) distributed by (c1);
insert into aqumv_t7 select i, i+1, i+2, i+3 from generate_series(1, 100) i;
insert into aqumv_t7 select i, i+1, i+2, i+3 from generate_series(1, 100) i;
analyze aqumv_t7;

create incremental materialized view aqumv_mvt7_0 as
  select c3 as cm3, c1 as mc1, c2 as mc2
  from aqumv_t7 where c1 > 90;
analyze aqumv_mvt7_0;

-- LIMIT
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3;

-- OFFSET
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3 offset 4;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3 offset 4;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3 offset 4;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 limit 3 offset 4;

-- FETCH
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows only;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows only;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows only;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows only;

-- WITH TIES
set local enable_answer_query_using_materialized_views = off;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows with ties;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows with ties;
set local enable_answer_query_using_materialized_views = on;
explain(costs off, verbose)
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows with ties;
select c2, c3 from aqumv_t7 where c1 > 90 order by c2, c3 fetch first 3 rows with ties;

abort;

--
-- Test external table
--
begin;

CREATE OR REPLACE FUNCTION write_to_file() RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_export' LANGUAGE C STABLE NO SQL;
CREATE OR REPLACE FUNCTION read_from_file() RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_import' LANGUAGE C STABLE NO SQL;
--start_ignore
DROP PROTOCOL IF EXISTS demoprot;
--end_ignore
CREATE TRUSTED PROTOCOL demoprot (readfunc = 'read_from_file', writefunc = 'write_to_file'); -- should succeed

CREATE WRITABLE EXTERNAL TABLE aqumv_ext_w(id int)
    LOCATION('demoprot://aqumvtextfile.txt') 

FORMAT 'text'
DISTRIBUTED BY (id);

INSERT INTO aqumv_ext_w SELECT * FROM generate_series(1, 10);

CREATE READABLE EXTERNAL TABLE aqumv_ext_r(id int)
    LOCATION('demoprot://aqumvtextfile.txt') 
FORMAT 'text';

create materialized view aqumv_ext_mv as
  select * from aqumv_ext_r;
analyze aqumv_ext_mv;

explain (costs off, verbose)
select * from aqumv_ext_r;
select * from aqumv_ext_r;
set local enable_answer_query_using_materialized_views = on;
set local aqumv_allow_foreign_table = on;
explain (costs off, verbose)
select * from aqumv_ext_r;
select * from aqumv_ext_r;

create index on aqumv_ext_mv(id);
set local enable_seqscan = off;
explain (costs off, verbose)
select * from aqumv_ext_r where id = 5;
select * from aqumv_ext_r where id = 5;

-- refresh matview has foreign tables should not go fast path. 
select * from aqumv_ext_mv;
INSERT INTO aqumv_ext_w SELECT * FROM generate_series(10, 15);
set local gp_enable_refresh_fast_path = on;
select datastatus from gp_matview_aux where mvoid = 'aqumv_ext_mv'::regclass::oid;
refresh materialized view aqumv_ext_mv;
select * from aqumv_ext_mv;
abort;
--
-- End of test external table
--
-- Test view has aggs
begin;
create table t(c1 int, c2 int, c3 int, c4 int) distributed by (c1);
insert into t select i, i+1, i+2, i+3 from generate_series(1, 100) i;
insert into t select i, i+1, i+2, i+3 from generate_series(1, 100) i;
analyze t;

create materialized view mv as
  select sum(c1) as mc1, count(c2) as mc2, avg(c3) as mc3, count(*) as mc4
  from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
analyze mv;

set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1), count(c2), avg(c3), abs(count(*) - 21) from t where c1 > 90;
select count(*), sum(c1), count(c2), avg(c3), abs(count(*) - 21) from t where c1 > 90;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1), count(c2), avg(c3), abs(count(*) - 21) from t where c1 > 90;
select count(*), sum(c1), count(c2), avg(c3), abs(count(*) - 21) from t where c1 > 90;

-- with HAVING quals
set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 having abs(count(*) - 21) > 0 and 2 > 1 and avg(c3) > 97;
select count(*), sum(c1) from t where c1 > 90 having abs(count(*) - 21) > 0 and 2 > 1 and avg(c3) > 97;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 having abs(count(*) - 21) > 0 and 2 > 1 and avg(c3) > 97;
select count(*), sum(c1) from t where c1 > 90 having abs(count(*) - 21) > 0 and 2 > 1 and avg(c3) > 97;

-- Test Order By elimination.
set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
truncate t;
set local enable_answer_query_using_materialized_views = off;
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;
set local enable_answer_query_using_materialized_views = on;
select count(*), sum(c1) from t where c1 > 90 order by 1, sum(c1 - 1) ASC;

abort;

-- Test query has limit while view has aggs.
begin;
create table t(c1 int, c2 int, c3 int, c4 int) distributed by (c1);
insert into t select i, i+1, i+2, i+3 from generate_series(1, 100) i;
insert into t select i, i+1, i+2, i+3 from generate_series(1, 100) i;
analyze t;

create materialized view mv as
  select sum(c1) as mc1, count(c2) as mc2, avg(c3) as mc3, count(*) as mc4
  from t where c1 > 90;
analyze mv;

set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit 2;
select count(*), sum(c1) from t where c1 > 90 limit 2;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit 2;
select count(*), sum(c1) from t where c1 > 90 limit 2;

-- offset
set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit 1 offset 1 ;
select count(*), sum(c1) from t where c1 > 90 limit 1 offset 1 ;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit 1 offset 1 ;
select count(*), sum(c1) from t where c1 > 90 limit 1 offset 1 ;

-- no real limit
set local enable_answer_query_using_materialized_views = off;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit all;
select count(*), sum(c1) from t where c1 > 90 limit all;
set local enable_answer_query_using_materialized_views = on;
explain(verbose, costs off)
select count(*), sum(c1) from t where c1 > 90 limit all;
select count(*), sum(c1) from t where c1 > 90 limit all;

abort;

--
-- test partitioned tables
--
create table par(a int, b int, c int) partition by range(b)
    subpartition by range(c) subpartition template (start (1) end (3) every (1))
    (start(1) end(3) every(1));
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
create materialized view mv_par as select    count(*) from par;
create materialized view mv_par1 as select   count(*) from par_1_prt_1;
create materialized view mv_par1_1 as select count(*) from par_1_prt_1_2_prt_1;
create materialized view mv_par1_2 as select count(*) from par_1_prt_1_2_prt_2;
create materialized view mv_par2 as select   count(*) from par_1_prt_2;
create materialized view mv_par2_2 as select count(*) from par_1_prt_2_2_prt_1;
create materialized view mv_par_prune as select count(*) from par where b = 1;
set enable_answer_query_using_materialized_views = on;

explain(costs off, verbose)
select count(*) from par;
explain(costs off, verbose)
select count(*) from par_1_prt_1;

-- test partition_pruning
set enable_partition_pruning = on;
explain(costs off, verbose)
select count(*) from par where b = 1;
set enable_partition_pruning = off;
explain(costs off, verbose)
select count(*) from par where b = 1;
reset enable_partition_pruning;
--
-- End of test partitioned tables
--

reset optimizer;
reset enable_answer_query_using_materialized_views;
-- start_ignore
drop schema aqumv cascade;
-- end_ignore
reset search_path;
