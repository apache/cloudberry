--
-- AQUMV rewrite regressions: MVs that cannot serve a query must be
-- declined, not produce "ORDER/GROUP BY expression not found in targetlist".
-- Postgres planner only.
set optimizer = off;
create schema aqumv_rw;
set search_path to aqumv_rw;

-- A grouped aggregate MV used to break ungrouped aggregate queries.
create table rw_t1(id int, k int, v numeric(12,2)) distributed by (id);
insert into rw_t1 select g, g % 50, (g % 1000)::numeric(12,2) from generate_series(1, 20000) g;
analyze rw_t1;

create materialized view rw_mv_grouped as
  select k, count(*) as c, sum(v) as s
  from rw_t1 where k is not null group by k distributed by (k);
analyze rw_mv_grouped;

set enable_answer_query_using_materialized_views = on;
-- used to fail with "ORDER/GROUP BY expression not found in targetlist"
select count(*) from rw_t1 where k is not null;
-- a grouped query matching the MV keeps working
select k, count(*) as c, sum(v) as s
  from rw_t1 where k is not null group by k order by k limit 3;

-- An exact-match join MV used to break queries ordered by a column
-- that is not in its SELECT list.
create table rw_ta(id int, k int) distributed by (id);
create table rw_tb(id int, w int) distributed by (id);
insert into rw_ta select g, g % 1000 from generate_series(1, 6000) g;
insert into rw_tb select g, g from generate_series(1, 6000) g;
analyze rw_ta;
analyze rw_tb;

create materialized view rw_mv_join as
  select a.id as aid from rw_ta a join rw_tb b on a.id = b.id
  where a.k > 995 order by b.w distributed by (aid);
analyze rw_mv_join;

-- the exact defining query; used to fail with the GUC on
select a.id as aid from rw_ta a join rw_tb b on a.id = b.id
  where a.k > 995 order by b.w;

-- ORDER BY over a selected column is still rewritten (2e27d4c2b3d)
create materialized view rw_mv_join_intlist as
  select a.id as aid, b.w as w from rw_ta a join rw_tb b on a.id = b.id
  where a.k > 995 order by a.id distributed by (aid);
analyze rw_mv_join_intlist;

explain (costs off)
  select a.id as aid, b.w as w from rw_ta a join rw_tb b on a.id = b.id
  where a.k > 995 order by a.id;
select a.id as aid, b.w as w from rw_ta a join rw_tb b on a.id = b.id
  where a.k > 995 order by a.id;

reset enable_answer_query_using_materialized_views;
drop schema aqumv_rw cascade;
