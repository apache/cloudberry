-- disable ORCA
SET optimizer TO off;
create schema matview_data_schema;
set search_path to matview_data_schema;

create table t1(a int, b int);
create table t2(a int, b int);
insert into t1 select i, i+1 from generate_series(1, 5) i;
insert into t1 select i, i+1 from generate_series(1, 3) i;
create materialized view mv0 as select * from t1;
create materialized view mv1 as select a, count(*), sum(b) from t1 group by a;
create materialized view mv2 as select * from t2;
-- all mv are up to date
select mvname, datastatus from gp_matview_aux where mvname in ('mv0','mv1', 'mv2');

-- truncate in self transaction
begin;
create table t3(a int, b int);
create materialized view mv3 as select * from t3;
select datastatus from gp_matview_aux where mvname = 'mv3';
truncate t3;
select datastatus from gp_matview_aux where mvname = 'mv3';
end;

-- trcuncate
refresh materialized view mv3;
select datastatus from gp_matview_aux where mvname = 'mv3';
truncate t3;
select datastatus from gp_matview_aux where mvname = 'mv3';

-- insert and refresh
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
insert into t1 values (1, 2); 
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';

-- insert but no rows changes
refresh materialized view mv0;
refresh materialized view mv1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
insert into t1 select * from t3; 
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';

-- update
refresh materialized view mv0;
refresh materialized view mv1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
update t1 set a = 10 where a = 1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';

-- delete
refresh materialized view mv0;
refresh materialized view mv1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
delete from t1 where a = 10;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';

-- vacuum
refresh materialized view mv0;
refresh materialized view mv1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
vacuum t1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
vacuum full t1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
-- insert after vacuum full 
insert into t1 values(1, 2);
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
-- vacuum full after insert
refresh materialized view mv0;
refresh materialized view mv1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
insert into t1 values(1, 2);
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';
vacuum full t1;
select datastatus from gp_matview_aux where mvname = 'mv0';
select datastatus from gp_matview_aux where mvname = 'mv1';

-- Refresh With No Data
refresh materialized view mv2;
select datastatus from gp_matview_aux where mvname = 'mv2';
refresh materialized view mv2 with no data;
select datastatus from gp_matview_aux where mvname = 'mv2';

-- Copy
refresh materialized view mv2;
select datastatus from gp_matview_aux where mvname = 'mv2';
-- 0 rows
COPY t2 from stdin;
\.
select datastatus from gp_matview_aux where mvname = 'mv2';

COPY t2 from stdin;
1	1
\.
select datastatus from gp_matview_aux where mvname = 'mv2';

--
-- test issue https://github.com/apache/cloudberry/issues/582
-- test inherits
--
begin;
create table tp_issue_582(i int, j int);
create table tc_issue_582(i int) inherits (tp_issue_582);
insert into tp_issue_582 values(1, 1), (2, 2);
insert into tc_issue_582 values(1, 1);
create materialized view mv_tp_issue_582 as select * from tp_issue_582;
-- should be null.
select mvname, datastatus from gp_matview_aux where mvname = 'mv_tp_issue_582';
abort;

--
-- Test multi-table JOIN materialized views
--
create table jt1(id int, val int);
create table jt2(id int, val int);
create table jt3(id int, val int);
insert into jt1 select i, i*10 from generate_series(1,5) i;
insert into jt2 select i, i*100 from generate_series(1,5) i;
insert into jt3 select i, i*1000 from generate_series(1,5) i;

-- Two-table INNER JOIN: verify registration
create materialized view mv_join2 as
  select jt1.id, jt1.val as v1, jt2.val as v2
  from jt1 join jt2 on jt1.id = jt2.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_join2'::regclass;

-- INSERT on table A: status -> 'i'
insert into jt1 values(6, 60);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- REFRESH: status -> 'u'
refresh materialized view mv_join2;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- INSERT on table B: status -> 'i'
insert into jt2 values(7, 700);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- UPDATE on table A: status -> 'e'
refresh materialized view mv_join2;
update jt1 set val = 99 where id = 1;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- DELETE on table B: status -> 'e'
refresh materialized view mv_join2;
delete from jt2 where id = 7;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- Implicit join (FROM t1, t2 WHERE ...): verify registration
refresh materialized view mv_join2;
create materialized view mv_implicit_join as
  select jt1.id, jt1.val as v1, jt2.val as v2
  from jt1, jt2 where jt1.id = jt2.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_implicit_join';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_implicit_join'::regclass;

-- Three-table join: verify 3 entries in gp_matview_tables
create materialized view mv_join3 as
  select jt1.id, jt1.val as v1, jt2.val as v2, jt3.val as v3
  from jt1 join jt2 on jt1.id = jt2.id join jt3 on jt2.id = jt3.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_join3'::regclass;
-- DML on middle table expires mv_join3
insert into jt2 values(8, 800);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';

-- Self-join: verify only 1 entry in gp_matview_tables
create materialized view mv_selfjoin as
  select a.id as aid, b.id as bid
  from jt1 a join jt1 b on a.id = b.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_selfjoin';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_selfjoin'::regclass;

-- LEFT/RIGHT/FULL OUTER JOIN: verify all register correctly
create materialized view mv_left_join as
  select jt1.id, jt1.val as v1, jt2.val as v2
  from jt1 left join jt2 on jt1.id = jt2.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_left_join';
create materialized view mv_right_join as
  select jt1.id, jt2.val as v2
  from jt1 right join jt2 on jt1.id = jt2.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_right_join';
create materialized view mv_full_join as
  select jt1.id as id1, jt2.id as id2
  from jt1 full join jt2 on jt1.id = jt2.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_full_join';

-- Partitioned table in join: verify partition DML propagates
create table jt_par(a int, b int) partition by range(a)
  (start(1) end(3) every(1));
insert into jt_par values(1, 10), (2, 20);
create materialized view mv_join_par as
  select jt1.id, jt1.val as v1, jt_par.a, jt_par.b
  from jt1 join jt_par on jt1.id = jt_par.a;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_par';
insert into jt_par values(1, 11);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_par';
refresh materialized view mv_join_par;
insert into jt1 values(9, 90);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_par';

-- VACUUM FULL on one base table of a join MV: status -> 'r'
refresh materialized view mv_join2;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';
vacuum full jt1;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- TRUNCATE on one base table of a join MV: status -> 'e'
refresh materialized view mv_join2;
truncate jt2;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';

-- CREATE WITH NO DATA: status -> 'e'
create materialized view mv_join_nodata as
  select jt1.id, jt3.val from jt1 join jt3 on jt1.id = jt3.id
  with no data;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_nodata';

-- DROP CASCADE: matview and aux entries removed
drop materialized view mv_join_nodata;
select count(*) from gp_matview_aux where mvname = 'mv_join_nodata';

-- Mixed join types in one view (INNER + LEFT)
create materialized view mv_mixed_join as
  select jt1.id, jt2.val as v2, jt3.val as v3
  from jt1 join jt2 on jt1.id = jt2.id left join jt3 on jt2.id = jt3.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_mixed_join';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_mixed_join'::regclass;

-- Join with GROUP BY and aggregates
create materialized view mv_join_agg as
  select jt1.id, count(*) as cnt, sum(jt2.val) as total
  from jt1 join jt2 on jt1.id = jt2.id group by jt1.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_agg';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_join_agg'::regclass;

-- Multiple MVs sharing base tables: DML on one table affects all dependent MVs
refresh materialized view mv_join2;
refresh materialized view mv_join3;
refresh materialized view mv_mixed_join;
refresh materialized view mv_join_agg;
select mvname, datastatus from gp_matview_aux
  where mvname in ('mv_join2', 'mv_join3', 'mv_mixed_join', 'mv_join_agg')
  order by mvname;
insert into jt2 values(10, 1000);
-- all four share jt2 as a base table
select mvname, datastatus from gp_matview_aux
  where mvname in ('mv_join2', 'mv_join3', 'mv_mixed_join', 'mv_join_agg')
  order by mvname;

-- Transaction: multiple DML on different base tables
refresh materialized view mv_join3;
begin;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
insert into jt1 values(20, 200);
-- after insert: 'i' (insert-only)
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
delete from jt2 where id = 10;
-- after delete: escalates to 'e' (expired)
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
end;
-- committed: status persists as 'e'
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';

-- Transaction rollback: status should revert
refresh materialized view mv_join3;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
begin;
update jt1 set val = 999 where id = 1;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';
abort;
-- after rollback: back to 'u'
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join3';

-- Transaction: insert then insert on different tables stays 'i'
refresh materialized view mv_join2;
begin;
insert into jt1 values(30, 300);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';
insert into jt2 values(31, 3100);
-- still 'i' since both are inserts
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join2';
abort;

-- Cross join (FROM t1, t2 with no WHERE): verify registration
create materialized view mv_cross_join as
  select jt1.id as id1, jt2.id as id2 from jt1, jt2;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_cross_join';
select count(*) from gp_matview_tables mt
  join pg_class c on mt.relid = c.oid
  where mt.mvoid = 'mv_cross_join'::regclass;

-- Drop base table CASCADE removes dependent join MVs and aux entries
create table jt_drop(id int, val int);
insert into jt_drop values(1, 10);
create materialized view mv_join_drop as
  select jt1.id, jt_drop.val from jt1 join jt_drop on jt1.id = jt_drop.id;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_join_drop';
drop table jt_drop cascade;
select count(*) from gp_matview_aux where mvname = 'mv_join_drop';

-- Clean up join test objects
drop materialized view mv_cross_join;
drop materialized view mv_join_agg;
drop materialized view mv_mixed_join;
drop materialized view mv_join_par;
drop table jt_par cascade;
drop materialized view mv_full_join;
drop materialized view mv_right_join;
drop materialized view mv_left_join;
drop materialized view mv_selfjoin;
drop materialized view mv_join3;
drop materialized view mv_implicit_join;
drop materialized view mv_join2;
drop table jt3;
drop table jt2;
drop table jt1;

--
-- Test AQUMV (Answer Query Using Materialized Views) with join queries.
-- Each matching test shows EXPLAIN + SELECT with GUC off (original plan),
-- then EXPLAIN + SELECT with GUC on (MV rewrite). Results must match.
--
create table aqj_t1(a int, b int) distributed by (a);
create table aqj_t2(a int, b int) distributed by (a);
create table aqj_t3(a int, b int) distributed by (a);
insert into aqj_t1 select i, i*10 from generate_series(1, 100) i;
insert into aqj_t2 select i, i*100 from generate_series(1, 100) i;
insert into aqj_t3 select i, i*1000 from generate_series(1, 100) i;
analyze aqj_t1;
analyze aqj_t2;
analyze aqj_t3;

-- 1. Two-table INNER JOIN exact match
create materialized view mv_aqj_join2 as
  select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;
analyze mv_aqj_join2;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;
select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a order by 1 limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;
select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a order by 1 limit 5;

-- 2. Join with WHERE clause
create materialized view mv_aqj_where as
  select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 5;
analyze mv_aqj_where;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 5;
select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 5 order by 1 limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 5;
select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 5 order by 1 limit 5;

-- 3. Join with GROUP BY + aggregate
create materialized view mv_aqj_agg as
  select aqj_t1.a, count(*) as cnt from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a group by aqj_t1.a;
analyze mv_aqj_agg;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, count(*) as cnt from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a group by aqj_t1.a;
select aqj_t1.a, count(*) as cnt from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a group by aqj_t1.a order by 1 limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, count(*) as cnt from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a group by aqj_t1.a;
select aqj_t1.a, count(*) as cnt from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a group by aqj_t1.a order by 1 limit 5;

-- 4. Non-match: different WHERE clause (should show Hash Join, not MV)
set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a where aqj_t1.a > 10;

-- 5. Non-match: different target list
explain(costs off) select aqj_t1.b, aqj_t2.a from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;

-- 6. Non-match: different join type (INNER vs LEFT)
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 left join aqj_t2 on aqj_t1.a = aqj_t2.a;

-- 7. Three-table join
create materialized view mv_aqj_join3 as
  select aqj_t1.a, aqj_t2.b, aqj_t3.b as c
  from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a join aqj_t3 on aqj_t2.a = aqj_t3.a;
analyze mv_aqj_join3;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, aqj_t2.b, aqj_t3.b as c
  from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a join aqj_t3 on aqj_t2.a = aqj_t3.a;
select aqj_t1.a, aqj_t2.b, aqj_t3.b as c
  from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a join aqj_t3 on aqj_t2.a = aqj_t3.a
  order by 1 limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, aqj_t2.b, aqj_t3.b as c
  from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a join aqj_t3 on aqj_t2.a = aqj_t3.a;
select aqj_t1.a, aqj_t2.b, aqj_t3.b as c
  from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a join aqj_t3 on aqj_t2.a = aqj_t3.a
  order by 1 limit 5;

-- 8. Implicit join (FROM t1, t2 WHERE ...)
create materialized view mv_aqj_implicit as
  select aqj_t1.a, aqj_t2.b from aqj_t1, aqj_t2 where aqj_t1.a = aqj_t2.a;
analyze mv_aqj_implicit;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1, aqj_t2 where aqj_t1.a = aqj_t2.a;
select aqj_t1.a, aqj_t2.b from aqj_t1, aqj_t2 where aqj_t1.a = aqj_t2.a order by 1 limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1, aqj_t2 where aqj_t1.a = aqj_t2.a;
select aqj_t1.a, aqj_t2.b from aqj_t1, aqj_t2 where aqj_t1.a = aqj_t2.a order by 1 limit 5;

-- 9. MV not up-to-date: after INSERT on base table
insert into aqj_t1 values(999, 9990);
set enable_answer_query_using_materialized_views = on;
-- Should NOT use mv_aqj_join2 (status is 'i')
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;

-- 10. After REFRESH: should use MV again
refresh materialized view mv_aqj_join2;
analyze mv_aqj_join2;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;

-- 11. GUC off: should NOT use MV
set enable_answer_query_using_materialized_views = off;
explain(costs off) select aqj_t1.a, aqj_t2.b from aqj_t1 join aqj_t2 on aqj_t1.a = aqj_t2.a;

--
-- More complex join AQUMV test cases with richer schemas
--

create table aqj_orders(
  order_id int,
  customer_id int,
  amount numeric(10,2),
  status text,
  order_date date
) distributed by (order_id);

create table aqj_customers(
  customer_id int,
  name text,
  region text,
  credit_limit numeric(10,2)
) distributed by (customer_id);

create table aqj_products(
  product_id int,
  name text,
  category text,
  price numeric(10,2)
) distributed by (product_id);

create table aqj_order_items(
  item_id int,
  order_id int,
  product_id int,
  quantity int
) distributed by (item_id);

insert into aqj_customers select i, 'cust_' || i, case when i % 3 = 0 then 'east' when i % 3 = 1 then 'west' else 'north' end, (i * 100)::numeric(10,2) from generate_series(1, 50) i;
insert into aqj_orders select i, (i % 50) + 1, (i * 10.5)::numeric(10,2), case when i % 4 = 0 then 'shipped' when i % 4 = 1 then 'pending' when i % 4 = 2 then 'delivered' else 'cancelled' end, '2024-01-01'::date + (i % 365) from generate_series(1, 200) i;
insert into aqj_products select i, 'prod_' || i, case when i % 5 = 0 then 'electronics' when i % 5 = 1 then 'books' when i % 5 = 2 then 'clothing' when i % 5 = 3 then 'food' else 'toys' end, (i * 5.99)::numeric(10,2) from generate_series(1, 30) i;
insert into aqj_order_items select i, (i % 200) + 1, (i % 30) + 1, (i % 10) + 1 from generate_series(1, 500) i;

analyze aqj_customers;
analyze aqj_orders;
analyze aqj_products;
analyze aqj_order_items;

-- 12. Join with multiple columns + WHERE on text column
create materialized view mv_aqj_orders_cust as
  select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped';
analyze mv_aqj_orders_cust;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped';
select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped'
  order by o.order_id limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped';
select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped'
  order by o.order_id limit 5;

-- 13. Four-table join
create materialized view mv_aqj_order_details as
  select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id;
analyze mv_aqj_order_details;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id;
select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  order by o.order_id, p.name limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id;
select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  order by o.order_id, p.name limit 5;

-- 14. GROUP BY on join with multiple aggregates: sum, count, avg
create materialized view mv_aqj_cust_summary as
  select c.region, count(*) as order_count, sum(o.amount) as total_amount, avg(o.amount) as avg_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region;
analyze mv_aqj_cust_summary;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select c.region, count(*) as order_count, sum(o.amount) as total_amount, avg(o.amount) as avg_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region;
select c.region, count(*) as order_count, sum(o.amount) as total_amount, avg(o.amount) as avg_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region
  order by c.region;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select c.region, count(*) as order_count, sum(o.amount) as total_amount, avg(o.amount) as avg_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region;
select c.region, count(*) as order_count, sum(o.amount) as total_amount, avg(o.amount) as avg_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region
  order by c.region;

-- 15. Join with expression in target list (arithmetic + function)
create materialized view mv_aqj_expr as
  select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id;
analyze mv_aqj_expr;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id;
select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  order by o.order_id limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id;
select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  order by o.order_id limit 5;

-- 16. Non-match: same tables + expressions, but extra WHERE (should NOT match mv_aqj_expr)
set enable_answer_query_using_materialized_views = on;
explain(costs off) select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where c.region = 'east';

-- 17. Non-match: same tables but different aggregate target list
explain(costs off)
  select c.region, sum(o.amount) as total_amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region;

-- 18. Non-match: different join order (o JOIN c vs c JOIN o)
explain(costs off) select o.order_id, o.amount, c.name, c.region
  from aqj_customers c join aqj_orders o on o.customer_id = c.customer_id
  where o.status = 'shipped';

-- 19. Join with compound WHERE (multiple AND conditions)
create materialized view mv_aqj_compound_where as
  select o.order_id, o.amount, c.name
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending' and c.region = 'west' and o.amount > 50;
analyze mv_aqj_compound_where;

set enable_answer_query_using_materialized_views = off;
explain(costs off) select o.order_id, o.amount, c.name
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending' and c.region = 'west' and o.amount > 50;
select o.order_id, o.amount, c.name
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending' and c.region = 'west' and o.amount > 50
  order by o.order_id limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off) select o.order_id, o.amount, c.name
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending' and c.region = 'west' and o.amount > 50;
select o.order_id, o.amount, c.name
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending' and c.region = 'west' and o.amount > 50
  order by o.order_id limit 5;

-- 20. Self-join
create materialized view mv_aqj_selfjoin as
  select o1.order_id as id1, o2.order_id as id2, o1.amount as amt1, o2.amount as amt2
  from aqj_orders o1 join aqj_orders o2 on o1.customer_id = o2.customer_id
  where o1.order_id < o2.order_id;
analyze mv_aqj_selfjoin;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select o1.order_id as id1, o2.order_id as id2, o1.amount as amt1, o2.amount as amt2
  from aqj_orders o1 join aqj_orders o2 on o1.customer_id = o2.customer_id
  where o1.order_id < o2.order_id;
select o1.order_id as id1, o2.order_id as id2, o1.amount as amt1, o2.amount as amt2
  from aqj_orders o1 join aqj_orders o2 on o1.customer_id = o2.customer_id
  where o1.order_id < o2.order_id
  order by o1.order_id, o2.order_id limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select o1.order_id as id1, o2.order_id as id2, o1.amount as amt1, o2.amount as amt2
  from aqj_orders o1 join aqj_orders o2 on o1.customer_id = o2.customer_id
  where o1.order_id < o2.order_id;
select o1.order_id as id1, o2.order_id as id2, o1.amount as amt1, o2.amount as amt2
  from aqj_orders o1 join aqj_orders o2 on o1.customer_id = o2.customer_id
  where o1.order_id < o2.order_id
  order by o1.order_id, o2.order_id limit 5;

-- 21. GROUP BY with multi-column key on join
create materialized view mv_aqj_grp_multi as
  select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status;
analyze mv_aqj_grp_multi;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status;
select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status
  order by c.region, o.status limit 6;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status;
select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status
  order by c.region, o.status limit 6;

-- 22. Four-table join with WHERE and aggregate
create materialized view mv_aqj_3way_agg as
  select c.region, p.category, sum(oi.quantity) as total_qty, count(*) as line_count
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  where o.status = 'delivered'
  group by c.region, p.category;
analyze mv_aqj_3way_agg;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select c.region, p.category, sum(oi.quantity) as total_qty, count(*) as line_count
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  where o.status = 'delivered'
  group by c.region, p.category;
select c.region, p.category, sum(oi.quantity) as total_qty, count(*) as line_count
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  where o.status = 'delivered'
  group by c.region, p.category
  order by c.region, p.category limit 6;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select c.region, p.category, sum(oi.quantity) as total_qty, count(*) as line_count
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  where o.status = 'delivered'
  group by c.region, p.category;
select c.region, p.category, sum(oi.quantity) as total_qty, count(*) as line_count
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  where o.status = 'delivered'
  group by c.region, p.category
  order by c.region, p.category limit 6;

-- 23. Implicit four-table join (comma style)
create materialized view mv_aqj_implicit3 as
  select o.order_id, c.name, p.name as product_name
  from aqj_orders o, aqj_customers c, aqj_order_items oi, aqj_products p
  where o.customer_id = c.customer_id and o.order_id = oi.order_id and oi.product_id = p.product_id
    and o.status = 'pending';
analyze mv_aqj_implicit3;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select o.order_id, c.name, p.name as product_name
  from aqj_orders o, aqj_customers c, aqj_order_items oi, aqj_products p
  where o.customer_id = c.customer_id and o.order_id = oi.order_id and oi.product_id = p.product_id
    and o.status = 'pending';
select o.order_id, c.name, p.name as product_name
  from aqj_orders o, aqj_customers c, aqj_order_items oi, aqj_products p
  where o.customer_id = c.customer_id and o.order_id = oi.order_id and oi.product_id = p.product_id
    and o.status = 'pending'
  order by o.order_id, p.name limit 5;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select o.order_id, c.name, p.name as product_name
  from aqj_orders o, aqj_customers c, aqj_order_items oi, aqj_products p
  where o.customer_id = c.customer_id and o.order_id = oi.order_id and oi.product_id = p.product_id
    and o.status = 'pending';
select o.order_id, c.name, p.name as product_name
  from aqj_orders o, aqj_customers c, aqj_order_items oi, aqj_products p
  where o.customer_id = c.customer_id and o.order_id = oi.order_id and oi.product_id = p.product_id
    and o.status = 'pending'
  order by o.order_id, p.name limit 5;

-- 24. Result correctness across DML + REFRESH cycle
insert into aqj_orders values(201, 1, 9999.99, 'shipped', '2025-12-31');
set enable_answer_query_using_materialized_views = on;
-- Stale: should NOT use MV
explain(costs off) select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped';
-- Refresh and verify MV is used again
refresh materialized view mv_aqj_orders_cust;
analyze mv_aqj_orders_cust;
explain(costs off) select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped';
-- The new row should appear in results via MV scan
select o.order_id, o.amount, c.name, c.region
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped' and o.order_id = 201;

-- 25. Post-DML comprehensive: refresh all, then verify GUC off vs on results
refresh materialized view mv_aqj_order_details;
refresh materialized view mv_aqj_expr;
refresh materialized view mv_aqj_selfjoin;
refresh materialized view mv_aqj_grp_multi;
refresh materialized view mv_aqj_3way_agg;
refresh materialized view mv_aqj_implicit3;
analyze mv_aqj_order_details;
analyze mv_aqj_expr;
analyze mv_aqj_selfjoin;
analyze mv_aqj_grp_multi;
analyze mv_aqj_3way_agg;
analyze mv_aqj_implicit3;

-- Verify four-table join results after DML+refresh
set enable_answer_query_using_materialized_views = off;
select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  order by o.order_id, p.name limit 5;

set enable_answer_query_using_materialized_views = on;
select o.order_id, c.name as customer_name, p.name as product_name, oi.quantity, p.price
  from aqj_orders o
  join aqj_customers c on o.customer_id = c.customer_id
  join aqj_order_items oi on o.order_id = oi.order_id
  join aqj_products p on oi.product_id = p.product_id
  order by o.order_id, p.name limit 5;

-- Verify expression MV results after DML+refresh
set enable_answer_query_using_materialized_views = off;
select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  order by o.order_id limit 5;

set enable_answer_query_using_materialized_views = on;
select o.order_id, o.amount * 1.1 as amount_with_tax, c.name, upper(c.region) as region_upper
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  order by o.order_id limit 5;

-- Verify multi-key GROUP BY results after DML+refresh
set enable_answer_query_using_materialized_views = off;
select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status
  order by c.region, o.status limit 6;

set enable_answer_query_using_materialized_views = on;
select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by c.region, o.status
  order by c.region, o.status limit 6;

-- 26. Non-match: LIMIT vs FETCH FIRST WITH TIES (limitOption differs)
create materialized view mv_aqj_limit_test as
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped'
  order by o.order_id limit 5;
analyze mv_aqj_limit_test;

set enable_answer_query_using_materialized_views = on;
-- Same tables/WHERE/ORDER BY but FETCH FIRST WITH TIES: should NOT match
explain(costs off)
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped'
  order by o.order_id fetch first 5 rows with ties;
-- Identical LIMIT query: should match
explain(costs off)
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'shipped'
  order by o.order_id limit 5;

-- 27. Match: FETCH FIRST WITH TIES exact match
create materialized view mv_aqj_with_ties as
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending'
  order by o.order_id fetch first 5 rows with ties;
analyze mv_aqj_with_ties;

set enable_answer_query_using_materialized_views = off;
explain(costs off)
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending'
  order by o.order_id fetch first 5 rows with ties;
select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending'
  order by o.order_id fetch first 5 rows with ties;

set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending'
  order by o.order_id fetch first 5 rows with ties;
select o.order_id, o.amount
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  where o.status = 'pending'
  order by o.order_id fetch first 5 rows with ties;

-- 28. Non-match: GROUP BY vs GROUP BY DISTINCT (groupDistinct differs)
-- MV mv_aqj_grp_multi uses GROUP BY (groupDistinct=false, registered in catalog)
-- Query uses GROUP BY DISTINCT — should NOT match
set enable_answer_query_using_materialized_views = on;
explain(costs off)
  select c.region, o.status, count(*) as cnt, sum(o.amount) as total
  from aqj_orders o join aqj_customers c on o.customer_id = c.customer_id
  group by distinct c.region, o.status;

-- Clean up AQUMV join test objects
drop materialized view mv_aqj_with_ties;
drop materialized view mv_aqj_limit_test;
drop materialized view mv_aqj_implicit3;
drop materialized view mv_aqj_3way_agg;
drop materialized view mv_aqj_grp_multi;
drop materialized view mv_aqj_selfjoin;
drop materialized view mv_aqj_compound_where;
drop materialized view mv_aqj_expr;
drop materialized view mv_aqj_cust_summary;
drop materialized view mv_aqj_order_details;
drop materialized view mv_aqj_orders_cust;
drop materialized view mv_aqj_implicit;
drop materialized view mv_aqj_join3;
drop materialized view mv_aqj_agg;
drop materialized view mv_aqj_where;
drop materialized view mv_aqj_join2;
drop table aqj_order_items;
drop table aqj_products;
drop table aqj_customers;
drop table aqj_orders;
drop table aqj_t3;
drop table aqj_t2;
drop table aqj_t1;

-- test drop table
select mvname, datastatus from gp_matview_aux where mvname in ('mv0','mv1', 'mv2', 'mv3');
drop materialized view mv2;
drop table t1 cascade;
select mvname, datastatus from gp_matview_aux where mvname in ('mv0','mv1', 'mv2', 'mv3');

--
-- test issue https://github.com/apache/cloudberry/issues/582
-- test rules
begin;
create table t1_issue_582(i int, j int);
create table t2_issue_582(i int, j int);
create table t3_issue_582(i int, j int);
create materialized view mv_t2_issue_582 as select j from t2_issue_582 where i = 1;
create rule r1 as on insert TO t1_issue_582 do also insert into t2_issue_582 values(1,1);
select count(*) from t1_issue_582;
select count(*) from t2_issue_582;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_t2_issue_582';
insert into t1_issue_582 values(1,1);
select count(*) from t1_issue_582;
select count(*) from t2_issue_582;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_t2_issue_582';
abort;

--
-- test issue https://github.com/apache/cloudberry/issues/582
-- test writable CTE
--
begin;
create table t_cte_issue_582(i int, j int);
create materialized view mv_t_cte_issue_582 as select j from t_cte_issue_582 where i = 1;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_t_cte_issue_582';
with mod1 as (insert into t_cte_issue_582 values(1, 1) returning *) select * from mod1;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_t_cte_issue_582';
abort;

-- test partitioned tables
create table par(a int, b int, c int) partition by range(b)
    subpartition by range(c) subpartition template (start (1) end (3) every (1))
    (start(1) end(3) every(1));
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
create materialized view mv_par as select * from par;
create materialized view mv_par1 as select * from  par_1_prt_1;
create materialized view mv_par1_1 as select * from par_1_prt_1_2_prt_1;
create materialized view mv_par1_2 as select * from par_1_prt_1_2_prt_2;
create materialized view mv_par2 as select * from  par_1_prt_2;
create materialized view mv_par2_1 as select * from  par_1_prt_2_2_prt_1;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
insert into par_1_prt_1 values (1, 1, 1);
-- mv_par1* shoud be updated
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
insert into par values (1, 2, 2);
-- mv_par* should be updated
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';

refresh materialized view mv_par;
refresh materialized view mv_par1;
refresh materialized view mv_par1_1;
refresh materialized view mv_par1_2;
refresh materialized view mv_par2;
refresh materialized view mv_par2_1;
begin;
insert into par_1_prt_2_2_prt_1 values (1, 2, 1);
-- mv_par1* should not be updated
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
truncate par_1_prt_2;
-- mv_par1* should not be updated
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;
truncate par_1_prt_2;
-- mv_par1* should not be updated
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';

refresh materialized view mv_par;
refresh materialized view mv_par1;
refresh materialized view mv_par1_1;
refresh materialized view mv_par1_2;
refresh materialized view mv_par2;
refresh materialized view mv_par2_1;
vacuum full par_1_prt_1_2_prt_1;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';

refresh materialized view mv_par;
refresh materialized view mv_par1;
refresh materialized view mv_par1_1;
refresh materialized view mv_par1_2;
refresh materialized view mv_par2;
refresh materialized view mv_par2_1;
vacuum full par;
-- all should be updated.
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';

refresh materialized view mv_par;
refresh materialized view mv_par1;
refresh materialized view mv_par1_1;
refresh materialized view mv_par1_2;
refresh materialized view mv_par2;
refresh materialized view mv_par2_1;
begin;
create table par_1_prt_1_2_prt_3  partition of par_1_prt_1 for values from  (3) to (4);
-- update status when partition of
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
drop table par_1_prt_1 cascade;
-- update status when drop table 
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
alter table par_1_prt_1 detach partition par_1_prt_1_2_prt_1;
-- update status when detach
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
create table new_par(a int, b int, c int);
-- update status when attach
alter table par_1_prt_1 attach partition new_par for values from (4) to (5);
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

--
-- Maintain materialized views on partitioned tables from bottom to up.
--
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
refresh materialized view mv_par;
refresh materialized view mv_par1;
refresh materialized view mv_par1_1;
refresh materialized view mv_par1_2;
refresh materialized view mv_par2;
refresh materialized view mv_par2_1;
begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
insert into par values(1, 1, 1), (1, 1, 2);
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
insert into par_1_prt_2_2_prt_1 values(2, 2, 1);
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
delete from par where b = 2  and c = 1;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
delete from par_1_prt_1_2_prt_2;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

-- Across partition update.
begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
update par set c = 2 where b = 1 and c = 1;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

-- Split Update with acrosss partition update.
begin;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
update par set c = 2, a = 2 where  b = 1 and c = 1;
select mvname, datastatus from gp_matview_aux where mvname like 'mv_par%';
abort;

-- Test report warning if extend protocol data is not consumed.
--start_ignore
drop extension gp_inject_fault;
create extension gp_inject_fault;
--end_ignore

select gp_inject_fault_infinite('consume_extend_protocol_data', 'skip', dbid)
from gp_segment_configuration where role = 'p' and content = -1;

begin;
update par set c = 2 where b = 1 and c = 1;
end;

begin;
insert into par values(1, 1, 1), (1, 1, 2);
end;

begin;
update par set c = 2, a = 2 where  b = 1 and c = 1;
end;

begin;
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
delete from par_1_prt_1_2_prt_2;
end;

begin;
insert into par values(1, 1, 1), (1, 1, 2), (2, 2, 1), (2, 2, 2);
update par set c = 2, a = 2 where  b = 1 and c = 1;
end;

select gp_inject_fault('consume_extend_protocol_data', 'reset', dbid)
from gp_segment_configuration where role = 'p' and content = -1;
--
-- End of Maintain materialized views on partitioned tables from bottom to up.
--

-- Test Rename matview.
begin;
create materialized view mv_name1 as
select * from par with no data;
select count(*) from gp_matview_aux where mvname = 'mv_name1';

alter materialized view mv_name1 rename to mv_name2;
select count(*) from gp_matview_aux where mvname = 'mv_name1';
select count(*) from gp_matview_aux where mvname = 'mv_name2';
abort;

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
-- end_ignore
create table par_normal_oid(a int, b int) partition by range(a) using ao_row distributed randomly;
select gp_inject_fault('bump_oid', 'skip', dbid) from gp_segment_configuration where role = 'p' and content = -1;
create table sub_par1_large_oid partition of par_normal_oid for values from (1) to (2) using ao_row;
select 'sub_par1_large_oid'::regclass::oid > x'7FFFFFFF'::bigint;
select gp_inject_fault('bump_oid', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = -1;
create materialized view mv_par_normal_oid as
    select count(*) from par_normal_oid;
select mvname, datastatus from gp_matview_aux where mvname = 'mv_par_normal_oid';
insert into par_normal_oid values(1, 2);
select mvname, datastatus from gp_matview_aux where mvname = 'mv_par_normal_oid';

--start_ignore
drop schema matview_data_schema cascade;
--end_ignore
reset enable_answer_query_using_materialized_views;
reset optimizer;
