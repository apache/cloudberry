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
