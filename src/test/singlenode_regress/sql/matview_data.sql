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

-- test drop table
select mvname, datastatus from gp_matview_aux where mvname in ('mv0','mv1', 'mv2', 'mv3');
drop materialized view mv2;
drop table t1 cascade;
select mvname, datastatus from gp_matview_aux where mvname in ('mv0','mv1', 'mv2', 'mv3');

--
-- Test triggers only for singlenode mode.
-- All tables are on QD, so triggers could work well, ex
-- modify another table.
create table tri_t1(a int, b int);
create table tri_t2(a int, b int);
create table tri_t3(a int, b int);
create materialized view tri_mv1 as select * from tri_t1;
create materialized view tri_mv2 as select * from tri_t2;
insert into tri_t3 values (1, 2);
create materialized view tri_mv3 as select * from tri_t3;

create function trigger_insert_tri_t2()
returns trigger AS
$$
begin
  execute 'insert into tri_t2 values(1, 1)';
  execute 'update tri_t3 set b = 10 where a = 1;';
  return NEW;
end;
$$
language plpgsql;

create trigger trigger_insert_tri_t2 before insert ON tri_t1
  for each row execute procedure trigger_insert_tri_t2();

select mvname, datastatus from gp_matview_aux where mvname in ('tri_mv1', 'tri_mv2', 'tri_mv3');
select * from tri_t1;
select * from tri_t2;
select * from tri_t3;

insert into tri_t1 values(1, 2);
select * from tri_t1;
-- should also insert data
select * from tri_t2;
-- shoud be updated
select * from tri_t3;
-- check mv status
select mvname, datastatus from gp_matview_aux where mvname in ('tri_mv1', 'tri_mv2', 'tri_mv3');
drop trigger trigger_insert_tri_t2 on tri_t1;
drop function trigger_insert_tri_t2;

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
--
-- End of Maintain materialized views on partitioned tables from bottom to up.
--

-- start_ignore
drop schema matview_data_schema cascade;
-- end_ignore
reset enable_answer_query_using_materialized_views;
reset optimizer;
