-- we ingore this in global init-file, replace words here
-- so that we can let only this case tested in this sql.
-- start_matchsubs
-- m/WARNING:  creating a table with no columns./
-- s/WARNING:  creating a table with no columns./WARNING:  create a table with no columns (misc_jiras)./
-- end_matchsubs

drop schema if exists misc_jiras;
create schema misc_jiras;

--
-- Test backward scanning of tuplestore spill files.
--
-- When tuplestore cannot store all the data in memory it will spill some of
-- the data to temporary files.  In gpdb we used to disable the backward
-- scanning from these spill files because we could not determine the tuple
-- type, memtup or heaptup, correctly.  The issue is fixed, the backward
-- scanning should be supported now.
--

create table misc_jiras.t1 (c1 int, c2 text, c3 smallint) distributed by (c1);
insert into misc_jiras.t1 select i % 13, md5(i::text), i % 3
  from generate_series(1, 80000) i;

-- tuplestore in windowagg uses statement_mem to control the in-memory data size,
-- set a small value to trigger the spilling.

set statement_mem to '1200kB';

-- Inject fault at 'winagg_after_spool_tuples' to show that the tuplestore spills
-- to disk.
select gp_inject_fault('winagg_after_spool_tuples', 'skip', dbid)
  from gp_segment_configuration WHERE role='p' AND content>=0;

select sum(cc) from (
    select c1
         , c2
         , case when count(c3) = 0 then -1.0
                else cume_dist() over (partition by c1,
                                       case when count(c3) > 0 then 1 else 0 end
                                       order by count(c3), c2)
           end as cc
      from misc_jiras.t1
     group by 1, 2
) tt;

select gp_inject_fault('winagg_after_spool_tuples', 'reset', dbid)
  from gp_segment_configuration WHERE role='p' AND content>=0;

reset statement_mem;
drop table misc_jiras.t1;
drop schema misc_jiras;

-- test for issue https://github.com/GreengageDB/greengage/issues/14539
-- the \c command to renew the session to make sure the global var
-- NextRecordTypmod is 0. For details please refer to the issue.
create table t_record_type_param_dispatch (a int, b int) distributed by (a);

explain (costs off)
with cte as (
  select * from t_record_type_param_dispatch order by random() limit
  ( select count(*) /2 from t_record_type_param_dispatch )
)
select *, case when t in (select t from cte t) then 'a' else 'b' end
from t_record_type_param_dispatch t;

\c

with cte as (
  select * from t_record_type_param_dispatch order by random() limit
  ( select count(*) /2 from t_record_type_param_dispatch )
)
select *, case when t in (select t from cte t) then 'a' else 'b' end
from t_record_type_param_dispatch t;

drop table t_record_type_param_dispatch;

-- Github Issue 17271
-- test create zero-column table will throw warning only on QD
-- test policy on each segment (including coordinator)
create table t_17271();
-- coordinator policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_distribution_policy where localoid = 't_17271'::regclass::oid;
-- segment policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_dist_random('gp_distribution_policy') where localoid = 't_17271'::regclass::oid;

drop table t_17271;

-- test for no column table CTAS
-- these tables should distributed randomly
create table t1_17271(a int, b varchar(20)) distributed by (a);
create table t2_17271 as
select distinct on (b) from t1_17271;
select * from t2_17271;

-- coordinator policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_distribution_policy where localoid = 't2_17271'::regclass::oid;
-- segment policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_dist_random('gp_distribution_policy') where localoid = 't2_17271'::regclass::oid;
