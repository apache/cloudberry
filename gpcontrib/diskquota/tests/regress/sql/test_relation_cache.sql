-- init
CREATE OR REPLACE FUNCTION diskquota.check_relation_cache()
RETURNS boolean
as $$
declare t1 oid[];
declare t2 oid[];
begin
t1 := (select array_agg(distinct((a).relid)) from diskquota.show_relation_cache_all_seg() as a where (a).relid != (a).primary_table_oid);
t2 := (select distinct((a).auxrel_oid) from diskquota.show_relation_cache_all_seg() as a where (a).relid = (a).primary_table_oid);
return t1 = t2;
end;
$$ LANGUAGE plpgsql;

-- heap table
begin;
create table t(i int) DISTRIBUTED BY (i);
insert into t select generate_series(1, 100000);

select count(*) from diskquota.show_relation_cache_all_seg();
commit;

select diskquota.wait_for_worker_new_epoch();
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- toast table
begin;
create table t(t text) DISTRIBUTED BY (t);
insert into t select array(select * from generate_series(1,1000)) from generate_series(1, 1000);

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select diskquota.wait_for_worker_new_epoch();
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- AO table
begin;
create table t(a int, b text) with(appendonly=true) DISTRIBUTED BY (a);
insert into t select generate_series(1,1000) as a, repeat('a', 1000) as b;

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select diskquota.wait_for_worker_new_epoch();
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

-- AOCS table
begin;
create table t(a int, b text) with(appendonly=true, orientation=column) DISTRIBUTED BY (a);
insert into t select generate_series(1,1000) as a, repeat('a', 1000) as b;

select count(*) from diskquota.show_relation_cache_all_seg();

select diskquota.check_relation_cache();
commit;

select diskquota.wait_for_worker_new_epoch();
select count(*) from diskquota.show_relation_cache_all_seg();
drop table t;

DROP FUNCTION diskquota.check_relation_cache();
