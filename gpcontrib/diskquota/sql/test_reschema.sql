-- Test re-set_schema_quota
create schema srE;
select diskquota.set_schema_quota('srE', '1 MB');
set search_path to srE;
create table a(i int);
-- expect insert fail
insert into a select generate_series(1,1000000000);
-- expect insert fail when exceed quota limit
insert into a select generate_series(1,1000);
-- set schema quota larger
select diskquota.set_schema_quota('srE', '1 GB');
select pg_sleep(5);
-- expect insert succeed
insert into a select generate_series(1,1000);

drop table a;
reset search_path;
drop schema srE;

