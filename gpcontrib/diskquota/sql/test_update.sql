-- Test Update
create schema s4;
select diskquota.set_schema_quota('s4', '1 MB');
set search_path to s4;
create table a(i int);
insert into a select generate_series(1,50000);
select pg_sleep(5);
-- expect update fail.
update a set i = 100;
drop table a;
reset search_path;
drop schema s4;

