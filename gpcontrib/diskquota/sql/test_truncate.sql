-- Test truncate
create schema s7;
select diskquota.set_schema_quota('s7', '1 MB');
set search_path to s7;
create table a (i int);
create table b (i int);
insert into a select generate_series(1,50000);
select pg_sleep(5);
-- expect insert fail
insert into a select generate_series(1,30);
insert into b select generate_series(1,30);
truncate table a;
select pg_sleep(5);
-- expect insert succeed
insert into a select generate_series(1,30);
insert into b select generate_series(1,30);

drop table a, b;
reset search_path;
drop schema s7;

