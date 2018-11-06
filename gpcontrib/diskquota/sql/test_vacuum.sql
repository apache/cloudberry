-- Test vacuum full
create schema s6;
select diskquota.set_schema_quota('s6', '1 MB');
set search_path to s6;
create table a (i int);
create table b (i int);
insert into a select generate_series(1,50000);
select pg_sleep(5);
-- expect insert fail
insert into a select generate_series(1,10);
-- expect insert fail
insert into b select generate_series(1,10);
delete from a where i > 10;
vacuum full a;
select pg_sleep(5);
-- expect insert succeed
insert into a select generate_series(1,10);
insert into b select generate_series(1,10);

drop table a, b;
reset search_path;
drop schema s6;

