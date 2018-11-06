-- Test Drop table
create schema sdrtbl;
select diskquota.set_schema_quota('sdrtbl', '1 MB');
set search_path to sdrtbl;
create table a(i int);
create table a2(i int);
insert into a select generate_series(1,100);
-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail
insert into a2 select generate_series(1,100);
drop table a;
select pg_sleep(5);
insert into a2 select generate_series(1,100);

drop table a2;
reset search_path;
drop schema sdrtbl;


