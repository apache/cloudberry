-- Test schema
create schema s1;
select diskquota.set_schema_quota('s1', '1 MB');
set search_path to s1;

create table a(i int);
insert into a select generate_series(1,100);
-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail
insert into a select generate_series(1,100);
create table a2(i int);
-- expect insert fail
insert into a2 select generate_series(1,100);

-- Test alter table set schema
create schema s2;
alter table s1.a set schema s2;
select pg_sleep(5);
-- expect insert succeed
insert into a2 select generate_series(1,200);
-- expect insert succeed
insert into s2.a select generate_series(1,200);

alter table s2.a set schema badquota;
-- expect failed
insert into badquota.a select generate_series(0, 100);

select schema_name, quota_in_mb from diskquota.show_schema_quota_view where schema_name = 's1';

reset search_path;
drop table s1.a2, badquota.a;
drop schema s1, s2;

