-- Test alter table add column
create schema scolumn;
select diskquota.set_schema_quota('scolumn', '1 MB');
set search_path to scolumn;
select pg_sleep(5);

create table a2(i int);
insert into a2 select generate_series(1,20000);
insert into a2 select generate_series(1,10);
ALTER TABLE a2 ADD COLUMN j varchar(50);
update a2 set j = 'add value for column j';
select pg_sleep(5);
-- expect insert failed after add column
insert into a2 select generate_series(1,10);

drop table a2;
reset search_path;
drop schema scolumn;

