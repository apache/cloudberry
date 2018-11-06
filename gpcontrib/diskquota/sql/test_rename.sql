-- test rename schema
create schema srs1;
select diskquota.set_schema_quota('srs1', '1 MB');
set search_path to srs1;
create table a(i int);
-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail
insert into a select generate_series(1,10);
alter schema srs1 rename to srs2;
set search_path to srs2;

-- expect insert fail
insert into a select generate_series(1,10);
-- test rename table
alter table a rename to a2;
-- expect insert fail
insert into a2 select generate_series(1,10);

drop table a2;
reset search_path;
drop schema srs2;

-- test rename role
create schema srr1;
create role srerole nologin;
select diskquota.set_role_quota('srerole', '1MB');
set search_path to srr1;
create table a(i int);
alter table a owner to srerole;

-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail
insert into a select generate_series(1,10);
alter role srerole rename to srerole2;
-- expect insert fail
insert into a select generate_series(1,10);
-- test rename table
alter table a rename to a2;
-- expect insert fail
insert into a2 select generate_series(1,10);

drop table a2;
drop role srerole2;
reset search_path;
drop schema srr1;

