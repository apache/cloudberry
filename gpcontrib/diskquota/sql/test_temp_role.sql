-- Test temp table restrained by role id
create schema strole;
create role u3temp nologin;
set search_path to strole;

select diskquota.set_role_quota('u3temp', '1MB');
create table a(i int);
alter table a owner to u3temp;
create temp table ta(i int);
alter table ta owner to u3temp;

-- expected failed: fill temp table
insert into ta select generate_series(1,100000000);
-- expected failed: 
insert into a select generate_series(1,100);
drop table ta;
select pg_sleep(5);
insert into a select generate_series(1,100);

drop table a;
reset search_path;
drop schema strole;
