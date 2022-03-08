\!gpconfig -s 'shared_preload_libraries'

create extension diskquota with version '1.0';
\!sleep 5

-- schema quota
create schema s1;
select diskquota.set_schema_quota('s1', '1 MB');
create table s1.a(i int) distributed by (i);
insert into s1.a select generate_series(1, 10000000); -- ok, but should fail after upgrade

-- role quota
create schema srole;
create role u1 nologin;
create table srole.b (t text) distributed by (t);
alter table srole.b owner to u1;
select diskquota.set_role_quota('u1', '1 MB');
insert into srole.b select generate_series(1,100000); -- ok, but should fail after upgrade

\!sleep 5

-- leaked resource:
-- 		role u1
-- 		table s1.a, srole.b
-- 		schema s1, srole
