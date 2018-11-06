-- Test role quota

create schema srole;
set search_path to srole;

CREATE role u1 NOLOGIN;
CREATE role u2 NOLOGIN;
CREATE TABLE b (t text);
ALTER TABLE b OWNER TO u1;
CREATE TABLE b2 (t text);
ALTER TABLE b2 OWNER TO u1;

select diskquota.set_role_quota('u1', '1 MB');

insert into b select generate_series(1,100);
-- expect insert fail
insert into b select generate_series(1,100000000);
-- expect insert fail
insert into b select generate_series(1,100);
-- expect insert fail
insert into b2 select generate_series(1,100);
alter table b owner to u2;
select pg_sleep(5);
-- expect insert succeed
insert into b select generate_series(1,100);
-- expect insert succeed
insert into b2 select generate_series(1,100);

drop table b, b2;
drop role u1, u2;
reset search_path;
drop schema srole;
