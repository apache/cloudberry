-- A failed DROP COLUMN must not leave stale distribution policy state in the
-- relcache after rolling back to a savepoint. Cover heap and AO relations.
drop table if exists drop_distkey_ao;
drop table if exists drop_distkey_heap;

create table drop_distkey_ao (a int, b int) with (appendonly=true) distributed by (a);
create table drop_distkey_heap (a int, b int) distributed by (a);
insert into drop_distkey_ao select i, i from generate_series(1, 10) i;
insert into drop_distkey_heap select i, i from generate_series(1, 10) i;

-- Load both relcache entries in session 1 before session 2 locks the catalog.
1: select count(*) from drop_distkey_ao;
1: select count(*) from drop_distkey_heap;

1: begin;
1: savepoint s1;
2: begin;
2: lock table gp_distribution_policy in access exclusive mode;
1: set local lock_timeout = '1s';
1: alter table drop_distkey_ao drop column a;
1: rollback to s1;
1: savepoint s2;
1: set local lock_timeout = '1s';
1: alter table drop_distkey_heap drop column a;
1: rollback to s2;
2: commit;

-- The failed statements did not change the catalog.
1: select localoid::regclass, distkey from gp_distribution_policy where localoid in ('drop_distkey_ao'::regclass, 'drop_distkey_heap'::regclass) order by 1;

-- Retrying after the catalog lock is released must switch both tables to
-- random distribution.
1: alter table drop_distkey_ao drop column a;
1: alter table drop_distkey_heap drop column a;
1: commit;

select localoid::regclass, distkey from gp_distribution_policy where localoid in ('drop_distkey_ao'::regclass, 'drop_distkey_heap'::regclass) order by 1;
select count(*) from drop_distkey_ao;
select count(*) from drop_distkey_heap;
insert into drop_distkey_ao values (100, 100);
insert into drop_distkey_heap values (100, 100);
select count(*) from drop_distkey_ao;
select count(*) from drop_distkey_heap;

drop table drop_distkey_ao;
drop table drop_distkey_heap;
