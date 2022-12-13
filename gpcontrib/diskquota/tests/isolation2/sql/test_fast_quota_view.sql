CREATE SCHEMA s1;
CREATE SCHEMA s2;

CREATE ROLE r LOGIN SUPERUSER;

!\retcode mkdir -p /tmp/spc1;
!\retcode mkdir -p /tmp/spc2;

DROP TABLESPACE IF EXISTS spc1;
CREATE TABLESPACE spc1 LOCATION '/tmp/spc1';
DROP TABLESPACE IF EXISTS spc2;
CREATE TABLESPACE spc2 LOCATION '/tmp/spc2';

SELECT diskquota.set_schema_quota('s1', '100 MB');
SELECT diskquota.set_schema_tablespace_quota('s2', 'spc1','100 MB');
SELECT diskquota.set_role_quota('r', '100 MB');
SELECT diskquota.set_role_tablespace_quota('r', 'spc2', '100 MB');

-- test show_fast_schema_quota_view and show_fast_schema_tablespace_quota_view
1: BEGIN;
1: CREATE TABLE s1.t(i int) DISTRIBUTED BY (i);
1: INSERT INTO s1.t SELECT generate_series(1, 100000);

1: CREATE TABLE s2.t(i int) TABLESPACE spc1 DISTRIBUTED BY (i);
1: INSERT INTO s2.t SELECT generate_series(1, 100000);

1: SELECT diskquota.wait_for_worker_new_epoch();

-- check schema quota view before transaction commits
2: SELECT schema_name, quota_in_mb, nspsize_in_bytes FROM diskquota.show_fast_schema_quota_view;
2: SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view;

1: COMMIT;
2: SELECT diskquota.wait_for_worker_new_epoch();
2: SELECT schema_name, quota_in_mb, nspsize_in_bytes FROM diskquota.show_fast_schema_quota_view;
2: SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view;

-- login r to test role quota view
1: SET ROLE r;

-- test show_fast_role_quota_view and show_fast_role_tablespace_quota_view
1: BEGIN;
1: CREATE TABLE t1(i int) DISTRIBUTED BY (i);
1: INSERT INTO t1 SELECT generate_series(1, 100000);

1: CREATE TABLE t2(i int) TABLESPACE spc2 DISTRIBUTED BY (i);
1: INSERT INTO t2 SELECT generate_series(1, 100000);

1: SELECT diskquota.wait_for_worker_new_epoch();

-- check role quota view before transaction commits
2: SELECT role_name, quota_in_mb, rolsize_in_bytes FROM diskquota.show_fast_role_quota_view;
2: SELECT role_name, tablespace_name, quota_in_mb, rolsize_tablespace_in_bytes FROM diskquota.show_fast_role_tablespace_quota_view;

1: COMMIT;
2: SELECT diskquota.wait_for_worker_new_epoch();
2: SELECT role_name, quota_in_mb, rolsize_in_bytes FROM diskquota.show_fast_role_quota_view;
2: SELECT role_name, tablespace_name, quota_in_mb, rolsize_tablespace_in_bytes FROM diskquota.show_fast_role_tablespace_quota_view;

DROP TABLE IF EXISTS s1.t;
DROP TABLE IF EXISTS s2.t;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

DROP SCHEMA IF EXISTS s1;
DROP SCHEMA IF EXISTS s2;
DROP ROLE IF EXISTS r;

DROP TABLESPACE IF EXISTS spc1;
DROP TABLESPACE IF EXISTS spc2;

!\retcode rm -rf /tmp/spc1;
!\retcode rm -rf /tmp/spc2;
