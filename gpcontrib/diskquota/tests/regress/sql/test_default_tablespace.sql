-- test role_tablespace_quota works with tables/databases in default tablespace
-- test role_tablespace_quota works with tables/databases in non-default tablespace with hard limits on

-- start_ignore
\! mkdir -p /tmp/custom_tablespace
-- end_ignore

DROP ROLE if EXISTS role1;
DROP ROLE if EXISTS role2;
CREATE ROLE role1 SUPERUSER;
CREATE ROLE role2 SUPERUSER;
SET ROLE role1;

DROP TABLE if EXISTS t;
CREATE TABLE t (i int) DISTRIBUTED BY (i);

-- with hard limits off
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null

SELECT diskquota.set_role_tablespace_quota('role1', 'pg_default', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to success
INSERT INTO t SELECT generate_series(1, 100);
INSERT INTO t SELECT generate_series(1, 1000000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to fail
INSERT INTO t SELECT generate_series(1, 1000000);

SELECT r.rolname, t.spcname, b.target_type
FROM diskquota.blackmap AS b, pg_tablespace AS t, pg_roles AS r
WHERE b.tablespace_oid = t.oid AND b.target_oid = r.oid AND r.rolname = 'role1'
ORDER BY r.rolname, t.spcname, b.target_type;

DROP TABLE IF EXISTS t;
SELECT diskquota.set_role_tablespace_quota('role1', 'pg_default', '-1');

SET ROLE role2;
CREATE TABLE t (i int) DISTRIBUTED BY (i);

-- with hard limits on
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

SELECT diskquota.set_role_tablespace_quota('role2', 'pg_default', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to fail because of hard limits
INSERT INTO t SELECT generate_series(1, 50000000);
DROP TABLE IF EXISTS t;

SET ROLE role1;
-- database in customized tablespace
CREATE TABLESPACE custom_tablespace LOCATION '/tmp/custom_tablespace';
CREATE DATABASE db_with_tablespace TABLESPACE custom_tablespace;
\c db_with_tablespace;
SET ROLE role1;
CREATE EXTENSION diskquota;

-- with hard limits off
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null

SELECT diskquota.set_role_tablespace_quota('role1', 'custom_tablespace', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to success
CREATE TABLE t_in_custom_tablespace (i) AS SELECT generate_series(1, 100) DISTRIBUTED BY (i);
INSERT INTO t_in_custom_tablespace SELECT generate_series(1, 1000000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to fail
INSERT INTO t_in_custom_tablespace SELECT generate_series(1, 1000000);

SELECT r.rolname, t.spcname, b.target_type
FROM diskquota.blackmap AS b, pg_tablespace AS t, pg_roles AS r
WHERE b.tablespace_oid = t.oid AND b.target_oid = r.oid AND r.rolname = 'role1'
ORDER BY r.rolname, t.spcname, b.target_type;

DROP TABLE IF EXISTS t_in_custom_tablespace;
SELECT diskquota.set_role_tablespace_quota('role1', 'custom_tablespace', '-1');
SELECT diskquota.wait_for_worker_new_epoch();
SET ROLE role2;

-- with hard limits on
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

SELECT diskquota.set_role_tablespace_quota('role2', 'custom_tablespace', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();

DROP TABLE IF EXISTS t_in_custom_tablespace;
-- expect insert to fail because of hard limits
CREATE TABLE t_in_custom_tablespace (i) AS SELECT generate_series(1, 50000000) DISTRIBUTED BY (i);

-- clean up
DROP TABLE IF EXISTS t_in_custom_tablespace;

\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null

SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION IF EXISTS diskquota;

\c contrib_regression;
SELECT diskquota.wait_for_worker_new_epoch();
DROP DATABASE IF EXISTS db_with_tablespace;
DROP TABLESPACE IF EXISTS custom_tablespace;

RESET ROLE;
DROP ROLE IF EXISTS role1;
DROP ROLE IF EXISTS role2;
