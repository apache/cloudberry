-- Test that diskquota is able to cancel a running CTAS query by the tablespace role quota.
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null
-- start_ignore
\! mkdir -p /tmp/ctas_rolespc
-- end_ignore

-- prepare role and tablespace.
DROP TABLESPACE IF EXISTS ctas_rolespc;
CREATE TABLESPACE ctas_rolespc LOCATION '/tmp/ctas_rolespc';
CREATE ROLE hardlimit_r;
GRANT USAGE ON SCHEMA diskquota TO hardlimit_r;
GRANT ALL ON TABLESPACE ctas_rolespc TO hardlimit_r;
SELECT diskquota.set_role_tablespace_quota('hardlimit_r', 'ctas_rolespc', '1 MB');
SET default_tablespace = ctas_rolespc;
SET ROLE hardlimit_r;

-- heap table
CREATE TABLE t1 AS SELECT generate_series(1, 100000000);
SELECT diskquota.wait_for_worker_new_epoch();

-- toast table
CREATE TABLE toast_table
  AS SELECT ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000);
SELECT diskquota.wait_for_worker_new_epoch();

-- ao table
CREATE TABLE ao_table WITH (appendonly=true) AS SELECT generate_series(1, 100000000);
SELECT diskquota.wait_for_worker_new_epoch();

-- aocs table
CREATE TABLE aocs_table WITH (appendonly=true, orientation=column)
  AS SELECT i, ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000) AS i;
SELECT diskquota.wait_for_worker_new_epoch();

-- disable hardlimit and do some clean-ups.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS toast_table;
DROP TABLE IF EXISTS ao_table;
DROP TABLE IF EXISTS aocs_table;
RESET ROLE;
RESET default_tablespace;
DROP TABLESPACE ctas_rolespc;
REVOKE USAGE ON SCHEMA diskquota FROM hardlimit_r;
DROP ROLE hardlimit_r;
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
