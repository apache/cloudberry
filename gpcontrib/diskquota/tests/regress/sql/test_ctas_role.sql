-- Test that diskquota is able to cancel a running CTAS query by the role quota.
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null
CREATE ROLE hardlimit_r;
SELECT diskquota.set_role_quota('hardlimit_r', '1MB');
GRANT USAGE ON SCHEMA diskquota TO hardlimit_r;
SET ROLE hardlimit_r;

-- heap table
CREATE TABLE t1 (i) AS SELECT generate_series(1, 10000000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- temp table
CREATE TEMP TABLE t2 (i) AS SELECT generate_series(1, 100000000);
SELECT diskquota.wait_for_worker_new_epoch();

-- toast table
CREATE TABLE toast_table (i) AS SELECT ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- ao table
CREATE TABLE ao_table (i) WITH (appendonly=true) AS SELECT generate_series(1, 100000000) DISTRIBUTED BY (i);
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
REVOKE USAGE ON SCHEMA diskquota FROM hardlimit_r;
DROP ROLE hardlimit_r;
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
