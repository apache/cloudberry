-- Test that diskquota is able to cancel a running CTAS query by the schema quota.
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

CREATE SCHEMA hardlimit_s;
SELECT diskquota.set_schema_quota('hardlimit_s', '1 MB');
SET search_path TO hardlimit_s;

-- heap table
CREATE TABLE t1 (i) AS SELECT generate_series(1, 100000000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- toast table
CREATE TABLE toast_table (i)
  AS SELECT ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- ao table
CREATE TABLE ao_table (i) WITH (appendonly=true) AS SELECT generate_series(1, 100000000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- aocs table
CREATE TABLE aocs_table WITH (appendonly=true, orientation=column)
  AS SELECT i, ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000) AS i;
SELECT diskquota.wait_for_worker_new_epoch();

-- disable hardlimit and do some clean-ups.
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS toast_table;
DROP TABLE IF EXISTS ao_table;
DROP TABLE IF EXISTS aocs_table;
RESET search_path;
DROP SCHEMA hardlimit_s;
