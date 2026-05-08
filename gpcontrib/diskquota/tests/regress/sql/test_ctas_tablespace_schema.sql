-- Test that diskquota is able to cancel a running CTAS query by the tablespace schema quota.
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

-- start_ignore
\! mkdir -p /tmp/ctas_schemaspc
-- end_ignore

-- prepare tablespace and schema
DROP TABLESPACE IF EXISTS ctas_schemaspc;
CREATE TABLESPACE ctas_schemaspc LOCATION '/tmp/ctas_schemaspc';
CREATE SCHEMA hardlimit_s;
SELECT diskquota.set_schema_tablespace_quota('hardlimit_s', 'ctas_schemaspc', '1 MB');
SET search_path TO hardlimit_s;
SET default_tablespace = ctas_schemaspc;

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
  AS SELECT i, ARRAY(SELECT generate_series(1,10000)) FROM generate_series(1, 100000) AS i DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();

-- disable hardlimit and do some clean-ups
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS toast_table;
DROP TABLE IF EXISTS ao_table;
DROP TABLE IF EXISTS aocs_table;
RESET search_path;
RESET default_tablespace;
DROP SCHEMA hardlimit_s;
DROP TABLESPACE ctas_schemaspc;
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
