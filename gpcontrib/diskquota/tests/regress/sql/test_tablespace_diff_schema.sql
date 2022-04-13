-- allow set quota for different schema in the same tablespace
-- delete quota for one schema will not drop other quotas with different schema in the same tablespace

-- start_ignore
\! mkdir -p /tmp/spc_diff_schema
-- end_ignore

CREATE TABLESPACE spc_diff_schema LOCATION '/tmp/spc_diff_schema';
CREATE SCHEMA schema_in_tablespc;
SET search_path TO schema_in_tablespc;

CREATE TABLE a(i int) TABLESPACE spc_diff_schema DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
SELECT diskquota.set_schema_tablespace_quota('schema_in_tablespc', 'spc_diff_schema','1 MB');
SELECT diskquota.wait_for_worker_new_epoch();

-- with hardlimits off, expect to success
INSERT INTO a SELECT generate_series(1,1000000);

-- wait for next loop for bgworker to add it to blackmap
SELECT diskquota.wait_for_worker_new_epoch();
-- expect to fail
INSERT INTO a SELECT generate_series(1,1000000);

SELECT schema_name, tablespace_name FROM diskquota.show_fast_schema_tablespace_quota_view;

SELECT diskquota.set_schema_tablespace_quota('schema_in_tablespc', 'pg_default','1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name, tablespace_name FROM diskquota.show_fast_schema_tablespace_quota_view;

SELECT diskquota.set_schema_tablespace_quota('schema_in_tablespc', 'pg_default','-1');
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name, tablespace_name FROM diskquota.show_fast_schema_tablespace_quota_view;

-- expect to fail
INSERT INTO a SELECT generate_series(1,1000000);

reset search_path;
DROP TABLE IF EXISTS schema_in_tablespc.a;
DROP tablespace IF EXISTS spc_diff_schema;
DROP SCHEMA IF EXISTS schema_in_tablespc;

-- start_ignore
\! rmdir /tmp/spc_diff_schema
  -- end_ignore
