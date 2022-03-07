CREATE ROLE no_table SUPERUSER;

CREATE SCHEMA no_table;

SELECT diskquota.set_schema_quota('no_table', '1 MB');

SELECT schema_name, quota_in_mb, nspsize_in_bytes
FROM diskquota.show_fast_schema_quota_view;

SELECT diskquota.set_role_quota('no_table', '1 MB');

SELECT role_name, quota_in_mb, rolsize_in_bytes 
FROM diskquota.show_fast_role_quota_view;

SELECT diskquota.set_schema_tablespace_quota('no_table', 'pg_default', '1 MB');

SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes 
FROM diskquota.show_fast_schema_tablespace_quota_view;

SELECT diskquota.set_role_tablespace_quota('no_table', 'pg_default', '1 MB');

SELECT role_name, tablespace_name , quota_in_mb, rolsize_tablespace_in_bytes
FROM diskquota.show_fast_role_tablespace_quota_view;

DROP ROLE no_table;

DROP SCHEMA no_table;

-- Wait until the quota configs are removed from the memory 
-- automatically after DROP.
SELECT diskquota.wait_for_worker_new_epoch();
