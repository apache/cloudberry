-- Ensure diskquota does not save information about temporary table during restart cluster by invalidates it at startup

!\retcode gpconfig -c diskquota.naptime -v 5 --skipvalidation;
!\retcode gpstop -u;

1: CREATE SCHEMA temporary_schema;
1: SET search_path TO temporary_schema;
1: SELECT diskquota.set_schema_quota('temporary_schema', '1 MB');
1: SELECT diskquota.wait_for_worker_new_epoch();
1: CREATE TEMPORARY TABLE temporary_table(id int) DISTRIBUTED BY (id);
1: INSERT INTO temporary_table SELECT generate_series(1, 10000);
-- Wait for the diskquota bgworker refreshing the size of 'temporary_table'.
1: SELECT diskquota.wait_for_worker_new_epoch();
1q:

-- Restart cluster fastly
!\retcode gpstop -afr;

-- Indicates that there is no temporary table in pg_catalog.pg_class
1: SELECT oid FROM pg_catalog.pg_class WHERE relname = 'temporary_table';
-- Indicates that there are no entries in diskquota.table_size that are not present in pg_catalog.pg_class
1: SELECT diskquota.wait_for_worker_new_epoch();
1: SELECT tableid FROM diskquota.table_size WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE tableid = oid) AND segid = -1;
1: DROP SCHEMA temporary_schema CASCADE;
1q:

!\retcode gpconfig -c diskquota.naptime -v 0 --skipvalidation;
!\retcode gpstop -u;
