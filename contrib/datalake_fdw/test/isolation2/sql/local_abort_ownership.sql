-- Run through ../run.py: @LOCAL_URI@ is a fresh coordinator-local pathname.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE SCHEMA pr2044_local_isolation;
CREATE FUNCTION pr2044_local_isolation.row_value(i int, fail boolean DEFAULT true) RETURNS int
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  IF i = 2 THEN
    PERFORM pg_advisory_xact_lock(2044, 1); /* in func */
    IF fail THEN
      RAISE EXCEPTION 'PR2044 injected writer A failure'; /* in func */
    END IF; /* in func */
  END IF; /* in func */
  RETURN i; /* in func */
END; /* in func */
$$;
CREATE FUNCTION pr2044_local_isolation.wait_for_writer() RETURNS boolean
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  FOR attempt IN 1..1000 LOOP
    IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
        AND classid = 2044 AND objid = 1 AND objsubid = 2 AND NOT granted
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())) THEN
      RETURN true; /* in func */
    END IF; /* in func */
    PERFORM pg_stat_clear_snapshot(); /* in func */
    PERFORM pg_sleep(0.05); /* in func */
  END LOOP; /* in func */
  RAISE EXCEPTION 'PR2044 writer did not reach the advisory lock'; /* in func */
END; /* in func */
$$;
1: SET optimizer = off;
1: SET statement_timeout = '60s';
2: SELECT pg_advisory_lock(2044, 1);
-- Fetch size 1 ensures the file is opened after row 1, before row 2 blocks.
1&: SELECT datalake_parquet_write(@LOCAL_URI@,
    'SELECT pr2044_local_isolation.row_value(g) AS id FROM generate_series(1, 2) g', 1, '');
2: SELECT pr2044_local_isolation.wait_for_writer();
-- A has created only private staging data, not the destination.
2: SELECT CASE WHEN (pg_stat_file(@LOCAL_PATH@, true)).size IS NULL
    THEN 'PR2044_LOCAL_PRIVATE_PASS' ELSE 'FAIL' END AS result;
2: SELECT datalake_storage_write_text(@LOCAL_URI@, 'committed writer B') AS bytes_written;
2: SELECT datalake_storage_read_text(@LOCAL_URI@) = 'committed writer B' AS b_exists_before_abort;
2: SELECT pg_advisory_unlock(2044, 1);
1<:
2: SELECT CASE WHEN datalake_storage_read_text(@LOCAL_URI@) = 'committed writer B'
    THEN 'PR2044_LOCAL_PASS' ELSE 'FAIL' END AS result;

-- Now install the replacement while A is already inside Abort().
2: SELECT datalake_storage_delete(@LOCAL_URI@);
2: SELECT pg_advisory_lock(2044, 1);
1&: SELECT datalake_parquet_write(@LOCAL_URI@,
    'SELECT pr2044_local_isolation.row_value(g) AS id FROM generate_series(1, 2) g', 1, '');
2: SELECT pr2044_local_isolation.wait_for_writer();
2: SELECT gp_inject_fault('datalake_local_abort_before_cleanup', 'suspend', dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
2: SELECT pg_advisory_unlock(2044, 1);
2: SELECT gp_wait_until_triggered_fault('datalake_local_abort_before_cleanup', 1, dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
-- Also runs against the former implementation with a pause after its inode
-- check: remove its visible partial file, if present, before replacing it.
2: SELECT CASE WHEN (pg_stat_file(@LOCAL_PATH@, true)).size IS NOT NULL
    THEN datalake_storage_delete(@LOCAL_URI@) ELSE true END AS ready;
2: SELECT datalake_storage_write_text(@LOCAL_URI@, 'replacement during cleanup');
2: SELECT gp_inject_fault('datalake_local_abort_before_cleanup', 'resume', dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
1<:
2: SELECT gp_inject_fault('datalake_local_abort_before_cleanup', 'reset', dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
2: SELECT CASE WHEN datalake_storage_read_text(@LOCAL_URI@) = 'replacement during cleanup'
    THEN 'PR2044_LOCAL_RACE_PASS' ELSE 'FAIL' END AS result;

-- A successful write must also refuse to publish over a competing writer.
2: SELECT datalake_storage_delete(@LOCAL_URI@);
2: SELECT pg_advisory_lock(2044, 1);
1&: SELECT datalake_parquet_write(@LOCAL_URI@,
    'SELECT pr2044_local_isolation.row_value(g, false) AS id FROM generate_series(1, 2) g', 1, '');
2: SELECT pr2044_local_isolation.wait_for_writer();
2: SELECT datalake_storage_write_text(@LOCAL_URI@, 'winner at close');
-- A's private file must not appear in recursive storage listings.
2: SELECT CASE WHEN count(*) = 1 AND min(path) = @LOCAL_PATH@
    THEN 'PR2044_LOCAL_LIST_PASS' ELSE 'FAIL' END AS result
    FROM datalake_storage_list(@LOCAL_DIR_URI@) AS path;
2: SELECT pg_advisory_unlock(2044, 1);
1<:
2: SELECT CASE WHEN datalake_storage_read_text(@LOCAL_URI@) = 'winner at close'
    THEN 'PR2044_LOCAL_PUBLISH_PASS' ELSE 'FAIL' END AS result;
-- Check the OS directory as well: neither failure may leave staging data.
2: SELECT CASE WHEN count(*) = 1 AND min(name) = 'replacement'
    THEN 'PR2044_LOCAL_CLEANUP_PASS' ELSE 'FAIL' END AS result
    FROM pg_ls_dir(@LOCAL_DIR@) AS name;
DROP SCHEMA pr2044_local_isolation CASCADE;
