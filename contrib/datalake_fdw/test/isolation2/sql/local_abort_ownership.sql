-- Run through ../run.py: @LOCAL_URI@ is a fresh coordinator-local pathname.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
CREATE SCHEMA pr2044_local_isolation;
CREATE FUNCTION pr2044_local_isolation.row_value(i int) RETURNS int
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  IF i = 2 THEN
    PERFORM pg_advisory_xact_lock(2044, 1); /* in func */
    RAISE EXCEPTION 'PR2044 injected writer A failure'; /* in func */
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
-- Delete must succeed: otherwise A never reached the intended state.
2: SELECT datalake_storage_delete(@LOCAL_URI@) AS removed_a;
2: SELECT datalake_storage_write_text(@LOCAL_URI@, 'committed writer B') AS bytes_written;
2: SELECT datalake_storage_read_text(@LOCAL_URI@) = 'committed writer B' AS b_exists_before_abort;
2: SELECT pg_advisory_unlock(2044, 1);
1<:
-- Healthy implementation returns the marker; PR #2044 reports a missing file.
2: SELECT CASE WHEN datalake_storage_read_text(@LOCAL_URI@) = 'committed writer B'
    THEN 'PR2044_LOCAL_PASS' ELSE 'PR2044_LOCAL_FAIL' END AS result;
DROP SCHEMA pr2044_local_isolation CASCADE;
