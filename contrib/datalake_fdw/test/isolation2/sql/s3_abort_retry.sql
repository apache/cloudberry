-- Requires a FAULT_INJECTOR build, an S3 service and check_uploads.py.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE SCHEMA pr2044_s3_isolation;
CREATE SERVER pr2044_isolation_volume FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (base_path @S3_URI@, endpoint @S3_ENDPOINT@, region 'us-east-1', path_style_access 'true');
CREATE USER MAPPING FOR CURRENT_USER SERVER pr2044_isolation_volume
    OPTIONS (access_key_id @ACCESS_KEY@, secret_access_key @SECRET_KEY@);
CREATE FUNCTION pr2044_s3_isolation.pad() RETURNS text LANGUAGE sql VOLATILE AS
    $$ SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 32) $$;
CREATE FUNCTION pr2044_s3_isolation.row_value(i int) RETURNS int
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  IF i = 14000 THEN
    PERFORM pg_advisory_xact_lock(2044, 2); /* in func */
    RAISE EXCEPTION 'PR2044 injected S3 writer failure'; /* in func */
  END IF; /* in func */
  RETURN i; /* in func */
END; /* in func */
$$;
CREATE FUNCTION pr2044_s3_isolation.wait_for_writer() RETURNS boolean
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  FOR attempt IN 1..1000 LOOP
    IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
        AND classid = 2044 AND objid = 2 AND objsubid = 2 AND NOT granted
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
1: SET statement_timeout = '120s';
2: SELECT pg_advisory_lock(2044, 2);
1&: SELECT datalake_parquet_write(@S3_OBJECT_URI@,
    'SELECT pr2044_s3_isolation.row_value(i) AS id, pr2044_s3_isolation.pad() AS padding FROM generate_series(1, 16000) i',
    2000, 'uncompressed', 'pr2044_isolation_volume');
2: SELECT pr2044_s3_isolation.wait_for_writer();
-- Prove a multipart upload exists before allowing the writer to fail.
! "$PR2044_PYTHON" "$PR2044_SUITE/check_uploads.py" before;
-- Inject exactly one failed SDK outcome, allowing destructor cleanup to retry.
2: SELECT gp_inject_fault('datalake_s3_abort_multipart_upload', 'skip', '', '', '', 1, 1, 0, dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
2: SELECT pg_advisory_unlock(2044, 2);
1<:
2: SELECT gp_inject_fault('datalake_s3_abort_multipart_upload', 'status', dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
! "$PR2044_PYTHON" "$PR2044_SUITE/check_uploads.py" after;
2: SELECT gp_inject_fault('datalake_s3_abort_multipart_upload', 'reset', dbid)
    FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
DROP SERVER pr2044_isolation_volume CASCADE;
DROP SCHEMA pr2044_s3_isolation CASCADE;
