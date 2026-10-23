-- An aborted AO VACUUM must not leave counters or a start time for the next
-- relation. Use a persistent utility connection so both commands necessarily
-- run in the same backend, independent of distributed gang replacement.
CREATE EXTENSION IF NOT EXISTS vacuum_stats;
CREATE SCHEMA ao_vacuum_stats_abort;
SET search_path = ao_vacuum_stats_abort, public;
SET statement_timeout = '30s';
CREATE TABLE barrier (id int) DISTRIBUTED BY (id);
CREATE TABLE aborted_row (id int, payload text) USING ao_row DISTRIBUTED BY (id);
CREATE TABLE next_row (id int, payload text) USING ao_row DISTRIBUTED BY (id);
CREATE TABLE aborted_column (id int, payload text) USING ao_column DISTRIBUTED BY (id);
CREATE TABLE next_column (id int, payload text) USING ao_column DISTRIBUTED BY (id);
0U: SET search_path = ao_vacuum_stats_abort, public;
0U: SET application_name = 'ao_vacuum_stats_abort';
0U: SET vacuum_cost_delay = 0;
0U: SET statement_timeout = '30s';
0U: CREATE TEMP TABLE vacuum_bounds (started timestamptz, finished timestamptz);

-- ANALYZE is sent after the VACUUM work report. Wait for that later message
-- rather than vacuum_count, which can arrive before the extended counters.
0U: CREATE FUNCTION pg_temp.wait_for_report(expected bigint) RETURNS bool AS $$
DECLARE i int; /* in func */
BEGIN /* in func */
  FOR i IN 1..200 LOOP /* in func */
    PERFORM pg_stat_clear_snapshot(); /* in func */
    IF pg_stat_get_analyze_count('barrier'::regclass) >= expected THEN /* in func */
      RETURN true; /* in func */
    END IF; /* in func */
    PERFORM pg_sleep(0.1); /* in func */
  END LOOP; /* in func */
  RETURN false; /* in func */
END; /* in func */
$$ LANGUAGE plpgsql;

-- AO row: leave both dead-tuple and pre-cleanup truncation counters behind.
0U: INSERT INTO aborted_row SELECT g, repeat('x', 200) FROM generate_series(1, 1000) g;
0U: DELETE FROM aborted_row WHERE id <= 500;
0U: BEGIN;
0U: INSERT INTO aborted_row SELECT g, repeat('z', 200) FROM generate_series(1001, 2000) g;
0U: ROLLBACK;
0U: INSERT INTO next_row SELECT g, 'y' FROM generate_series(1, 100) g;
SELECT gp_inject_fault('vacuum_ao_after_compact', 'suspend', dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
0U&: VACUUM aborted_row;
SELECT gp_wait_until_triggered_fault('vacuum_ao_after_compact', 1, dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
SELECT num_dead_tuples = 500 AS counted_dead_tuples,
       heap_blks_vacuumed > 0 AS truncated_bytes
FROM gp_dist_random('pg_stat_progress_vacuum')
WHERE relid = 'aborted_row'::regclass;
SELECT pg_cancel_backend(pid) FROM gp_dist_random('pg_stat_activity')
WHERE application_name = 'ao_vacuum_stats_abort' AND query = 'VACUUM aborted_row;';
SELECT gp_inject_fault('vacuum_ao_after_compact', 'reset', dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
0U<:

-- Exclude the aborted run and the intervening idle time from the next run.
-- Compare with the measured command interval, without a machine-speed limit.
0U: SELECT pg_sleep(0.05);
0U: TRUNCATE vacuum_bounds;
0U: INSERT INTO vacuum_bounds VALUES (clock_timestamp(), NULL);
0U: VACUUM next_row;
0U: UPDATE vacuum_bounds SET finished = clock_timestamp();
0U: ANALYZE barrier;
0U: SELECT pg_temp.wait_for_report(1);
0U: SELECT tuples_deleted = 0 AS fresh_tuples, bytes_removed = 0 AS fresh_bytes,
           total_vacuum_time > 0 AND
           total_vacuum_time <= extract(epoch FROM (finished - started)) * 1000 + 1 AS fresh_time
    FROM pg_stat_vacuum_tables, vacuum_bounds WHERE relid = 'next_row'::regclass;

-- A subsequent successful run must still accumulate its own work. This test
-- also runs with track_vacuum_statistics off, when only timing is collected.
0U: DELETE FROM next_row WHERE id <= 50;
0U: VACUUM next_row;
0U: ANALYZE barrier;
0U: SELECT pg_temp.wait_for_report(2);
0U: SELECT tuples_deleted = CASE WHEN current_setting('track_vacuum_statistics')::bool
                                THEN 50 ELSE 0 END AS own_tuples
    FROM pg_stat_vacuum_tables WHERE relid = 'next_row'::regclass;

-- AO column: leave both dead-tuple and pre-cleanup truncation counters behind.
0U: INSERT INTO aborted_column SELECT g, repeat('x', 200) FROM generate_series(1, 1000) g;
0U: DELETE FROM aborted_column WHERE id <= 500;
0U: BEGIN;
0U: INSERT INTO aborted_column SELECT g, repeat('z', 200) FROM generate_series(1001, 2000) g;
0U: ROLLBACK;
0U: INSERT INTO next_column SELECT g, 'y' FROM generate_series(1, 100) g;
SELECT gp_inject_fault('vacuum_ao_after_compact', 'error', dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
0U: VACUUM aborted_column;
SELECT gp_inject_fault('vacuum_ao_after_compact', 'reset', dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';

-- Exclude the aborted run and the intervening idle time from the next run.
-- Compare with the measured command interval, without a machine-speed limit.
0U: SELECT pg_sleep(0.05);
0U: TRUNCATE vacuum_bounds;
0U: INSERT INTO vacuum_bounds VALUES (clock_timestamp(), NULL);
0U: VACUUM next_column;
0U: UPDATE vacuum_bounds SET finished = clock_timestamp();
0U: ANALYZE barrier;
0U: SELECT pg_temp.wait_for_report(3);
0U: SELECT tuples_deleted = 0 AS fresh_tuples, bytes_removed = 0 AS fresh_bytes,
           total_vacuum_time > 0 AND
           total_vacuum_time <= extract(epoch FROM (finished - started)) * 1000 + 1 AS fresh_time
    FROM pg_stat_vacuum_tables, vacuum_bounds WHERE relid = 'next_column'::regclass;

-- A subsequent successful run must still accumulate its own work. This test
-- also runs with track_vacuum_statistics off, when only timing is collected.
0U: DELETE FROM next_column WHERE id <= 50;
0U: VACUUM next_column;
0U: ANALYZE barrier;
0U: SELECT pg_temp.wait_for_report(4);
0U: SELECT tuples_deleted = CASE WHEN current_setting('track_vacuum_statistics')::bool
                                THEN 50 ELSE 0 END AS own_tuples
    FROM pg_stat_vacuum_tables WHERE relid = 'next_column'::regclass;

0Uq:
RESET search_path;
RESET statement_timeout;
DROP SCHEMA ao_vacuum_stats_abort CASCADE;
