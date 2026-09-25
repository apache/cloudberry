-- backends reports exactly one participating backend per primary segment for a
-- single-gang query -- a strict count rather than the has_backends>0 smoke
-- check in gpsc_pqs_running.
--
-- A plain scan+count is one gang, so the QE list must match the number of
-- primary segments.
--
-- Extensions come from setup.sql.

CREATE TABLE qs_segcount_t (id int) DISTRIBUTED BY (id);
INSERT INTO qs_segcount_t SELECT generate_series(1, 100);

SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

1: SET application_name TO 'qs_segcount_target';
1&: SELECT count(*) FROM qs_segcount_t;

SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- One backend per primary segment.
2: SELECT count(*) = (SELECT count(*) FROM gp_segment_configuration
                      WHERE role = 'p' AND content > -1) AS matches_primaries
     FROM gpsc.pg_query_state_backends(
       (SELECT pid FROM pg_stat_activity
        WHERE application_name = 'qs_segcount_target' AND pid <> pg_backend_pid()
        ORDER BY backend_start LIMIT 1));

SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1<:
1q:
2q:

DROP TABLE qs_segcount_t;
