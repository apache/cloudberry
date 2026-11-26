-- Happy path: a query suspended mid-execution on the QEs is observed live.
--
-- Session 1 launches a query that hits an 'executor_pre_tuple_processed'
-- suspend fault on every primary segment, so its QE backends sit inside the
-- executor with a live plan tree.  Session 2 then:
--   * pg_query_state_backends(pid) -> at least one participating backend,
--   * pg_query_state(pid)          -> succeeds (fire-and-forget, returns void).
-- The fault is reset and the suspended query is reaped.
--
-- Extensions (gp_stats_collector, gp_inject_fault) come from setup.sql.

CREATE TABLE qs_running_t (id int) DISTRIBUTED BY (id);
INSERT INTO qs_running_t SELECT generate_series(1, 100);

-- Suspend execution on all primary segments.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- Session 1: tag the connection, then launch a query that hangs on the QEs.
1: SET application_name TO 'qs_running_target';
1&: SELECT count(*) FROM qs_running_t;

-- Wait until the fault has been hit on the segments.
SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- Session 2: the running query has live QE backends, and polling succeeds.
2: SELECT count(*) > 0 AS has_backends FROM gpsc.pg_query_state_backends(
     (SELECT pid FROM pg_stat_activity
      WHERE application_name = 'qs_running_target' AND pid <> pg_backend_pid()
      ORDER BY backend_start LIMIT 1));
2: SELECT gpsc.pg_query_state(
     (SELECT pid FROM pg_stat_activity
      WHERE application_name = 'qs_running_target' AND pid <> pg_backend_pid()
      ORDER BY backend_start LIMIT 1),
     '\x00112233445566778899aabbccddeeff'::bytea);

-- Release the fault and reap the suspended query.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1<:
1q:
2q:

DROP TABLE qs_running_t;
