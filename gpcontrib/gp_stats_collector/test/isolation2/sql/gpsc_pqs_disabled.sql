-- STAT_DISABLED: when the target backend has pg_query_state.enable = off, its
-- SendCdbComponents reply is STAT_DISABLED, so polling reports an empty backend
-- list even though a query is actively running on the segments.
--
-- Distinguishes "disabled" from "idle": here the query really is executing
-- (suspended on a fault), yet the disabled module yields nothing.
--
-- Extensions come from setup.sql.

CREATE TABLE qs_disabled_t (id int) DISTRIBUTED BY (id);
INSERT INTO qs_disabled_t SELECT generate_series(1, 100);

SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- Target disables the module for its own session, then runs a query that hangs.
1: SET application_name TO 'qs_disabled_target';
1: SET pg_query_state.enable TO off;
1&: SELECT count(*) FROM qs_disabled_t;

SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- Running, but module disabled on the target -> empty backend list.
2: SELECT count(*) AS n_backends FROM gpsc.pg_query_state_backends(
     (SELECT pid FROM pg_stat_activity
      WHERE application_name = 'qs_disabled_target' AND pid <> pg_backend_pid()
      ORDER BY backend_start LIMIT 1));

SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1<:
1q:
2q:

DROP TABLE qs_disabled_t;
