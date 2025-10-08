-- start_ignore
CREATE EXTENSION gp_inject_fault;
-- end_ignore

CREATE TABLE t_part(i int)
PARTITION BY RANGE (i) (START (0) END (5) EVERY (1));

-- Create a lot of locks.
1: BEGIN;
1: LOCK TABLE t_part IN ACCESS EXCLUSIVE MODE;

--
-- Test pg_locks view behavior.
--

SELECT gp_inject_fault('pg_lock_status_local_locks_collected', 'skip', dbid),
          gp_inject_fault('pg_lock_status_squelched', 'error', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Materializes everything regardless of LIMIT clause, as of now.
SELECT locktype = 'nothing' AS f FROM pg_locks LIMIT 1;

SELECT gp_wait_until_triggered_fault('pg_lock_status_local_locks_collected', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' and content = -1;

SELECT gp_inject_fault('all', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

--
-- Test pg_lock_status() behavior with LIMIT clause that should return results
-- only from coordinator.
--

SELECT gp_inject_fault('pg_lock_status_local_locks_collected', 'error', dbid),
          gp_inject_fault('pg_lock_status_squelched', 'skip', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Doesn't materialize anything, gets squelched after the first row from
-- coordinator.
SELECT pg_lock_status()::text = 'nothing' AS f LIMIT 1;

SELECT gp_wait_until_triggered_fault('pg_lock_status_squelched', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' and content = -1;

SELECT gp_inject_fault('all', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

--
-- Test pg_lock_status() behavior with LIMIT clause that should return results
-- both from coordinator and from segments.
--

SELECT gp_inject_fault('pg_lock_status_local_locks_collected', 'skip', dbid),
          gp_inject_fault('pg_lock_status_squelched', 'skip', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Retrieves some rows from coordinator and from a segment, then gets
-- squelched.
SELECT pg_lock_status()::text = 'nothing' AS f LIMIT 10;

SELECT gp_wait_until_triggered_fault('pg_lock_status_squelched', 1, dbid),
          gp_wait_until_triggered_fault('pg_lock_status_local_locks_collected', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' and content = -1;

SELECT gp_inject_fault('all', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

1: ROLLBACK;

DROP TABLE t_part;
