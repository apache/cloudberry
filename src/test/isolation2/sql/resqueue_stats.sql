-- Test pg_stat_resqueues cumulative statistics for resource queues.

0:CREATE RESOURCE QUEUE rq_stats_test WITH (active_statements = 1);
0:CREATE ROLE role_stats_test RESOURCE QUEUE rq_stats_test;

-- Session 1 holds the queue slot so session 2 will block.
1:SET role role_stats_test;
1:BEGIN;
1:DECLARE c1 CURSOR FOR SELECT 1;

-- Session 2 submits a query that will block.
2:SET role role_stats_test;
2&:SELECT pg_sleep(0);

-- Verify session 2 is waiting on the resource queue.
0:SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(0);';

-- Cancel the blocked query (increments queries_rejected).
0:SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(0);' AND wait_event = 'ResourceQueue';

2<:

-- Release session 1's slot so later sessions can proceed.
1:CLOSE c1;
1:END;

-- Session 3 runs a query that should be admitted and complete normally.
3:SET role role_stats_test;
3:SELECT 1;

-- Session 4 runs another query that completes normally.
4:SET role role_stats_test;
4:SELECT 2;

-- All resqueue stats are written directly to shared memory (no flush needed).
-- Check that the view shows the expected minimum counts.
-- queries_submitted >= 3: sessions 2 (rejected), 3, 4
-- queries_admitted  >= 2: sessions 3 and 4
-- queries_completed >= 2: sessions 3 and 4
0:SELECT
    queuename,
    queries_submitted >= 3 AS submitted_ok,
    queries_admitted  >= 2 AS admitted_ok,
    queries_completed >= 2 AS completed_ok
FROM pg_stat_resqueues
WHERE queuename = 'rq_stats_test';

-- All counter columns must be non-negative.
0:SELECT
    queries_submitted >= 0 AS sub_nn,
    queries_admitted  >= 0 AS adm_nn,
    queries_rejected  >= 0 AS rej_nn,
    queries_completed >= 0 AS cmp_nn,
    total_wait_time_secs >= 0 AS wait_nn,
    max_wait_secs     >= 0 AS maxw_nn,
    total_exec_time_secs >= 0 AS exec_nn,
    max_exec_secs     >= 0 AS maxe_nn,
    total_cost        >= 0 AS cost_nn,
    total_memory_kb   >= 0 AS mem_nn
FROM pg_stat_resqueues
WHERE queuename = 'rq_stats_test';

-- Verify pg_stat_get_resqueue_stats() returns data directly.
-- The function has OUT parameters so no column definition list is needed.
0:SELECT queries_submitted >= 0 AS ok
FROM pg_stat_get_resqueue_stats(
        (SELECT oid FROM pg_resqueue WHERE rsqname = 'rq_stats_test')
     ) AS s;

-- Cleanup.
0:DROP ROLE role_stats_test;
0:DROP RESOURCE QUEUE rq_stats_test;
