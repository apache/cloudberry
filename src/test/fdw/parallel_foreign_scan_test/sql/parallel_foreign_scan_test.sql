-- Test parallel foreign scan support in the Cloudberry kernel.
-- Uses a mock FDW that generates synthetic rows (id, val).

CREATE EXTENSION parallel_foreign_scan_test_fdw;

-- ============================================================
-- PART 1: Coordinator mode (deterministic, tests planner paths)
-- ============================================================

CREATE SERVER coord_srv FOREIGN DATA WRAPPER parallel_foreign_scan_test_fdw
  OPTIONS (mpp_execute 'coordinator');

CREATE FOREIGN TABLE parallel_ft (id int, val text)
  SERVER coord_srv OPTIONS (num_rows '10000');

SET optimizer = off;
SET enable_parallel = on;
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- EXPLAIN: should show Gather + Parallel Foreign Scan
EXPLAIN (costs off) SELECT count(*) FROM parallel_ft;

-- Correctness: count
SELECT count(*) FROM parallel_ft;

-- Correctness: sum
SELECT sum(id) FROM parallel_ft;

-- Correctness: ordered rows
SELECT id, val FROM parallel_ft ORDER BY id LIMIT 5;

-- Parallel off: no Gather in plan
SET enable_parallel = off;
EXPLAIN (costs off) SELECT count(*) FROM parallel_ft;
SELECT count(*) FROM parallel_ft;

-- ============================================================
-- PART 2: All-segments mode (tests execMain.c parallel launch)
-- Each segment produces 1000 rows; verify parallel doesn't
-- change the total by comparing EXPLAIN plans.
-- ============================================================

CREATE SERVER seg_srv FOREIGN DATA WRAPPER parallel_foreign_scan_test_fdw
  OPTIONS (mpp_execute 'all segments');

CREATE FOREIGN TABLE parallel_ft_seg (id int, val text)
  SERVER seg_srv OPTIONS (num_rows '100');

-- Non-parallel plan
SET enable_parallel = off;
EXPLAIN (costs off) SELECT count(*) FROM parallel_ft_seg;

-- Parallel plan: should show Gather under Gather Motion
SET enable_parallel = on;
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
EXPLAIN (costs off) SELECT count(*) FROM parallel_ft_seg;

-- ============================================================
-- Cleanup
-- ============================================================

DROP FOREIGN TABLE parallel_ft;
DROP FOREIGN TABLE parallel_ft_seg;
DROP SERVER coord_srv;
DROP SERVER seg_srv;
DROP EXTENSION parallel_foreign_scan_test_fdw;
