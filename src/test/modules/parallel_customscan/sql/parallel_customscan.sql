-- start_matchsubs
-- m/\(actual rows=[^)]*\)/
-- s/\(actual rows=[^)]*\)/(actual rows=...)/
-- end_matchsubs
-- start_matchignore
-- m/^\s*Buckets: \d+  Batches: \d+  Memory Usage: \d+kB/
-- end_matchignore

CREATE EXTENSION parallel_customscan;

-- Set up data BEFORE enabling our hook so ANALYZE doesn't traverse it.
CREATE TABLE pcs_t (a int)
    WITH (parallel_workers = 2)
    DISTRIBUTED BY (a);
INSERT INTO pcs_t SELECT generate_series(1, 1000);
ANALYZE pcs_t;

SET optimizer = off;
SET enable_parallel = on;
SET enable_seqscan = off;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET max_parallel_workers_per_gather = 2;

-- (1) Enabling the hook replaces the Parallel Seq Scan with a Custom Scan.
EXPLAIN (COSTS OFF) SELECT count(*) FROM pcs_t;
SET parallel_customscan.enabled = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pcs_t;

-- (2) Correctness: results match only if every parallel worker runs our scan.
SELECT count(*) FROM pcs_t;
SELECT sum(a) FROM pcs_t;

-- (3) Scan-level qualifier and projection through the custom scan.
EXPLAIN (COSTS OFF) SELECT a FROM pcs_t WHERE a > 990;
SELECT a FROM pcs_t WHERE a > 990 ORDER BY a;
SELECT count(*) FROM pcs_t WHERE a % 2 = 0;

-- (4) Join with both inputs scanned by the custom scan, plus a scan-level qual.
CREATE TABLE pcs_t2 (a int)
    WITH (parallel_workers = 2)
    DISTRIBUTED BY (a);
INSERT INTO pcs_t2 SELECT generate_series(1, 500);
ANALYZE pcs_t2;
EXPLAIN (COSTS OFF)
    SELECT count(*) FROM pcs_t x JOIN pcs_t2 y ON x.a = y.a WHERE x.a <= 100;
SELECT count(*) FROM pcs_t x JOIN pcs_t2 y ON x.a = y.a WHERE x.a <= 100;

-- (5) Empty relation: the custom scan must handle an immediate end-of-scan.
CREATE TABLE pcs_empty (a int)
    WITH (parallel_workers = 2)
    DISTRIBUTED BY (a);
ANALYZE pcs_empty;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pcs_empty;
SELECT count(*) FROM pcs_empty;
SELECT * FROM pcs_empty;

-- (6) Serial path: with no workers, a non-parallel Custom Scan is used.
SET max_parallel_workers_per_gather = 0;
EXPLAIN (COSTS OFF) SELECT count(*) FROM pcs_t;
SELECT count(*) FROM pcs_t;
SELECT a FROM pcs_t WHERE a > 995 ORDER BY a;
SET max_parallel_workers_per_gather = 2;

-- (7) EXPLAIN ANALYZE: exercises planstate_walk_kids' custom_ps recursion.
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF)
    SELECT count(*) FROM pcs_t;
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF)
    SELECT count(*) FROM pcs_t x JOIN pcs_t2 y ON x.a = y.a;

-- cleanup
DROP TABLE pcs_t2;
DROP TABLE pcs_empty;
DROP TABLE pcs_t;
DROP EXTENSION parallel_customscan;
