SET optimizer TO OFF;

-- Test Suit: IndexScan on AO tables
SHOW gp_enable_ao_indexscan;

CREATE TABLE ao_tbl(a int, b int, c int) WITH (appendonly='true',  orientation='row');
CREATE TABLE aocs_tbl(a int, b int, c int) WITH (appendonly='true',  orientation='column');

-- Generate small data set
INSERT INTO ao_tbl 
    SELECT i, (((i%10007)*(i%997))+11451)%100000, 1
    FROM generate_series(1, 100000) s(i);
INSERT INTO aocs_tbl SELECT * FROM ao_tbl;
CREATE INDEX ON ao_tbl(b);
CREATE INDEX ON aocs_tbl(b);
ANALYZE ao_tbl, aocs_tbl;

-- Test with optimization off
SET gp_enable_ao_indexscan TO OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 1;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 1000;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 1;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 1000;

-- Test with optimization on
SET gp_enable_ao_indexscan TO ON;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 1;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 10;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 100;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 1000;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 1;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 10;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 100;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 1000;

-- Test with subquery
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c FROM ao_tbl t1, 
    (SELECT * FROM ao_tbl ORDER BY b LIMIT 10) t2
    WHERE t1.a = t2.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c FROM aocs_tbl t1, 
    (SELECT * FROM aocs_tbl ORDER BY b LIMIT 10) t2
    WHERE t1.a = t2.a;

-- Ensure that IndexOnlyScan is disabled
SET enable_seqscan TO OFF;
SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM ao_tbl ORDER BY b LIMIT 1;

EXPLAIN (COSTS OFF)
SELECT * FROM aocs_tbl ORDER BY b LIMIT 1;

-- Cleanup
DROP TABLE ao_tbl, aocs_tbl;
SET gp_enable_ao_indexscan TO OFF;
SET enable_seqscan TO ON;
SET enable_indexscan TO ON;
SET enable_bitmapscan TO ON;

-- Final step
SET optimizer TO default;