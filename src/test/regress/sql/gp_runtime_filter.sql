-- Disable ORCA
SET optimizer TO off;

-- Test Suit 1: runtime filter main case
DROP TABLE IF EXISTS fact_rf, dim_rf;
CREATE TABLE fact_rf (fid int, did int, val int);
CREATE TABLE dim_rf (did int, proj_id int, filter_val int);

-- Generating data, fact_rd.did and dim_rf.did is 80% matched
INSERT INTO fact_rf SELECT i, i % 8000 + 1, i FROM generate_series(1, 100000) s(i);
INSERT INTO dim_rf SELECT i, i % 10, i FROM generate_series(1, 10000) s(i);
ANALYZE fact_rf, dim_rf;

SET gp_enable_runtime_filter TO off;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2;

SET gp_enable_runtime_filter TO on;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2;

-- Test bad filter rate
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 7;

-- Test outer join
-- LeftJoin (eliminated and applicatable)
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2;

-- LeftJoin
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id IS NULL OR proj_id < 2;

-- RightJoin (applicatable)
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM
    fact_rf RIGHT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2;

-- SemiJoin
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM fact_rf
    WHERE fact_rf.did IN (SELECT did FROM dim_rf WHERE proj_id < 2);

-- SemiJoin -> InnerJoin and deduplicate
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM dim_rf
    WHERE dim_rf.did IN (SELECT did FROM fact_rf) AND proj_id < 2;

-- Test correctness
SELECT * FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND dim_rf.filter_val = 1
    ORDER BY fid;

SELECT * FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE dim_rf.filter_val = 1
    ORDER BY fid;

SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2;

SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id IS NULL OR proj_id < 2;

SELECT COUNT(*) FROM
    fact_rf RIGHT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2;

SELECT COUNT(*) FROM fact_rf
    WHERE fact_rf.did IN (SELECT did FROM dim_rf WHERE proj_id < 2);

SELECT COUNT(*) FROM dim_rf
    WHERE dim_rf.did IN (SELECT did FROM fact_rf) AND proj_id < 2;

-- Test bloom filter pushdown
-- case 1: join on distribution table and replicated table.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(c1 int, c2 int, c3 int, c4 int, c5 int) with (appendonly=true, orientation=column) distributed by (c1);
CREATE TABLE t2(c1 int, c2 int, c3 int, c4 int, c5 int) with (appendonly=true, orientation=column) distributed REPLICATED;

INSERT INTO t1 VALUES (5,5,5,5,5);
INSERT INTO t2 VALUES (1,1,1,1,1), (2,2,2,2,2), (3,3,3,3,3), (4,4,4,4,4);

INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;

INSERT INTO t2 select * FROM t2;
INSERT INTO t2 select * FROM t2;
INSERT INTO t2 select * FROM t2;

ANALYZE;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT t1.c3 FROM t1, t2 WHERE t1.c2 = t2.c2;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT t1.c3 FROM t1, t2 WHERE t1.c2 = t2.c2;

RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- case 2: join on partition table and replicated table.
CREATE TABLE t1 (c1 INT, c2 INT) DISTRIBUTED BY (c1) PARTITION BY RANGE (c2) (START (1) END (100) EVERY (50));
CREATE TABLE t2 (c1 INT, c2 INT) DISTRIBUTED REPLICATED;
INSERT INTO t1 SELECT generate_series(1, 99), generate_series(1, 99);
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 SELECT generate_series(1, 5), generate_series(1, 5);
INSERT INTO t2 SELECT generate_series(51, 51), generate_series(51, 51);
ANALYZE;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM t1, t2 WHERE t1.c2 = t2.c2;

SET gp_enable_runtime_filter_pushdown TO on;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT * FROM t1, t2 WHERE t1.c2 = t2.c2;

RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- case 3: bug fix with explain
DROP TABLE IF EXISTS test_tablesample;
CREATE TABLE test_tablesample (dist int, id int, name text) WITH (fillfactor=10) DISTRIBUTED BY (dist);
INSERT INTO test_tablesample SELECT 0, i, repeat(i::text, 875) FROM generate_series(0, 9) s(i) ORDER BY i;
INSERT INTO test_tablesample SELECT 3, i, repeat(i::text, 875) FROM generate_series(10, 19) s(i) ORDER BY i;
INSERT INTO test_tablesample SELECT 5, i, repeat(i::text, 875) FROM generate_series(20, 29) s(i) ORDER BY i;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (COSTS OFF) SELECT id FROM test_tablesample TABLESAMPLE SYSTEM (50) REPEATABLE (2);
RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS test_tablesample;

-- case 4: show debug info only when gp_enable_runtime_filter_pushdown is on
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(c1 int, c2 int);
CREATE TABLE t2(c1 int, c2 int);
INSERT INTO t1 SELECT GENERATE_SERIES(1, 1000), GENERATE_SERIES(1, 1000);
INSERT INTO t2 SELECT * FROM t1;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT count(t1.c2) FROM t1, t2 WHERE t1.c1 = t2.c1;
RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- case 5: hashjoin + result + seqsacn
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(c1 int, c2 int, c3 char(50), c4 char(50), c5 char(50)) DISTRIBUTED REPLICATED;
CREATE TABLE t2(c1 int, c2 int, c3 char(50), c4 char(50), c5 char(50));
INSERT INTO t1 VALUES (5,5,5,5,5), (3,3,3,3,3), (4,4,4,4,4);
INSERT INTO t2 VALUES (1,1,1,1,1), (2,2,2,2,2), (3,3,3,3,3), (4,4,4,4,4);
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 select * FROM t2;
ANALYZE;

SET optimizer TO on;
SET gp_enable_runtime_filter_pushdown TO off;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT t1.c3 FROM t1, t2 WHERE t1.c1 = t2.c1;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT t1.c3 FROM t1, t2 WHERE t1.c1 = t2.c1;

RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- case 6: hashjoin + hashjoin + seqscan
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1(c1 int, c2 int, c3 char(50), c4 char(50), c5 char(50)) DISTRIBUTED REPLICATED;
CREATE TABLE t2(c1 int, c2 int, c3 char(50), c4 char(50), c5 char(50)) DISTRIBUTED REPLICATED;
CREATE TABLE t3(c1 int, c2 int, c3 char(50), c4 char(50), c5 char(50)) DISTRIBUTED REPLICATED;
INSERT INTO t1 VALUES (1,1,1,1,1), (2,2,2,2,2), (5,5,5,5,5);
INSERT INTO t2 VALUES (1,1,1,1,1), (2,2,2,2,2), (3,3,3,3,3), (4,4,4,4,4);
INSERT INTO t3 VALUES (1,1,1,1,1), (2,2,2,2,2), (3,3,3,3,3), (4,4,4,4,4);
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t1 SELECT * FROM t1;
INSERT INTO t2 select * FROM t2;
INSERT INTO t2 select * FROM t2;
INSERT INTO t3 select * FROM t3;
ANALYZE;

SET optimizer TO off;
SET gp_enable_runtime_filter_pushdown TO off;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM t1, t2, t3 WHERE t1.c1 = t2.c1 AND t1.c2 = t3.c2;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM t1, t2, t3 WHERE t1.c1 = t2.c1 AND t1.c2 = t3.c2;

RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

-- Clean up: reset guc
SET gp_enable_runtime_filter TO off;
SET optimizer TO default;
