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

-- Clean up: reset guc
SET gp_enable_runtime_filter TO off;
SET optimizer TO default;
