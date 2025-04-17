SET optimizer TO on;

-- Test Suit 1: runtime filter main case
DROP TABLE IF EXISTS fact_rf, dim_rf;
CREATE TABLE fact_rf (fid int, did int, val int) using pax WITH(minmax_columns='fid,did,val');
CREATE TABLE dim_rf (did int, proj_id int, filter_val int) using pax WITH(minmax_columns='did,proj_id,filter_val');

-- Generating data, fact_rd.did and dim_rf.did is 80% matched
INSERT INTO fact_rf SELECT i, i % 8000 + 1, i FROM generate_series(1, 100000) s(i);
INSERT INTO dim_rf SELECT i, i % 10, i FROM generate_series(1, 10000) s(i);
ANALYZE fact_rf, dim_rf;

SET gp_enable_runtime_filter_pushdown TO off;
EXPLAIN ANALYZE SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2 AND filter_val <= 1000;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN ANALYZE SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2 AND filter_val <= 1000;

-- Test bad filter rate
EXPLAIN ANALYZE SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 7;

-- Test outer join
-- LeftJoin (eliminated and applicatable)
EXPLAIN ANALYZE SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2 AND filter_val <= 1000;

-- LeftJoin
EXPLAIN ANALYZE SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id IS NULL OR proj_id < 2 AND filter_val <= 1000;

-- RightJoin (applicatable)
EXPLAIN ANALYZE SELECT COUNT(*) FROM
    fact_rf RIGHT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2 AND filter_val <= 1000;

-- SemiJoin
EXPLAIN ANALYZE SELECT COUNT(*) FROM fact_rf
    WHERE fact_rf.did IN (SELECT did FROM dim_rf WHERE proj_id < 2 AND filter_val <= 1000);

-- SemiJoin -> InnerJoin and deduplicate
EXPLAIN ANALYZE SELECT COUNT(*) FROM dim_rf
    WHERE dim_rf.did IN (SELECT did FROM fact_rf) AND proj_id < 2 AND filter_val <= 1000;

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
    WHERE proj_id < 2 AND filter_val <= 1000;

SELECT COUNT(*) FROM
    fact_rf LEFT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id IS NULL OR proj_id < 2 AND filter_val <= 1000;

SELECT COUNT(*) FROM
    fact_rf RIGHT JOIN dim_rf ON fact_rf.did = dim_rf.did
    WHERE proj_id < 2 AND filter_val <= 1000;

SELECT COUNT(*) FROM fact_rf
    WHERE fact_rf.did IN (SELECT did FROM dim_rf WHERE proj_id < 2 AND filter_val <= 1000);

SELECT COUNT(*) FROM dim_rf
    WHERE dim_rf.did IN (SELECT did FROM fact_rf) AND proj_id < 2 AND filter_val <= 1000;

-- Test contain null values
INSERT INTO dim_rf VALUES (NULL,1, 1);
EXPLAIN ANALYZE SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2 AND filter_val <= 1000;
SELECT COUNT(*) FROM fact_rf, dim_rf
    WHERE fact_rf.did = dim_rf.did AND proj_id < 2 AND filter_val <= 1000;

-- Clean up: reset guc
SET gp_enable_runtime_filter_pushdown TO off;
RESET optimizer;
