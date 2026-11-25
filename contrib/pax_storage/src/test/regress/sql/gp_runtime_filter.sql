-- start_matchignore
-- m/^.*Extra Text:.*/
-- m/^.*Buckets:.*/
-- end_matchignore
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
SET enable_parallel TO off;

-- case 1: join on distribution table and replicated table.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(c1 int, c2 int, c3 int, c4 int, c5 int) distributed by (c1);
CREATE TABLE t2(c1 int, c2 int, c3 int, c4 int, c5 int) distributed REPLICATED;

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
DROP TABLE IF EXISTS test_tablesample1;
CREATE TABLE test_tablesample1 (dist int, id int, name text) DISTRIBUTED BY (dist);
INSERT INTO test_tablesample1 SELECT 0, i, repeat(i::text, 875) FROM generate_series(0, 9) s(i) ORDER BY i;
INSERT INTO test_tablesample1 SELECT 3, i, repeat(i::text, 875) FROM generate_series(10, 19) s(i) ORDER BY i;
INSERT INTO test_tablesample1 SELECT 5, i, repeat(i::text, 875) FROM generate_series(20, 29) s(i) ORDER BY i;

SET gp_enable_runtime_filter_pushdown TO on;
EXPLAIN (COSTS OFF) SELECT id FROM test_tablesample1 TABLESAMPLE SYSTEM (50) REPEATABLE (2);
RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS test_tablesample1;

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

-- case 7: scan partition table with dynamic scan
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

SET optimizer TO on;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT t1.c2 FROM t1, t2 WHERE t1.c2 = t2.c2;

SET gp_enable_runtime_filter_pushdown TO on;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT t1.c2 FROM t1, t2 WHERE t1.c2 = t2.c2;

set pax.enable_row_filter TO on;
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
SELECT t1.c2 FROM t1, t2 WHERE t1.c2 = t2.c2;


RESET pax.enable_row_filter;
RESET gp_enable_runtime_filter_pushdown;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

SET optimizer TO off;

-- Isolate executor pushdown from the planner's separate RuntimeFilter node.
-- That node hashes raw integer Datums on this co-located join.
SET gp_enable_runtime_filter TO off;
-- Execute a plain SELECT in each mode and compare its complete row multiset.
-- In particular, the correlated rescan query must not acquire CTAS writer
-- distribution requirements. Sort on the coordinator after execution.
CREATE FUNCTION rf_query_rows(q text, pushdown boolean) RETURNS jsonb[]
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    rows jsonb[] := '{}';
    previous text := current_setting('gp_enable_runtime_filter_pushdown');
BEGIN
    PERFORM set_config('gp_enable_runtime_filter_pushdown', pushdown::text, true);
    FOR r IN EXECUTE q LOOP
        rows := array_append(rows, to_jsonb(r));
    END LOOP;
    PERFORM set_config('gp_enable_runtime_filter_pushdown', previous, true);
    RETURN ARRAY(SELECT v FROM unnest(rows) AS t(v) ORDER BY v);
END $$;

-- Negative keys with range keys pushed into PAX.  The build side is AOCO and
-- co-located with the probe, so the Hash node receives AOCO's zero-extended
-- int4 Datums; the Bloom filter and the min/max range must still describe the
-- negative values, or PAX drops rows that join.  The int8 build key checks
-- that equal int4 and int8 keys hash and compare as the same integer.
SET optimizer TO off;
SET pax.enable_row_filter TO on;
CREATE TABLE rf_neg_probe (k int, v int) USING pax DISTRIBUTED BY (k);
INSERT INTO rf_neg_probe SELECT -(i % 1000), i FROM generate_series(1, 50000) i;
CREATE TABLE rf_neg_build4 (k int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO rf_neg_build4 SELECT -i FROM generate_series(1, 20) i;
CREATE TABLE rf_neg_build8 (k int8) USING ao_column DISTRIBUTED RANDOMLY;
INSERT INTO rf_neg_build8 SELECT -i FROM generate_series(1, 20) i;
ANALYZE rf_neg_probe, rf_neg_build4, rf_neg_build8;
DO $$
DECLARE
    build text;
    q text;
    baseline jsonb[];
    filtered jsonb[];
    line text;
    planner_filter boolean := false;
BEGIN
    FOREACH build IN ARRAY ARRAY['rf_neg_build4', 'rf_neg_build8'] LOOP
        q := format('SELECT p.v FROM rf_neg_probe p JOIN %I b ON p.k = b.k', build);
        baseline := rf_query_rows(q, false);
        filtered := rf_query_rows(q, true);
        IF cardinality(baseline) <> 1000 OR filtered IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'negative-key mismatch for % (baseline rows %, filtered rows %)',
                build, cardinality(baseline), cardinality(filtered);
        END IF;
    END LOOP;
    -- Exercise the planner's node separately on the same AOCO/PAX join.
    q := 'SELECT p.v FROM rf_neg_probe p JOIN rf_neg_build4 b ON p.k = b.k';
    baseline := rf_query_rows(q, false);
    PERFORM set_config('gp_enable_runtime_filter', 'on', true);
    FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        planner_filter := planner_filter OR line LIKE '%->  RuntimeFilter%';
    END LOOP;
    filtered := rf_query_rows(q, false);
    PERFORM set_config('gp_enable_runtime_filter', 'off', true);
    IF NOT planner_filter OR filtered IS DISTINCT FROM baseline THEN
        RAISE EXCEPTION 'planner RuntimeFilter failed negative-key check (planned %, rows %)',
            planner_filter, cardinality(filtered);
    END IF;
END $$;

DROP TABLE rf_neg_probe, rf_neg_build4, rf_neg_build8;

-- A hash join rescanned with new build keys: for each outer row the nested
-- loop runs the LATERAL subquery again, and the hash table is rebuilt from the
-- build rows of that row's group, a different set of keys every time.  The
-- PAX scan below must use the new filter on every iteration, not keep the
-- copy of the first one it was given (its rescan does not take new keys, so
-- the executor restarts the scan instead).
CREATE TABLE rf_rs_outer (g int) DISTRIBUTED REPLICATED;
INSERT INTO rf_rs_outer SELECT i FROM generate_series(1, 5) i;
CREATE TABLE rf_rs_probe (k int, v int) USING pax DISTRIBUTED REPLICATED;
INSERT INTO rf_rs_probe SELECT i % 100, i FROM generate_series(1, 20000) i;
CREATE TABLE rf_rs_build (k int, g int) DISTRIBUTED REPLICATED;
INSERT INTO rf_rs_build SELECT i, i % 5 + 1 FROM generate_series(0, 99) i;
ANALYZE rf_rs_outer, rf_rs_probe, rf_rs_build;
SET enable_mergejoin TO off;

DO $$
DECLARE
    q constant text := 'SELECT o.g, s.c, s.total FROM rf_rs_outer o,
        LATERAL (SELECT count(*) AS c, sum(p.v) AS total
          FROM rf_rs_probe p JOIN rf_rs_build b ON p.k = b.k WHERE b.g = o.g) s';
    line text;
    nestloop boolean := false;
    hashjoin boolean := false;
    baseline jsonb[];
    filtered jsonb[];
    matches bigint;
BEGIN
    FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        nestloop := nestloop OR line LIKE '%Nested Loop%';
        hashjoin := hashjoin OR line LIKE '%Hash Join%';
    END LOOP;
    IF NOT (nestloop AND hashjoin) THEN
        RAISE EXCEPTION 'expected a rescanned hash join below a nested loop';
    END IF;
    baseline := rf_query_rows(q, false);
    filtered := rf_query_rows(q, true);
    SELECT sum((v->>'c')::bigint) INTO matches FROM unnest(baseline) AS t(v);
    IF cardinality(baseline) <> 5 OR matches IS DISTINCT FROM 20000::bigint OR
        filtered IS DISTINCT FROM baseline THEN
        RAISE EXCEPTION 'rescan mismatch (groups %, matches %, filtered groups %)',
            cardinality(baseline), matches, cardinality(filtered);
    END IF;
END $$;

DROP FUNCTION rf_query_rows(text, boolean);
DROP TABLE rf_rs_outer, rf_rs_probe, rf_rs_build;
RESET enable_mergejoin;
RESET gp_enable_runtime_filter_pushdown;
RESET pax.enable_row_filter;

RESET enable_parallel;
SET optimizer TO default;
