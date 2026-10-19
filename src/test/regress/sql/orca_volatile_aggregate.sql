-- A volatile aggregate final function must run separately for each outer row.
SET optimizer = on;
SET optimizer_trace_fallback = on;
CREATE SCHEMA volatile_agg_sq;
SET search_path = volatile_agg_sq, public;
CREATE TABLE o(d int) DISTRIBUTED BY (d);
CREATE TABLE i(c int, b int) DISTRIBUTED BY (c);
INSERT INTO o SELECT g%1000 FROM generate_series(1,2000) g;
INSERT INTO i SELECT g%1000,1 FROM generate_series(1,10000) g;
ANALYZE o;
ANALYZE i;
CREATE FUNCTION finish_random(int) RETURNS float8 LANGUAGE sql VOLATILE
AS 'SELECT random()';
CREATE AGGREGATE random_agg(int)
    (SFUNC=int4pl, STYPE=int4, FINALFUNC=finish_random);

-- Sharing one aggregate result per key would give exactly 1000 values.
-- Independent evaluations need only one pair to differ for this to be true.
CREATE TABLE samples AS
SELECT d,(SELECT random_agg(b) FROM i WHERE c=o.d) AS v FROM o
DISTRIBUTED BY (d);
SELECT count(DISTINCT v)>1000 AS separate_final_calls FROM samples;

EXPLAIN (COSTS OFF)
SELECT d,(SELECT random_agg(b) FROM i WHERE c=o.d) FROM o;

-- Quantified subqueries must retain the same execution boundary.
EXPLAIN (COSTS OFF)
SELECT d FROM o WHERE 0 <= ANY (SELECT random_agg(b) FROM i WHERE c=o.d);
EXPLAIN (COSTS OFF)
SELECT d FROM o WHERE 0 <= ALL (SELECT random_agg(b) FROM i WHERE c=o.d);

DROP AGGREGATE random_agg(int);
DROP FUNCTION finish_random(int);
DROP TABLE samples, i, o;
DROP SCHEMA volatile_agg_sq;
RESET search_path;
RESET optimizer_trace_fallback;
RESET optimizer;
