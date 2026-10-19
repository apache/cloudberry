-- Preserve the row produced by a scalar aggregate on empty input, before
-- evaluating its projections, HAVING, or the surrounding scalar expression.
SET optimizer = on;
SET optimizer_trace_fallback = on;
CREATE SCHEMA agg_sq;
SET search_path = agg_sq, public;
CREATE TABLE o(a int, d int) DISTRIBUTED BY (d);
CREATE TABLE i(a int, b int) DISTRIBUTED BY (a);
INSERT INTO o VALUES (0,0), (2,1), (1,2), (3,3), (NULL,0), (0,NULL);
INSERT INTO i VALUES (1,10), (1,20), (2,NULL);
ANALYZE o;
ANALYZE i;

SELECT a, d,
    (SELECT sum(b)+1 FROM i WHERE i.a=o.d) AS sum_plus_one,
    (SELECT coalesce(sum(b),0)+1 FROM i WHERE i.a=o.d) AS coalesce_sum,
    (SELECT count(*)+coalesce(sum(b),0) FROM i WHERE i.a=o.d) AS multiple_aggs,
    (SELECT sum(b) IS NULL FROM i WHERE i.a=o.d) AS null_sum
FROM o ORDER BY d,a;

-- HAVING can remove either a real group or the scalar aggregate's empty row.
SELECT a, d,
    (SELECT count(*) FROM i WHERE i.a=o.d HAVING count(*)>0) AS nonempty,
    (SELECT count(*) FROM i WHERE i.a=o.d HAVING count(*)<2) AS small,
    (SELECT coalesce(sum(b),0) FROM i WHERE i.a=o.d HAVING count(*)<2) AS projected
FROM o ORDER BY d,a;

-- Pull HAVING above the aggregate join. An absent input group supplies zero
-- to COUNT, but a false or NULL HAVING removes the scalar row altogether.
SELECT a, d,
    (SELECT count(*) FROM i WHERE i.a=o.d HAVING NULL::bool) AS unknown,
    (SELECT count(*)+1 FROM i WHERE i.a=o.d HAVING count(*)<2) AS small_expr,
    (SELECT count(b) FROM i WHERE i.a=o.d HAVING count(b)<2) AS nonnull_count,
    (SELECT count(*) FROM i WHERE i.a=o.d HAVING count(*)<o.a) AS outer_limit
FROM o ORDER BY d,a;
SELECT a,d FROM o WHERE
    0=(SELECT count(*) FROM i WHERE i.a=o.d HAVING count(*)>0)
ORDER BY d,a;

-- Projections above HAVING must not run for the rows it rejects.
SELECT a,d,
    (SELECT 100/count(*) FROM i WHERE i.a=o.d HAVING count(*)>0) AS positive,
    (SELECT 100/(count(*)-2) FROM i WHERE i.a=o.d HAVING count(*)<2) AS small
FROM o ORDER BY d,a;

SELECT (SELECT count(*) FROM i HAVING count(*)>10) AS rejected,
       (SELECT count(*) FROM i GROUP BY a HAVING count(*)>10) AS grouped;
SELECT a,d,(SELECT coalesce(sum(b),0) FROM i WHERE i.a<o.d) AS inequality
FROM o ORDER BY d,a;

-- MIN/MAX need no empty-input SubPlan. Keep the inner join predicate and
-- distinguish an absent group from an existing all-NULL group in projections.
CREATE TABLE j(a int) DISTRIBUTED BY (a);
INSERT INTO j VALUES (1),(1),(3);
ANALYZE j;
SELECT a,d,
    (SELECT max(b) FROM i JOIN j ON i.a=j.a WHERE i.a=o.d) AS joined_max,
    (SELECT coalesce(min(b),0)+1 FROM i WHERE i.a=o.d) AS min_expr,
    (SELECT max(b) FROM i WHERE i.a=o.d HAVING count(*)<2) AS max_having
FROM o ORDER BY d,a;
SELECT a,d FROM o
WHERE 20=(SELECT max(b) FROM i JOIN j ON i.a=j.a WHERE i.a=o.d)
ORDER BY d,a;
DROP TABLE j;

-- A user aggregate named max need not return NULL on empty input. Type
-- metadata must not identify it as the built-in MIN/MAX optimization.
CREATE TYPE agg_state AS ENUM ('empty','a');
CREATE FUNCTION take_state(agg_state,agg_state) RETURNS agg_state
LANGUAGE sql IMMUTABLE STRICT AS 'SELECT $2';
CREATE AGGREGATE max(agg_state)
    (SFUNC=take_state,STYPE=agg_state,INITCOND='empty');
SELECT a,d,(SELECT max('a'::agg_state) FROM i WHERE i.a=o.d) AS custom_max
FROM o ORDER BY d,a;
DROP AGGREGATE max(agg_state);
DROP FUNCTION take_state(agg_state,agg_state);
DROP TYPE agg_state;

-- Only catalog-identified built-ins may use the NULL-on-empty shortcut.
-- User aggregates with the same names and argument types can return a value.
CREATE AGGREGATE agg_sq.sum(int) (SFUNC=int4pl, STYPE=int4, INITCOND='7');
CREATE AGGREGATE agg_sq.avg(int) (SFUNC=int4pl, STYPE=int4, INITCOND='9');
SELECT a,d,
    (SELECT agg_sq.sum(b) FROM i WHERE i.a=o.d) AS custom_sum,
    (SELECT agg_sq.avg(b) FROM i WHERE i.a=o.d) AS custom_avg
FROM o ORDER BY d,a;
DROP AGGREGATE agg_sq.sum(int);
DROP AGGREGATE agg_sq.avg(int);

-- Non-count aggregates also have non-NULL empty-input results.
CREATE FUNCTION zero_final(int) RETURNS int LANGUAGE sql IMMUTABLE
AS 'SELECT coalesce($1,0)';
CREATE AGGREGATE sum_zero(int) (SFUNC=int4pl, STYPE=int4, FINALFUNC=zero_final);
SELECT a, d,
    (SELECT regr_count(a,b) FROM i WHERE i.a=o.d) AS regr,
    (SELECT rank(5) WITHIN GROUP (ORDER BY b) FROM i WHERE i.a=o.d) AS rank,
    (SELECT cume_dist(5) WITHIN GROUP (ORDER BY b) FROM i WHERE i.a=o.d) AS cume,
    (SELECT sum_zero(b)+1 FROM i WHERE i.a=o.d) AS custom
FROM o ORDER BY d,a;

-- Projection expressions must not run for unrelated groups or an empty outer.
SELECT d, (SELECT 100/coalesce(sum(b),0) FROM i WHERE i.a=o.d)
FROM o WHERE d=1;
CREATE TABLE empty_o (d int) DISTRIBUTED BY (d);
SELECT d, (SELECT 100/coalesce(sum(b),0) FROM i WHERE i.a=empty_o.d)
FROM empty_o;

-- The original aggregate finalizes before evaluating HAVING.
CREATE FUNCTION throw_empty(int) RETURNS int LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF $1 IS NULL THEN RAISE EXCEPTION 'empty aggregate state'; END IF;
    RETURN $1;
END
$$;
CREATE AGGREGATE sum_throw(int) (SFUNC=int4pl, STYPE=int4, FINALFUNC=throw_empty);
SELECT d, (SELECT sum_throw(b) FROM i WHERE i.a=o.d HAVING count(*)>0)
FROM o WHERE a=0 AND d=0;
SELECT d, (SELECT sum_throw(b) FROM i WHERE i.a=empty_o.d) FROM empty_o;

-- Enough rows for decorrelation, including a missing key and an unrelated
-- all-NULL group whose projection would throw if evaluated before the join.
CREATE TABLE many_o (d int) DISTRIBUTED BY (d);
CREATE TABLE many_i (a int, b int) DISTRIBUTED BY (a);
INSERT INTO many_o SELECT g%1001 FROM generate_series(0,2001) g;
INSERT INTO many_i SELECT g%1000,1 FROM generate_series(1,10000) g;
INSERT INTO many_i VALUES (1001,NULL);
ANALYZE many_o;
ANALYZE many_i;
EXPLAIN (COSTS OFF)
SELECT d FROM many_o o
WHERE 1=(SELECT count(*) FROM many_i i WHERE i.a=o.d HAVING count(*)<10);
SELECT count(*) FROM many_o o
WHERE 10=(SELECT count(*) FROM many_i i WHERE i.a=o.d HAVING count(*)<11);
SELECT count(*) FROM many_o o
WHERE 0=(SELECT count(*) FROM many_i i WHERE i.a=o.d HAVING count(*)<10);
EXPLAIN (COSTS OFF)
SELECT d,(SELECT coalesce(sum(b),0)+1 FROM many_i i WHERE i.a=o.d)
FROM many_o o;
SELECT count(*) AS rows, sum(v) AS total FROM (
    SELECT (SELECT coalesce(sum(b),0)+1 FROM many_i i WHERE i.a=o.d) AS v
    FROM many_o o
) q;
SELECT count(*) AS rows, sum(v) AS total FROM (
    SELECT (SELECT sum_zero(b)+1 FROM many_i i WHERE i.a=o.d) AS v
    FROM many_o o
) q;
SELECT count(*) AS rows, sum(v) AS total FROM (
    SELECT (SELECT 100/coalesce(sum(b),0) FROM many_i i WHERE i.a=o.d) AS v
    FROM many_o o WHERE d<1000
) q;

-- Filter context may discard a missing scalar row only when the predicate
-- rejects NULL. Test aggregates and ordinary scalar subqueries alike.
SELECT a,d FROM o WHERE
    (SELECT sum(b)+1 FROM i WHERE i.a=o.d) IS NULL ORDER BY d,a;
SELECT a,d FROM o WHERE
    (SELECT count(*) FROM i WHERE i.a=o.d HAVING count(*)>0) IS NULL
ORDER BY d,a;
SELECT a,d FROM o WHERE
    (SELECT sum(b) FROM i WHERE i.a=o.d GROUP BY i.a) IS NULL ORDER BY d,a;
SELECT a,d FROM o WHERE
    (SELECT b FROM i WHERE i.a=o.d AND b=10) IS NULL ORDER BY d,a;
SELECT a,d FROM o WHERE
    coalesce((SELECT b FROM i WHERE i.a=o.d AND b=10),0)=0 ORDER BY d,a;
SELECT a,d FROM o WHERE
    (SELECT b FROM i WHERE i.a=o.d AND b=10) IS NOT DISTINCT FROM NULL
ORDER BY d,a;

-- Replacing all subqueries by NULL at once is insufficient: either operand
-- can supply the COALESCE result when the other one returns no rows.
SELECT a,d FROM o WHERE coalesce(
    (SELECT b FROM i WHERE i.a=o.d AND b=999),
    (SELECT b FROM i WHERE i.a=o.d AND b=10))=10 ORDER BY d,a;
SELECT a,d FROM o WHERE coalesce(
    (SELECT b FROM i WHERE i.a=o.d AND b=10),
    (SELECT b FROM i WHERE i.a=o.d AND b=999))=10 ORDER BY d,a;
SELECT a,d FROM o WHERE
    coalesce((SELECT b FROM i WHERE i.a=o.d AND b=10),(SELECT 0))=0
ORDER BY d,a;
SELECT a,d FROM o WHERE
    (SELECT b FROM i WHERE i.a=o.d AND b=10)=10 AND d=1 ORDER BY d,a;

-- Fractional estimates below the correlated aggregate must not make a join
-- scale factor smaller than one and force a fallback while planning HAVING.
CREATE TABLE k(x int) DISTRIBUTED BY (x);
INSERT INTO k VALUES (0),(2);
ANALYZE k;
EXPLAIN (COSTS OFF)
SELECT d,(SELECT count(*) FROM i WHERE i.a=o.d
          HAVING count(*)>(SELECT max(x) FROM k)) AS v FROM o ORDER BY d;
SELECT d,(SELECT count(*) FROM i WHERE i.a=o.d
          HAVING count(*)>(SELECT max(x) FROM k)) AS v FROM o ORDER BY d;

DROP TABLE k, many_i, many_o, empty_o, i, o;
DROP AGGREGATE sum_throw(int);
DROP AGGREGATE sum_zero(int);
DROP FUNCTION throw_empty(int);
DROP FUNCTION zero_final(int);
RESET search_path;
DROP SCHEMA agg_sq;
RESET optimizer_trace_fallback;
RESET optimizer;
