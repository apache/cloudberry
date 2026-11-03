--
-- Test ORCA optimizer handling of aggregates with ORDER BY, comparing
-- with the results of the Postgres planner.
--

-- Force ORCA and make fallback visible
SET optimizer TO on;
SET optimizer_trace_fallback TO on;

CREATE SCHEMA orca_agg_order_by;
SET search_path = orca_agg_order_by;

-- ======================================================================
-- Test: aggregates with ORDER BY are not split into partial aggregation
-- ======================================================================

-- string_agg() and array_agg() have combine functions, but with an
-- ORDER BY each segment would only sort its own rows, and the combine
-- function would concatenate the partial results in arrival order.
CREATE TABLE t_agg_order (id int, v text) DISTRIBUTED BY (id);
INSERT INTO t_agg_order SELECT g, lpad(g::text, 2, '0')
FROM generate_series(1, 30) g;
ANALYZE t_agg_order;

EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_order;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_order;
SELECT array_agg(id ORDER BY id DESC) FROM t_agg_order;
SELECT count(DISTINCT id % 7), string_agg(v, ',' ORDER BY v) FROM t_agg_order;

-- aggregates without ORDER BY, and order insensitive aggregates with
-- ORDER BY, are still split
EXPLAIN (COSTS OFF) SELECT sum(id), count(*) FROM t_agg_order;
EXPLAIN (COSTS OFF) SELECT sum(id ORDER BY id), max(v ORDER BY v) FROM t_agg_order;
SELECT sum(id ORDER BY id), max(v ORDER BY v) FROM t_agg_order;

-- Any aggregate that is not known to be order insensitive is not split when
-- it has an ORDER BY, including user-defined ones
CREATE AGGREGATE my_concat(text) (
    sfunc = textcat, stype = text, combinefunc = textcat, initcond = '');
SET optimizer_force_multistage_agg TO on;
EXPLAIN (COSTS OFF) SELECT my_concat(v ORDER BY v) FROM t_agg_order;
SELECT my_concat(v || ',' ORDER BY v) FROM t_agg_order;
EXPLAIN (COSTS OFF) SELECT my_concat(v) FROM t_agg_order;

-- MIN/MAX is only order insensitive if equal values are identical, unlike
-- numeric, where ORDER BY decides which of 4.0 and 4.00 max() returns
EXPLAIN (COSTS OFF) SELECT max(id ORDER BY v), max(v ORDER BY id) FROM t_agg_order;
EXPLAIN (COSTS OFF) SELECT max(id::numeric ORDER BY id) FROM t_agg_order;

-- floating point sums depend on the order of the input
EXPLAIN (COSTS OFF) SELECT sum(id::float8 ORDER BY id) FROM t_agg_order;
RESET optimizer_force_multistage_agg;
DROP AGGREGATE my_concat(text);

DROP TABLE t_agg_order;

-- ======================================================================
-- Test: presorted input for order sensitive aggregates
-- ======================================================================

-- The input of string_agg() and array_agg() with ORDER BY is sorted on the
-- segments and merged by the Gather Motion, so that the aggregate doesn't
-- need to sort it on the coordinator.
CREATE TABLE t_agg_presort (id int, v text, b bytea) DISTRIBUTED BY (id);
INSERT INTO t_agg_presort SELECT g, lpad(g::text, 2, '0'), lpad(g::text, 2, '0')::bytea
FROM generate_series(1, 30) g;
-- the sort on the segments and the merging Gather Motion must place the
-- NULLs according to NULLS FIRST / LAST
INSERT INTO t_agg_presort SELECT g, NULL, NULL FROM generate_series(31, 35) g;
ANALYZE t_agg_presort;

EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v DESC) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v DESC) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT array_agg(id ORDER BY v DESC NULLS LAST, id) FROM t_agg_presort;
SELECT array_agg(id ORDER BY v DESC NULLS LAST, id) FROM t_agg_presort;
SELECT string_agg(b, ',' ORDER BY b) FROM t_agg_presort;
EXPLAIN (COSTS OFF) SELECT gp_array_agg(v ORDER BY v) FROM t_agg_presort;
SELECT gp_array_agg(v ORDER BY v DESC) FROM t_agg_presort;

-- aggregates with the same ORDER BY share the sorted input
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v), max(id ORDER BY v), count(*) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v), max(id ORDER BY v), count(*) FROM t_agg_presort;

-- not used with different orders, DISTINCT, an ORDER BY expression, or
-- only order insensitive aggregates
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v), array_agg(id ORDER BY id) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT array_agg(v ORDER BY v), array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v), array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT string_agg(DISTINCT v, ',' ORDER BY v) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v || 'x') FROM t_agg_presort;
EXPLAIN (COSTS OFF) SELECT sum(id ORDER BY id) FROM t_agg_presort;

-- not used with an ORDER BY on an outer reference, which the input of the
-- aggregate cannot be sorted by; this must not fail to produce a plan
SELECT o.id, (SELECT length(string_agg(i.v, ',' ORDER BY o.v))
              FROM t_agg_presort i WHERE i.id % 3 = o.id % 3)
FROM t_agg_presort o WHERE o.id <= 3 ORDER BY 1;

-- not used when disabled
SET optimizer_enable_presorted_agg TO off;
EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
RESET optimizer_enable_presorted_agg;

DROP TABLE t_agg_presort;

-- ======================================================================
-- Test: GROUP BY with aggregates with ORDER BY
-- ======================================================================

-- A hash aggregate cannot sort the input of an aggregate with ORDER BY
-- per group, so ORCA must use a stream aggregate instead of falling back.
CREATE TABLE t_agg_group (id int, k int, v text) DISTRIBUTED BY (id);
INSERT INTO t_agg_group SELECT g, g % 3, lpad(g::text, 2, '0')
FROM generate_series(1, 30) g;
ANALYZE t_agg_group;

EXPLAIN (COSTS OFF)
SELECT k, string_agg(v, ',' ORDER BY v) FROM t_agg_group GROUP BY k;
SELECT k, string_agg(v, ',' ORDER BY v DESC) FROM t_agg_group GROUP BY k ORDER BY k;
SELECT k, array_agg(id ORDER BY id), count(*) FROM t_agg_group GROUP BY k ORDER BY k;
EXPLAIN (COSTS OFF)
SELECT k, sum(id ORDER BY id) FROM t_agg_group GROUP BY k;
SELECT k, sum(id ORDER BY id) FROM t_agg_group GROUP BY k ORDER BY k;

-- without ORDER BY, a hash aggregate is still used
EXPLAIN (COSTS OFF) SELECT k, sum(id) FROM t_agg_group GROUP BY k;

DROP TABLE t_agg_group;

-- ======================================================================
-- Cleanup
-- ======================================================================

RESET optimizer_trace_fallback;
RESET optimizer;
DROP SCHEMA orca_agg_order_by CASCADE;
