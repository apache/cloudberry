--
-- Test ORCA optimizer handling of COLLATE "C" in en_US.UTF-8 database.
--
-- This test verifies that ORCA correctly propagates column-level and
-- expression-level collation through its internal DXL representation,
-- producing plans with correct collation semantics.
--
-- Prerequisites: database must have LC_COLLATE=en_US.UTF-8 (non-C locale)
-- so that C collation differs from the default.
--

-- Force ORCA and make fallback visible
SET optimizer TO on;
SET optimizer_trace_fallback TO on;

CREATE SCHEMA collate_orca;
SET search_path = collate_orca;

-- ======================================================================
-- Setup: tables with COLLATE "C" columns
-- ======================================================================

CREATE TABLE t_c_collation (
    id int,
    name text COLLATE "C",
    val varchar(50) COLLATE "C"
) DISTRIBUTED BY (id);

CREATE TABLE t_default_collation (
    id int,
    name text,
    val varchar(50)
) DISTRIBUTED BY (id);

-- Mixed: some columns C, some default
CREATE TABLE t_mixed_collation (
    id int,
    c_name text COLLATE "C",
    d_name text
) DISTRIBUTED BY (id);

-- Insert test data: uppercase letters have lower byte values than lowercase in ASCII
-- C collation: 'ABC' < 'abc' (byte order)
-- en_US.UTF-8: 'abc' < 'ABC' (case-insensitive primary sort)
INSERT INTO t_c_collation VALUES
    (1, 'abc', 'apple'),
    (2, 'ABC', 'APPLE'),
    (3, 'def', 'banana'),
    (4, 'DEF', 'BANANA'),
    (5, 'ghi', 'cherry'),
    (6, 'GHI', 'CHERRY');

INSERT INTO t_default_collation SELECT * FROM t_c_collation;
INSERT INTO t_mixed_collation SELECT id, name, name FROM t_c_collation;

ANALYZE t_c_collation;
ANALYZE t_default_collation;
ANALYZE t_mixed_collation;

-- ======================================================================
-- Test 8.3: ORDER BY uses C collation, not en_US.UTF-8
-- ======================================================================

-- C collation: uppercase before lowercase (byte order: A=65 < a=97)
-- If ORCA incorrectly uses default collation, order will be different
SELECT name FROM t_c_collation ORDER BY name;

-- Compare with default collation table (should have different order)
SELECT name FROM t_default_collation ORDER BY name;

-- Verify sort order is strictly byte-based
SELECT name, val FROM t_c_collation ORDER BY val;

-- ORDER BY DESC
SELECT name FROM t_c_collation ORDER BY name DESC;

-- Multi-column ORDER BY with C collation
SELECT name, val FROM t_c_collation ORDER BY name, val;

-- ======================================================================
-- Test 8.4: WHERE = comparison uses C collation
-- ======================================================================

-- Equality: case-sensitive under C collation
SELECT id, name FROM t_c_collation WHERE name = 'abc';
SELECT id, name FROM t_c_collation WHERE name = 'ABC';

-- These should return different rows (C is case-sensitive)
SELECT count(*) FROM t_c_collation WHERE name = 'abc';
SELECT count(*) FROM t_c_collation WHERE name = 'ABC';

-- Range comparison: under C, 'Z' < 'a' (byte order)
SELECT name FROM t_c_collation WHERE name < 'a' ORDER BY name;
SELECT name FROM t_c_collation WHERE name >= 'a' ORDER BY name;

-- IN list with C collation
SELECT name FROM t_c_collation WHERE name IN ('abc', 'DEF') ORDER BY name;

-- ======================================================================
-- Test 8.5: JOIN on COLLATE "C" columns
-- ======================================================================

-- Inner join: should match on exact case under C collation
SELECT a.id, a.name, b.id, b.name
FROM t_c_collation a JOIN t_c_collation b ON a.name = b.name
WHERE a.id < b.id
ORDER BY a.name, a.id;

-- Join between C-collation and default-collation tables
-- The join should still work (both sides produce same text)
SELECT c.id, c.name, d.id, d.name
FROM t_c_collation c JOIN t_default_collation d ON c.id = d.id
WHERE c.name = d.name
ORDER BY c.name;

-- Self-join with inequality (tests collation in merge/hash join)
SELECT a.name, b.name
FROM t_c_collation a JOIN t_c_collation b ON a.name < b.name
WHERE a.id = 1 AND b.id IN (2, 4, 6)
ORDER BY a.name, b.name;

-- ======================================================================
-- Test 8.6: GROUP BY on COLLATE "C" column
-- ======================================================================

-- Under C collation, 'abc' and 'ABC' are different groups
SELECT name, count(*) FROM t_c_collation GROUP BY name ORDER BY name;

-- Aggregate with C collation grouping
SELECT name, min(val), max(val)
FROM t_c_collation GROUP BY name ORDER BY name;

-- GROUP BY on expression involving C collation column
SELECT upper(name) as uname, count(*)
FROM t_c_collation GROUP BY upper(name) ORDER BY uname;

-- HAVING with C collation
SELECT name, count(*) as cnt
FROM t_c_collation GROUP BY name HAVING name > 'Z' ORDER BY name;

-- ======================================================================
-- Test 8.7: Window functions with COLLATE "C" PARTITION BY
-- ======================================================================

-- Partition by C-collation column
SELECT name, val,
       row_number() OVER (PARTITION BY name ORDER BY val) as rn
FROM t_c_collation
ORDER BY name, val;

-- Window with ORDER BY on C-collation column
SELECT name, val,
       rank() OVER (ORDER BY name) as rnk
FROM t_c_collation
ORDER BY name, val;

-- Multiple window functions
SELECT name, val,
       count(*) OVER (PARTITION BY name) as grp_cnt,
       first_value(val) OVER (PARTITION BY name ORDER BY val) as first_val
FROM t_c_collation
ORDER BY name, val;

-- Window with expression-level COLLATE "C" on default-collation table
SELECT name, val,
       row_number() OVER (PARTITION BY name COLLATE "C" ORDER BY val COLLATE "C") as rn
FROM t_default_collation
ORDER BY name COLLATE "C", val COLLATE "C";

-- ======================================================================
-- Test 8.8: EXPLAIN shows correct collation in plan
-- ======================================================================

-- The sort key should reflect C collation, not default
EXPLAIN (COSTS OFF) SELECT name FROM t_c_collation ORDER BY name;

-- Join plan should use correct collation
EXPLAIN (COSTS OFF)
SELECT a.name, b.name
FROM t_c_collation a JOIN t_c_collation b ON a.name = b.name
ORDER BY a.name;

-- Aggregate plan
EXPLAIN (COSTS OFF)
SELECT name, count(*) FROM t_c_collation GROUP BY name ORDER BY name;

-- ======================================================================
-- Test 8.9: Mixed default + C collation columns in same query
-- ======================================================================

-- Query referencing both C and default collation columns
SELECT c_name, d_name
FROM t_mixed_collation
ORDER BY c_name;

SELECT c_name, d_name
FROM t_mixed_collation
ORDER BY d_name;

-- Mixed columns in WHERE
SELECT id, c_name, d_name
FROM t_mixed_collation
WHERE c_name = 'abc' AND d_name = 'abc';

-- Mixed columns in GROUP BY
SELECT c_name, d_name, count(*)
FROM t_mixed_collation
GROUP BY c_name, d_name
ORDER BY c_name, d_name;

-- Join on C column, filter on default column
SELECT m.id, m.c_name, m.d_name
FROM t_mixed_collation m JOIN t_c_collation c ON m.c_name = c.name
WHERE m.d_name > 'D'
ORDER BY m.c_name;

-- ======================================================================
-- Test: Collation resolution for mixed-collation argument lists
-- gpdb::ExprCollation(List) must match PG's merge_collation_state() rule:
-- non-default implicit collation always beats default, regardless of
-- argument order.  Previously the translator just returned the first
-- non-InvalidOid collation, so (default, C) picked default — wrong.
-- ======================================================================

-- coalesce: DEFAULT column first, C column second
-- Must sort in C byte order (A < B < a < b), not locale order (a < A < b < B)
SELECT coalesce(d_name, c_name) AS r FROM t_mixed_collation ORDER BY coalesce(d_name, c_name);

-- coalesce: C column first, DEFAULT column second (control — always worked)
SELECT coalesce(c_name, d_name) AS r FROM t_mixed_collation ORDER BY coalesce(c_name, d_name);

-- Verify both orders produce identical results
SELECT coalesce(d_name, c_name) AS dc, coalesce(c_name, d_name) AS cd
FROM t_mixed_collation
ORDER BY coalesce(d_name, c_name);

-- EXPLAIN must show COLLATE "C" on the sort key regardless of arg order
EXPLAIN (COSTS OFF)
SELECT * FROM t_mixed_collation ORDER BY coalesce(d_name, c_name);

-- Operator expression: 'literal' || c_col  (DEFAULT || C → should pick C)
SELECT d_name || c_name AS r FROM t_mixed_collation ORDER BY d_name || c_name;

-- min/max with mixed-collation coalesce argument
SELECT min(coalesce(d_name, c_name)), max(coalesce(d_name, c_name)) FROM t_mixed_collation;

-- CASE result with mixed collation branches
-- WHEN branch returns d_name (default), ELSE returns c_name (C).
-- Output collation should be C.
SELECT CASE WHEN id <= 3 THEN d_name ELSE c_name END AS r
FROM t_mixed_collation
ORDER BY CASE WHEN id <= 3 THEN d_name ELSE c_name END;

-- ======================================================================
-- Test: Expression-level COLLATE "C"
-- ======================================================================

-- COLLATE in WHERE clause on default-collation table
SELECT name FROM t_default_collation WHERE name COLLATE "C" < 'a' ORDER BY name COLLATE "C";

-- COLLATE in ORDER BY on default-collation table
SELECT name FROM t_default_collation ORDER BY name COLLATE "C";

-- COLLATE in expression
SELECT name, name COLLATE "C" < 'a' as is_upper
FROM t_default_collation ORDER BY name COLLATE "C";

-- ======================================================================
-- Test: Subqueries and CTEs with C collation
-- ======================================================================

-- Subquery preserves C collation
SELECT * FROM (
    SELECT name, val FROM t_c_collation ORDER BY name
) sub
ORDER BY name;

-- CTE with C collation
WITH ranked AS (
    SELECT name, val, row_number() OVER (ORDER BY name) as rn
    FROM t_c_collation
)
SELECT * FROM ranked ORDER BY rn;

-- Correlated subquery
SELECT c.name, c.val
FROM t_c_collation c
WHERE c.name = (SELECT min(name) FROM t_c_collation WHERE val = c.val)
ORDER BY c.name;

-- ======================================================================
-- Test: UNION / INTERSECT / EXCEPT with C collation
-- ======================================================================

SELECT name FROM t_c_collation WHERE name < 'a'
UNION ALL
SELECT name FROM t_c_collation WHERE name >= 'a'
ORDER BY name;

SELECT name FROM t_c_collation
INTERSECT
SELECT name FROM t_default_collation
ORDER BY name;

-- ======================================================================
-- Test: DISTINCT C collation column
-- ======================================================================

-- Under C collation, 'abc' and 'ABC' are distinct
SELECT DISTINCT name FROM t_c_collation ORDER BY name;

-- ======================================================================
-- Test: String functions with C collation
-- ======================================================================

SELECT name, length(name), upper(name), lower(name)
FROM t_c_collation ORDER BY name;

-- min/max aggregate should respect C collation
SELECT min(name), max(name) FROM t_c_collation;

-- string_agg with ORDER BY using C collation
SELECT string_agg(name, ',' ORDER BY name) FROM t_c_collation;

-- ======================================================================
-- Test: LIKE / pattern matching with C collation
-- ======================================================================

-- LIKE is byte-based under C collation
SELECT name FROM t_c_collation WHERE name LIKE 'a%' ORDER BY name;
SELECT name FROM t_c_collation WHERE name LIKE 'A%' ORDER BY name;

-- BETWEEN uses C collation ordering
-- Under C: 'D' < 'Z' < 'a', so BETWEEN 'A' AND 'Z' gets only uppercase
SELECT name FROM t_c_collation WHERE name BETWEEN 'A' AND 'Z' ORDER BY name;

-- ======================================================================
-- Test: Index scan with C collation
-- ======================================================================

CREATE INDEX idx_c_name ON t_c_collation (name);
ANALYZE t_c_collation;

-- Index scan should respect C collation ordering
SET enable_seqscan TO off;
SELECT name FROM t_c_collation WHERE name > 'Z' ORDER BY name;
SELECT name FROM t_c_collation WHERE name <= 'Z' ORDER BY name;
RESET enable_seqscan;

DROP INDEX idx_c_name;

-- ======================================================================
-- Test: CASE expression with C collation comparison
-- ======================================================================

SELECT name,
       CASE WHEN name < 'a' THEN 'uppercase' ELSE 'lowercase' END as case_type
FROM t_c_collation
ORDER BY name;

-- ======================================================================
-- Test: Aggregate functions with C collation
-- ======================================================================

-- count with GROUP BY preserves C collation grouping
SELECT name, count(*), sum(id)
FROM t_c_collation GROUP BY name ORDER BY name;

-- array_agg with ORDER BY should use C collation
SELECT array_agg(name ORDER BY name) FROM t_c_collation;

-- min/max on varchar(50) COLLATE "C" column
SELECT min(val), max(val) FROM t_c_collation;

-- ======================================================================
-- Test: LIMIT / OFFSET with C collation ORDER BY
-- ======================================================================

SELECT name FROM t_c_collation ORDER BY name LIMIT 3;
SELECT name FROM t_c_collation ORDER BY name LIMIT 3 OFFSET 3;

-- ======================================================================
-- Test: EXCEPT with C collation
-- ======================================================================

-- All uppercase names (< 'a' under C) except DEF
SELECT name FROM t_c_collation WHERE name < 'a'
EXCEPT
SELECT name FROM t_c_collation WHERE name = 'DEF'
ORDER BY name;

-- ======================================================================
-- Test: INSERT INTO ... SELECT preserves C collation
-- ======================================================================

CREATE TABLE t_c_copy (id int, name text COLLATE "C") DISTRIBUTED BY (id);
INSERT INTO t_c_copy SELECT id, name FROM t_c_collation;
SELECT name FROM t_c_copy ORDER BY name;
DROP TABLE t_c_copy;

-- ======================================================================
-- Test: CTAS with C collation column
-- ======================================================================

CREATE TABLE t_c_ctas AS SELECT id, name FROM t_c_collation DISTRIBUTED BY (id);
-- Verify the new table inherits C collation
SELECT name FROM t_c_ctas ORDER BY name;
DROP TABLE t_c_ctas;

-- ======================================================================
-- Test: Multiple aggregates in same query
-- ======================================================================

SELECT min(name), max(name), min(val), max(val),
       count(DISTINCT name)
FROM t_c_collation;

-- ======================================================================
-- Test: Window functions with C collation ordering
-- ======================================================================

-- lag/lead should follow C collation order
SELECT name,
       lag(name) OVER (ORDER BY name) as prev_name,
       lead(name) OVER (ORDER BY name) as next_name
FROM t_c_collation
ORDER BY name;

-- ntile with C collation partitioning
SELECT name,
       ntile(2) OVER (ORDER BY name) as bucket
FROM t_c_collation
ORDER BY name;

-- ======================================================================
-- Test: Nested subquery with C collation
-- ======================================================================

SELECT name FROM t_c_collation
WHERE name IN (SELECT name FROM t_c_collation WHERE name < 'a')
ORDER BY name;

-- Scalar subquery with min/max on C collation
SELECT name,
       (SELECT min(b.name) FROM t_c_collation b WHERE b.name > a.name) as next_min
FROM t_c_collation a
ORDER BY name;

-- ======================================================================
-- Test: UPDATE/DELETE with C collation WHERE clause
-- ======================================================================

CREATE TABLE t_c_dml (id int, name text COLLATE "C") DISTRIBUTED BY (id);
INSERT INTO t_c_dml SELECT id, name FROM t_c_collation;

-- DELETE rows where name < 'a' (uppercase under C collation)
DELETE FROM t_c_dml WHERE name < 'a';
SELECT name FROM t_c_dml ORDER BY name;

-- Re-insert and UPDATE
INSERT INTO t_c_dml SELECT id, name FROM t_c_collation WHERE name < 'a';
UPDATE t_c_dml SET name = name || '_updated' WHERE name < 'a';
SELECT name FROM t_c_dml ORDER BY name;

DROP TABLE t_c_dml;

-- ======================================================================
-- Cleanup
-- ======================================================================

RESET optimizer_trace_fallback;
RESET optimizer;
DROP SCHEMA collate_orca CASCADE;
