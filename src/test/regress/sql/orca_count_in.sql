-- Correlated IN/ANY over a global count must compare against zero when
-- the inner input is empty, including after decorrelation into GROUP BY.
SET optimizer = on;
SET optimizer_trace_fallback = on;
CREATE TABLE count_in_outer (a int, d int) DISTRIBUTED BY (d);
CREATE TABLE count_in_inner (a int, b int) DISTRIBUTED BY (a);
INSERT INTO count_in_outer VALUES (0,0), (2,1), (1,2), (3,3),
    (NULL,0), (0,NULL), (NULL,NULL);
INSERT INTO count_in_inner VALUES (1,10), (1,20), (2,NULL), (NULL,30);
ANALYZE count_in_outer;
ANALYZE count_in_inner;

SELECT a, d FROM count_in_outer o
WHERE a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d)
ORDER BY d, a;

-- The same comparison in value context must retain SQL NULL semantics.
SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d) AS matches,
    a NOT IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d) AS differs
FROM count_in_outer o ORDER BY d, a;

SELECT a, d FROM count_in_outer o
WHERE a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d) OR d = 3
ORDER BY d, a;

-- count(expression) and DISTINCT also return zero on empty input.
SELECT a, d,
    0 IN (SELECT count(b) FROM count_in_inner i WHERE i.a = o.d) AS all_null,
    a = ANY (SELECT count(DISTINCT b) FROM count_in_inner i WHERE i.a = o.d) AS distinct_count
FROM count_in_outer o ORDER BY d, a;

-- Reuse the scalar count projection fix-up before evaluating the comparison.
SELECT a, d FROM count_in_outer o
WHERE a + 1 IN (SELECT count(*) + 1 FROM count_in_inner i WHERE i.a = o.d)
ORDER BY d, a;

SELECT a, d FROM count_in_outer o
WHERE a::text IN (SELECT count(*)::text FROM count_in_inner i WHERE i.a = o.d)
ORDER BY d, a;

SELECT a, d, a < ANY (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d)
FROM count_in_outer o ORDER BY d, a;

-- Process subqueries on the left of the comparison in the outer context.
SELECT a, d FROM count_in_outer o
WHERE (SELECT o.a) IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d)
ORDER BY d, a;

SELECT a, d,
    (SELECT b FROM count_in_inner i WHERE i.a = o.d AND b < 0) IN
    (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d) AS empty_lhs
FROM count_in_outer o ORDER BY d, a;

SELECT a, d FROM count_in_outer o
WHERE (o.a IN (SELECT i.b FROM count_in_inner i)) IN
    (SELECT count(*) = 0 FROM count_in_inner i WHERE i.a = o.d)
ORDER BY d, a;

-- HAVING can remove the single aggregate row: ANY(empty) is false even
-- for a NULL left operand. It must not become a comparison against zero.
SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner i
          WHERE i.a = o.d HAVING count(*) > 0) AS nonempty,
    a IN (SELECT count(*) FROM count_in_inner i
          WHERE i.a = o.d HAVING count(*) < 2) AS small
FROM count_in_outer o ORDER BY d, a;

SELECT a, d FROM count_in_outer o
WHERE a IN (SELECT count(*) FROM count_in_inner i
            WHERE i.a = o.d HAVING count(*) < 2)
ORDER BY d, a;

SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner i
          GROUP BY () HAVING o.d = 0) AS correlated_having
FROM count_in_outer o ORDER BY d, a;

-- Explicit grouping and LIMIT/OFFSET can also yield an empty set.
SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d GROUP BY i.a) AS grouped,
    a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d LIMIT 0) AS limited,
    a IN (SELECT count(*) FROM count_in_inner i WHERE i.a = o.d OFFSET 1) AS skipped
FROM count_in_outer o ORDER BY d, a;

-- Unsupported projections still require the original aggregate semantics.
SELECT a, d,
    0 IN (SELECT count(*) + COALESCE(sum(b),0)
          FROM count_in_inner i WHERE i.a = o.d) AS mixed_aggregates,
    a IN (SELECT count(*) + (SELECT max(b) FROM count_in_inner)
          FROM count_in_inner i WHERE i.a = o.d) AS nested_subquery
FROM count_in_outer o ORDER BY d, a;

SELECT a, d,
    0 IN (SELECT count(*) FROM
          (SELECT count(*) AS n FROM count_in_inner i WHERE i.a = o.d) q
          WHERE q.n > 1) AS nested_count
FROM count_in_outer o ORDER BY d, a;

-- A non-equality correlation cannot be pulled through GROUP BY.
SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner i WHERE i.a < o.d) AS inequality
FROM count_in_outer o ORDER BY d, a;

-- Uncorrelated and multirow subqueries retain their existing behavior.
SELECT a, d,
    a IN (SELECT count(*) FROM count_in_inner) AS uncorrelated,
    a IN (SELECT count(*) FROM count_in_inner GROUP BY a) AS multiple_groups
FROM count_in_outer o ORDER BY d, a;

CREATE TABLE count_in_empty (a int, d int) DISTRIBUTED BY (d);
SELECT a FROM count_in_empty o
WHERE a IN (SELECT 100 / count(*) FROM count_in_inner i WHERE i.a = o.d);
DROP TABLE count_in_empty;

-- Enough groups to choose decorrelation: the inner table should be read
-- once and the missing group should still compare against zero.
CREATE TABLE count_in_many_outer (a int, d int) DISTRIBUTED BY (d);
CREATE TABLE count_in_many_inner (a int) DISTRIBUTED BY (a);
INSERT INTO count_in_many_outer
SELECT CASE WHEN g % 1001 = 1000 THEN 0 ELSE 10 END, g % 1001
FROM generate_series(0,2001) g;
INSERT INTO count_in_many_inner SELECT g % 1000 FROM generate_series(1,10000) g;
ANALYZE count_in_many_outer;
ANALYZE count_in_many_inner;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM count_in_many_outer o
WHERE a IN (SELECT count(*) FROM count_in_many_inner i WHERE i.a = o.d);
SELECT count(*) FROM count_in_many_outer o
WHERE a IN (SELECT count(*) FROM count_in_many_inner i WHERE i.a = o.d);

DROP TABLE count_in_many_inner, count_in_many_outer;
DROP TABLE count_in_inner, count_in_outer;
RESET optimizer_trace_fallback;
RESET optimizer;
