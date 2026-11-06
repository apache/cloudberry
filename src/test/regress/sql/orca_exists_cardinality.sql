-- An EXISTS input can return many rows even when its target is a scalar
-- aggregate subquery. Failed decorrelation must not make that input scalar.
SET optimizer = on;
CREATE TEMP TABLE card_l(id int) DISTRIBUTED BY (id);
CREATE TEMP TABLE card_r(id int, lid int, k int) DISTRIBUTED BY (id);
CREATE TEMP TABLE card_s(id int, k int) DISTRIBUTED BY (id);
INSERT INTO card_l VALUES (1), (2), (3), (4);
INSERT INTO card_r VALUES (10,1,7), (11,1,NULL), (12,3,8), (13,5,NULL);
INSERT INTO card_s VALUES (100,7), (101,7), (102,8), (103,NULL), (104,9);
ANALYZE card_l;
ANALYZE card_r;
ANALYZE card_s;

-- The FULL JOIN includes actual NULLs and NULL-extended rows on both sides.
-- There are two inner matches for k = 7. NULL LIMIT/OFFSET are neutral.
EXPLAIN (COSTS OFF)
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT NULL OFFSET NULL)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid;

SET optimizer = off;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT NULL OFFSET NULL)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT NULL OFFSET NULL)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

SET optimizer = off;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k IS NOT DISTINCT FROM r.k
               LIMIT NULL OFFSET NULL)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k IS NOT DISTINCT FROM r.k
               LIMIT NULL OFFSET NULL)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

-- A nullable outer reference can select all five inner rows. After OFFSET
-- there are still two rows, which is valid for EXISTS and NOT EXISTS.
SET optimizer = off;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2),
       NOT EXISTS (SELECT (SELECT count(*) FROM card_s)
                   FROM card_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2),
       NOT EXISTS (SELECT (SELECT count(*) FROM card_s)
                   FROM card_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

-- Preserve empty inputs and the boundary where OFFSET removes the last row.
SET optimizer = off;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT 0),
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT 1 OFFSET 1)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id,
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT 0),
       EXISTS (SELECT (SELECT count(*) FROM card_s)
               FROM card_s s WHERE s.k = r.k LIMIT 1 OFFSET 1)
FROM card_l l FULL JOIN card_r r ON l.id = r.lid
ORDER BY 1, 2;

-- Quantified subqueries share the outer-Apply transformation. Preserve
-- their multirow behavior, including UNKNOWN for the matching NULL key.
SET optimizer = off;
SELECT r.id,
       r.k = ANY (SELECT s.k FROM card_s s
                  WHERE s.k IS NOT DISTINCT FROM r.k LIMIT NULL OFFSET NULL),
       r.k = ALL (SELECT s.k FROM card_s s
                  WHERE s.k IS NOT DISTINCT FROM r.k LIMIT NULL OFFSET NULL)
FROM card_r r ORDER BY 1;

SET optimizer = on;
SELECT r.id,
       r.k = ANY (SELECT s.k FROM card_s s
                  WHERE s.k IS NOT DISTINCT FROM r.k LIMIT NULL OFFSET NULL),
       r.k = ALL (SELECT s.k FROM card_s s
                  WHERE s.k IS NOT DISTINCT FROM r.k LIMIT NULL OFFSET NULL)
FROM card_r r ORDER BY 1;

-- Genuine scalar subqueries with at most one row remain valid, including
-- an aggregate over empty input and a correlated top-1 query.
SET optimizer = off;
SELECT r.id,
       (SELECT count(*) FROM card_s s WHERE s.k = r.k),
       (SELECT s.id FROM card_s s WHERE s.k = r.k ORDER BY s.id LIMIT 1)
FROM card_r r ORDER BY 1;

SET optimizer = on;
SELECT r.id,
       (SELECT count(*) FROM card_s s WHERE s.k = r.k),
       (SELECT s.id FROM card_s s WHERE s.k = r.k ORDER BY s.id LIMIT 1)
FROM card_r r ORDER BY 1;

-- An aggregate at the EXISTS level still produces one row for empty input.
SET optimizer = off;
SELECT r.id, EXISTS (SELECT count(*) FROM card_s s WHERE s.k = -1)
FROM card_r r ORDER BY 1;

SET optimizer = on;
SELECT r.id, EXISTS (SELECT count(*) FROM card_s s WHERE s.k = -1)
FROM card_r r ORDER BY 1;

-- Do not suppress a real scalar cardinality violation.
SET optimizer = off;
DO $$
BEGIN
    PERFORM (SELECT s.id FROM card_s s WHERE s.k = r.k LIMIT 2)
    FROM card_r r WHERE r.id = 10;
    RAISE EXCEPTION 'missing scalar cardinality violation';
EXCEPTION WHEN cardinality_violation THEN
    RAISE NOTICE 'expected scalar cardinality violation';
END
$$;

SET optimizer = on;
DO $$
BEGIN
    PERFORM (SELECT s.id FROM card_s s WHERE s.k = r.k LIMIT 2)
    FROM card_r r WHERE r.id = 10;
    RAISE EXCEPTION 'missing scalar cardinality violation';
EXCEPTION WHEN cardinality_violation THEN
    RAISE NOTICE 'expected scalar cardinality violation';
END
$$;

DROP TABLE card_l, card_r, card_s;
RESET optimizer;
