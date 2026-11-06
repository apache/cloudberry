-- Result projection folding must not orphan a SubPlan's Motion slices.
SET optimizer = on;
CREATE TEMP TABLE motion_l(id int) DISTRIBUTED BY (id);
CREATE TEMP TABLE motion_r(id int, lid int, k int) DISTRIBUTED BY (id);
CREATE TEMP TABLE motion_s(id int, k int) DISTRIBUTED BY (id);
INSERT INTO motion_l VALUES (1), (2), (3), (4);
INSERT INTO motion_r VALUES (10,1,7), (11,1,NULL), (12,3,8), (13,5,NULL);
INSERT INTO motion_s VALUES (100,7), (101,7), (102,8), (103,NULL), (104,9);
ANALYZE motion_l;
ANALYZE motion_r;
ANALYZE motion_s;

-- The count SubPlan is unused by EXISTS, but its slice has already been
-- allocated. EXPLAIN must still reach it after Result projection folding.
EXPLAIN (COSTS OFF)
SELECT l.id, r.id
FROM motion_l l LEFT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k LIMIT 1);

SET optimizer = off;
SELECT l.id, r.id
FROM motion_l l LEFT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k LIMIT 1)
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id
FROM motion_l l LEFT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k LIMIT 1)
ORDER BY 1, 2;

-- NOT EXISTS and a positive OFFSET: only k = 7 has a second matching row.
-- Include both real NULL keys and NULL-extended rows from a FULL JOIN.
SET optimizer = off;
SELECT l.id, r.id
FROM motion_l l FULL JOIN motion_r r ON l.id = r.lid
WHERE NOT EXISTS (SELECT (SELECT count(*) FROM motion_s)
                  FROM motion_s s WHERE s.k = r.k LIMIT 1 OFFSET 1)
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id
FROM motion_l l FULL JOIN motion_r r ON l.id = r.lid
WHERE NOT EXISTS (SELECT (SELECT count(*) FROM motion_s)
                  FROM motion_s s WHERE s.k = r.k LIMIT 1 OFFSET 1)
ORDER BY 1, 2;

-- A positive OFFSET must also preserve a nonempty EXISTS result.
SET optimizer = off;
SELECT l.id, r.id
FROM motion_l l FULL JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2)
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id
FROM motion_l l FULL JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE r.id IS NULL LIMIT 2 OFFSET 2)
ORDER BY 1, 2;

-- LIMIT 0 still makes EXISTS false, even with a scalar aggregate target.
SET optimizer = off;
SELECT l.id, r.id
FROM motion_l l LEFT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k LIMIT 0)
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id
FROM motion_l l LEFT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k LIMIT 0)
ORDER BY 1, 2;

-- Keep every unused SubPlan when the discarded projection owns several.
-- NULL LIMIT/OFFSET are neutral; matching NULL keys must remain visible.
SET optimizer = off;
SELECT l.id, r.id
FROM motion_l l RIGHT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s),
                     (SELECT max(id) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k
              LIMIT NULL OFFSET NULL)
ORDER BY 1, 2;

SET optimizer = on;
SELECT l.id, r.id
FROM motion_l l RIGHT JOIN motion_r r ON l.id = r.lid
WHERE EXISTS (SELECT (SELECT count(*) FROM motion_s),
                     (SELECT max(id) FROM motion_s)
              FROM motion_s s WHERE s.k IS NOT DISTINCT FROM r.k
              LIMIT NULL OFFSET NULL)
ORDER BY 1, 2;

DROP TABLE motion_l, motion_r, motion_s;
RESET optimizer;
