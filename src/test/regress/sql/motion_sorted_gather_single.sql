-- A sorted GatherSingle can receive the result of a replicated DML CTE.
SET optimizer = off;
CREATE TABLE motion_sorted_gather_single (v int) DISTRIBUTED REPLICATED;
WITH ins AS (INSERT INTO motion_sorted_gather_single VALUES (1),(1) RETURNING v)
SELECT v FROM ins ORDER BY v;
-- An unsorted GatherSingle control.
WITH ins AS (INSERT INTO motion_sorted_gather_single VALUES (2),(2) RETURNING v)
SELECT v FROM ins;
DROP TABLE motion_sorted_gather_single;
RESET optimizer;
