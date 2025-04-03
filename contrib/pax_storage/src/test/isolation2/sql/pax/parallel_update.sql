-- @Description Tests that a update operation in progress will block all other updates
-- until the transaction is committed.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,10) AS i;

-- The actual test begins
1: BEGIN;
2: BEGIN;
2: UPDATE pax_tbl SET b = 42 WHERE a = 2;
-- The case here should update a tuple at the same seg with(2).
-- Under jump hash, (2) and (3) are on the same seg(seg0).
1&: UPDATE pax_tbl SET b = 42 WHERE a = 3;
2: COMMIT;
1<:
1: COMMIT;
3: SELECT * FROM pax_tbl WHERE a < 5 ORDER BY a;
