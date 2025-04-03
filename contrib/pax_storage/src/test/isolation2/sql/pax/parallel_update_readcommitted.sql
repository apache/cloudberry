-- @Description Tests that a update operation in progress will block all other updates
-- until the transaction is committed.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,10) AS i;

-- The actual test begins
1: BEGIN;
2: BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
1: UPDATE pax_tbl SET b = 42 WHERE b = 1;
2&: UPDATE pax_tbl SET b = -1 WHERE b = 1;
1: COMMIT;
2<:
2: COMMIT;
SELECT * FROM pax_tbl WHERE b < 2;
