-- @Description Tests the basic phantom read behavior of GPDB w.r.t to delete using
-- the default isolation level.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

1: BEGIN;
1: SELECT * FROM pax_tbl WHERE b BETWEEN 20 AND 30 ORDER BY a;
2: BEGIN;
2: DELETE FROM pax_tbl where b = 25;
2: COMMIT;
1: SELECT * FROM pax_tbl WHERE b BETWEEN 20 AND 30 ORDER BY a;
1: COMMIT;
