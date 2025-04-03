-- @Description Tests the basic phantom read behavior of GPDB with serializable.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

1: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
1: SELECT * FROM pax_tbl WHERE b BETWEEN 20 AND 30 ORDER BY a;
2: BEGIN;
2: INSERT INTO pax_tbl VALUES (101, 25);
2: COMMIT;
1: SELECT * FROM pax_tbl WHERE b BETWEEN 20 AND 30 ORDER BY a;
1: COMMIT;
