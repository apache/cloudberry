-- @Description Ensures that an update before a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

DELETE FROM pax_tbl WHERE a < 12;
1: BEGIN;
1: SELECT COUNT(*) FROM pax_tbl;
1>: UPDATE pax_tbl SET b=1 WHERE a > 0;COMMIT;
2: VACUUM pax_tbl;
1<:
1: SELECT COUNT(*) FROM pax_tbl;
3: INSERT INTO pax_tbl VALUES (0);
