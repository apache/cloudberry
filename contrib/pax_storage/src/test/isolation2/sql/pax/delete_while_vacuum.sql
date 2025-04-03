-- @Description Ensures that a delete before a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;


DELETE FROM pax_tbl WHERE a < 12;
1: BEGIN;
1: SELECT COUNT(*) FROM pax_tbl;
1>: DELETE FROM pax_tbl WHERE a < 90;COMMIT;
2: VACUUM pax_tbl;
1<:
1: SELECT COUNT(*) FROM pax_tbl;
3: INSERT INTO pax_tbl VALUES (0);
