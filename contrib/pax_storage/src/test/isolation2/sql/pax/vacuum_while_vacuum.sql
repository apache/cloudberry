-- @Description Ensures that an vacuum while a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 10000) AS i;

DELETE FROM pax_tbl WHERE a < 1200;
1: SELECT COUNT(*) FROM pax_tbl;
1>: VACUUM pax_tbl;
2: VACUUM pax_tbl;
1<:
1: SELECT COUNT(*) FROM pax_tbl;
3: INSERT INTO pax_tbl VALUES (0);
