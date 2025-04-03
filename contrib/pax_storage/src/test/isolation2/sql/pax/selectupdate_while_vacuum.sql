-- @Description Ensures that an update during a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,1000) AS i;

DELETE FROM pax_tbl WHERE a < 128;
4: BEGIN;
4: SELECT COUNT(*) FROM pax_tbl;
5: BEGIN;
4: SELECT COUNT(*) FROM pax_tbl;
4: BEGIN;
4: SELECT COUNT(*) FROM pax_tbl;
2>: VACUUM pax_tbl;
4: SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;BEGIN;UPDATE pax_tbl SET b=1 WHERE a > 500;UPDATE pax_tbl SET b=1 WHERE a > 400;COMMIT;
2<:
3: SELECT COUNT(*) FROM pax_tbl WHERE b = 1;
3: INSERT INTO pax_tbl VALUES (0);
