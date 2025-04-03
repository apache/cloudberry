-- @Description Ensures that a select during a delete operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1,100);

2: BEGIN;
2: SELECT * FROM pax_tbl WHERE a < 5 ORDER BY a;
2: DELETE FROM pax_tbl WHERE a < 5;
1: SELECT * FROM pax_tbl WHERE a >= 5 AND a < 10 ORDER BY a;
3: SELECT * FROM pax_tbl WHERE a < 5 ORDER BY a;
2: COMMIT;
2: SELECT * FROM pax_tbl WHERE a < 5 ORDER BY a;
4: SELECT * FROM pax_tbl WHERE a < 10 ORDER BY a;
4: INSERT INTO pax_tbl VALUES (0);
