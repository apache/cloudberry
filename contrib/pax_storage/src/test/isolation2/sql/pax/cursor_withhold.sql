-- @Description Tests the visibility of an "with hold" cursor w.r.t. deletes.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1,100);

1: DECLARE cur CURSOR WITH HOLD FOR SELECT a FROM pax_tbl ORDER BY a;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
2: BEGIN;
2: DELETE FROM pax_tbl WHERE a < 5;
2: COMMIT;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: CLOSE cur;
3: DECLARE cur CURSOR WITH HOLD FOR SELECT a FROM pax_tbl ORDER BY a;
3: FETCH NEXT IN cur;
3: CLOSE cur;
