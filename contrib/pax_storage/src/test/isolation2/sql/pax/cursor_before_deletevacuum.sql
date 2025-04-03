-- @Description Tests the visibility when a cursor has been created before the delete.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1, 10);

1: BEGIN;
1: DECLARE cur CURSOR FOR SELECT a FROM pax_tbl ORDER BY a;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
2: BEGIN;
2: DELETE FROM pax_tbl WHERE a < 5;
2: COMMIT;
2&: VACUUM FULL pax_tbl;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: CLOSE cur;
1: COMMIT;
2<:
3: BEGIN;
3: DECLARE cur CURSOR FOR SELECT a FROM pax_tbl ORDER BY a;
3: FETCH NEXT IN cur;
3: CLOSE cur;
3: COMMIT;
