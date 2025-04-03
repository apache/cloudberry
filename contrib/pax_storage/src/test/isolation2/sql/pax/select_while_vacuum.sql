-- @Description Ensures that a select during a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);

DELETE FROM pax_tbl WHERE a < 128;
1: BEGIN;
1>: SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;COMMIT;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;SELECT COUNT(*) FROM pax_tbl;COMMIT;
2: VACUUM pax_tbl;
1<:
3: INSERT INTO pax_tbl VALUES (0);
