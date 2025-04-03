-- @Description Ensures that a serializable select before during a vacuum operation blocks the vacuum.
-- 
DROP TABLE IF EXISTS pax_tbl;
DROP TABLE IF EXISTS pax_tbl2;
CREATE TABLE pax_tbl (a INT);
CREATE TABLE pax_tbl2 (a INT);
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
insert into pax_tbl2 select generate_series(1,1000);

DELETE FROM pax_tbl WHERE a < 128;
1: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
1: SELECT COUNT(*) FROM pax_tbl2;
2: VACUUM pax_tbl;
1: SELECT COUNT(*) FROM pax_tbl;
1: COMMIT;
3: INSERT INTO pax_tbl VALUES (0);
