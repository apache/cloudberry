-- @Description Ensures that an insert during a vacuum operation is ok
-- 
CREATE TABLE selectinsert_while_vacuum_pax_tbl (a INT);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);
insert into selectinsert_while_vacuum_pax_tbl select generate_series(1,1000);

DELETE FROM selectinsert_while_vacuum_pax_tbl WHERE a < 128;
4: BEGIN;
4: SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl;
5: BEGIN;
4: SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl;
4: BEGIN;
4: SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl;
2>: VACUUM selectinsert_while_vacuum_pax_tbl;
4: SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl;SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl;BEGIN;insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);insert into selectinsert_while_vacuum_pax_tbl select generate_series(1001,2000);COMMIT;
2<:
3: SELECT COUNT(*) FROM selectinsert_while_vacuum_pax_tbl WHERE a = 1500;
3: INSERT INTO selectinsert_while_vacuum_pax_tbl VALUES (0);
