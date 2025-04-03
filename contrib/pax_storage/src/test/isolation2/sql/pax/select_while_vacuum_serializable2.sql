-- @Description Run vacuum, while a serializable transaction is holding a lock
-- on the table.
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
1: BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
1: SELECT COUNT(*) FROM pax_tbl;
2: SELECT segment_id, ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
2: VACUUM pax_tbl;
1: SELECT COUNT(*) FROM pax_tbl;
1: COMMIT;
3: INSERT INTO pax_tbl VALUES (0);
2: SELECT segment_id, ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
