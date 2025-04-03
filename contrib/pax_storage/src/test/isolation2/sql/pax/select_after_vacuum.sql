-- @Description Ensures that a select after a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
DROP TABLE IF EXISTS pax_tbl2;
CREATE TABLE pax_tbl2 (a INT);
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
insert into pax_tbl2 select generate_series(1,1000);

-- The actual test begins
DELETE FROM pax_tbl WHERE a < 128;
1: BEGIN;
1: SELECT COUNT(*) FROM pax_tbl2;
0: SELECT ptblockname, case when pttupcount = 0 then 'zero' when pttupcount = 1 then 'one' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('pax_tbl');
2: VACUUM pax_tbl;
1: SELECT COUNT(*) FROM pax_tbl;
1: COMMIT;
1: SELECT COUNT(*) FROM pax_tbl;
3: INSERT INTO pax_tbl VALUES (0);
0: SELECT ptblockname, case when pttupcount = 0 then 'zero' when pttupcount = 1 then 'one' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('pax_tbl');
0: SELECT * FROM get_pax_aux_table_all('pax_tbl');
