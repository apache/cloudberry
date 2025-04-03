-- @Description Tests the pax_tbl segment file selection policy
--
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
-- Case 1: Both transactions insert initial segment files into pax_tbl.
1: BEGIN;
2: BEGIN;
1: INSERT INTO pax_tbl VALUES (1);
-- Segment file 1 should be created
3: SELECT ptblockname FROM get_pax_aux_table_all('pax_tbl');
2: INSERT INTO pax_tbl VALUES (1);
-- Segment file 2 should be created
3: SELECT ptblockname FROM get_pax_aux_table_all('pax_tbl');
2: COMMIT;
-- Transaction 2 should commit before 1.  It validates that
-- transaction 2 chose a different segfile than transaction 1.
1: COMMIT;
3: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');

-- Case 2: Concurrent inserts with existing segment files in pax_tbl.
1: INSERT INTO pax_tbl VALUES (1);
3: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
-- Here we aim to insert a tuple to the same seg as (1).
-- Under jump jash, (15) and (1) are on the same seg(seg1).
1: INSERT INTO pax_tbl VALUES (15);
3: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
1: BEGIN;
1: INSERT INTO pax_tbl VALUES (15);
2: BEGIN;
2: INSERT INTO pax_tbl VALUES (15);
1: COMMIT;
2: COMMIT;
3: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
1: insert into pax_tbl select generate_series(1,100000);
1: INSERT INTO pax_tbl VALUES (15);
3: SELECT ptblockname, case when pttupcount = 0 then 'zero' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('pax_tbl');
