-- @Description Ensures that a vacuum with serializable works ok
-- 
DROP TABLE IF EXISTS pax_tbl;
DROP TABLE IF EXISTS pax_tbl2;
CREATE TABLE pax_tbl (a INT, b INT);
CREATE TABLE pax_tbl2 (a INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

DELETE FROM pax_tbl WHERE a <= 30;
1: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
1: SELECT COUNT(*) FROM pax_tbl;
1: BEGIN;
1: SELECT COUNT(*) FROM pax_tbl2;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
2: VACUUM pax_tbl;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
1: SELECT COUNT(*) FROM pax_tbl;
1: COMMIT;
