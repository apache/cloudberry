-- @Description Ensures that a vacuum with serializable works ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

DELETE FROM pax_tbl WHERE a <= 30;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM pax_tbl;
SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
VACUUM pax_tbl;
-- in case there's autovacuum worker running in the backend, the pax_tbl will not be dropped which has state = 2
SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl') where pttupcount > 0;
SELECT COUNT(*) FROM pax_tbl;
