-- @Description Tests the basic behavior of (lazy) vacuum when called from utility mode
-- 
DROP TABLE IF EXISTS foo;
CREATE TABLE foo (a INT, b INT, c CHAR(128));
CREATE INDEX foo_index ON foo(b);
INSERT INTO foo SELECT i as a, 1 as b, 'hello world' as c FROM generate_series(1, 100) AS i;

DELETE FROM foo WHERE a < 20;
SELECT COUNT(*) FROM foo;
0U: SELECT segment_id, case when pttupcount = 0 then 'zero' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('foo');
0U: VACUUM foo;
SELECT COUNT(*) FROM foo;
0U: SELECT segment_id, case when pttupcount = 0 then 'zero' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('foo');
