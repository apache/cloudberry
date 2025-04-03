-- @Description Tests the behavior of full vacuum w.r.t. the pg_class statistics
-- 
DROP TABLE IF EXISTS foo;

CREATE TABLE foo (a INT, b INT, c CHAR(128)) DISTRIBUTED BY (a);
CREATE INDEX foo_index ON foo(b);
INSERT INTO foo SELECT i as a, i as b, 'hello world' as c FROM generate_series(1, 50) AS i;
INSERT INTO foo SELECT i as a, i as b, 'hello world' as c FROM generate_series(51, 100) AS i;
ANALYZE foo;

-- ensure that the scan go through the index
SET enable_seqscan=false;
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo';
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo_index';
DELETE FROM foo WHERE a < 16;
VACUUM FULL foo;
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo';
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo_index';
-- in case there's autovacuum worker running in the backend, the aoseg will not be dropped which has state = 2
SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('foo') where pttupcount > 0;
