-- Create new schema for running tests.
CREATE SCHEMA s_appendonly;
SET search_path TO s_appendonly;

CREATE TABLE t_ao(i int) WITH (appendonly=true) DISTRIBUTED BY (i);
CREATE TABLE t_aoco(i int) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
-- Create an index on t_ao so that there will be pg_aoblkdir_XXX relations.
CREATE INDEX index_t ON t_ao(i);
CREATE INDEX index_t2 ON t_aoco(i);

-- 1. Show that the relation's size in diskquota.table_size
--    is identical to the result of pg_table_size().
INSERT INTO t_ao SELECT generate_series(1, 100);
INSERT INTO t_aoco SELECT generate_series(1, 100);

SELECT diskquota.wait_for_worker_new_epoch();

-- Query the size of t_ao.
SELECT tableid::regclass, size
  FROM diskquota.table_size
  WHERE tableid=(SELECT oid FROM pg_class WHERE relname='t_ao') and segid=-1;

SELECT pg_table_size('t_ao');

-- Query the size of t_aoco.
SELECT tableid::regclass, size
  FROM diskquota.table_size
  WHERE tableid=(SELECT oid FROM pg_class WHERE relname='t_aoco') and segid=-1;

SELECT pg_table_size('t_aoco');

-- 2. Test that we are able to perform quota limit on appendonly tables.
SELECT diskquota.set_schema_quota('s_appendonly', '1 MB');
-- expect success.
INSERT INTO t_ao SELECT generate_series(1, 1000);

SELECT diskquota.wait_for_worker_new_epoch();

-- expect fail.
INSERT INTO t_ao SELECT generate_series(1, 10);
INSERT INTO t_aoco SELECT generate_series(1, 10);

DROP TABLE t_ao;
DROP TABLE t_aoco;

SET search_path TO DEFAULT;
DROP SCHEMA s_appendonly;
