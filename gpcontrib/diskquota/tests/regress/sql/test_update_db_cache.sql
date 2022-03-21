--start_ignore
CREATE DATABASE test_db_cache;
--end_ignore

\c test_db_cache
CREATE EXTENSION diskquota;

-- Wait until the db cache gets updated 
SELECT diskquota.wait_for_worker_new_epoch();

CREATE TABLE t(i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid
FROM diskquota.table_size
WHERE tableid = 't'::regclass
ORDER BY segid;

DROP EXTENSION diskquota;

-- Create table without extension
CREATE TABLE t_no_extension(i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;

-- Sleep until the worker adds the current db to cache so that it can be found 
-- when DROP EXTENSION.
-- FIXME: We cannot use wait_for_worker_new_epoch() here because 
-- diskquota.state is not clean. Change sleep() to wait() after removing
-- diskquota.state
SELECT pg_sleep(5);

-- Should find nothing since t_no_extension is not recorded.
SELECT diskquota.diskquota_fetch_table_stat(0, ARRAY[]::oid[])
FROM gp_dist_random('gp_id');

DROP TABLE t;
DROP TABLE t_no_extension;

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_db_cache;
