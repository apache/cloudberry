\! gpconfig -c shared_preload_libraries -v '' > /dev/null 
\! gpstop -far > /dev/null
\c

CREATE ROLE test SUPERUSER;

SET ROLE test;

-- Create table with diskquota disabled
CREATE TABLE t_without_diskquota (i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

\! gpconfig -c shared_preload_libraries -v $(./data/current_binary_name) > /dev/null
\! gpstop -far > /dev/null
\c

SET ROLE test;

-- Init table_size to include the table
SELECT diskquota.init_table_size_table();

-- Restart to load diskquota.table_size to the memory.
\! gpstop -far > /dev/null
\c
SET ROLE test;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size
WHERE tableid = 't_without_diskquota'::regclass ORDER BY segid;

-- Ensure that the table is not active
SELECT diskquota.diskquota_fetch_table_stat(0, ARRAY[]::oid[])
FROM gp_dist_random('gp_id');

SELECT diskquota.set_role_quota(current_role, '1MB');

SELECT diskquota.wait_for_worker_new_epoch();

-- Expect that current role is in the rejectmap 
SELECT rolname FROM pg_authid, diskquota.rejectmap WHERE oid = target_oid;

SELECT diskquota.set_role_quota(current_role, '-1');

SELECT diskquota.wait_for_worker_new_epoch();

SELECT rolname FROM pg_authid, diskquota.rejectmap WHERE oid = target_oid;

DROP TABLE t_without_diskquota;

RESET ROLE;

DROP ROLE test;
