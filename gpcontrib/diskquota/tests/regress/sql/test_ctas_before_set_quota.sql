CREATE ROLE test SUPERUSER;

SET ROLE test;

CREATE TABLE t_before_set_quota (i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid FROM diskquota.table_size
WHERE tableid = 't_before_set_quota'::regclass ORDER BY segid;

-- Ensure that the table is not active
SELECT diskquota.diskquota_fetch_table_stat(0, ARRAY[]::oid[])
FROM gp_dist_random('gp_id');

SELECT diskquota.set_role_quota(current_role, '1MB');

SELECT diskquota.wait_for_worker_new_epoch();

-- Expect that current role is in the rejectmap 
SELECT rolname FROM pg_authid, diskquota.rejectmap WHERE oid = target_oid;

SELECT diskquota.set_role_quota(current_role, '-1');

SELECT diskquota.wait_for_worker_new_epoch();

DROP TABLE t_before_set_quota;

RESET ROLE;

DROP ROLE test;
