-- This file tests various race conditions when performing 'VACUUM FULL'.

-- 1. When the gpdb is performing 'VACUUM FULL' on some relation, it can be summarized
--    as the following 3 steps:
--    s1) create a new temporary relation (smgrcreate hook will be triggered, newly
--        created relfilenode will be put into shmem).
--    s2) insert data into the newly created relation from the old relation (smgrextend
--        hook will be triggered, newly created relfilenode will be put into shmem).
--    s3) change the old relation's relfilenode to the newly created one.
--    Consider the following situation:
--    If the diskquota bgworker pulls active oids before the 'VACUUM FULL' operation finishing,
--    the newly created relfilenode is translated to the newly created temporary relation's oid,
--    the old relation's size cannot be updated. We resolve it by making altered relations' oids
--    constantly active so that the diskquota bgworker keeps updating the altered relation size
--    during 'VACUUM FULL'.
CREATE TABLE dummy_t1(i int) DISTRIBUTED BY (i);
INSERT INTO dummy_t1 SELECT generate_series(1, 1000);
DELETE FROM dummy_t1;
-- Wait for the diskquota bgworker refreshing the size of 'dummy_t1'.
SELECT diskquota.wait_for_worker_new_epoch();
-- Shows that the result of pg_table_size() and diskquota.table_size are identical.
SELECT pg_table_size('dummy_t1');
SELECT tableid::regclass, size, segid FROM diskquota.table_size
  WHERE tableid='dummy_t1'::regclass ORDER BY segid;
SELECT gp_inject_fault_infinite('object_access_post_alter', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content<>-1;
SELECT diskquota.wait_for_worker_new_epoch();
1&: VACUUM FULL dummy_t1;
-- Wait for the diskquota bgworker 'consumes' the newly created relfilenode from shmem.
SELECT diskquota.wait_for_worker_new_epoch();
SELECT gp_inject_fault_infinite('object_access_post_alter', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content<>-1;
1<:

-- Wait for the diskquota bgworker refreshing the size of 'dummy_t1'.
SELECT diskquota.wait_for_worker_new_epoch();
-- Shows that the result of pg_table_size() and diskquota.table_size are identical.
SELECT pg_table_size('dummy_t1');
SELECT tableid::regclass, size, segid FROM diskquota.table_size
  WHERE tableid='dummy_t1'::regclass ORDER BY segid;
DROP TABLE dummy_t1;
