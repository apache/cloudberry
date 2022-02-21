-- Test various race conditions for TRUNCATE.

-- Case 1: Pulling active table before swapping relfilenode
CREATE TABLE dummy_t1(i int) DISTRIBUTED BY (i);
INSERT INTO dummy_t1 SELECT generate_series(1, 1000);
-- Wait for the diskquota bgworker refreshing the size of 'dummy_t1'.
SELECT diskquota.wait_for_worker_new_epoch();
-- Shows that the result of pg_table_size() and diskquota.table_size are identical.
SELECT pg_table_size('dummy_t1');
SELECT tableid::regclass, size, segid FROM diskquota.table_size
  WHERE tableid='dummy_t1'::regclass ORDER BY segid;

SELECT gp_inject_fault_infinite('diskquota_after_smgrcreate', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content<>-1;
SELECT diskquota.wait_for_worker_new_epoch();
1&: TRUNCATE dummy_t1;
-- Wait for the diskquota bgworker 'consumes' the newly created relfilenode from shmem.
SELECT diskquota.wait_for_worker_new_epoch();
SELECT gp_inject_fault_infinite('diskquota_after_smgrcreate', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content<>-1;
1<:

-- Wait for the diskquota bgworker refreshing the size of 'dummy_t1'.
SELECT diskquota.wait_for_worker_new_epoch();
-- Shows that the result of pg_table_size() and diskquota.table_size are identical.
SELECT pg_table_size('dummy_t1');
SELECT tableid::regclass, size, segid FROM diskquota.table_size
  WHERE tableid='dummy_t1'::regclass ORDER BY segid;
DROP TABLE dummy_t1;
