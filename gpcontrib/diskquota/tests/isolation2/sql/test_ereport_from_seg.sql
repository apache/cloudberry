CREATE SCHEMA efs1;
SELECT diskquota.set_schema_quota('efs1', '1MB');
CREATE TABLE efs1.t(i int);

INSERT INTO efs1.t SELECT generate_series(1, 10000);
-- wait for refresh of diskquota and check the quota size
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name, quota_in_mb, nspsize_in_bytes FROM diskquota.show_fast_schema_quota_view WHERE schema_name = 'efs1';

-- Enable check quota by relfilenode on seg0.
SELECT gp_inject_fault_infinite('ereport_warning_from_segment', 'skip', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO efs1.t SELECT generate_series(1, 10000);

-- wait for refresh of diskquota and check whether the quota size changes
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name, quota_in_mb, nspsize_in_bytes FROM diskquota.show_fast_schema_quota_view WHERE schema_name = 'efs1';

DROP TABLE efs1.t;
DROP SCHEMA efs1;

-- Reset fault injection points set by us at the top of this test.
SELECT gp_inject_fault_infinite('ereport_warning_from_segment', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
