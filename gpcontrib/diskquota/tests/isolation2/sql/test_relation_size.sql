--
-- 1. Test that when a relation is dropped before diskquota.relation_size()
--    applying stat(2) on the physical file, diskquota.relation_size() consumes
--    the error and returns 0.
--

CREATE TABLE t_dummy_rel(i int);
-- Insert a small amount of data to 't_dummy_rel'.
INSERT INTO t_dummy_rel SELECT generate_series(1, 100);
-- Shows that the size of relfilenode is not zero.
SELECT diskquota.relation_size('t_dummy_rel', false);

-- Inject 'suspension' to servers.
SELECT gp_inject_fault_infinite('diskquota_before_stat_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p';

-- Session 1 will hang before applying stat(2) to the physical file.
1&: SELECT diskquota.relation_size('t_dummy_rel', false);
-- Drop the table.
DROP TABLE t_dummy_rel;
-- Remove the injected 'suspension'.
SELECT gp_inject_fault_infinite('diskquota_before_stat_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p';
-- Session 1 will continue and returns 0.
1<:
