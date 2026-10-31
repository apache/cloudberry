-- @Description Concurrent changes while a parallel scan of an AOCS table is in
-- progress.
--
-- The scan's processes are suspended at executor_pre_tuple_processed: the
-- chunk layout is in shared memory and every process has its own copy, but no
-- row has been read yet.  Another session then changes the table and commits.
-- The suspended scan must return exactly the rows its snapshot sees.
--
-- All tables are distributed by a constant key, so their rows are on one
-- segment.  Each segment runs 4 parallel processes, so the fault is suspended
-- for the first 4 hits on every segment; later hits, such as the other
-- session's statements, pass.

CREATE TABLE aocs_pscan_insert (k int, a int, c text)
  USING ao_column WITH (parallel_workers = 4) DISTRIBUTED BY (k);
INSERT INTO aocs_pscan_insert SELECT 0, i, md5(i::text) FROM generate_series(1, 100000) i;
CREATE TABLE aocs_pscan_delete (k int, a int, c text)
  USING ao_column WITH (parallel_workers = 4) DISTRIBUTED BY (k);
INSERT INTO aocs_pscan_delete SELECT 0, i, md5(i::text) FROM generate_series(1, 100000) i;
CREATE TABLE aocs_pscan_vacuum (k int, a int, c text)
  USING ao_column WITH (parallel_workers = 4) DISTRIBUTED BY (k);
INSERT INTO aocs_pscan_vacuum SELECT 0, i, md5(i::text) FROM generate_series(1, 100000) i;
DELETE FROM aocs_pscan_vacuum WHERE a % 2 = 0;
CREATE TABLE aocs_pscan_snapshot (k int, a int, c text)
  USING ao_column WITH (parallel_workers = 4) DISTRIBUTED BY (k);
INSERT INTO aocs_pscan_snapshot SELECT 0, i, md5(i::text) FROM generate_series(1, 100000) i;

1: SET enable_parallel = on;
1: SET optimizer = off;
1: SET max_parallel_workers_per_gather = 4;
1: EXPLAIN (COSTS OFF) SELECT count(*), sum(a) FROM aocs_pscan_insert;

-- Concurrent insert: the new rows are appended to the segment file after
-- the scan's chunk layout was computed, so the scan does not see them.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', '', '', '', 1, 4, 0, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1&: SELECT count(*), sum(a) FROM aocs_pscan_insert;
SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 4, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
2: INSERT INTO aocs_pscan_insert SELECT 0, i, md5(i::text) FROM generate_series(100001, 150000) i;
SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1<:
1: SELECT count(*), sum(a) FROM aocs_pscan_insert;

-- Concurrent delete: the deleted rows are still visible to the scan's
-- snapshot.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', '', '', '', 1, 4, 0, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1&: SELECT count(*), sum(a) FROM aocs_pscan_delete;
SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 4, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
2: DELETE FROM aocs_pscan_delete WHERE a % 2 = 0;
SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1<:
1: SELECT count(*), sum(a) FROM aocs_pscan_delete;

-- Concurrent VACUUM: compaction moves the visible rows to a new segment file
-- and marks the old one awaiting drop.  It cannot drop the old one while the
-- scan's snapshot can still see it, and the scan reads the old one.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'suspend', '', '', '', 1, 4, 0, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1&: SELECT count(*), sum(a) FROM aocs_pscan_vacuum;
SELECT gp_wait_until_triggered_fault('executor_pre_tuple_processed', 4, dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
2: VACUUM aocs_pscan_vacuum;
-- The old segment file is awaiting drop (state 2), and the 50000 visible rows
-- are in a new one.
SELECT segno, tupcount, state FROM gp_toolkit.__gp_aocsseg('aocs_pscan_vacuum')
  WHERE column_num = 0 AND (tupcount > 0 OR state = 2) ORDER BY segno;
SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE content >= 0 AND role = 'p';
1<:
1: SELECT count(*), sum(a) FROM aocs_pscan_vacuum;
2: VACUUM aocs_pscan_vacuum;
1: SELECT count(*), sum(a) FROM aocs_pscan_vacuum;

-- A snapshot taken before a concurrent insert: the chunk layout is computed
-- from the scan's snapshot, so the new rows are not part of it.
1: BEGIN ISOLATION LEVEL REPEATABLE READ;
1: SELECT count(*) FROM aocs_pscan_snapshot;
2: INSERT INTO aocs_pscan_snapshot SELECT 0, i, md5(i::text) FROM generate_series(100001, 150000) i;
1: SELECT count(*), sum(a) FROM aocs_pscan_snapshot;
1: COMMIT;
1: SELECT count(*), sum(a) FROM aocs_pscan_snapshot;

-- An error in one of the parallel processes fails the query, and the next
-- parallel scan works.
SELECT gp_inject_fault('executor_pre_tuple_processed', 'error', '', '', '', 2, 2, 0, dbid)
  FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
1: SELECT count(*), sum(a) FROM aocs_pscan_insert;
SELECT gp_inject_fault('executor_pre_tuple_processed', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
1: SELECT count(*), sum(a) FROM aocs_pscan_insert;

1q:
2q:
DROP TABLE aocs_pscan_insert;
DROP TABLE aocs_pscan_delete;
DROP TABLE aocs_pscan_vacuum;
DROP TABLE aocs_pscan_snapshot;
