-- Test that a failure to create a new shared snapshot DSM segment in
-- updateSharedLocalSnapshot() does not leave a stale handle in the shared
-- snapshot slot.  Before the fix, the old segment was unpinned before
-- dsm_create(), so an error there (e.g. "No space left on device" on
-- /dev/shm) left the slot with the handle of an already destroyed segment.
-- The next growth of the snapshot then failed with "cannot unpin unknown
-- segment handle", and at backend exit SharedSnapshotRemove() raised it as
-- FATAL, which made the postmaster reset the whole cluster.

CREATE TABLE sharedsnapshot_dsm_t(a int) DISTRIBUTED BY (a);

-- Session 1 allocates its shared snapshot segment while the snapshot is small.
1: SELECT count(*) FROM sharedsnapshot_dsm_t;

-- Session 6 will exit immediately after an allocation failure.  Allocate
-- its initial segment before the concurrent transactions start.
6: SELECT count(*) FROM sharedsnapshot_dsm_t;

-- Open some concurrent transactions so that the next snapshots taken by
-- sessions 1 and 6 are larger and need new DSM segments.
2: BEGIN;
2: CREATE TEMP TABLE sharedsnapshot_dsm_tmp2(a int) DISTRIBUTED BY (a);
2: INSERT INTO sharedsnapshot_dsm_t VALUES (2);
3: BEGIN;
3: CREATE TEMP TABLE sharedsnapshot_dsm_tmp3(a int) DISTRIBUTED BY (a);
3: INSERT INTO sharedsnapshot_dsm_t VALUES (3);
4: BEGIN;
4: CREATE TEMP TABLE sharedsnapshot_dsm_tmp4(a int) DISTRIBUTED BY (a);
4: INSERT INTO sharedsnapshot_dsm_t VALUES (4);
-- A snapshot only lists in-progress transactions below its xmax, which is
-- the highest completed XID plus one.  Complete a later transaction, local
-- and distributed, so that the three above fall below the xmax of the next
-- snapshots of sessions 1 and 6.
5: CREATE TABLE sharedsnapshot_dsm_bump(a int) DISTRIBUTED BY (a);

-- Check cleanup directly after the error, without a successful replacement.
-- Arm the completion signal before the failing query so session 6 can quit
-- immediately, without running another SQL statement after the error.
6: SELECT gp_inject_fault('shared_snapshot_remove_done', 'skip', dbid, current_setting('gp_session_id')::int) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
6: SELECT gp_inject_fault('update_shared_local_snapshot_create_dsm', 'error', dbid, current_setting('gp_session_id')::int) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
6: SELECT count(*) FROM sharedsnapshot_dsm_t;
6q:
5: SELECT gp_wait_until_triggered_fault('shared_snapshot_remove_done', 1, dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
5: SELECT gp_inject_fault('shared_snapshot_remove_done', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
5: SELECT gp_inject_fault('update_shared_local_snapshot_create_dsm', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Simulate a dsm_create() failure on the coordinator for session 1 only.
1: SELECT gp_inject_fault('update_shared_local_snapshot_create_dsm', 'error', dbid, current_setting('gp_session_id')::int) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
1: SELECT count(*) FROM sharedsnapshot_dsm_t;
1: SELECT gp_inject_fault('update_shared_local_snapshot_create_dsm', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

-- The slot must still reference a valid segment: replacing it must work.
1: SELECT count(*) FROM sharedsnapshot_dsm_t;

-- Releasing the slot at backend exit must not fail either.  Wait until the
-- exiting backend has actually released it, not merely left pg_stat_activity.
1: SELECT gp_inject_fault('shared_snapshot_remove_done', 'skip', dbid, current_setting('gp_session_id')::int) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
1q:
5: SELECT gp_wait_until_triggered_fault('shared_snapshot_remove_done', 1, dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
5: SELECT gp_inject_fault('shared_snapshot_remove_done', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

-- The cluster must not have been reset: the open transactions survive.
2: COMMIT;
3: COMMIT;
4: COMMIT;
5: SELECT count(*) FROM sharedsnapshot_dsm_t;

5: DROP TABLE sharedsnapshot_dsm_t;
5: DROP TABLE sharedsnapshot_dsm_bump;
