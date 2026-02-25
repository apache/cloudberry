-- Test: COPY TO concurrent with ALTER TABLE SET WITH (reorganize=true)
-- Issue: https://github.com/apache/cloudberry/issues/1545
--
-- Tests 2.1: Core fix (relation-based COPY TO)
-- Tests 2.2-2.5: Extended fixes for query-based, partitioned, RLS, and CTAS paths

-- ============================================================
-- Test 2.1: relation-based COPY TO + concurrent reorganize
-- Reproduces issue #1545: COPY TO should return correct row count
-- after waiting for reorganize to release AccessExclusiveLock.
-- ============================================================

CREATE TABLE copy_reorg_test (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO copy_reorg_test SELECT i, i FROM generate_series(1, 1000) i;

-- Record original row count
SELECT count(*) FROM copy_reorg_test;

-- Session 1: Begin reorganize (holds AccessExclusiveLock)
1: BEGIN;
1: ALTER TABLE copy_reorg_test SET WITH (reorganize=true);

-- Session 2: relation-based COPY TO should block on AccessShareLock
-- At this point PortalRunUtility has already acquired a snapshot (before reorganize commits),
-- then DoCopy tries to acquire the lock and blocks.
2&: COPY copy_reorg_test TO '/tmp/copy_reorg_test.csv';

-- Confirm Session 2 is waiting for the lock
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_reorg_test%' AND wait_event_type = 'Lock';

-- Session 1: Commit reorganize, releasing AccessExclusiveLock
1: COMMIT;

-- Session 2: Should return 1000 rows (fixed), not 0 rows (broken)
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_reorg_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_reorg_verify FROM '/tmp/copy_reorg_test.csv';
SELECT count(*) FROM copy_reorg_verify;

-- Cleanup
DROP TABLE copy_reorg_verify;
DROP TABLE copy_reorg_test;

-- ============================================================
-- Test 2.2: query-based COPY TO + concurrent reorganize
-- Fixed: BeginCopy() refreshes snapshot after pg_analyze_and_rewrite()
-- acquires all relation locks via AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_query_reorg_test (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO copy_query_reorg_test SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_query_reorg_test;

-- Session 1: reorganize holds AccessExclusiveLock
1: BEGIN;
1: ALTER TABLE copy_query_reorg_test SET WITH (reorganize=true);

-- Session 2: query-based COPY TO blocks (lock acquired in pg_analyze_and_rewrite -> AcquireRewriteLocks)
2&: COPY (SELECT * FROM copy_query_reorg_test) TO '/tmp/copy_query_reorg_test.csv';

-- Confirm Session 2 is blocked
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY (SELECT%copy_query_reorg_test%' AND wait_event_type = 'Lock';

-- Session 1: Commit
1: COMMIT;

-- Session 2: Complete
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_query_reorg_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_query_reorg_verify FROM '/tmp/copy_query_reorg_test.csv';
SELECT count(*) FROM copy_query_reorg_verify;

-- Cleanup
DROP TABLE copy_query_reorg_verify;
DROP TABLE copy_query_reorg_test;

-- ============================================================
-- Test 2.3: partitioned table COPY TO + child partition concurrent reorganize
-- Fixed: DoCopy() calls find_all_inheritors() to eagerly lock all child
-- partitions before refreshing the snapshot, ensuring the snapshot sees all
-- child reorganize commits before the query is built.
-- ============================================================

CREATE TABLE copy_part_parent (a INT, b INT) PARTITION BY RANGE (a) DISTRIBUTED BY (a);
CREATE TABLE copy_part_child1 PARTITION OF copy_part_parent FOR VALUES FROM (1) TO (501);
CREATE TABLE copy_part_child2 PARTITION OF copy_part_parent FOR VALUES FROM (501) TO (1001);
INSERT INTO copy_part_parent SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_part_parent;

-- Session 1: reorganize the child partition
1: BEGIN;
1: ALTER TABLE copy_part_child1 SET WITH (reorganize=true);

-- Session 2: COPY parent TO (internally converted to query-based, child lock acquired in analyze phase)
2&: COPY copy_part_parent TO '/tmp/copy_part_parent.csv';

-- Confirm Session 2 is blocked
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_part_parent%' AND wait_event_type = 'Lock';

-- Session 1: Commit
1: COMMIT;

-- Session 2: Complete
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_part_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_part_verify FROM '/tmp/copy_part_parent.csv';
SELECT count(*) FROM copy_part_verify;

-- Cleanup
DROP TABLE copy_part_verify;
DROP TABLE copy_part_parent;

-- ============================================================
-- Test 2.4: RLS table COPY TO + policy-referenced table concurrent reorganize
-- Fixed: same as 2.2 — BeginCopy() refreshes snapshot after AcquireRewriteLocks()
-- which also acquires the lock on the RLS policy's lookup table.
-- ============================================================

CREATE TABLE copy_rls_lookup (cat INT) DISTRIBUTED BY (cat);
INSERT INTO copy_rls_lookup SELECT i FROM generate_series(1, 2) i;

CREATE TABLE copy_rls_main (a INT, category INT) DISTRIBUTED BY (a);
INSERT INTO copy_rls_main SELECT i, (i % 5) + 1 FROM generate_series(1, 1000) i;

ALTER TABLE copy_rls_main ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_rls ON copy_rls_main USING (category IN (SELECT cat FROM copy_rls_lookup));

-- Create non-superuser to trigger RLS (needs pg_write_server_files to COPY TO file)
CREATE ROLE copy_rls_testuser;
GRANT pg_write_server_files TO copy_rls_testuser;
GRANT ALL ON copy_rls_main TO copy_rls_testuser;
GRANT ALL ON copy_rls_lookup TO copy_rls_testuser;

SELECT count(*) FROM copy_rls_main;

-- Baseline: verify RLS filters correctly (should return 400 rows: categories 1 and 2 only)
2: SET ROLE copy_rls_testuser; COPY copy_rls_main TO '/tmp/copy_rls_main.csv';

-- Session 1: reorganize the lookup table
1: BEGIN;
1: ALTER TABLE copy_rls_lookup SET WITH (reorganize=true);

-- Session 2: COPY TO as non-superuser (RLS active, internally converted to query-based)
2&: SET ROLE copy_rls_testuser; COPY copy_rls_main TO '/tmp/copy_rls_main.csv';

-- Confirm Session 2 is blocked
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE '%COPY copy_rls_main%' AND wait_event_type = 'Lock';

-- Session 1: Commit
1: COMMIT;

-- Session 2: Complete
2<:

-- Reset session 2's role to avoid leaking to subsequent tests
2: RESET ROLE;

-- Verify: should match baseline count (400 rows filtered by RLS)
RESET ROLE;
CREATE TABLE copy_rls_verify (a INT, category INT) DISTRIBUTED BY (a);
COPY copy_rls_verify FROM '/tmp/copy_rls_main.csv';
SELECT count(*) FROM copy_rls_verify;

-- Cleanup
DROP TABLE copy_rls_verify;
DROP POLICY p_rls ON copy_rls_main;
DROP TABLE copy_rls_main;
DROP TABLE copy_rls_lookup;
DROP ROLE copy_rls_testuser;

-- ============================================================
-- Test 2.5: CTAS + concurrent reorganize
-- Fixed as a side effect: CTAS goes through pg_analyze_and_rewrite() +
-- AcquireRewriteLocks(), so the snapshot refresh in BeginCopy() also fixes it.
-- ============================================================

CREATE TABLE ctas_reorg_src (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO ctas_reorg_src SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM ctas_reorg_src;

-- Session 1: reorganize
1: BEGIN;
1: ALTER TABLE ctas_reorg_src SET WITH (reorganize=true);

-- Session 2: CTAS should block (lock acquired in executor or analyze phase)
2&: CREATE TABLE ctas_reorg_dst AS SELECT * FROM ctas_reorg_src DISTRIBUTED BY (a);

-- Confirm Session 2 is blocked
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'CREATE TABLE ctas_reorg_dst%' AND wait_event_type = 'Lock';

-- Session 1: Commit
1: COMMIT;

-- Session 2: Complete
2<:

-- Verify row count after CTAS completes
SELECT count(*) FROM ctas_reorg_dst;

-- Cleanup
DROP TABLE ctas_reorg_dst;
DROP TABLE ctas_reorg_src;

-- NOTE: Test 2.6 (change distribution key + query-based COPY TO) removed because
-- ALTER TABLE SET DISTRIBUTED BY + concurrent query-based COPY TO causes a server
-- crash (pre-existing Cloudberry bug, not related to this fix).

-- ============================================================
-- Test 2.1a: AO row table — relation-based COPY TO + concurrent reorganize
-- Same as 2.1 but using append-optimized row-oriented table.
-- ============================================================

CREATE TABLE copy_reorg_ao_row_test (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
INSERT INTO copy_reorg_ao_row_test SELECT i, i FROM generate_series(1, 1000) i;

-- Record original row count
SELECT count(*) FROM copy_reorg_ao_row_test;

-- Session 1: Begin reorganize (holds AccessExclusiveLock)
1: BEGIN;
1: ALTER TABLE copy_reorg_ao_row_test SET WITH (reorganize=true);

-- Session 2: relation-based COPY TO should block on AccessShareLock
2&: COPY copy_reorg_ao_row_test TO '/tmp/copy_reorg_ao_row_test.csv';

-- Confirm Session 2 is waiting for the lock
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_reorg_ao_row_test%' AND wait_event_type = 'Lock';

-- Session 1: Commit reorganize, releasing AccessExclusiveLock
1: COMMIT;

-- Session 2: Should return 1000 rows (fixed), not 0 rows (broken)
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_reorg_ao_row_verify (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
COPY copy_reorg_ao_row_verify FROM '/tmp/copy_reorg_ao_row_test.csv';
SELECT count(*) FROM copy_reorg_ao_row_verify;

-- Cleanup
DROP TABLE copy_reorg_ao_row_verify;
DROP TABLE copy_reorg_ao_row_test;

-- ============================================================
-- Test 2.1b: AO column table — relation-based COPY TO + concurrent reorganize
-- Same as 2.1 but using append-optimized column-oriented table.
-- ============================================================

CREATE TABLE copy_reorg_ao_col_test (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
INSERT INTO copy_reorg_ao_col_test SELECT i, i FROM generate_series(1, 1000) i;

-- Record original row count
SELECT count(*) FROM copy_reorg_ao_col_test;

-- Session 1: Begin reorganize (holds AccessExclusiveLock)
1: BEGIN;
1: ALTER TABLE copy_reorg_ao_col_test SET WITH (reorganize=true);

-- Session 2: relation-based COPY TO should block on AccessShareLock
2&: COPY copy_reorg_ao_col_test TO '/tmp/copy_reorg_ao_col_test.csv';

-- Confirm Session 2 is waiting for the lock
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_reorg_ao_col_test%' AND wait_event_type = 'Lock';

-- Session 1: Commit reorganize, releasing AccessExclusiveLock
1: COMMIT;

-- Session 2: Should return 1000 rows (fixed), not 0 rows (broken)
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_reorg_ao_col_verify (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
COPY copy_reorg_ao_col_verify FROM '/tmp/copy_reorg_ao_col_test.csv';
SELECT count(*) FROM copy_reorg_ao_col_verify;

-- Cleanup
DROP TABLE copy_reorg_ao_col_verify;
DROP TABLE copy_reorg_ao_col_test;

-- ============================================================
-- Test 2.2a: AO row — query-based COPY TO + concurrent reorganize
-- Fixed: BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_query_reorg_ao_row_test (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
INSERT INTO copy_query_reorg_ao_row_test SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_query_reorg_ao_row_test;

1: BEGIN;
1: ALTER TABLE copy_query_reorg_ao_row_test SET WITH (reorganize=true);

2&: COPY (SELECT * FROM copy_query_reorg_ao_row_test) TO '/tmp/copy_query_reorg_ao_row_test.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY (SELECT%copy_query_reorg_ao_row_test%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_query_reorg_ao_row_verify (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
COPY copy_query_reorg_ao_row_verify FROM '/tmp/copy_query_reorg_ao_row_test.csv';
SELECT count(*) FROM copy_query_reorg_ao_row_verify;

DROP TABLE copy_query_reorg_ao_row_verify;
DROP TABLE copy_query_reorg_ao_row_test;

-- ============================================================
-- Test 2.2b: AO column — query-based COPY TO + concurrent reorganize
-- Fixed: BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_query_reorg_ao_col_test (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
INSERT INTO copy_query_reorg_ao_col_test SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_query_reorg_ao_col_test;

1: BEGIN;
1: ALTER TABLE copy_query_reorg_ao_col_test SET WITH (reorganize=true);

2&: COPY (SELECT * FROM copy_query_reorg_ao_col_test) TO '/tmp/copy_query_reorg_ao_col_test.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY (SELECT%copy_query_reorg_ao_col_test%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_query_reorg_ao_col_verify (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
COPY copy_query_reorg_ao_col_verify FROM '/tmp/copy_query_reorg_ao_col_test.csv';
SELECT count(*) FROM copy_query_reorg_ao_col_verify;

DROP TABLE copy_query_reorg_ao_col_verify;
DROP TABLE copy_query_reorg_ao_col_test;

-- ============================================================
-- Test 2.3a: AO row — partitioned table COPY TO + child partition concurrent reorganize
-- Fixed: DoCopy() calls find_all_inheritors() to lock all child partitions first.
-- ============================================================

CREATE TABLE copy_part_parent_ao_row (a INT, b INT) PARTITION BY RANGE (a) DISTRIBUTED BY (a);
CREATE TABLE copy_part_child1_ao_row PARTITION OF copy_part_parent_ao_row FOR VALUES FROM (1) TO (501) USING ao_row;
CREATE TABLE copy_part_child2_ao_row PARTITION OF copy_part_parent_ao_row FOR VALUES FROM (501) TO (1001) USING ao_row;
INSERT INTO copy_part_parent_ao_row SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_part_parent_ao_row;

1: BEGIN;
1: ALTER TABLE copy_part_child1_ao_row SET WITH (reorganize=true);

2&: COPY copy_part_parent_ao_row TO '/tmp/copy_part_parent_ao_row.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_part_parent_ao_row%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_part_ao_row_verify (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
COPY copy_part_ao_row_verify FROM '/tmp/copy_part_parent_ao_row.csv';
SELECT count(*) FROM copy_part_ao_row_verify;

DROP TABLE copy_part_ao_row_verify;
DROP TABLE copy_part_parent_ao_row;

-- ============================================================
-- Test 2.3b: AO column — partitioned table COPY TO + child partition concurrent reorganize
-- Fixed: DoCopy() calls find_all_inheritors() to lock all child partitions first.
-- ============================================================

CREATE TABLE copy_part_parent_ao_col (a INT, b INT) PARTITION BY RANGE (a) DISTRIBUTED BY (a);
CREATE TABLE copy_part_child1_ao_col PARTITION OF copy_part_parent_ao_col FOR VALUES FROM (1) TO (501) USING ao_column;
CREATE TABLE copy_part_child2_ao_col PARTITION OF copy_part_parent_ao_col FOR VALUES FROM (501) TO (1001) USING ao_column;
INSERT INTO copy_part_parent_ao_col SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_part_parent_ao_col;

1: BEGIN;
1: ALTER TABLE copy_part_child1_ao_col SET WITH (reorganize=true);

2&: COPY copy_part_parent_ao_col TO '/tmp/copy_part_parent_ao_col.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_part_parent_ao_col%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_part_ao_col_verify (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
COPY copy_part_ao_col_verify FROM '/tmp/copy_part_parent_ao_col.csv';
SELECT count(*) FROM copy_part_ao_col_verify;

DROP TABLE copy_part_ao_col_verify;
DROP TABLE copy_part_parent_ao_col;

-- ============================================================
-- Test 2.4a: AO row — RLS table COPY TO + policy-referenced table concurrent reorganize
-- Fixed: same as 2.4 — BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_rls_ao_row_lookup (cat INT) USING ao_row DISTRIBUTED BY (cat);
INSERT INTO copy_rls_ao_row_lookup SELECT i FROM generate_series(1, 2) i;

CREATE TABLE copy_rls_ao_row_main (a INT, category INT) USING ao_row DISTRIBUTED BY (a);
INSERT INTO copy_rls_ao_row_main SELECT i, (i % 5) + 1 FROM generate_series(1, 1000) i;

ALTER TABLE copy_rls_ao_row_main ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_rls_ao_row ON copy_rls_ao_row_main USING (category IN (SELECT cat FROM copy_rls_ao_row_lookup));

CREATE ROLE copy_rls_ao_row_testuser;
GRANT pg_write_server_files TO copy_rls_ao_row_testuser;
GRANT ALL ON copy_rls_ao_row_main TO copy_rls_ao_row_testuser;
GRANT ALL ON copy_rls_ao_row_lookup TO copy_rls_ao_row_testuser;

SELECT count(*) FROM copy_rls_ao_row_main;

-- Baseline: verify RLS filters correctly (should return 400 rows: categories 1 and 2 only)
2: SET ROLE copy_rls_ao_row_testuser; COPY copy_rls_ao_row_main TO '/tmp/copy_rls_ao_row_main.csv';

1: BEGIN;
1: ALTER TABLE copy_rls_ao_row_lookup SET WITH (reorganize=true);

2&: SET ROLE copy_rls_ao_row_testuser; COPY copy_rls_ao_row_main TO '/tmp/copy_rls_ao_row_main.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE '%COPY copy_rls_ao_row_main%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

2: RESET ROLE;

RESET ROLE;
CREATE TABLE copy_rls_ao_row_verify (a INT, category INT) USING ao_row DISTRIBUTED BY (a);
COPY copy_rls_ao_row_verify FROM '/tmp/copy_rls_ao_row_main.csv';
SELECT count(*) FROM copy_rls_ao_row_verify;

DROP TABLE copy_rls_ao_row_verify;
DROP POLICY p_rls_ao_row ON copy_rls_ao_row_main;
DROP TABLE copy_rls_ao_row_main;
DROP TABLE copy_rls_ao_row_lookup;
DROP ROLE copy_rls_ao_row_testuser;

-- ============================================================
-- Test 2.4b: AO column — RLS table COPY TO + policy-referenced table concurrent reorganize
-- Fixed: same as 2.4 — BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_rls_ao_col_lookup (cat INT) USING ao_column DISTRIBUTED BY (cat);
INSERT INTO copy_rls_ao_col_lookup SELECT i FROM generate_series(1, 2) i;

CREATE TABLE copy_rls_ao_col_main (a INT, category INT) USING ao_column DISTRIBUTED BY (a);
INSERT INTO copy_rls_ao_col_main SELECT i, (i % 5) + 1 FROM generate_series(1, 1000) i;

ALTER TABLE copy_rls_ao_col_main ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_rls_ao_col ON copy_rls_ao_col_main USING (category IN (SELECT cat FROM copy_rls_ao_col_lookup));

CREATE ROLE copy_rls_ao_col_testuser;
GRANT pg_write_server_files TO copy_rls_ao_col_testuser;
GRANT ALL ON copy_rls_ao_col_main TO copy_rls_ao_col_testuser;
GRANT ALL ON copy_rls_ao_col_lookup TO copy_rls_ao_col_testuser;

SELECT count(*) FROM copy_rls_ao_col_main;

-- Baseline: verify RLS filters correctly (should return 400 rows: categories 1 and 2 only)
2: SET ROLE copy_rls_ao_col_testuser; COPY copy_rls_ao_col_main TO '/tmp/copy_rls_ao_col_main.csv';

1: BEGIN;
1: ALTER TABLE copy_rls_ao_col_lookup SET WITH (reorganize=true);

2&: SET ROLE copy_rls_ao_col_testuser; COPY copy_rls_ao_col_main TO '/tmp/copy_rls_ao_col_main.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE '%COPY copy_rls_ao_col_main%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

2: RESET ROLE;

RESET ROLE;
CREATE TABLE copy_rls_ao_col_verify (a INT, category INT) USING ao_column DISTRIBUTED BY (a);
COPY copy_rls_ao_col_verify FROM '/tmp/copy_rls_ao_col_main.csv';
SELECT count(*) FROM copy_rls_ao_col_verify;

DROP TABLE copy_rls_ao_col_verify;
DROP POLICY p_rls_ao_col ON copy_rls_ao_col_main;
DROP TABLE copy_rls_ao_col_main;
DROP TABLE copy_rls_ao_col_lookup;
DROP ROLE copy_rls_ao_col_testuser;

-- ============================================================
-- Test 2.5a: AO row — CTAS + concurrent reorganize
-- Fixed as a side effect via BeginCopy() snapshot refresh.
-- ============================================================

CREATE TABLE ctas_reorg_ao_row_src (a INT, b INT) USING ao_row DISTRIBUTED BY (a);
INSERT INTO ctas_reorg_ao_row_src SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM ctas_reorg_ao_row_src;

1: BEGIN;
1: ALTER TABLE ctas_reorg_ao_row_src SET WITH (reorganize=true);

2&: CREATE TABLE ctas_reorg_ao_row_dst AS SELECT * FROM ctas_reorg_ao_row_src DISTRIBUTED BY (a);

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'CREATE TABLE ctas_reorg_ao_row_dst%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

SELECT count(*) FROM ctas_reorg_ao_row_dst;

DROP TABLE ctas_reorg_ao_row_dst;
DROP TABLE ctas_reorg_ao_row_src;

-- ============================================================
-- Test 2.5b: AO column — CTAS + concurrent reorganize
-- Fixed as a side effect via BeginCopy() snapshot refresh.
-- ============================================================

CREATE TABLE ctas_reorg_ao_col_src (a INT, b INT) USING ao_column DISTRIBUTED BY (a);
INSERT INTO ctas_reorg_ao_col_src SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM ctas_reorg_ao_col_src;

1: BEGIN;
1: ALTER TABLE ctas_reorg_ao_col_src SET WITH (reorganize=true);

2&: CREATE TABLE ctas_reorg_ao_col_dst AS SELECT * FROM ctas_reorg_ao_col_src DISTRIBUTED BY (a);

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'CREATE TABLE ctas_reorg_ao_col_dst%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

SELECT count(*) FROM ctas_reorg_ao_col_dst;

DROP TABLE ctas_reorg_ao_col_dst;
DROP TABLE ctas_reorg_ao_col_src;

-- NOTE: Tests 2.6a/2.6b (AO variants of change distribution key + query-based COPY TO)
-- removed for the same reason as test 2.6 (server crash, pre-existing bug).
