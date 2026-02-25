-- Test: PAX table — relation-based COPY TO concurrent with ALTER TABLE SET WITH (reorganize=true)
-- Issue: https://github.com/apache/cloudberry/issues/1545
-- Same as test 2.1 in the main isolation2 suite but for PAX storage.

CREATE TABLE copy_reorg_pax_test (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO copy_reorg_pax_test SELECT i, i FROM generate_series(1, 1000) i;

-- Record original row count
SELECT count(*) FROM copy_reorg_pax_test;

-- Session 1: Begin reorganize (holds AccessExclusiveLock)
1: BEGIN;
1: ALTER TABLE copy_reorg_pax_test SET WITH (reorganize=true);

-- Session 2: relation-based COPY TO should block on AccessShareLock
2&: COPY copy_reorg_pax_test TO '/tmp/copy_reorg_pax_test.csv';

-- Confirm Session 2 is waiting for the lock
1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_reorg_pax_test%' AND wait_event_type = 'Lock';

-- Session 1: Commit reorganize, releasing AccessExclusiveLock
1: COMMIT;

-- Session 2: Should return 1000 rows (fixed), not 0 rows (broken)
2<:

-- Verify the output file contains all rows
CREATE TABLE copy_reorg_pax_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_reorg_pax_verify FROM '/tmp/copy_reorg_pax_test.csv';
SELECT count(*) FROM copy_reorg_pax_verify;

-- Cleanup
DROP TABLE copy_reorg_pax_verify;
DROP TABLE copy_reorg_pax_test;

-- ============================================================
-- Test 2.2c: PAX — query-based COPY TO + concurrent reorganize
-- Fixed: BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_query_reorg_pax_test (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO copy_query_reorg_pax_test SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_query_reorg_pax_test;

1: BEGIN;
1: ALTER TABLE copy_query_reorg_pax_test SET WITH (reorganize=true);

2&: COPY (SELECT * FROM copy_query_reorg_pax_test) TO '/tmp/copy_query_reorg_pax_test.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY (SELECT%copy_query_reorg_pax_test%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_query_reorg_pax_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_query_reorg_pax_verify FROM '/tmp/copy_query_reorg_pax_test.csv';
SELECT count(*) FROM copy_query_reorg_pax_verify;

DROP TABLE copy_query_reorg_pax_verify;
DROP TABLE copy_query_reorg_pax_test;

-- ============================================================
-- Test 2.3c: PAX — partitioned table COPY TO + child partition concurrent reorganize
-- Fixed: DoCopy() calls find_all_inheritors() to lock all child partitions first.
-- ============================================================

CREATE TABLE copy_part_parent_pax (a INT, b INT) PARTITION BY RANGE (a) DISTRIBUTED BY (a);
CREATE TABLE copy_part_child1_pax PARTITION OF copy_part_parent_pax FOR VALUES FROM (1) TO (501);
CREATE TABLE copy_part_child2_pax PARTITION OF copy_part_parent_pax FOR VALUES FROM (501) TO (1001);
INSERT INTO copy_part_parent_pax SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM copy_part_parent_pax;

1: BEGIN;
1: ALTER TABLE copy_part_child1_pax SET WITH (reorganize=true);

2&: COPY copy_part_parent_pax TO '/tmp/copy_part_parent_pax.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'COPY copy_part_parent_pax%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

CREATE TABLE copy_part_pax_verify (a INT, b INT) DISTRIBUTED BY (a);
COPY copy_part_pax_verify FROM '/tmp/copy_part_parent_pax.csv';
SELECT count(*) FROM copy_part_pax_verify;

DROP TABLE copy_part_pax_verify;
DROP TABLE copy_part_parent_pax;

-- ============================================================
-- Test 2.4c: PAX — RLS table COPY TO + policy-referenced table concurrent reorganize
-- Fixed: same as 2.2c — BeginCopy() refreshes snapshot after AcquireRewriteLocks().
-- ============================================================

CREATE TABLE copy_rls_pax_lookup (cat INT) DISTRIBUTED BY (cat);
INSERT INTO copy_rls_pax_lookup SELECT i FROM generate_series(1, 2) i;

CREATE TABLE copy_rls_pax_main (a INT, category INT) DISTRIBUTED BY (a);
INSERT INTO copy_rls_pax_main SELECT i, (i % 5) + 1 FROM generate_series(1, 1000) i;

ALTER TABLE copy_rls_pax_main ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_rls_pax ON copy_rls_pax_main USING (category IN (SELECT cat from copy_rls_pax_lookup));

CREATE ROLE copy_rls_pax_testuser;
GRANT pg_write_server_files TO copy_rls_pax_testuser;
GRANT ALL ON copy_rls_pax_main TO copy_rls_pax_testuser;
GRANT ALL ON copy_rls_pax_lookup TO copy_rls_pax_testuser;

SELECT count(*) FROM copy_rls_pax_main;

2: SET ROLE copy_rls_pax_testuser; COPY copy_rls_pax_main TO '/tmp/copy_rls_pax_main.csv';

1: BEGIN;
1: ALTER TABLE copy_rls_pax_lookup SET WITH (reorganize=true);

2&: SET ROLE copy_rls_pax_testuser; COPY copy_rls_pax_main TO '/tmp/copy_rls_pax_main.csv';

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE '%COPY copy_rls_pax_main%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

-- Reset session 2's role to avoid leaking to subsequent tests
2: RESET ROLE;

RESET ROLE;
CREATE TABLE copy_rls_pax_verify (a INT, category INT) DISTRIBUTED BY (a);
COPY copy_rls_pax_verify FROM '/tmp/copy_rls_pax_main.csv';
SELECT count(*) FROM copy_rls_pax_verify;

DROP TABLE copy_rls_pax_verify;
DROP POLICY p_rls_pax ON copy_rls_pax_main;
DROP TABLE copy_rls_pax_main;
DROP TABLE copy_rls_pax_lookup;
DROP ROLE copy_rls_pax_testuser;

-- ============================================================
-- Test 2.5c: PAX — CTAS + concurrent reorganize
-- Fixed as a side effect via BeginCopy() snapshot refresh.
-- ============================================================

CREATE TABLE ctas_reorg_pax_src (a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO ctas_reorg_pax_src SELECT i, i FROM generate_series(1, 1000) i;

SELECT count(*) FROM ctas_reorg_pax_src;

1: BEGIN;
1: ALTER TABLE ctas_reorg_pax_src SET WITH (reorganize=true);

2&: CREATE TABLE ctas_reorg_pax_dst AS SELECT * FROM ctas_reorg_pax_src DISTRIBUTED BY (a);

1: SELECT count(*) > 0 FROM pg_stat_activity
   WHERE query LIKE 'CREATE TABLE ctas_reorg_pax_dst%' AND wait_event_type = 'Lock';

1: COMMIT;
2<:

SELECT count(*) FROM ctas_reorg_pax_dst;

DROP TABLE ctas_reorg_pax_dst;
DROP TABLE ctas_reorg_pax_src;

-- NOTE: Test 2.6c (PAX variant of change distribution key + query-based COPY TO)
-- removed for the same reason as test 2.6 (server crash, pre-existing bug).
