-- Test: QE→QD pgstat collection
-- Verifies that DML stats from QE segments reach the QD coordinator's
-- pg_stat_user_tables, enabling autovacuum to see modification counts.
-- Also verifies that gp_stat_user_tables_summary remains accurate on QEs.

--
-- Setup: disable autovacuum and auto_stats to prevent interference.
--
ALTER SYSTEM SET autovacuum = off;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
SET gp_autostats_mode = none;

--
-- Test 1: Distributed (hash) table — INSERT/UPDATE/DELETE stats reach QD
--
CREATE TABLE test_pgstat_dist(id int, val int) DISTRIBUTED BY (id);

INSERT INTO test_pgstat_dist SELECT i, 0 FROM generate_series(1, 1000) i;
SELECT gp_stat_force_next_flush();

-- QD should see the stats sent from QEs
SELECT n_tup_ins, n_mod_since_analyze
  FROM pg_stat_user_tables WHERE relname = 'test_pgstat_dist';

-- QE summary should also show the same counts
SELECT n_tup_ins, n_mod_since_analyze
  FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_dist';

-- UPDATE non-distribution-key column so it's a real update, not split update
UPDATE test_pgstat_dist SET val = 1 WHERE id <= 100;
SELECT gp_stat_force_next_flush();

SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'test_pgstat_dist';
SELECT n_tup_upd FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_dist';

DELETE FROM test_pgstat_dist WHERE id <= 50;
SELECT gp_stat_force_next_flush();

SELECT n_tup_del FROM pg_stat_user_tables WHERE relname = 'test_pgstat_dist';
SELECT n_tup_del FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_dist';

--
-- Test 2: Replicated table — stats not double-counted
-- With 3 segments, each segment has all 500 rows, but only one segment
-- should report stats to QD.
--
CREATE TABLE test_pgstat_repl(id int) DISTRIBUTED REPLICATED;

INSERT INTO test_pgstat_repl SELECT i FROM generate_series(1, 500) i;
SELECT gp_stat_force_next_flush();

-- QD should show exactly 500, not 1500 (3 segments * 500)
SELECT n_tup_ins, n_mod_since_analyze
  FROM pg_stat_user_tables WHERE relname = 'test_pgstat_repl';

-- QE summary divides replicated table stats by numsegments, so also 500
SELECT n_tup_ins, n_mod_since_analyze
  FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_repl';

--
-- Test 3: Transaction — committed DML stats are counted
--
CREATE TABLE test_pgstat_xact(id int) DISTRIBUTED BY (id);

BEGIN;
INSERT INTO test_pgstat_xact SELECT i FROM generate_series(1, 300) i;
DELETE FROM test_pgstat_xact WHERE id <= 100;
COMMIT;
SELECT gp_stat_force_next_flush();

SELECT n_tup_ins, n_tup_del FROM pg_stat_user_tables WHERE relname = 'test_pgstat_xact';
SELECT n_tup_ins, n_tup_del FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_xact';
SELECT count(*) FROM test_pgstat_xact;

--
-- Test 4: Subtransaction rollback — n_tup_ins counts all attempted inserts
-- (PG counts attempted actions regardless of commit/abort)
--
CREATE TABLE test_pgstat_subxact(id int) DISTRIBUTED BY (id);

BEGIN;
INSERT INTO test_pgstat_subxact SELECT i FROM generate_series(1, 200) i;
SAVEPOINT sp1;
INSERT INTO test_pgstat_subxact SELECT i FROM generate_series(201, 700) i;
ROLLBACK TO sp1;
COMMIT;
SELECT gp_stat_force_next_flush();

-- n_tup_ins counts all attempted inserts (200 + 500 = 700)
-- but only 200 rows are actually in the table
SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname = 'test_pgstat_subxact';
SELECT n_tup_ins FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_subxact';
SELECT count(*) FROM test_pgstat_subxact;

--
-- Test 5: Nested subtransactions — RELEASE merges into parent, ROLLBACK TO discards
--
CREATE TABLE test_pgstat_nested(id int) DISTRIBUTED BY (id);

BEGIN;
INSERT INTO test_pgstat_nested SELECT i FROM generate_series(1, 100) i;
SAVEPOINT sp1;
INSERT INTO test_pgstat_nested SELECT i FROM generate_series(101, 200) i;
SAVEPOINT sp2;
INSERT INTO test_pgstat_nested SELECT i FROM generate_series(201, 300) i;
RELEASE SAVEPOINT sp2;
ROLLBACK TO sp1;
COMMIT;
SELECT gp_stat_force_next_flush();

-- All 300 attempted inserts counted (100 outer + 100 sp1 + 100 sp2)
-- but only 100 rows remain (sp1 rollback discards sp1 and released sp2)
SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname = 'test_pgstat_nested';
SELECT n_tup_ins FROM gp_stat_user_tables_summary WHERE relname = 'test_pgstat_nested';
SELECT count(*) FROM test_pgstat_nested;

--
-- Test 6: Catalog (entry) table — QE doesn't crash on catalog DML
-- Catalog tables are filtered out (POLICYTYPE_ENTRY), so the stats
-- infrastructure should simply skip them without error.
--
CREATE FUNCTION test_pgstat_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
DROP FUNCTION test_pgstat_func();

--
-- Cleanup
--
DROP TABLE test_pgstat_dist;
DROP TABLE test_pgstat_repl;
DROP TABLE test_pgstat_xact;
DROP TABLE test_pgstat_subxact;
DROP TABLE test_pgstat_nested;

ALTER SYSTEM RESET autovacuum;
SELECT pg_reload_conf();
