-- Test crash recovery when the coordinator panics right before
-- removing the database's tuple from pg_database.
--
-- The fault injection point 'after_dbdrop_tuple_update_tuple' is
-- reached in dropdb() after all the preconditions have been checked
-- (permissions, active backends, etc.) and after the DROP has been
-- dispatched to the segments, but *before* the pg_database tuple is
-- deleted on the coordinator.  If the coordinator panics here, the
-- transaction aborts and the tuple must remain, so the database
-- should still be present in pg_database after recovery.

-- start_matchsubs
-- m/PANIC:  fault triggered, fault name:'after_dbdrop_tuple_update_tuple' fault type:'panic'\n/
-- s/PANIC:  fault triggered, fault name:'after_dbdrop_tuple_update_tuple' fault type:'panic'\n//
-- end_matchsubs

-- Create the extension that provides the fault injector functions.
1:CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

-- Create a database to be dropped.
1:DROP DATABASE IF EXISTS dropdb_crash_test;
1:CREATE DATABASE dropdb_crash_test;

-- Inject a panic fault on the coordinator (content = -1, role = p),
-- at the point right before the pg_database tuple is removed.
1:SELECT gp_inject_fault('after_dbdrop_tuple_update_tuple', 'panic', dbid)
  FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

-- DROP DATABASE will panic right before removing the tuple.
1:DROP DATABASE dropdb_crash_test;

-- Wait for the coordinator to come back up after crash recovery.
2:SELECT 1;

-- The database tuple should still be in pg_database because the
-- transaction that removes it was aborted by the panic.
2:SELECT datname FROM pg_database WHERE datname = 'dropdb_crash_test';

-- Reset any leftover faults as a safety net in case the panic did
-- not fire for some reason.
2:SELECT gp_inject_fault('after_dbdrop_tuple_update_tuple', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

-- Now drop the database for real, to clean up.
2:DROP DATABASE dropdb_crash_test;

-- Verify it's gone.
2:SELECT datname FROM pg_database WHERE datname = 'dropdb_crash_test';
