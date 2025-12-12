--
-- ORCA tests which require gp_fault_injector
--

CREATE SCHEMA gporca_faults;
SET search_path = gporca_faults, public;

CREATE TABLE foo (a int, b int);
INSERT INTO foo VALUES (1,1);

-- test interruption requests to optimization
select gp_inject_fault('opt_relcache_translator_catalog_access', 'reset', 1);
select gp_inject_fault('opt_relcache_translator_catalog_access', 'interrupt', 1);
select count(*) from foo;

-- Ensure that ORCA is not called on any process other than the master QD
CREATE FUNCTION func1_nosql_vol(x int) RETURNS int AS $$
BEGIN
  RETURN $1 +1;
END
$$ LANGUAGE plpgsql VOLATILE;

-- Query that runs the function on an additional non-QD master slice
-- Include the EXPLAIN to ensure that this happens in the plan.
EXPLAIN SELECT * FROM func1_nosql_vol(5), foo;

select gp_inject_fault('opt_relcache_translator_catalog_access', 'reset', 1);
select gp_inject_fault('opt_relcache_translator_catalog_access', 'interrupt', 1);
SELECT * FROM func1_nosql_vol(5), foo;

-- The fault should *not* be hit above when optimizer = off, to reset it now.
SELECT gp_inject_fault('opt_relcache_translator_catalog_access', 'reset', 1);

-- Test to check that GPOPTOptimizedPlan() does not cause std::terminate() by throwing an uncaught exception.
CREATE TABLE test_orca_uncaught_exc(a int, b int) DISTRIBUTED RANDOMLY;
-- Since ORCA cannot optimize this query, an exception is generated. We then inject a second exception when the
-- first is caught and verify that it is not propagated further (no std::terminate() is called and backend is alive).
SELECT gp_inject_fault('opt_clone_error_msg', 'skip', 1);
SELECT sum(distinct a), count(distinct b) FROM test_orca_uncaught_exc;
SELECT gp_inject_fault('opt_clone_error_msg', 'reset', 1);

-- start_ignore
DROP TABLE test_orca_uncaught_exc;
-- end_ignore
