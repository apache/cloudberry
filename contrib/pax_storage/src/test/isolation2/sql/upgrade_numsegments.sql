-- This test case is used to test if we can get the correct
-- numsegments when the connection is in utility mode.
-- With introducing the numsegments in gp_distribution_policy
-- pg_upgrade can not run correctly because it is run in
-- utility mode and the numsegments can not get correctly in
-- utility mode, now we support to get the numsegments in
-- utility mode when the connection is running for pg_upgrade.
-- We can sure that CREATE TABLE command will try to get
-- numsegments in utility mode, so we use it to test the
-- function.
-1U: create temp table t1(c1 int, c2 int);
0U: create temp table t1(c1 int, c2 int);
1U: create temp table t1(c1 int, c2 int);
2U: create temp table t1(c1 int, c2 int);
-- start_ignore
-1Uq:
0Uq:
1Uq:
2Uq:
-- end_ignore
