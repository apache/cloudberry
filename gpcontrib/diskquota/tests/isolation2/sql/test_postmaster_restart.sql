!\retcode gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null;
!\retcode gpstop -u > /dev/null;

1: CREATE SCHEMA postmaster_restart_s;
1: SET search_path TO postmaster_restart_s;

1: SELECT diskquota.set_schema_quota('postmaster_restart_s', '1 MB');
1: SELECT diskquota.wait_for_worker_new_epoch();

-- expect fail
1: CREATE TABLE t1 AS SELECT generate_series(1,10000000);
1q:

-- launcher should exist
-- [p]ostgres is to filter out the pgrep itself
!\retcode pgrep -f "[p]ostgres.*launcher";
-- bgworker should exist
!\retcode pgrep -f "[p]ostgres.*diskquota.*isolation2test";

-- stop postmaster
!\retcode pg_ctl -D $MASTER_DATA_DIRECTORY -w stop;

-- launcher should be terminated
!\retcode pgrep -f "[p]ostgres.*launcher";
-- bgworker should be terminated
!\retcode pgrep -f "[p]ostgres.*diskquota.*isolation2test";

-- start postmaster
-- -E needs to be changed to "-c gp_role=dispatch" for GPDB7
-- See https://github.com/greenplum-db/gpdb/pull/9396
!\retcode pg_ctl -D $MASTER_DATA_DIRECTORY -w -o "-E" start;
-- Hopefully the bgworker can be started in 5 seconds
!\retcode sleep 5;

-- launcher should be restarted
!\retcode pgrep -f "[p]ostgres.*launcher";
-- bgworker should be restarted
!\retcode pgrep -f "[p]ostgres.*diskquota.*isolation2test";

1: SET search_path TO postmaster_restart_s;
1: SELECT diskquota.wait_for_worker_new_epoch();
-- expect fail
1: CREATE TABLE t2 AS SELECT generate_series(1,10000000);
-- enlarge the quota limits
1: SELECT diskquota.set_schema_quota('postmaster_restart_s', '100 MB');
1: SELECT diskquota.wait_for_worker_new_epoch();
-- expect succeed
1: CREATE TABLE t3 AS SELECT generate_series(1,1000000);

1: DROP SCHEMA postmaster_restart_s CASCADE;
1q:
