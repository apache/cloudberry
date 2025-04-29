-- Test a bug where the last XLOG segment is renamed to .partial before prepared
-- transactions contained in it are recovered. This would result in segfault at
-- RecoverPreparedTransactions. The failure is well described in PG hackers:
-- https://www.postgresql.org/message-id/743b9b45a2d4013bd90b6a5cba8d6faeb717ee34.camel%40cybertec.at
-- However, in GP due to an older version of PG this error is masked by always caching 
-- the last opened xlog segment in XLogRead(). To actually trigger the failure we need to
-- create unfinished prepared transactions in TWO different xlog segments.

-- Allow extra time for mirror promotion to complete recovery to avoid
-- gprecoverseg BEGIN failures due to gang creation failure as some primaries
-- are not up. Setting these increase the number of retries in gang creation in
-- case segment is in recovery. Approximately we want to wait 30 seconds.
!\retcode gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --masteronly;
!\retcode gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly;

-- The last xlog segment is renamed to .partial only when archive mode is on
!\retcode gpconfig -c archive_mode -v on;
!\retcode gpconfig -c archive_command -v '/bin/true';
!\retcode gpstop -rai;

1: create extension if not exists gp_inject_fault;
1: create table t_rename1 (a int);
1: create table t_rename2 (a int);

-- generate an orphaned prepare transaction in first segment
1: select gp_inject_fault('dtm_broadcast_prepare', 'suspend', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;
-- assume (2), (1) are on different segments and one tuple is on the first segment.
-- the test finally double-check that.
2&: insert into t_rename1 values(2),(1);
1: select gp_wait_until_triggered_fault('dtm_broadcast_prepare', 1, dbid)
  from gp_segment_configuration where role = 'p' and content = -1;

-- start another xlog segment (will be renamed to .partial on failover)
-- start_ignore
0U: select pg_switch_xlog();
-- end_ignore

-- inject another fault and prepare transaction in the new xlog segment
1: select gp_inject_fault('dtm_broadcast_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;
1: select gp_inject_fault('dtm_broadcast_prepare', 'suspend', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;
-- assume (4), (3) are on different segments and one tuple is on the first segment.
-- the test finally double-check that.
3&: insert into t_rename2 values(2),(1);
1: select gp_wait_until_triggered_fault('dtm_broadcast_prepare', 1, dbid)
  from gp_segment_configuration where role = 'p' and content = -1;

-- shutdown primary and make sure the segment is down
-1U: select pg_ctl((SELECT datadir from gp_segment_configuration c
  where c.role='p' and c.content=0), 'stop', 'immediate');
1: select gp_request_fts_probe_scan();
1: select role, preferred_role from gp_segment_configuration where content = 0;

-- double confirm that promote succeeds.
-- also double confirm that
--  1. tuples (2) and (1) are located on two segments (thus we are testing 2pc with prepared transaction).
--  2. there are tuples on the first segment (we have been testing on the first segment).
1: insert into t_rename1 values(2),(1);
1: select gp_segment_id, * from t_rename1 order by a;

1: select gp_inject_fault('dtm_broadcast_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;
2<:
3<:

-- confirm the "orphaned" prepared trnasaction commits finally.
1: select * from t_rename1 order by a;
1: select * from t_rename2 order by a;

-- recovery the nodes
!\retcode gprecoverseg -a;
1: select wait_until_segment_synchronized(0);

!\retcode gprecoverseg -ar;
1: select wait_until_segment_synchronized(0);

-- verify the first segment is recovered to the original state.
1: select role, preferred_role from gp_segment_configuration where content = 0;

-- cleanup
1: drop table t_rename1;
1: drop table t_rename2;
!\retcode gpconfig -r gp_gang_creation_retry_count --skipvalidation;
!\retcode gpconfig -r gp_gang_creation_retry_timer --skipvalidation;
!\retcode gpconfig -r archive_mode --skipvalidation;
!\retcode gpconfig -r archive_command --skipvalidation;
!\retcode gpstop -rai;
