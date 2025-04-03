-- Try to verify that a session fatal due to OOM should have no effect on other sessions.
-- Report on https://github.com/greenplum-db/gpdb/issues/12399

create extension if not exists gp_inject_fault;

1: select gp_inject_fault('make_dispatch_result_error', 'skip', dbid) from gp_segment_configuration where role = 'p' and content = -1;
2: begin;

-- session1 will be fatal.
1: select count(*) > 0 from gp_dist_random('pg_class');

-- session2 should be ok.
2: select count(*) > 0 from gp_dist_random('pg_class');
2: commit;
1q:
2q:

select gp_inject_fault('make_dispatch_result_error', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = -1;

--
-- Test case for the WaitEvent of ShareInputScan
--

create table test_waitevent(i int);
insert into test_waitevent select generate_series(1,1000);

1: set optimizer = off;
1: set gp_cte_sharing to on;
1: set max_parallel_workers_per_gather = 0;
1: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'suspend', 2);
1&: WITH a1 as (select * from test_waitevent), a2 as (select * from test_waitevent) SELECT sum(a1.i)  FROM a1 INNER JOIN a2 ON a2.i = a1.i  UNION ALL SELECT count(a1.i)  FROM a1 INNER JOIN a2 ON a2.i = a1.i;
-- start_ignore
-- query pg_stat_get_activity on segment to watch the ShareInputScan event
2: copy (select pg_stat_get_activity(NULL) from gp_dist_random('gp_id') where gp_segment_id=0) to '/tmp/_gpdb_test_output.txt';
-- end_ignore
2: select gp_wait_until_triggered_fault('shareinput_writer_notifyready', 1, 2);
2: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'resume', 2);
2: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'reset', 2);
2q:
1<:
1q:

!\retcode grep ShareInputScan /tmp/_gpdb_test_output.txt;

--
-- Test for issue https://github.com/greenplum-db/gpdb/issues/12703
--

-- Case for cdbgang_createGang_async
1: create table t_12703(a int);

1:begin;
-- make a cursor so that we have a named portal
1: declare cur12703 cursor for select * from t_12703;

2: select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=1), 'stop');
-- next sql will trigger FTS to mark seg1 as down
2: select gp_request_fts_probe_scan();
!\retcode gpfts -A -D;
-- sleep some seconds until the promotion of mirror 0 is done
2: select pg_sleep(2);

-- this will go to cdbgang_createGang_async's code path
-- for some segments are DOWN. It should not PANIC even
-- with a named portal existing.
1: select * from t_12703;
1: abort;

1q:
2q:

-- Case for cdbCopyEndInternal
-- Provide some data to copy in
4: insert into t_12703 select * from generate_series(1, 10)i;
4: copy t_12703 to '/tmp/t_12703';
-- make copy in statement hang at the entry point of cdbCopyEndInternal
4: select gp_inject_fault('cdb_copy_end_internal_start', 'suspend', dbid) from gp_segment_configuration where role = 'p' and content = -1;
4q:
1&: copy t_12703 from '/tmp/t_12703';
select gp_wait_until_triggered_fault('cdb_copy_end_internal_start', 1, dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- make Gang connection is BAD
select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=2), 'stop');
!\retcode gpfts -A -D;
2: select gp_request_fts_probe_scan();
2: begin;
select gp_inject_fault('cdb_copy_end_internal_start', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- continue copy it should not PANIC
1<:
1q:
-- session 2 still alive (means not PANIC happens)
2: select 1;
2: end;
2q:

!\retcode gprecoverseg -aF --no-progress;

-- loop while segments come in sync
!\retcode gpfts -A -D;
select wait_until_all_segments_synchronized();

!\retcode gprecoverseg -ar;

-- loop while segments come in sync
!\retcode gpfts -A -D;
select wait_until_all_segments_synchronized();

-- verify no segment is down after recovery
select count(*) from gp_segment_configuration where status = 'd';
