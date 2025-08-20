-- start_ignore
create extension if not exists gp_inject_fault;
drop index if exists t_orphaned_r_i, t_orphaned_c_i;
drop table if exists t_orphaned_h, t_orphaned_r, t_orphaned_c,
                     t_top, t_sub1, t_sub2;
-- Increase the number of connection attempts to a segment to 120, reduce
-- the interval between attempts to 1 second. So the segments will have 120
-- seconds to recover after segfault.  The demo cluser don't fail over to 
-- a mirror if 120 second is enough for recovery
\! gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly
\! gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --masteronly
\! gpstop -u
-- end_ignore

-- start_matchsubs
-- m/ERROR:  Error on receive from seg\d+ slice\d+ \d+.\d+.\d+.\d+:\d+ pid=\d+: server closed the connection unexpectedly/
-- s/ERROR:  Error on receive from seg\d+ slice\d+ \d+.\d+.\d+.\d+:\d+ pid=\d+: server closed the connection unexpectedly/ERROR:  Error on receive from segX sliceX X.X.X.X:X pid=X: server closed the connection unexpectedly/
-- end_matchsubs

-- Test case 1
-- Check that orphaned files are not left on the coordinator and the standby
-- when the files are created after checkpoint

-- Create tables of different access methods and return command to check their
-- files existence on the coordinator and the standby
create or replace function createTables() returns text as
$$
declare
  cmd text;
begin
  create table t_orphaned_h(i int)
  distributed by (i);

  create table t_orphaned_r(i int)
  with (appendonly=true, orientation=row)
  distributed by (i);
  -- Create index to create block directory table
  create index t_orphaned_r_i on t_orphaned_r(i);

  create table t_orphaned_c(i int)
  with (appendonly=true, orientation=column)
  distributed by (i);
  -- Create index to create block directory table
  create index t_orphaned_c_i on t_orphaned_c(i);

  -- Ensure that the mirrors have applied the filesystem changes
  perform force_mirrors_to_catch_up();

  -- The command do not output PGDATA directories to make it possible to run
  -- the test without docker
  select '\! ' ||
         string_agg('cd ' || datadir || '&&' || lswc, ';' order by datadir)
  into cmd
  from (
    select 'ls ' || string_agg(pg_relation_filepath(a.unnest), ' ')
                 || ' 2>/dev/null | wc -l' lswc
    from (
      select unnest(array['t_orphaned_h'::regclass,
                          't_orphaned_r'::regclass, 't_orphaned_r_i'::regclass,
                          't_orphaned_c'::regclass, 't_orphaned_c_i'::regclass])
      union all
      select unnest(array[segrelid,
                          blkdirrelid, blkdiridxid,
                          visimaprelid, visimapidxid])
        from pg_catalog.pg_appendonly
       where relid in ('t_orphaned_r'::regclass, 't_orphaned_c'::regclass)
    ) a
  ) f,
  (select datadir from gp_segment_configuration where content = -1) d;

  return cmd;
end
$$ language plpgsql;

checkpoint;

-- Skip checkpoints on the coordinator
select gp_inject_fault_infinite('checkpoint', 'skip', dbid)
  from gp_segment_configuration
 where role = 'p' and content = -1;

-- Create tables in subtransactions
begin;
create table t_top(i int) distributed by (i);
savepoint sp1;
create table t_sub1(i int) distributed by (i);
savepoint sp2;
create table t_sub2(i int) distributed by (i);
commit;

-- Start transaction and create tables in it
begin;
select createTables() check_files
\gset
 
-- Make sure that the tables files exist on the coordinator and the standby
:check_files

-- Get segfault on the coordinator and reconnect after its restart
select gp_inject_fault('exec_simple_query_start', 'segv', dbid)
  from gp_segment_configuration
 where role = 'p' and content = -1;

-- The error message from psql can be different, so ignore it
\! psql postgres -c "select 1" 2> /dev/null
-- Wait for the coordinator to be recovered
\! while [ `psql -tc "select 1;" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
\c regression

-- All the inject faults have been reset after the coordinator restart

select force_mirrors_to_catch_up();

-- Check that the tables files don't exist on the coordinator and the standby
:check_files

-- Check that the coordinator recovery didn't remove files of the tables which
-- were created in subtransactions
table t_sub1;
table t_sub2;

-- Clean up
drop table t_top, t_sub1, t_sub2;
\unset check_files


-- Test case 2
-- Check that files are left untouched on the coordinator and the standby
-- when the corresponding distributed commit record exists in WAL
select gp_inject_fault('dtm_xlog_distributed_commit', 'segv', dbid)
  from gp_segment_configuration
 where role = 'p' and content = -1;

-- Create tables in a transaction. Get segfault right after the distributed
-- commit record is flushed
\! psql regression -c "begin; select createTables(); commit;"
-- Wait for the coordinator to be recovered
\! while [ `psql -tc "select 1;" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
\c regression

select force_mirrors_to_catch_up();

-- Check that all the tables and its indexes files exist
select '\! ' ||
       string_agg('cd ' || datadir || '&&' || lswc, ';' order by datadir) lswc
  from (
    select 'ls ' || string_agg(pg_relation_filepath(a.unnest), ' ')
                 || ' 2>/dev/null | wc -l' lswc
    from (
      select unnest(array['t_orphaned_h'::regclass,
                          't_orphaned_r'::regclass, 't_orphaned_r_i'::regclass,
                          't_orphaned_c'::regclass, 't_orphaned_c_i'::regclass])
      union all
      select unnest(array[segrelid,
                          blkdirrelid, blkdiridxid,
                          visimaprelid, visimapidxid])
        from pg_catalog.pg_appendonly
       where relid in ('t_orphaned_r'::regclass, 't_orphaned_c'::regclass)
    ) a
  ) f,
  (select datadir from gp_segment_configuration where content = -1) d
\gset

:lswc

-- Check that we can read data from the tables
table t_orphaned_h;
table t_orphaned_r;
table t_orphaned_c;

-- Clean up
drop table t_orphaned_h, t_orphaned_r, t_orphaned_c;
drop function createTables();


-- Test case 3
-- Check that orphaned files are not left on segments when the files are
-- created after checkpoint

create or replace function getTableSegFiles
(t regclass, out gp_contentid smallint, out filepath text)
as 'select current_setting(''gp_contentid'')::smallint, pg_relation_filepath(t)'
language sql
execute on all segments;

-- Get list of the tables file names on each segment
create or replace function createTables() returns text as
$$
declare
  cmd text;
begin
  -- Minimal fillfactor to minimize rows number for creating second main fork file
  create table t_orphaned_h(i int)
  with (fillfactor=10)
  distributed by (i);
  -- Create the .1 file. Separate insert to create FSM. 
  insert into t_orphaned_h select generate_series(1,9000000);

  create table t_orphaned_r(i int)
  with (appendonly=true, orientation=row)
  distributed by (i);
  -- Create the .1 file
  insert into t_orphaned_r select generate_series(1,100);

  -- Create the .128 file
  create table t_orphaned_c
  with (appendonly=true, orientation=column) as
  select i as i, i*2 as j from generate_series(1,100) i
  distributed by (i);
  -- Create the .1 and .129 files
  insert into t_orphaned_c
  select i as i, i*2 as j from generate_series(1,100) i;

  -- Ensure that the mirrors have applied the filesystem changes
  perform force_mirrors_to_catch_up();

  -- The command do not output PGDATA directories to make it possible to run
  -- the test without docker
  select '\! ' ||
         string_agg('cd ' || datadir || '&&' || lswc, ';' order by datadir)
  into cmd
  from (
    select gp_contentid,
           'ls ' || string_agg(f, ' ') || ' 2>/dev/null | wc -l' lswc
    from (
      select gp_contentid, filepath || suf f
        from getTableSegFiles('t_orphaned_h'),
             (values(''), ('.1'), ('_fsm')) v(suf)
      union all
      select gp_contentid, filepath || suf
        from getTableSegFiles('t_orphaned_r'),
             (values(''), ('.1')) v(suf)
      union all
      select gp_contentid, filepath || suf
        from getTableSegFiles('t_orphaned_c'),
             (values(''), ('.1'), ('.128'), ('.129')) v(suf)
    ) a
    group by gp_contentid
  ) f,
  (select content, datadir from gp_segment_configuration where content > -1) d
  where f.gp_contentid = d.content;

  return cmd;
end
$$ language plpgsql;

create or replace function resetInjectFaults(p_contentid int) returns void as
$$
begin
  perform gp_inject_fault('qe_exec_finished', 'reset', dbid),
          gp_inject_fault('checkpoint',       'reset', dbid)
  from gp_segment_configuration
  where role = 'p' and content = p_contentid;
end
$$ language plpgsql;

-- Skip FTS probes
select gp_inject_fault_infinite('fts_probe', 'skip', 1);

-- Test case 3.1
-- Segfault on all segments
checkpoint;

-- Skip checkpoints
select gp_inject_fault_infinite('checkpoint', 'skip', dbid)
  from gp_segment_configuration
 where role = 'p' and content > -1;

-- Create tables in subtransactions
begin;
create table t_top(i int) distributed by (i);
savepoint sp1;
create table t_sub1(i int) distributed by (i);
savepoint sp2;
create table t_sub2(i int) distributed by (i);
commit;

-- Start transaction and create tables in it
begin;
select createTables() check_files
\gset

-- Make sure that all the tables files exist on the segments
:check_files

-- Get segfault on all segments
select gp_inject_fault('qe_exec_finished', 'segv', dbid)
  from gp_segment_configuration
 where role = 'p' and content != -1;

select 1 from gp_dist_random('gp_id');

-- Rollback the transaction to make it possible to run queries after the error
rollback;

-- start_ignore
-- The MPP operation can be cancelled on some segments, because the cancel
-- request can come faster than the segfault happens on these segments. So we
-- should reset the qe_exec_finished inject fault explicitly to avoid segfaults.
select resetInjectFaults(0);
select resetInjectFaults(1);
select resetInjectFaults(2);
-- end_ignore

select force_mirrors_to_catch_up();

-- Make a checkpoint to remove orphaned files from segments where segfault did
-- not happen
checkpoint;

-- Check that the tables files don't exist on the segments
:check_files

-- Check that the segments recovery didn't remove files of the tables which
-- were created in subtransactions
table t_sub1;
table t_sub2;

-- Clean up
drop table t_top, t_sub1, t_sub2;


-- Test case 3.2
-- Segfault on one segment
checkpoint;

-- Skip checkpoints
select gp_inject_fault_infinite('checkpoint', 'skip', dbid)
  from gp_segment_configuration
 where role = 'p' and content > -1;

-- Create tables in subtransactions
begin;
create table t_top(i int) distributed by (i);
savepoint sp1;
create table t_sub1(i int) distributed by (i);
savepoint sp2;
create table t_sub2(i int) distributed by (i);
commit;

-- Start transaction and create tables in it
begin;
select createTables() check_files
\gset

-- Make sure that all the tables files exist on the segments
:check_files

-- Get segfault on a segment
select gp_inject_fault('qe_exec_finished', 'segv', dbid)
  from gp_segment_configuration
 where role = 'p' and content = 1;

select 1 from gp_dist_random('gp_id');

-- Rollback the transaction to make it possible to run queries after the error
rollback;

select force_mirrors_to_catch_up();

-- Make a checkpoint to remove orphaned files from segments where segfault did
-- not happen
select gp_inject_fault_infinite('checkpoint', 'reset', dbid)
  from gp_segment_configuration
 where role = 'p' and content > -1;
checkpoint;

-- Check that the tables files don't exist on the segments
:check_files

-- Check that the segment recovery didn't remove files of the tables which
-- were created in subtransactions
table t_sub1;
table t_sub2;


select gp_inject_fault_infinite('fts_probe', 'reset', 1);


-- Clean up
\unset check_files
drop table t_top, t_sub1, t_sub2;
drop function createTables();
drop function resetInjectFaults(p_contentid int);
drop function getTableSegFiles(t regclass, out gp_contentid smallint, out filepath text);
-- start_ignore
\! gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly
\! gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly
\! gpstop -u
-- end_ignore
