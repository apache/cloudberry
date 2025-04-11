-- start_ignore
1: create extension if not exists gp_inject_fault;
1: drop index if exists t_orphaned_r1_i, t_orphaned_c1_i,
                        t_orphaned_r2_i, t_orphaned_c2_i;
1: drop table if exists t_orphaned_h1, t_orphaned_r1, t_orphaned_c1,
                        t_orphaned_h2, t_orphaned_r2, t_orphaned_c2, t;
-- end_ignore


-- Test case 1
-- Check that orphaned files are not left on the coordinator and the standby
-- when the files are created before checkpoint

-- Create tables of different access methods and return command to check their
-- files existence on the coordinator and the standby
1: create or replace function createTables(n text) returns text as
$$
declare
  cmd text; /**/
begin
  execute 'create table t_orphaned_h'||n||'(i int) distributed by (i)'; /**/

  execute 'create table t_orphaned_r'||n||'(i int)
           with (appendonly=true, orientation=row)
           distributed by (i)'; /**/
  -- Create index to create block directory table
  execute 'create index t_orphaned_r'||n||'_i on t_orphaned_r'||n||'(i)'; /**/

  execute 'create table t_orphaned_c'||n||'(i int)
           with (appendonly=true, orientation=column)
           distributed by (i)'; /**/
  /* Create index to create block directory table */
  execute 'create index t_orphaned_c'||n||'_i on t_orphaned_c'||n||'(i)'; /**/

  /* Ensure that the mirrors have applied the filesystem changes */
  perform force_mirrors_to_catch_up(); /**/

  /* The command do not output PGDATA directories to make it possible to run
     the test without docker */
  select string_agg('cd ' || datadir || '&&' || lswc, ';' order by datadir)
  into cmd
  from (
    select 'ls ' || string_agg(pg_relation_filepath(a.unnest), ' ')
                 || ' 2>/dev/null | wc -l' lswc
    from (
      select unnest(array[('t_orphaned_h'||n)::regclass,
                          ('t_orphaned_r'||n)::regclass,
                          ('t_orphaned_r'||n||'_i')::regclass,
                          ('t_orphaned_c'||n)::regclass,
                          ('t_orphaned_c'||n||'_i')::regclass])
      union all
      select unnest(array[segrelid,
                          blkdirrelid, blkdiridxid,
                          visimaprelid, visimapidxid])
        from pg_catalog.pg_appendonly
       where relid in (('t_orphaned_r'||n)::regclass,
                       ('t_orphaned_c'||n)::regclass)
    ) a
  ) f,
  (select datadir from gp_segment_configuration where content = -1) d; /**/

  return cmd; /**/
end
$$ language plpgsql;

-- Start transaction and create tables in it before checkpoint
1: begin;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files.sh' :
             select createTables('1') check_files;

2: begin;
2: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/gp_orphaned_files.sh' :
             select createTables('2') check_files;

1: checkpoint;

-- Make sure that the tables files exist on the coordinator and the standby
1: ! sh /tmp/gp_orphaned_files.sh;

-- Get segfault on the coordinator and reconnect after its restart
1: select gp_inject_fault('exec_simple_query_start', 'segv', dbid)
     from gp_segment_configuration
    where role = 'p' and content = -1;

-- The error message can be different, so ignore it
1: @post_run 'echo ""' : select 1;
-- Wait for the coordinator to be recovered
! while [ `psql -tc "select 1;" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
1q:
2q:

1: select force_mirrors_to_catch_up();

-- Check that the tables files don't exist on the coordinator and the standby
! sh /tmp/gp_orphaned_files.sh;

-- Cleanup
! rm /tmp/gp_orphaned_files.sh;
1: drop function createTables(n text);


-- Test case 2
-- Check that orphaned files are not left on segments when the files are created
-- before checkpoint

1: create or replace function getTableSegFiles
(t regclass, out gp_contentid smallint, out filepath text)
as 'select current_setting(''gp_contentid'')::smallint, pg_relation_filepath(t)'
language sql
execute on all segments;

1: create or replace function createTables(n text) returns text as
$$
declare
  cmd text; /**/
begin
  /* Minimal fillfactor to minimize rows number for creating second main fork
     file */
  execute 'create table t_orphaned_h'||n||'(i int)
           with (fillfactor=10)
           distributed by (i)'; /**/
  /* Create the .1 file. Separate insert to create FSM. */
  execute 'insert into t_orphaned_h'||n||'
           select generate_series(1,9000000)'; /**/

  execute 'create table t_orphaned_r'||n||'(i int)
           with (appendonly=true, orientation=row)
           distributed by (i)'; /**/
  /* Create the .1 file */
  execute 'insert into t_orphaned_r'||n||'
           select generate_series(1,100)'; /**/

  /* Create the .128 file */
  execute 'create table t_orphaned_c'||n||'
           with (appendonly=true, orientation=column) as
           select i as i, i*2 as j from generate_series(1,100) i
           distributed by (i)'; /**/
  /* Create the .1 and .129 files */
  execute 'insert into t_orphaned_c'||n||'
           select i as i, i*2 as j from generate_series(1,100) i'; /**/

  /* Ensure that the mirrors have applied the filesystem changes */
  perform force_mirrors_to_catch_up(); /**/

  /* The command do not output PGDATA directories to make it possible to run
     the test without docker */
  select string_agg('cd ' || datadir || '&&' || lswc, ';' order by datadir)
  into cmd
  from (
    select gp_contentid,
           'ls ' || string_agg(f, ' ') || ' 2>/dev/null | wc -l' lswc
    from (
      select gp_contentid, filepath || suf f
        from getTableSegFiles('t_orphaned_h'||n),
             (values(''), ('.1'), ('_fsm')) v(suf)
      union all
      select gp_contentid, filepath || suf
        from getTableSegFiles('t_orphaned_r'||n),
             (values(''), ('.1')) v(suf)
      union all
      select gp_contentid, filepath || suf
        from getTableSegFiles('t_orphaned_c'||n),
             (values(''), ('.1'), ('.128'), ('.129')) v(suf)
    ) a
    group by gp_contentid
  ) f,
  (select content, datadir from gp_segment_configuration where content > -1) d
  where f.gp_contentid = d.content; /**/

  return cmd; /**/
end
$$ language plpgsql;

-- Test case 2.1
-- Segfault on all segments

-- Start transaction and create tables in it before checkpoint
1: begin;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files.sh' :
             select createTables('1') check_files;

2: begin;
2: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/gp_orphaned_files.sh' :
             select createTables('2') check_files;

1: checkpoint;

-- Make sure that all the tables files exist on the segments
1: ! sh /tmp/gp_orphaned_files.sh;

-- Get segfault on all segments
1: select gp_inject_fault('qe_exec_finished', 'segv', dbid)
     from gp_segment_configuration
    where role = 'p' and content != -1;

-- The error message can be different, so ignore it
1: @post_run 'echo ""' : select 1 from gp_dist_random('gp_id');

-- Rollback the transaction to make it possible to run queries after the error
1: rollback;
2: rollback;

1: select force_mirrors_to_catch_up();

-- Check that the tables files don't exist on the segments
! sh /tmp/gp_orphaned_files.sh;


-- Test case 2.2
-- Segfault on one segment

-- Start transaction and create tables in it before checkpoint
1: begin;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files.sh' :
             select createTables('1') check_files;

2: begin;
2: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/gp_orphaned_files.sh' :
             select createTables('2') check_files;

1: checkpoint;

-- Make sure that all the tables files exist on the segments
1: ! sh /tmp/gp_orphaned_files.sh;

-- Get segfault on a segment
1: select gp_inject_fault('qe_exec_finished', 'segv', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;

-- The error message can be different, so ignore it
1: @post_run 'echo ""' : select 1 from gp_dist_random('gp_id');

-- Rollback the transaction to make it possible to run queries after the error
1: rollback;
2: rollback;

1: select force_mirrors_to_catch_up();

-- Make a checkpoint to remove orphaned files from segments where segfault did
-- not happen
1: select gp_inject_fault_infinite('checkpoint', 'reset', dbid)
     from gp_segment_configuration
    where role = 'p' and content > -1;
1: checkpoint;

-- Check that the tables files don't exist on the segments
! sh /tmp/gp_orphaned_files.sh;


-- Cleanup
! rm /tmp/gp_orphaned_files.sh;
1: drop function createTables(n text);
1: drop function getTableSegFiles
   (t regclass, out gp_contentid smallint, out filepath text);


-- Test case 3
-- Check that table files are not deleted in the case of prepared transaction

-- Don't create checkpoints on the segment number 1
1: select gp_inject_fault_infinite('checkpoint', 'skip', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;

-- Stop after `MyPgXact->delayChkpt = false` and before `PostPrepare_smgr()`
-- Stop at the beginning of the checkpointer loop
1: select gp_inject_fault_infinite('end_prepare_two_phase', 'suspend', dbid),
          gp_inject_fault_infinite('ckpt_loop_begin', 'suspend', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;

1&: select gp_wait_until_triggered_fault('end_prepare_two_phase', 1, dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;

2&: create table t(i int) distributed by (i);
1<:

1&: select gp_wait_until_triggered_fault('ckpt_loop_begin', 1, dbid)
      from gp_segment_configuration
     where role = 'p' and content = 1;

-- Create a checkpoint and the XLOG_PENDING_DELETE WAL record with RelFileNode
-- of the created table. No more creating checkpoint
3: select gp_inject_fault_infinite('checkpoint', 'reset', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
3&: checkpoint;
1<:
1: select gp_inject_fault_infinite('ckpt_loop_end', 'suspend', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
1: select gp_inject_fault_infinite('ckpt_loop_begin', 'reset', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
1: select gp_wait_until_triggered_fault('ckpt_loop_end', 1, dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
3<:
3q:
1: select gp_inject_fault_infinite('checkpoint', 'skip', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
1: select gp_inject_fault_infinite('ckpt_loop_end', 'reset', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;

-- Get a segfault on the segment number 1 at the beginning of the prepared
-- transaction commit
1: select gp_inject_fault_infinite('finish_prepared_start_of_function', 'segv', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
1: select gp_inject_fault_infinite('end_prepare_two_phase', 'resume', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 1;
1q:
2<:
2q:

-- Check that the table files are not removed
1: select * from t;

-- Cleanup
1: drop table t;
