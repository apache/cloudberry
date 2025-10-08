-- start_ignore
-- Increase the number of connection attempts to a segment to 120, reduce
-- the interval between attempts to 1 second. So the segments will have 120
-- seconds to recover after segfault.
! gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly;
! gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --masteronly;
! gpstop -u;
1: create extension if not exists gp_inject_fault;
-- end_ignore

include: helpers/server_helpers.sql;

1: create or replace function getTableSegFiles
(t regclass, out gp_contentid smallint, out filepath text)
as 'select current_setting(''gp_contentid'')::smallint, pg_relation_filepath(t)'
language sql
execute on all segments;

1: create or replace function createTables(n text, mirror_catch_up bool default true) returns text as
$$
declare
  cmd text; /**/
begin
  execute 'create table t_orphaned_h'||n||'(i int)
           distributed by (i)'; /**/
  execute 'insert into t_orphaned_h'||n||'
           select generate_series(1,100)'; /**/

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

  if mirror_catch_up then
	/* Ensure that the mirrors have applied the filesystem changes */
	perform force_mirrors_to_catch_up(); /**/
  end if; /**/

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

-- A copy of standard 'force_mirrors_to_catch_up()', but it forces all mirrors
-- except the one specified by the argument
-- (should be used in case one of mirrors is currently down).
1: create or replace function force_mirrors_to_catch_up_with_exception(excluded_content int) returns void as
$$
begin
	perform pg_switch_xlog(); /**/
	perform pg_switch_xlog() from gp_dist_random('gp_id'); /**/
	perform gp_inject_fault('after_xlog_redo_noop', 'sleep', dbid) from gp_segment_configuration where role='m' and content <> excluded_content; /**/
	perform insert_noop_xlog_record(); /**/
	perform insert_noop_xlog_record() from gp_dist_random('gp_id'); /**/
	perform gp_wait_until_triggered_fault('after_xlog_redo_noop', 1, dbid) from gp_segment_configuration where role='m' and content <> excluded_content; /**/
	perform gp_inject_fault('after_xlog_redo_noop', 'reset', dbid) from gp_segment_configuration where role='m' and content <> excluded_content; /**/
end
$$ language plpgsql;

-- Test case 1
-- Check removal of orphaned files together with mirror promotion

-- Start transaction and create tables in it before checkpoint
1: begin;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files_tx1.sh' :
             select createTables('_tx1');

-- Let 2nd transaction to commit
2: begin;
2: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files_tx2.sh' :
             select createTables('_tx2');
2: commit;
1: checkpoint;

-- Create another bunch of tables after savepoint
1: savepoint sp1;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/gp_orphaned_files_tx1.sh' :
             select createTables('_tx1_sp1');

-- Make sure that all the tables files exist on the segments
1: ! sh /tmp/gp_orphaned_files_tx1.sh;

-- shutdown primary and make sure the segment is down
-1U: select pg_ctl((SELECT datadir from gp_segment_configuration c
  where c.role='p' and c.content=0), 'stop', 'immediate');
select gp_request_fts_probe_scan();
select role, preferred_role, status from gp_segment_configuration where content = 0;

-- Rollback the transaction to make it possible to run queries after the error
1: rollback;

-- Make a checkpoint to remove orphaned files from segments that are still up
1: checkpoint;

1: select force_mirrors_to_catch_up_with_exception(0);

-- Check that the tables files don't exist on the segments (except ex-primary 0, which is yet down)
! sh /tmp/gp_orphaned_files_tx1.sh;

-- recovery the nodes
!\retcode gprecoverseg -a;
select wait_until_segment_synchronized(0);

-- Check that the tables files don't exist on all segments now
! sh /tmp/gp_orphaned_files_tx1.sh;

!\retcode gprecoverseg -ar;
select wait_until_segment_synchronized(0);

-- verify the first segment is recovered to the original state.
select role, preferred_role, status from gp_segment_configuration where content = 0;

-- Check that the tables from the committed transaction still exist
! sh /tmp/gp_orphaned_files_tx2.sh;

drop table t_orphaned_h_tx2, t_orphaned_r_tx2, t_orphaned_c_tx2;

-- Test case 2
-- Check that orphaned files are not removed after prepare is done
-- together with mirror promotion
-- and with orphaned files created (and later cleaned up) when the mirror is down.

-- Start transaction and create tables in it before checkpoint
1: begin;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files_tx1.sh' :
             select createTables('_tx1');

-- Let 2nd transaction to commit
2: begin;
2: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files_tx2.sh' :
             select createTables('_tx2');
2: commit;
1: checkpoint;

-- Create another bunch of tables after savepoint
1: savepoint sp1;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/gp_orphaned_files_tx1.sh' :
             select createTables('_tx1_sp1');

-- Make sure that all the tables files exist on the segments
1: ! sh /tmp/gp_orphaned_files_tx1.sh;

-- Suspend commit after prepare
select gp_inject_fault('dtm_broadcast_prepare', 'suspend', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;

1&: commit;
select gp_wait_until_triggered_fault('dtm_broadcast_prepare', 1, dbid)
  from gp_segment_configuration where role = 'p' and content = -1;

-- shutdown primary and make sure the segment is down
-1U: select pg_ctl((SELECT datadir from gp_segment_configuration c
  where c.role='p' and c.content=0), 'stop', 'immediate');
select gp_request_fts_probe_scan();
select role, preferred_role, status from gp_segment_configuration where content = 0;

3: begin;
3: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' > /tmp/gp_orphaned_files_tx3.sh' :
             select createTables('_tx3', false);

-- Get segfault on a segment
3: select gp_inject_fault('qe_exec_finished', 'segv', dbid)
     from gp_segment_configuration
    where role = 'p' and content = 0;

-- The error message can be different, so ignore it
3: @post_run 'echo ""' : select 1 from gp_dist_random('gp_id');

3: rollback;
3: checkpoint;

3: select force_mirrors_to_catch_up_with_exception(0);

! sh /tmp/gp_orphaned_files_tx3.sh;

-- recovery the nodes
!\retcode gprecoverseg -a;
select wait_until_segment_synchronized(0);

!\retcode gprecoverseg -ar;
select wait_until_segment_synchronized(0);

-- verify the first segment is recovered to the original state.
select role, preferred_role, status from gp_segment_configuration where content = 0;

select gp_inject_fault('dtm_broadcast_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = -1;
1<:

-- Check that the tables from the committed transactions still exist
! sh /tmp/gp_orphaned_files_tx1.sh;
! sh /tmp/gp_orphaned_files_tx2.sh;

-- Check that the tables from the not committed transaction don't exist
! sh /tmp/gp_orphaned_files_tx3.sh;

-- Cleanup
drop table t_orphaned_h_tx1, t_orphaned_r_tx1, t_orphaned_c_tx1;
drop table t_orphaned_h_tx1_sp1, t_orphaned_r_tx1_sp1, t_orphaned_c_tx1_sp1;
drop table t_orphaned_h_tx2, t_orphaned_r_tx2, t_orphaned_c_tx2;

drop function force_mirrors_to_catch_up_with_exception(excluded_content int);
drop function createTables(n text, mirror_catch_up bool);
drop function getTableSegFiles(t regclass, out gp_contentid smallint, out filepath text);

! rm /tmp/gp_orphaned_files_tx1.sh;
! rm /tmp/gp_orphaned_files_tx2.sh;
! rm /tmp/gp_orphaned_files_tx3.sh;
-- start_ignore
! gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly;
! gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly;
! gpstop -u;
-- end_ignore
