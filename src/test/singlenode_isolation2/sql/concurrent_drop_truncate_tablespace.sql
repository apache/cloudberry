-- While a tablespace is being dropped, if any table is created
-- in the same tablespace, only one query can be successful.
-- The behavior guarantees that the table will never use a
-- dropped or invalid tablespace.

-- start_matchsubs
-- m/ERROR:  could not create directory "pg_tblspc.*: No such file or directory/
-- s/ERROR:  could not create directory "pg_tblspc.*: No such file or directory/ERROR:  could not create directory "pg_tblspc\/XXXX":  No such file or directory/
-- end_matchsubs

-- create a tablespace directory
!\retcode rm -rf /tmp/concurrent_tblspace;
!\retcode mkdir -p /tmp/concurrent_tblspace;

CREATE TABLESPACE concurrent_tblspace LOCATION '/tmp/concurrent_tblspace';

-- test 1:
-- when creating a table using a tablespace, after the tuple of tablespace
-- is locked, the tablespace is not allowed to drop
2: begin;
2: CREATE TABLE t_in_tablespace(a int, b int) TABLESPACE concurrent_tblspace;

-- drop tablespace will fail: can't acuqire the lock
DROP TABLESPACE concurrent_tblspace;
2: rollback;

-- test 2:
-- if  DROP TABLESPACE acquires lock first and rollback, the blocking CREATE
-- TABLE will be successful.

-- suspend execution after tablespace lock is acquired
SELECT gp_inject_fault('drop_tablespace_after_acquire_lock', 'suspend', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';
1&: DROP TABLESPACE concurrent_tblspace;

-- wait for the fault to be triggered
SELECT gp_wait_until_triggered_fault('drop_tablespace_after_acquire_lock', 1, dbid)
   from gp_segment_configuration where content <> -1 and role='p';

2&: CREATE TABLE t_in_tablespace(a int, b int) TABLESPACE concurrent_tblspace;
-- inject an error to ensure that the above DROP command will rollback
SELECT gp_inject_fault('after_xlog_tblspc_drop', 'error', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';
SELECT gp_inject_fault('drop_tablespace_after_acquire_lock', 'reset', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';
-- fail
1<:
-- success
2<:
-- drop the above table, so the tablespace is empty.
2: DROP TABLE t_in_tablespace;
SELECT gp_inject_fault('after_xlog_tblspc_drop', 'reset', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';

-- test 3:
-- if DROP TABLESPACE acquires lock first and going to drop, any CREATE TABLE
-- will fail

-- suspend execution after tablespace lock is acquired
SELECT gp_inject_fault('drop_tablespace_after_acquire_lock', 'suspend', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';
1&: DROP TABLESPACE concurrent_tblspace;

-- wait for the fault to be triggered
SELECT gp_wait_until_triggered_fault('drop_tablespace_after_acquire_lock', 1, dbid)
   from gp_segment_configuration where content <> -1 and role='p';

-- create a table in the same tablespace which is being dropped via a concurrent session
2&:CREATE TABLE drop_tablespace_tbl(a int, b int) TABLESPACE concurrent_tblspace DISTRIBUTED BY (a);
-- reset the fault, drop tablespace command will continue
SELECT gp_inject_fault('drop_tablespace_after_acquire_lock', 'reset', dbid) FROM gp_segment_configuration WHERE content <> -1 and role='p';
-- success
1<:
-- fail
2<:
