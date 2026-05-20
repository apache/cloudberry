-- Vacuum every day at 10:00am (GMT)
create task vacuum_db SCHEDULE '0 10 * * *' AS 'vacuum';

-- Stop scheduling a task
drop task vacuum_db;

-- Invalid input: missing parts
create task missing_parts schedule '* * * *' as 'select 1';

-- Invalid input: trailing characters
create task trail_char schedule '5 secondc' as 'select 1';
create task trail_char schedule '50 seconds c' as 'select 1';

-- Invalid input: seconds out of range
create task invalid_seconds schedule '-1 seconds' as 'select 1';
create task invalid_seconds schedule '0 seconds' as 'select 1';
create task invalid_seconds schedule '60 seconds' as 'select 1';
create task invalid_seconds schedule '1000000000000 seconds' as 'select 1';

-- Vacuum every day at 10:00am (GMT)
create task vacuum_db SCHEDULE '0 10 * * *' AS 'vacuum';
select schedule, command, active, jobname from pg_task order by jobid;

-- Make that 11:00am (GMT)
alter task vacuum_db schedule '0 11 * * *';
select schedule, command, active, jobname from pg_task order by jobid;

-- Make that VACUUM FULL
alter task vacuum_db as 'vacuum full';
select schedule, command, active, jobname from pg_task order by jobid;

-- Update to a non existing database
alter task vacuum_db database hopedoesnotexist;

-- Create a database that does not allow connection
create database task_dbno;
revoke CONNECT on DATABASE task_dbno from PUBLIC;

-- create a test user
create user task_cron with password 'pwd';

-- Create a task for another user
create task another_user_task schedule '* 10 * * *'  database task_dbno user task_cron as 'vacuum';

-- Schedule a task for this user on the database that does not accept connections
alter task vacuum_db database task_dbno user task_cron;

-- Schedule a task that user doest not exist
alter task vacuum_db user hopedoesnotexist;

-- valid interval tasks
create task valid_task_1 schedule '1 second' as 'select 1';
create task valid_task_2 schedule ' 30 sEcOnDs ' as 'select 1';
create task valid_task_3 schedule '59 seconds' as 'select 1';
create task valid_task_4 schedule '17  seconds ' as 'select 1';

-- task in function
DO $$
BEGIN
	create task fn_task schedule '5 * * * * ' as 'select 1';
END $$;
select jobname from pg_task where jobname = 'fn_task';

DO $$
BEGIN
	alter task fn_task schedule '6 * * * *';
END $$;
select schedule from pg_task where jobname = 'fn_task';

DO $$
DECLARE
	jn text;
BEGIN
	SELECT jobname from pg_task where jobname = 'fn_task' into jn;
	EXECUTE FORMAT('DROP TASK %I', jn);
END $$;
select jobname from pg_task where jobname = 'fn_task';

-- clean up
drop database task_dbno;
drop user task_cron;

drop task vacuum_db;
drop task valid_task_1;
drop task valid_task_2;
drop task valid_task_3;
drop task valid_task_4;
