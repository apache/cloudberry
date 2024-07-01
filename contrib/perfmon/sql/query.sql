-- start_ignore
-- wait a while as sometimes the gpmmon is not ready
\c gpperfmon
CREATE OR REPLACE FUNCTION wait_for_gpmmon_work() RETURNS void AS $$
DECLARE
DECLARE
start_time timestamptz := clock_timestamp();
updated bool;
BEGIN
	-- we don't want to wait forever; loop will exit after 60 seconds
	FOR i IN 1 .. 1000 LOOP
		SELECT(SELECT count(*) > 0 from queries_history ) INTO updated;
		EXIT WHEN updated;

		-- wait a little
		PERFORM pg_sleep_for('100 milliseconds');
	END LOOP;
	-- report time waited in postmaster log (where it won't change test output)
	RAISE log 'wait_for_gpmmon_work delayed % seconds',
	EXTRACT(epoch FROM clock_timestamp() - start_time);
END
$$ LANGUAGE plpgsql;
select wait_for_gpmmon_work();
\c contrib_regression
select sess_id from pg_stat_activity where pg_backend_pid()=pid;
\gset
-- end_ignore

CREATE TABLE foo(a int);
CREATE TABLE test(a int);
INSERT INTO foo SELECT generate_series(0,10);
INSERT INTO test SELECT generate_series(0,10);
select count(*) from foo,test where foo.a=test.a;
DROP TABLE foo;
DROP TABLE test;

\c gpperfmon
select pg_sleep(100);
analyze system_history;
analyze database_history;
analyze diskspace_history;
analyze queries_history;
select count(*) > 0 from system_now;
select count(*) > 0 from database_now;
select count(*) > 0 from diskspace_now;
select count(*) > 0 from system_history;
select count(*) > 0 from database_history;
select count(*) > 0 from diskspace_history;

select status, query_text, length(query_plan) > 0 from queries_history
where ssid = :sess_id and 
query_text = 'select count(*) from foo,test where foo.a=test.a;';
