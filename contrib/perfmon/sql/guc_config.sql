-- Disable perfmon.enable
-- start_ignore
\! gpconfig -c perfmon.enable -v false
\! gpstop -ari
-- end_ignore
\! ps -ef | grep '\[gpmmon\]' | wc -l
\c gpperfmon
show perfmon.enable;

-- start_ignore
\! gpconfig -c perfmon.enable -v true 
\! gpconfig -c perfmon.port -v 8848
\! gpstop -ari
-- end_ignore
\! ps -ef | grep '\[gpmmon\]' | wc -l
\c gpperfmon
show perfmon.enable;
show perfmon.port;
CREATE OR REPLACE FUNCTION wait_for_gpsmon_work() RETURNS void AS $$
DECLARE
start_time timestamptz := clock_timestamp();
updated bool;
starttime timestamptz;
BEGIN
	select COALESCE(ctime,CURRENT_TIMESTAMP) from diskspace_now into starttime;
	-- we don't want to wait forever; loop will exit after 60 seconds
	FOR i IN 1 .. 600 LOOP
		SELECT(SELECT count(*) > 0 from diskspace_now 
			WHERE ctime > starttime) INTO updated;
		EXIT WHEN updated;

		-- wait a little
		PERFORM pg_sleep_for('100 milliseconds');
	END LOOP;
	-- report time waited in postmaster log (where it won't change test output)
	RAISE log 'wait_for_gpsmon_work delayed % seconds',
	EXTRACT(epoch FROM clock_timestamp() - start_time);
END
$$ LANGUAGE plpgsql;
select wait_for_gpsmon_work(); 
select count(*) from diskspace_now;
\! netstat -anp | grep udp | grep gpsmon | wc -l
\! ps -ef | grep gpsmon | grep -v grep | wc -l
