-- start_ignore
drop database if exists gpperfmon;
\! gpperfmon_install --enable --port $PGPORT --password 123
\! gpstop -ari
-- end_ignore
-- check cluster state
\c postgres
select pg_sleep(10);
SELECT sync_state FROM pg_stat_get_wal_senders();
\c contrib_regression
select
	case
		when setting = 'on'  then 'perfmon is running'
	else
		'perfmon is not running'
	end
from pg_settings
where name='perfmon.enable';
