-- start_ignore
drop database if exists gpperfmon;
\! gpperfmon_install --enable --port $PGPORT --password 123
\! gpstop -ari
-- end_ignore
\c contrib_regression
select
	case
		when setting = 'on'  then 'perfmon is running'
	else
		'perfmon is not running'
	end
from pg_settings
where name='perfmon.enable';
