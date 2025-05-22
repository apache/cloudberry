-- start_ignore
drop database if exists gpperfmon;
\! rm -rf $COORDINATOR_DATA_DIRECTORY/gpperfmon/conf/gpperfmon.conf
\! gpperfmon_install --enable --port $PGPORT --password 123
\! cp sql/gpperfmon.conf $COORDINATOR_DATA_DIRECTORY/gpperfmon/conf/
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
