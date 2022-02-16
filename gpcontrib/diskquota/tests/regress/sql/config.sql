--start_ignore
CREATE DATABASE diskquota;

\! gpconfig -c shared_preload_libraries -v diskquota 
\! gpstop -raf

\! gpconfig -c diskquota.naptime -v 0 
\! gpconfig -c max_worker_processes -v 20
\! gpstop -raf
--end_ignore

\c
-- Show the values of all GUC variables
SHOW diskquota.naptime;
SHOW diskquota.max_active_tables;
SHOW diskquota.worker_timeout;
