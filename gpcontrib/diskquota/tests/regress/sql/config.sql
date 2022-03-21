--start_ignore
CREATE DATABASE diskquota;

\! gpconfig -c shared_preload_libraries -v $(./data/current_binary_name);
\! gpconfig -c diskquota.naptime -v 0 --skipvalidation
\! gpconfig -c max_worker_processes -v 20 --skipvalidation
\! gpconfig -c diskquota.hard_limit -v "off" --skipvalidation

\! gpstop -raf
--end_ignore

\c
-- Show the values of all GUC variables
-- start_ignore
SHOW diskquota.naptime;
-- end_ignore
SHOW diskquota.max_active_tables;
SHOW diskquota.worker_timeout;
SHOW diskquota.hard_limit;
