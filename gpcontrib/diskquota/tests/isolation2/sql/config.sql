--start_ignore
CREATE DATABASE diskquota;
--end_ignore

!\retcode gpconfig -c shared_preload_libraries -v $(./data/current_binary_name);
!\retcode gpconfig -c diskquota.naptime -v 0 --skipvalidation;
!\retcode gpconfig -c max_worker_processes -v 20 --skipvalidation;

!\retcode gpstop -raf;

-- Show the values of all GUC variables
--start_ignore
-- naptime cannot be 0 for release build
1: SHOW diskquota.naptime;
--end_ignore
1: SHOW diskquota.max_active_tables;
1: SHOW diskquota.worker_timeout;
