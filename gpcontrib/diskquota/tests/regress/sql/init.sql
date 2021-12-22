-- start_ignore
CREATE DATABASE diskquota;
-- end_ignore

-- start_ignore
\! gpconfig -c shared_preload_libraries -v diskquota > /dev/null
-- end_ignore
\! echo $?
-- start_ignore
\! gpconfig -c diskquota.naptime -v 0 > /dev/null
-- end_ignore
\! echo $?
-- start_ignore
\! gpconfig -c max_worker_processes -v 20 > /dev/null
-- end_ignore
\! echo $?

-- start_ignore
\! gpstop -raf > /dev/null
-- end_ignore
\! echo $?
