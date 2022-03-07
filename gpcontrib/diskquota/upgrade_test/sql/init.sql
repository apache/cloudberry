-- start_ignore
\! gpconfig -c shared_preload_libraries -v $(./data/current_binary_name) > /dev/null
-- end_ignore
\! echo $?
-- start_ignore
\! gpconfig -c diskquota.naptime -v 2 > /dev/null
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

\! sleep 10
