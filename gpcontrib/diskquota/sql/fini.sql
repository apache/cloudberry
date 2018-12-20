\! gpconfig -c diskquota.monitor_databases -v postgres > /dev/null
\! echo $?
-- start_ignore
\! gpstop -u > /dev/null
\! echo $?
-- end_ignore

\! sleep 2
