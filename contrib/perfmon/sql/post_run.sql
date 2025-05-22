-- start_ignore
\! gpconfig -c perfmon.enable -v 'off'
\! gpstop -ari
-- end_ignore
\! gpconfig -s perfmon.enable
-- start_ignore
\c contrib_regression
drop database if exists gpperfmon;
-- end_ignore
