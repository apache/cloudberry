-- start_ignore
\! gpconfig -c diskquota.max_workers -v 10;
\! gpconfig -c diskquota.naptime -v 4;
\! gpstop -arf;
\c
DROP DATABASE IF EXISTS t1;
DROP DATABASE IF EXISTS t2;
--end_ignore

CREATE DATABASE t1;
CREATE DATABASE t2;
\c t1
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

\! pgrep -f "[p]ostgres.*bgworker.*t1" | xargs kill;
\! sleep 0.5 ; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
-- start_ignore
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep
--end_ignore
\c contrib_regression
DROP DATABASE t1;
\c t2
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

\c t2
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE t2;
--start_ignore
\! gpconfig -r diskquota.naptime;
\! gpconfig -r diskquota.max_workers;
\! gpstop -arf;
--end_ignore
