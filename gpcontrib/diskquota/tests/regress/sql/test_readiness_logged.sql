CREATE DATABASE test_log_readiness;
\c test_log_readiness

CREATE TABLE t (i int) DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;
SELECT pg_sleep(1); --Wait for the check completes

SELECT count(*) FROM gp_toolkit.gp_log_database
WHERE logmessage = '[diskquota] diskquota is not ready';

\! gpstop -raf > /dev/null
\c
SELECT pg_sleep(1); --Wait for the check completes

SELECT count(*) FROM gp_toolkit.gp_log_database
WHERE logmessage = '[diskquota] diskquota is not ready';

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_log_readiness;
