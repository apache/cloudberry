CREATE DATABASE test_readiness_logged;
\c test_readiness_logged

-- Get bgworker's log by database name.
-- 1. select bgworker pid by database name.
-- 2. select logmessage by bgworker pid.
CREATE VIEW logmessage_count_view AS WITH logp AS(
    SELECT
        MAX(logpid) as max_logpid
    FROM
        gp_toolkit.__gp_log_master_ext
    WHERE
        position(
            '[diskquota] start disk quota worker process to monitor database' in logmessage
        ) > 0
        AND position(current_database() in logmessage) > 0
)
SELECT
    count(*)
FROM
    gp_toolkit.__gp_log_master_ext,
    logp
WHERE
    logmessage = '[diskquota] diskquota is not ready'
    and logpid = max_logpid;

CREATE TABLE t (i int) DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;
CREATE EXTENSION diskquota_test;
SELECT diskquota_test.wait('SELECT diskquota_test.check_cur_db_status(''UNREADY'');');

-- logmessage count should be 1
SELECT * FROM logmessage_count_view;

\! gpstop -raf > /dev/null
\c
SELECT diskquota_test.wait('SELECT diskquota_test.check_cur_db_status(''UNREADY'');');

-- logmessage count should be 1
SELECT * FROM logmessage_count_view;

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_readiness_logged;
