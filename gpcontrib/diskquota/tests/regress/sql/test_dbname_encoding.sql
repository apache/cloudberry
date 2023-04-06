-- create a database with non-ascii characters
CREATE DATABASE 数据库1;

\c 数据库1

CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
-- check whether current database name is logged.
SELECT
    count(logpid) > 0
FROM
    gp_toolkit.__gp_log_master_ext
WHERE
    position(
        '[diskquota] start disk quota worker process to monitor database' in logmessage
    ) > 0
    AND position(current_database() in logmessage) > 0;

DROP EXTENSION diskquota;
\c contrib_regression
DROP DATABASE 数据库1;