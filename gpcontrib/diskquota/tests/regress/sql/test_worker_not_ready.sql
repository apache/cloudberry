CREATE DATABASE db_not_ready;
\c db_not_ready;

CREATE TABLE t (i int) DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;

SELECT diskquota.set_role_quota(CURRENT_ROLE, '1 MB');

SELECT diskquota.pause();

-- diskquota.wait_for_worker_new_epoch() cannot be used here because 
-- diskquota.state is not clean.
SELECT pg_sleep(5);

DROP EXTENSION diskquota;

\c contrib_regression

DROP DATABASE db_not_ready;
