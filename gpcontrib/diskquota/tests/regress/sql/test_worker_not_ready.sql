CREATE DATABASE db_not_ready;
\c db_not_ready;

CREATE TABLE t (i int) DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;
CREATE EXTENSION diskquota_test;

SELECT diskquota.set_role_quota(CURRENT_ROLE, '1 MB');

SELECT diskquota.pause();

-- diskquota.wait_for_worker_new_epoch() cannot be used here because 
-- diskquota.state is not clean.
SELECT diskquota_test.wait('SELECT diskquota_test.check_cur_db_status(''UNREADY'');');

DROP EXTENSION diskquota;

\c contrib_regression

DROP DATABASE db_not_ready;
