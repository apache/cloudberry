CREATE DATABASE test_drop_db;

\c test_drop_db

CREATE EXTENSION diskquota;
CREATE EXTENSION gp_inject_fault;
SELECT diskquota.init_table_size_table();

SELECT diskquota.set_schema_quota(current_schema, '1MB');
CREATE TABLE t(i int);

DROP EXTENSION gp_inject_fault;

-- expect success
INSERT INTO t SELECT generate_series(1, 100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect fail
INSERT INTO t SELECT generate_series(1, 100000);

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_drop_db;
