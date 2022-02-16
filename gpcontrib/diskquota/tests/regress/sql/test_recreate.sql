\c

CREATE DATABASE test_recreate;

\c diskquota

INSERT INTO diskquota_namespace.database_list(dbid) SELECT oid FROM pg_database WHERE datname = 'test_recreate';

\c test_recreate
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch(); -- shoud be ok
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_recreate;
