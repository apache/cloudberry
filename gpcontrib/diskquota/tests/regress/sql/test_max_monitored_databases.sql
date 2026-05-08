--start_ignore
\! gpconfig -c diskquota.max_monitored_databases -v 3
\! gpstop -ari
--end_ignore

\c

DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP DATABASE IF EXISTS test_db3;

CREATE DATABASE test_db1;
CREATE DATABASE test_db2;
CREATE DATABASE test_db3;

\c test_db1
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

\c test_db2
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

-- expect fail
\c test_db3
CREATE EXTENSION diskquota;

-- clean extension
\c test_db1
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c test_db2
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

-- clean database
\c contrib_regression
DROP DATABASE test_db1;
DROP DATABASE test_db2;
DROP DATABASE test_db3;

-- start_ignore
\! gpconfig -r diskquota.max_monitored_databases
\! gpstop -ari
-- end_ignore