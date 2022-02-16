CREATE DATABASE test_drop_after_pause;

\c test_drop_after_pause

CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

SELECT diskquota.enable_hardlimit();

CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a SELECT generate_series(1,1000000); -- expect insert fail

SELECT diskquota.disable_hardlimit();
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c contrib_regression

DROP DATABASE test_drop_after_pause;
