CREATE DATABASE test_drop_after_pause;

\c test_drop_after_pause

CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a SELECT generate_series(1,10000000); -- expect insert fail

\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c contrib_regression

DROP DATABASE test_drop_after_pause;
