CREATE DATABASE db_insert_after_drop;
\c db_insert_after_drop
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
-- Test Drop Extension
CREATE SCHEMA sdrtbl;
SELECT diskquota.set_schema_quota('sdrtbl', '1 MB');
SET search_path TO sdrtbl;
CREATE TABLE a(i int);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT pg_sleep(10);
INSERT INTO a SELECT generate_series(1,100);
DROP EXTENSION diskquota;
-- sleep 1 second in case of system slow
SELECT pg_sleep(1);
INSERT INTO a SELECT generate_series(1,100);

DROP TABLE a;
\c postgres
DROP DATABASE db_insert_after_drop;
