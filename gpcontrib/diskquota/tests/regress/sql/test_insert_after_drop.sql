CREATE DATABASE db_insert_after_drop;
\c db_insert_after_drop
CREATE EXTENSION diskquota;
-- Test Drop Extension
CREATE SCHEMA sdrtbl;
SELECT diskquota.set_schema_quota('sdrtbl', '1 MB');
SET search_path TO sdrtbl;
CREATE TABLE a(i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO a SELECT generate_series(1,100);
DROP EXTENSION diskquota;
INSERT INTO a SELECT generate_series(1,100);

DROP TABLE a;
\c postgres
DROP DATABASE db_insert_after_drop;
