CREATE DATABASE db_insert_after_drop;
\c db_insert_after_drop
CREATE EXTENSION diskquota;
-- Test Drop Extension
CREATE SCHEMA sdrtbl;
SELECT diskquota.set_schema_quota('sdrtbl', '1 MB');
SET search_path TO sdrtbl;
CREATE TABLE a(i int);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000000);
SELECT pg_sleep(20);
INSERT INTO a SELECT generate_series(1,100);
DROP EXTENSION diskquota;
-- no sleep, it will take effect immediately
INSERT INTO a SELECT generate_series(1,100);

DROP TABLE a;
\c postgres
DROP DATABASE db_insert_after_drop;
