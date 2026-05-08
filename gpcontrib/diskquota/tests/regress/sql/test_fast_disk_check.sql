-- Test SCHEMA
CREATE SCHEMA s1;
SET search_path to s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,200000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT (pg_database_size(oid)-dbsize)/dbsize < 0.1  FROM pg_database, diskquota.show_fast_database_size_view WHERE datname='contrib_regression';
RESET search_path;
DROP TABLE s1.a;
DROP SCHEMA s1;

