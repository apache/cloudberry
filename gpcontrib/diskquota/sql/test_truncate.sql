-- Test truncate
CREATE SCHEMA s7;
SELECT diskquota.set_schema_quota('s7', '1 MB');
SET search_path TO s7;
CREATE TABLE a (i int);
CREATE TABLE b (i int);
INSERT INTO a SELECT generate_series(1,100000000);
SELECT pg_sleep(20);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,30);
INSERT INTO b SELECT generate_series(1,30);
TRUNCATE TABLE a;
SELECT pg_sleep(20);
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,30);
INSERT INTO b SELECT generate_series(1,30);

DROP TABLE a, b;
RESET search_path;
DROP SCHEMA s7;

