-- Test vacuum full
CREATE SCHEMA s6;
SELECT diskquota.set_schema_quota('s6', '1 MB');
SET search_path TO s6;
CREATE TABLE a (i int);
CREATE TABLE b (i int);
INSERT INTO a SELECT generate_series(1,100000000);
SELECT pg_sleep(20);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
-- expect insert fail
INSERT INTO b SELECT generate_series(1,10);
DELETE FROM a WHERE i > 10;
VACUUM FULL a;
SELECT pg_sleep(20);
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,10);
INSERT INTO b SELECT generate_series(1,10);

DROP TABLE a, b;
RESET search_path;
DROP SCHEMA s6;

