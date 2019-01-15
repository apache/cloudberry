-- Test Update
CREATE SCHEMA s4;
SELECT diskquota.set_schema_quota('s4', '1 MB');
SET search_path TO s4;
CREATE TABLE a(i int);
INSERT INTO a SELECT generate_series(1,100000000);
SELECT pg_sleep(20);
-- expect update fail.
UPDATE a SET i = 100;
DROP TABLE a;
RESET search_path;
DROP SCHEMA s4;

