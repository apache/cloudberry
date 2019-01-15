-- Test Drop table
CREATE SCHEMA sdrtbl;
SELECT diskquota.set_schema_quota('sdrtbl', '1 MB');
SET search_path TO sdrtbl;
CREATE TABLE a(i INT);
CREATE TABLE a2(i INT);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000000);
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,100);
DROP TABLE a;
SELECT pg_sleep(20);
INSERT INTO a2 SELECT generate_series(1,100);

DROP TABLE a2;
RESET search_path;
DROP SCHEMA sdrtbl;


