-- Test re-set_schema_quota
CREATE SCHEMA srE;
SELECT diskquota.set_schema_quota('srE', '1 MB');
SET search_path TO srE;
CREATE TABLE a(i int);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,1000000000);
-- expect insert fail when exceed quota limit
INSERT INTO a SELECT generate_series(1,1000);
-- set schema quota larger
SELECT diskquota.set_schema_quota('srE', '1 GB');
SELECT pg_sleep(20);
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,1000);

DROP TABLE a;
RESET search_path;
DROP SCHEMA srE;

