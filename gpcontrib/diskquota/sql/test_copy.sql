-- Test copy
CREATE SCHEMA s3;
SELECT diskquota.set_schema_quota('s3', '1 MB');
SET search_path TO s3;

CREATE TABLE c (i int);
COPY c FROM '/tmp/csmall.txt';
-- expect failed 
INSERT INTO c SELECT generate_series(1,100000000);
SELECT pg_sleep(20);
-- expect copy fail
COPY c FROM '/tmp/csmall.txt';

DROP TABLE c;
RESET search_path;
DROP SCHEMA s3;
