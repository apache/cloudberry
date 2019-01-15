-- Test alter table add column
CREATE SCHEMA scolumn;
SELECT diskquota.set_schema_quota('scolumn', '1 MB');
SET search_path TO scolumn;
SELECT pg_sleep(20);

CREATE TABLE a2(i INT);
-- expect fail
INSERT INTO a2 SELECT generate_series(1,100000000);
-- expect fail
INSERT INTO a2 SELECT generate_series(1,10);
ALTER TABLE a2 ADD COLUMN j VARCHAR(50);
UPDATE a2 SET j = 'add value for column j';
SELECT pg_sleep(20);
-- expect insert failed after add column
INSERT INTO a2 SELECT generate_series(1,10);

DROP TABLE a2;
RESET search_path;
DROP SCHEMA scolumn;

