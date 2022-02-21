-- Test alter table add column
CREATE SCHEMA scolumn;
SELECT diskquota.set_schema_quota('scolumn', '1 MB');
SET search_path TO scolumn;
SELECT diskquota.wait_for_worker_new_epoch();

CREATE TABLE a2(i INT) DISTRIBUTED BY (i);
-- expect fail
INSERT INTO a2 SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect fail
INSERT INTO a2 SELECT generate_series(1,10);
ALTER TABLE a2 ADD COLUMN j VARCHAR(50);
UPDATE a2 SET j = 'add value for column j';
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert failed after add column
INSERT INTO a2 SELECT generate_series(1,10);

DROP TABLE a2;
RESET search_path;
DROP SCHEMA scolumn;

