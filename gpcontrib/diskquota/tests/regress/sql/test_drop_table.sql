-- Test Drop table
CREATE SCHEMA sdrtbl;
SELECT diskquota.set_schema_quota('sdrtbl', '1 MB');
SET search_path TO sdrtbl;
CREATE TABLE a(i INT) DISTRIBUTED BY (i);
CREATE TABLE a2(i INT) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,100);
DROP TABLE a;
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO a2 SELECT generate_series(1,100);

DROP TABLE a2;
RESET search_path;
DROP SCHEMA sdrtbl;
