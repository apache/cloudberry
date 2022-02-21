-- Test truncate
CREATE SCHEMA s7;
SELECT diskquota.set_schema_quota('s7', '1 MB');
SET search_path TO s7;
CREATE TABLE a (i int) DISTRIBUTED BY (i);
CREATE TABLE b (i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,30);
INSERT INTO b SELECT generate_series(1,30);
TRUNCATE TABLE a;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,30);
INSERT INTO b SELECT generate_series(1,30);

DROP TABLE a, b;
RESET search_path;
DROP SCHEMA s7;

