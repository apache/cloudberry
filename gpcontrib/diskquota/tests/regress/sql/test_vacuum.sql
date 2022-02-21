-- Test vacuum full
CREATE SCHEMA s6;
SELECT diskquota.set_schema_quota('s6', '1 MB');
SET search_path TO s6;
CREATE TABLE a (i int) DISTRIBUTED BY (i);
CREATE TABLE b (i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
-- expect insert fail
INSERT INTO b SELECT generate_series(1,10);
DELETE FROM a WHERE i > 10;
SELECT diskquota.wait_for_worker_new_epoch();
VACUUM FULL a;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid from diskquota.table_size WHERE tableid::regclass::name NOT LIKE '%.%' ORDER BY size, segid DESC;

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,10);
INSERT INTO b SELECT generate_series(1,10);

DROP TABLE a, b;
RESET search_path;
DROP SCHEMA s6;

