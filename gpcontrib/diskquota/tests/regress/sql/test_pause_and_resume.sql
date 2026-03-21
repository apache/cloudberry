-- Test pause and resume.
CREATE SCHEMA s1;
SET search_path TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- pause extension
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid FROM diskquota.table_size 
WHERE tableid = 'a'::regclass AND segid = -1;

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100000);

-- resume extension
SELECT diskquota.resume();
SELECT diskquota.wait_for_worker_new_epoch();

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- table size should be updated after resume
SELECT tableid::regclass, size, segid FROM diskquota.table_size 
WHERE tableid = 'a'::regclass AND segid = -1;

RESET search_path;
DROP TABLE s1.a;
DROP SCHEMA s1;
