-- Test schema
CREATE SCHEMA s1;
SET search_path TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert success
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);
CREATE TABLE a2(i int) DISTRIBUTED BY (i);
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,100);

-- Test alter table set schema
CREATE SCHEMA s2;
ALTER TABLE s1.a SET SCHEMA s2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO a2 SELECT generate_series(1,200);
-- expect insert succeed
INSERT INTO s2.a SELECT generate_series(1,200);

-- prepare a schema that has reached quota limit
CREATE SCHEMA badquota;
DROP ROLE IF EXISTS testbody;
CREATE ROLE testbody;
CREATE TABLE badquota.t1(i INT) DISTRIBUTED BY (i);
ALTER TABLE badquota.t1 OWNER TO testbody;
INSERT INTO badquota.t1 SELECT generate_series(0, 100000);
SELECT diskquota.init_table_size_table();
SELECT diskquota.set_schema_quota('badquota', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
SELECT size, segid FROM diskquota.table_size
  WHERE tableid IN (SELECT oid FROM pg_class WHERE relname='t1')
  ORDER BY segid DESC;
-- expect fail
INSERT INTO badquota.t1 SELECT generate_series(0, 10);

ALTER TABLE s2.a SET SCHEMA badquota;
-- expect failed
INSERT INTO badquota.a SELECT generate_series(0, 100);

SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name, quota_in_mb FROM diskquota.show_fast_schema_quota_view WHERE schema_name = 's1';

RESET search_path;
DROP TABLE s1.a2, badquota.a;
DROP SCHEMA s1, s2;

DROP TABLE badquota.t1;
DROP ROLE testbody;
DROP SCHEMA badquota;
