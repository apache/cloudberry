CREATE EXTENSION diskquota;
-- start_ignore
\! gpstop -u
-- end_ignore
\! cp data/csmall.txt /tmp/csmall.txt

-- disable hardlimit feature.
SELECT diskquota.disable_hardlimit();

-- prepare a schema that has reached quota limit
CREATE SCHEMA badquota;
DROP ROLE IF EXISTS testbody;
CREATE ROLE testbody;
CREATE TABLE badquota.t1(i INT);
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
