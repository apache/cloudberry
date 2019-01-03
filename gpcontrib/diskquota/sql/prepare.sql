CREATE EXTENSION diskquota;
-- start_ignore
\! gpstop -u
SELECT diskquota.init_table_size_table();
-- end_ignore
SELECT pg_sleep(1);
\! cp data/csmall.txt /tmp/csmall.txt
SELECT pg_sleep(15);

-- prepare a schema that has reached quota limit
CREATE SCHEMA badquota;
SELECT diskquota.set_schema_quota('badquota', '1 MB');
CREATE ROLE testbody;
CREATE TABLE badquota.t1(i INT);
ALTER TABLE badquota.t1 OWNER TO testbody;
INSERT INTO badquota.t1 SELECT generate_series(0, 100000000);
SELECT pg_sleep(5);
-- expect fail
INSERT INTO badquota.t1 SELECT generate_series(0, 10);
