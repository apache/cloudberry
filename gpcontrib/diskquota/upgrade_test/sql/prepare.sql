CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
-- start_ignore
\! gpstop -u
SELECT diskquota.init_table_size_table();
-- end_ignore
SELECT pg_sleep(15);

-- prepare a schema that has reached quota limit
CREATE SCHEMA badquota;
SELECT diskquota.set_schema_quota('badquota', '1 MB');
DROP ROLE IF EXISTS testbody;
CREATE ROLE testbody;
CREATE TABLE badquota.t1(i INT) DISTRIBUTED BY (i);
ALTER TABLE badquota.t1 OWNER TO testbody;
INSERT INTO badquota.t1 SELECT generate_series(0, 100000);
SELECT pg_sleep(10);
-- expect fail
INSERT INTO badquota.t1 SELECT generate_series(0, 10);

-- prepare a role that has reached quota limit
DROP SCHEMA IF EXISTS badbody_schema;
CREATE SCHEMA badbody_schema;
DROP ROLE IF EXISTS badbody;
CREATE ROLE badbody;
SELECT diskquota.set_role_quota('badbody', '2 MB');
CREATE TABLE badbody_schema.t2(i INT) DISTRIBUTED BY (i);
ALTER TABLE badbody_schema.t2 OWNER TO badbody;
INSERT INTO badbody_schema.t2 SELECT generate_series(0, 100000);
SELECT pg_sleep(10);
-- expect fail
INSERT INTO badbody_schema.t2 SELECT generate_series(0, 10);
