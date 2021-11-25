CREATE TEMP TABLE t1(i int);
INSERT INTO t1 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t1', true);
SELECT pg_table_size('t1');

CREATE TABLE t2(i int);
INSERT INTO t2 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t2', false);
SELECT pg_table_size('t2');

-- start_ignore
\! mkdir /tmp/test_spc
-- end_ignore
DROP TABLESPACE IF EXISTS test_spc;
CREATE TABLESPACE test_spc LOCATION '/tmp/test_spc';

ALTER TABLE t1 SET TABLESPACE test_spc;
INSERT INTO t1 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t1', true);
SELECT pg_table_size('t1');

ALTER TABLE t2 SET TABLESPACE test_spc;
INSERT INTO t2 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t2', false);
SELECT pg_table_size('t2');


DROP TABLE t1, t2;
DROP TABLESPACE test_spc;
\! rm -rf /tmp/test_spc
