CREATE TEMP TABLE t1(i int);
INSERT INTO t1 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t1');
SELECT pg_table_size('t1');

CREATE TABLE t2(i int) DISTRIBUTED BY (i);
INSERT INTO t2 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t2');
SELECT pg_table_size('t2');

-- start_ignore
\! mkdir -p /tmp/test_spc
-- end_ignore
DROP TABLESPACE IF EXISTS test_spc;
CREATE TABLESPACE test_spc LOCATION '/tmp/test_spc';

ALTER TABLE t1 SET TABLESPACE test_spc;
INSERT INTO t1 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t1');
SELECT pg_table_size('t1');

ALTER TABLE t2 SET TABLESPACE test_spc;
INSERT INTO t2 SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('t2');
SELECT pg_table_size('t2');

DROP TABLE t1, t2;
DROP TABLESPACE test_spc;
-- start_ignore
\! rm -rf /tmp/test_spc
  -- end_ignore

CREATE TABLE ao (i int) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO ao SELECT generate_series(1, 10000);
SELECT diskquota.relation_size('ao');
SELECT pg_relation_size('ao');
DROP TABLE ao;

CREATE TABLE aocs (i int, t text) WITH (appendonly=true,  orientation=column) DISTRIBUTED BY (i);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;
SELECT diskquota.relation_size('aocs');
SELECT pg_relation_size('aocs');
DROP TABLE aocs;
