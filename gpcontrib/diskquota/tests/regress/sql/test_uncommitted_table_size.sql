-- temp table
begin;
CREATE TEMP TABLE t1(i int);
INSERT INTO t1 SELECT generate_series(1, 100000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't1'::regclass and segid = -1;
SELECT pg_table_size('t1');
commit;

DROP table t1;

-- heap table
begin;
CREATE TABLE t2(i int) DISTRIBUTED BY (i);
INSERT INTO t2 SELECT generate_series(1, 100000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't2'::regclass and segid = -1;
SELECT pg_table_size('t2');
commit;

-- heap table index
begin;
CREATE INDEX idx2 on t2(i);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'idx2'::regclass and segid = -1;
SELECT pg_table_size('idx2');
commit;

DROP table t2;

-- toast table
begin;
CREATE TABLE t3(t text) DISTRIBUTED BY (t);
INSERT INTO t3 SELECT repeat('a', 10000) FROM generate_series(1, 1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 't3'::regclass and segid = -1;
SELECT pg_table_size('t3');
commit;

DROP table t3;

-- AO table
begin;
CREATE TABLE ao (i int) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO ao SELECT generate_series(1, 100000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT (SELECT size FROM diskquota.table_size WHERE tableid = 'ao'::regclass and segid = -1)=
       (SELECT pg_table_size('ao'));
commit;

-- AO table index
begin;
CREATE INDEX ao_idx on ao(i);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'ao_idx'::regclass and segid = -1;
SELECT pg_table_size('ao_idx');
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'ao'::regclass and segid = -1;
SELECT pg_table_size('ao');
commit;

DROP TABLE ao;

-- AO table CTAS
begin;
CREATE TABLE ao (i) WITH(appendonly=true) AS SELECT generate_series(1, 10000) DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT (SELECT size FROM diskquota.table_size WHERE tableid = 'ao'::regclass and segid = -1)=
       (SELECT pg_table_size('ao'));
commit;
DROP TABLE ao;

-- AOCS table
begin;
CREATE TABLE aocs (i int, t text) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'aocs'::regclass and segid = -1;
SELECT pg_table_size('aocs');
commit;

-- AOCS table index
begin;
CREATE INDEX aocs_idx on aocs(i);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'aocs_idx'::regclass and segid = -1;
SELECT pg_table_size('aocs_idx');
commit;

DROP TABLE aocs;

-- AOCS table CTAS
begin;
CREATE TABLE aocs WITH(appendonly=true, orientation=column) AS SELECT i, array(select * from generate_series(1,1000)) FROM generate_series(1, 100) AS i DISTRIBUTED BY (i);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'aocs'::regclass and segid = -1;
SELECT pg_table_size('aocs');
commit;
DROP TABLE aocs;
