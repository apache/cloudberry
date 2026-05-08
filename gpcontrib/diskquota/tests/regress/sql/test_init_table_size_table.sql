-- heap table
CREATE TABLE t(i int) DISTRIBUTED BY (i);
INSERT INTO t SELECT generate_series(1, 100000);

-- heap table index
CREATE INDEX idx on t(i);

-- toast table
CREATE TABLE toast(t text)  DISTRIBUTED BY (t);
INSERT INTO toast SELECT repeat('a', 10000) FROM generate_series(1, 1000);

-- toast table index
CREATE INDEX toast_idx on toast(t);

-- AO table
CREATE TABLE ao (i int) WITH (appendonly=true)  DISTRIBUTED BY (i);
INSERT INTO ao SELECT generate_series(1, 100000);

-- AO table index
CREATE INDEX ao_idx on ao(i);

-- AOCS table
CREATE TABLE aocs (i int, t text) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO aocs SELECT i, repeat('a', 1000) FROM generate_series(1, 10000) AS i;

-- AOCS table index
CREATE INDEX aocs_idx on aocs(i);

SELECT diskquota.wait_for_worker_new_epoch();

-- Tables here are fetched by diskquota_fetch_table_stat()
SELECT tableid::regclass, size, segid
FROM diskquota.table_size 
WHERE segid = -1 AND tableid::regclass::name NOT LIKE '%.%'
ORDER BY tableid;

-- init diskquota.table_size
SELECT diskquota.init_table_size_table();

-- diskquota.table_size should not change after init_table_size_table()
SELECT tableid::regclass, size, segid
FROM diskquota.table_size 
WHERE segid = -1 AND tableid::regclass::name NOT LIKE '%.%'
ORDER BY tableid;


DROP TABLE t;
DROP TABLE toast;
DROP TABLE ao;
DROP TABLE aocs;
