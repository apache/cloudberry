CREATE DATABASE test_relkind;
\c test_relkind
CREATE TYPE test_type AS (
        "dbid" oid,
        "datname" text
);
CREATE VIEW v AS select * from pg_class;
CREATE EXTENSION diskquota;
CREATE table test(a int);
SELECT diskquota.init_table_size_table();
-- diskquota.table_size should not change after creating a new type
SELECT tableid::regclass, size, segid
FROM diskquota.table_size 
WHERE segid = -1 AND tableid::regclass::name NOT LIKE '%.%'
ORDER BY tableid;

SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c contrib_regression
DROP DATABASE test_relkind;
