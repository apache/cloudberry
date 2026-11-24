-- Reject concurrent AO/AOCS reindex before creating replacement indexes.
CREATE SCHEMA reindex_concurrent_ao;
CREATE TABLE reindex_concurrent_ao.t_row (a int)
  WITH (appendonly=true, orientation=row) DISTRIBUTED BY (a);
CREATE TABLE reindex_concurrent_ao.t_column (a int)
  WITH (appendonly=true, orientation=column) DISTRIBUTED BY (a);
INSERT INTO reindex_concurrent_ao.t_row SELECT i FROM generate_series(1,20) i;
INSERT INTO reindex_concurrent_ao.t_column SELECT i FROM generate_series(1,20) i;
CREATE INDEX t_row_idx ON reindex_concurrent_ao.t_row (a);
CREATE INDEX t_column_idx ON reindex_concurrent_ao.t_column (a);

REINDEX INDEX CONCURRENTLY reindex_concurrent_ao.t_row_idx;
REINDEX TABLE CONCURRENTLY reindex_concurrent_ao.t_row;
REINDEX INDEX CONCURRENTLY reindex_concurrent_ao.t_column_idx;
REINDEX TABLE CONCURRENTLY reindex_concurrent_ao.t_column;
-- Use a separate schema so the order of relations is deterministic.
CREATE SCHEMA reindex_concurrent_schema;
CREATE TABLE reindex_concurrent_schema.t_schema (a int)
  WITH (appendonly=true) DISTRIBUTED BY (a);
CREATE INDEX t_schema_idx ON reindex_concurrent_schema.t_schema(a);
REINDEX SCHEMA CONCURRENTLY reindex_concurrent_schema;
DROP SCHEMA reindex_concurrent_schema CASCADE;

-- A partitioned index must reject its AO leaf before creating a replacement.
CREATE TABLE reindex_concurrent_ao.t_part (a int)
  WITH (appendonly=true, orientation=row) DISTRIBUTED BY (a)
  PARTITION BY RANGE (a) (START (1) END (21) EVERY (10));
INSERT INTO reindex_concurrent_ao.t_part SELECT i FROM generate_series(1,20) i;
CREATE INDEX t_part_idx ON reindex_concurrent_ao.t_part (a);
REINDEX INDEX CONCURRENTLY reindex_concurrent_ao.t_part_idx;
REINDEX TABLE CONCURRENTLY reindex_concurrent_ao.t_part;

-- Neither the coordinator nor the segments should retain replacements.
SELECT count(*) FROM pg_class
  WHERE relnamespace = 'reindex_concurrent_ao'::regnamespace AND relname LIKE '%_ccnew%';
SELECT count(*) FROM gp_dist_random('pg_class')
  WHERE relnamespace = 'reindex_concurrent_ao'::regnamespace AND relname LIKE '%_ccnew%';

-- Original indexes stay usable, including after a subsequent insert.
INSERT INTO reindex_concurrent_ao.t_row VALUES (21);
INSERT INTO reindex_concurrent_ao.t_column VALUES (21);
SET enable_seqscan=off;
SELECT * FROM reindex_concurrent_ao.t_row WHERE a=21;
SELECT * FROM reindex_concurrent_ao.t_column WHERE a=21;
RESET enable_seqscan;

-- Ordinary REINDEX remains available for AO/AOCS.
REINDEX TABLE reindex_concurrent_ao.t_row;
REINDEX TABLE reindex_concurrent_ao.t_column;
DROP SCHEMA reindex_concurrent_ao CASCADE;

-- Temporary tables use the ordinary non-concurrent reindex path.
CREATE TEMP TABLE reindex_temp_ao (a int) WITH (appendonly=true) DISTRIBUTED BY (a);
INSERT INTO reindex_temp_ao VALUES (1);
CREATE INDEX reindex_temp_ao_idx ON reindex_temp_ao(a);
REINDEX INDEX CONCURRENTLY reindex_temp_ao_idx;
SELECT count(*) FROM reindex_temp_ao;
DROP TABLE reindex_temp_ao;

-- DATABASE mode also stops before creating a replacement for an AO index.
CREATE DATABASE reindex_concurrent_ao_db TEMPLATE template0;
1:@db_name reindex_concurrent_ao_db: CREATE TABLE t_db_ao (a int) WITH (appendonly=true) DISTRIBUTED BY (a);
1: CREATE INDEX t_db_ao_idx ON t_db_ao(a);
1: REINDEX DATABASE CONCURRENTLY reindex_concurrent_ao_db;
1: SELECT count(*) FROM pg_class WHERE relname LIKE 't_db_ao_idx_ccnew%';
1q:
DROP DATABASE reindex_concurrent_ao_db;
