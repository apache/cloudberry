-- Concurrent reindex of AO/AOCS must fail before creating a replacement index.
CREATE SCHEMA reindex_concurrent_ao;
CREATE TABLE reindex_concurrent_ao.t_row (a int)
  WITH (appendonly=true, orientation=row) DISTRIBUTED BY (a);
CREATE TABLE reindex_concurrent_ao.t_column (a int)
  WITH (appendonly=true, orientation=column) DISTRIBUTED BY (a);
CREATE INDEX t_row_idx ON reindex_concurrent_ao.t_row (a);
CREATE INDEX t_column_idx ON reindex_concurrent_ao.t_column (a);

REINDEX INDEX CONCURRENTLY reindex_concurrent_ao.t_row_idx;
REINDEX INDEX CONCURRENTLY reindex_concurrent_ao.t_column_idx;

-- No replacement index should remain on the coordinator or segments.
SELECT count(*) FROM pg_class
  WHERE relnamespace = 'reindex_concurrent_ao'::regnamespace AND relname LIKE '%_ccnew%';
SELECT count(*) FROM gp_dist_random('pg_class')
  WHERE relnamespace = 'reindex_concurrent_ao'::regnamespace AND relname LIKE '%_ccnew%';

-- The original indexes remain usable after the failed commands.
INSERT INTO reindex_concurrent_ao.t_row VALUES (21);
INSERT INTO reindex_concurrent_ao.t_column VALUES (21);
SET enable_seqscan=off;
SELECT * FROM reindex_concurrent_ao.t_row WHERE a=21;
SELECT * FROM reindex_concurrent_ao.t_column WHERE a=21;
RESET enable_seqscan;

DROP SCHEMA reindex_concurrent_ao CASCADE;
