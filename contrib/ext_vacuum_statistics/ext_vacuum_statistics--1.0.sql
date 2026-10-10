-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to you under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/*-------------------------------------------------------------------------
 *
 * ext_vacuum_statistics--1.0.sql
 *    Extended vacuum statistics via hook and custom storage
 *
 * This extension collects extended vacuum statistics via set_report_vacuum_hook
 * and stores them in shared memory.
 *
 *-------------------------------------------------------------------------
 */

\echo Use "CREATE EXTENSION ext_vacuum_statistics" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS ext_vacuum_statistics;

COMMENT ON SCHEMA ext_vacuum_statistics IS
  'Extended vacuum statistics (heap, index, database)';

-- These resets leave native VACUUM work counters untouched.
-- Local reset functions; use the gp_ wrappers below for a cluster-wide reset.
CREATE OR REPLACE FUNCTION ext_vacuum_statistics.extvac_reset_entry(
    dboid oid,
    relid oid
)
RETURNS void
AS 'MODULE_PATHNAME', 'extvac_reset_entry'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION ext_vacuum_statistics.extvac_reset_db_entry(dboid oid)
RETURNS void
AS 'MODULE_PATHNAME', 'extvac_reset_db_entry'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION ext_vacuum_statistics.vacuum_statistics_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'vacuum_statistics_reset'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

COMMENT ON FUNCTION ext_vacuum_statistics.extvac_reset_entry(oid, oid) IS
  'Reset extension-owned vacuum metrics for one table or index on the connected instance only';
COMMENT ON FUNCTION ext_vacuum_statistics.extvac_reset_db_entry(oid) IS
  'Reset extension-owned vacuum metrics for a database and its relations on the connected instance only';
COMMENT ON FUNCTION ext_vacuum_statistics.vacuum_statistics_reset() IS
  'Reset extension-owned vacuum metrics for all databases on the connected instance only';

-- Reset privileges can be delegated explicitly, as for pg_stat_reset().
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.extvac_reset_entry(oid, oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.extvac_reset_db_entry(oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.vacuum_statistics_reset() FROM PUBLIC;

-- Internal C function to fetch table VACUUM resources
CREATE OR REPLACE FUNCTION ext_vacuum_statistics.pg_stats_get_vacuum_tables(
    IN  dboid oid,
    IN  reloid oid,
    OUT relid oid,
    OUT total_blks_read bigint,
    OUT total_blks_hit bigint,
    OUT total_blks_dirtied bigint,
    OUT total_blks_written bigint,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT blk_read_time double precision,
    OUT blk_write_time double precision,
    OUT rel_blks_read bigint,
    OUT rel_blks_hit bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_tables'
LANGUAGE C STRICT STABLE;

-- Internal C function to fetch index VACUUM resources
CREATE OR REPLACE FUNCTION ext_vacuum_statistics.pg_stats_get_vacuum_indexes(
    IN  dboid oid,
    IN  reloid oid,
    OUT relid oid,
    OUT total_blks_read bigint,
    OUT total_blks_hit bigint,
    OUT total_blks_dirtied bigint,
    OUT total_blks_written bigint,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT blk_read_time double precision,
    OUT blk_write_time double precision,
    OUT rel_blks_read bigint,
    OUT rel_blks_hit bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_indexes'
LANGUAGE C STRICT STABLE;

-- Internal C function to fetch database vacuum stats
CREATE OR REPLACE FUNCTION ext_vacuum_statistics.pg_stats_get_vacuum_database(
    IN  dboid oid,
    OUT dbid oid,
    OUT total_blks_read bigint,
    OUT total_blks_hit bigint,
    OUT total_blks_dirtied bigint,
    OUT total_blks_written bigint,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT blk_read_time double precision,
    OUT blk_write_time double precision
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_database'
LANGUAGE C STRICT STABLE;

-- View: vacuum statistics per table (heap)
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_tables AS
SELECT
  rel.oid AS relid,
  ns.nspname AS schema,
  rel.relname AS relname,
  db.datname AS dbname,
  COALESCE(stats.total_blks_read, 0) AS total_blks_read,
  COALESCE(stats.total_blks_hit, 0) AS total_blks_hit,
  COALESCE(stats.total_blks_dirtied, 0) AS total_blks_dirtied,
  COALESCE(stats.total_blks_written, 0) AS total_blks_written,
  COALESCE(stats.wal_records, 0) AS wal_records,
  COALESCE(stats.wal_fpi, 0) AS wal_fpi,
  COALESCE(stats.wal_bytes, 0) AS wal_bytes,
  COALESCE(stats.blk_read_time, 0) AS blk_read_time,
  COALESCE(stats.blk_write_time, 0) AS blk_write_time,
  COALESCE(stats.rel_blks_read, 0) AS rel_blks_read,
  COALESCE(stats.rel_blks_hit, 0) AS rel_blks_hit,
  work.tuples_deleted,
  work.pages_scanned,
  work.pages_removed
FROM pg_class rel
JOIN pg_namespace ns ON ns.oid = rel.relnamespace
JOIN pg_database db ON db.datname = current_database()
CROSS JOIN LATERAL pg_catalog.pg_stat_get_vacuum_stats(rel.oid) work
LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(db.oid, rel.oid) stats ON true
WHERE rel.relkind IN ('r', 'm', 't', 'o', 'b', 'M');

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_tables IS
  'Extended vacuum statistics per table (heap)';

-- View: vacuum statistics per index
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_indexes AS
SELECT
  rel.oid AS indexrelid,
  ns.nspname AS schema,
  rel.relname AS indexrelname,
  db.datname AS dbname,
  COALESCE(stats.total_blks_read, 0) AS total_blks_read,
  COALESCE(stats.total_blks_hit, 0) AS total_blks_hit,
  COALESCE(stats.total_blks_dirtied, 0) AS total_blks_dirtied,
  COALESCE(stats.total_blks_written, 0) AS total_blks_written,
  COALESCE(stats.wal_records, 0) AS wal_records,
  COALESCE(stats.wal_fpi, 0) AS wal_fpi,
  COALESCE(stats.wal_bytes, 0) AS wal_bytes,
  COALESCE(stats.blk_read_time, 0) AS blk_read_time,
  COALESCE(stats.blk_write_time, 0) AS blk_write_time,
  COALESCE(stats.rel_blks_read, 0) AS rel_blks_read,
  COALESCE(stats.rel_blks_hit, 0) AS rel_blks_hit,
  work.tuples_deleted,
  work.pages_deleted
FROM pg_class rel
JOIN pg_namespace ns ON ns.oid = rel.relnamespace
JOIN pg_database db ON db.datname = current_database()
CROSS JOIN LATERAL pg_catalog.pg_stat_get_vacuum_stats(rel.oid) work
LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_indexes(db.oid, rel.oid) stats ON true
WHERE rel.relkind = 'i';

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_indexes IS
  'Extended vacuum statistics per index';

-- View: vacuum statistics per database (aggregate)
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_database AS
SELECT
  db.oid AS dboid,
  db.datname AS dbname,
  stats.total_blks_read AS db_blks_read,
  stats.total_blks_hit AS db_blks_hit,
  stats.total_blks_dirtied AS db_blks_dirtied,
  stats.total_blks_written AS db_blks_written,
  stats.wal_records AS db_wal_records,
  stats.wal_fpi AS db_wal_fpi,
  stats.wal_bytes AS db_wal_bytes,
  stats.blk_read_time AS db_blk_read_time,
  stats.blk_write_time AS db_blk_write_time
FROM pg_database db
LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_database(db.oid) stats ON db.oid = stats.dbid;

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_database IS
  'Extended vacuum statistics per database (aggregate)';

--
-- Cloudberry: cluster-wide views.
--
-- Every instance of the cluster keeps the statistics of the vacuums it runs
-- itself, and the pg_stats_vacuum_* views above only show those of the
-- instance they are queried on.  The gp_stats_vacuum_* views show the rows
-- of all instances, with the gp_segment_id of each (-1 for the
-- coordinator), like the gp_stat_* views of the core.
--
CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_tables AS
SELECT gp_execution_segment() AS gp_segment_id, *
  FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_tables')
UNION ALL
SELECT -1 AS gp_segment_id, *
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_tables IS
  'Extended vacuum statistics per table, on every instance of the cluster';

CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_indexes AS
SELECT gp_execution_segment() AS gp_segment_id, *
  FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_indexes')
UNION ALL
SELECT -1 AS gp_segment_id, *
  FROM ext_vacuum_statistics.pg_stats_vacuum_indexes;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_indexes IS
  'Extended vacuum statistics per index, on every instance of the cluster';

CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_database AS
SELECT gp_execution_segment() AS gp_segment_id, *
  FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_database')
UNION ALL
SELECT -1 AS gp_segment_id, *
  FROM ext_vacuum_statistics.pg_stats_vacuum_database;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_database IS
  'Extended vacuum statistics per database, on every instance of the cluster';

--
-- The *_summary views add the numbers up over the cluster, the way the
-- gp_stat_*_summary views of the core do: user relations are summed over
-- the segments (a replicated table is stored and vacuumed in full on every
-- segment, so its sums are divided by the number of segments), and the
-- system catalogs, which every instance keeps its own copy of, are shown as
-- the coordinator counts them.
--
CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_tables_summary AS
SELECT
  s.relid,
  s.schema,
  s.relname,
  s.dbname,
  (sum(s.total_blks_read) / s.divisor)::bigint AS total_blks_read,
  (sum(s.total_blks_hit) / s.divisor)::bigint AS total_blks_hit,
  (sum(s.total_blks_dirtied) / s.divisor)::bigint AS total_blks_dirtied,
  (sum(s.total_blks_written) / s.divisor)::bigint AS total_blks_written,
  (sum(s.wal_records) / s.divisor)::bigint AS wal_records,
  (sum(s.wal_fpi) / s.divisor)::bigint AS wal_fpi,
  (sum(s.wal_bytes) / s.divisor) AS wal_bytes,
  (sum(s.blk_read_time) / s.divisor) AS blk_read_time,
  (sum(s.blk_write_time) / s.divisor) AS blk_write_time,
  (sum(s.rel_blks_read) / s.divisor)::bigint AS rel_blks_read,
  (sum(s.rel_blks_hit) / s.divisor)::bigint AS rel_blks_hit,
  (sum(s.tuples_deleted) / s.divisor)::bigint AS tuples_deleted,
  (sum(s.pages_scanned) / s.divisor)::bigint AS pages_scanned,
  (sum(s.pages_removed) / s.divisor)::bigint AS pages_removed
FROM (
  SELECT v.*,
         CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END AS divisor
  FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_tables') v
  LEFT JOIN (SELECT relid, unnest(ARRAY[segrelid, blkdirrelid, visimaprelid]) AS auxrelid
               FROM pg_appendonly) a ON a.auxrelid = v.relid
  LEFT JOIN gp_distribution_policy d ON d.localoid = coalesce(a.relid, v.relid)
  WHERE v.relid >= 16384
) s
GROUP BY s.relid, s.schema, s.relname, s.dbname, s.divisor
UNION ALL
SELECT
  relid,
  schema,
  relname,
  dbname,
  total_blks_read,
  total_blks_hit,
  total_blks_dirtied,
  total_blks_written,
  wal_records,
  wal_fpi,
  wal_bytes,
  blk_read_time,
  blk_write_time,
  rel_blks_read,
  rel_blks_hit,
  tuples_deleted,
  pages_scanned,
  pages_removed
FROM ext_vacuum_statistics.pg_stats_vacuum_tables
WHERE relid < 16384;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_tables_summary IS
  'Extended vacuum statistics per table, summed over the cluster';

CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_indexes_summary AS
SELECT
  s.indexrelid,
  s.schema,
  s.indexrelname,
  s.dbname,
  (sum(s.total_blks_read) / s.divisor)::bigint AS total_blks_read,
  (sum(s.total_blks_hit) / s.divisor)::bigint AS total_blks_hit,
  (sum(s.total_blks_dirtied) / s.divisor)::bigint AS total_blks_dirtied,
  (sum(s.total_blks_written) / s.divisor)::bigint AS total_blks_written,
  (sum(s.wal_records) / s.divisor)::bigint AS wal_records,
  (sum(s.wal_fpi) / s.divisor)::bigint AS wal_fpi,
  (sum(s.wal_bytes) / s.divisor) AS wal_bytes,
  (sum(s.blk_read_time) / s.divisor) AS blk_read_time,
  (sum(s.blk_write_time) / s.divisor) AS blk_write_time,
  (sum(s.rel_blks_read) / s.divisor)::bigint AS rel_blks_read,
  (sum(s.rel_blks_hit) / s.divisor)::bigint AS rel_blks_hit,
  (sum(s.tuples_deleted) / s.divisor)::bigint AS tuples_deleted,
  (sum(s.pages_deleted) / s.divisor)::bigint AS pages_deleted
FROM (
  SELECT v.*,
         CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END AS divisor
  FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_indexes') v
  JOIN pg_index i ON i.indexrelid = v.indexrelid
  LEFT JOIN (SELECT relid, unnest(ARRAY[segrelid, blkdirrelid, visimaprelid]) AS auxrelid
               FROM pg_appendonly) a ON a.auxrelid = i.indrelid
  LEFT JOIN gp_distribution_policy d ON d.localoid = coalesce(a.relid, i.indrelid)
  WHERE v.indexrelid >= 16384
) s
GROUP BY s.indexrelid, s.schema, s.indexrelname, s.dbname, s.divisor
UNION ALL
SELECT
  indexrelid,
  schema,
  indexrelname,
  dbname,
  total_blks_read,
  total_blks_hit,
  total_blks_dirtied,
  total_blks_written,
  wal_records,
  wal_fpi,
  wal_bytes,
  blk_read_time,
  blk_write_time,
  rel_blks_read,
  rel_blks_hit,
  tuples_deleted,
  pages_deleted
FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
WHERE indexrelid < 16384;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_indexes_summary IS
  'Extended vacuum statistics per index, summed over the cluster';

-- The database aggregates are summed over all instances, the coordinator
-- included: they are the vacuum work done in the database cluster-wide.
CREATE VIEW ext_vacuum_statistics.gp_stats_vacuum_database_summary AS
SELECT
  dboid,
  dbname,
  sum(db_blks_read)::bigint AS db_blks_read,
  sum(db_blks_hit)::bigint AS db_blks_hit,
  sum(db_blks_dirtied)::bigint AS db_blks_dirtied,
  sum(db_blks_written)::bigint AS db_blks_written,
  sum(db_wal_records)::bigint AS db_wal_records,
  sum(db_wal_fpi)::bigint AS db_wal_fpi,
  sum(db_wal_bytes) AS db_wal_bytes,
  sum(db_blk_read_time) AS db_blk_read_time,
  sum(db_blk_write_time) AS db_blk_write_time
FROM ext_vacuum_statistics.gp_stats_vacuum_database
GROUP BY dboid, dbname;

COMMENT ON VIEW ext_vacuum_statistics.gp_stats_vacuum_database_summary IS
  'Extended vacuum statistics per database, summed over the cluster';

--
-- Cloudberry: resetting on the whole cluster.  The reset functions above act
-- on the instance they are called on; these run them on the coordinator and
-- on every primary segment. Call these wrappers from a normal coordinator
-- connection; use the local functions in utility mode.
--
CREATE FUNCTION ext_vacuum_statistics.gp_vacuum_statistics_reset()
RETURNS void
AS $$
  SELECT ext_vacuum_statistics.vacuum_statistics_reset() FROM gp_dist_random('gp_id');
  SELECT ext_vacuum_statistics.vacuum_statistics_reset();
$$ LANGUAGE sql VOLATILE;

CREATE FUNCTION ext_vacuum_statistics.gp_extvac_reset_entry(dboid oid, relid oid)
RETURNS void
AS $$
  SELECT ext_vacuum_statistics.extvac_reset_entry(dboid, relid) FROM gp_dist_random('gp_id');
  SELECT ext_vacuum_statistics.extvac_reset_entry(dboid, relid);
$$ LANGUAGE sql STRICT VOLATILE;

CREATE FUNCTION ext_vacuum_statistics.gp_extvac_reset_db_entry(dboid oid)
RETURNS void
AS $$
  SELECT ext_vacuum_statistics.extvac_reset_db_entry(dboid) FROM gp_dist_random('gp_id');
  SELECT ext_vacuum_statistics.extvac_reset_db_entry(dboid);
$$ LANGUAGE sql STRICT VOLATILE;

COMMENT ON FUNCTION ext_vacuum_statistics.gp_extvac_reset_entry(oid, oid) IS
  'Reset extension-owned vacuum metrics for one table or index on the coordinator and all primary segments; call on the coordinator';
COMMENT ON FUNCTION ext_vacuum_statistics.gp_extvac_reset_db_entry(oid) IS
  'Reset extension-owned vacuum metrics for a database and its relations on the coordinator and all primary segments; call on the coordinator';
COMMENT ON FUNCTION ext_vacuum_statistics.gp_vacuum_statistics_reset() IS
  'Reset extension-owned vacuum metrics for all databases on the coordinator and all primary segments; call on the coordinator';

REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.gp_extvac_reset_entry(oid, oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.gp_extvac_reset_db_entry(oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.gp_vacuum_statistics_reset() FROM PUBLIC;
