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

-- Reset functions
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
  'Reset vacuum statistics for one table or index on the connected instance only';
COMMENT ON FUNCTION ext_vacuum_statistics.extvac_reset_db_entry(oid) IS
  'Reset vacuum statistics for a database and its relations on the connected instance only';
COMMENT ON FUNCTION ext_vacuum_statistics.vacuum_statistics_reset() IS
  'Reset vacuum statistics for all databases on the connected instance only';

-- Reset privileges can be delegated explicitly, as for pg_stat_reset().
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.extvac_reset_entry(oid, oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.extvac_reset_db_entry(oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ext_vacuum_statistics.vacuum_statistics_reset() FROM PUBLIC;

-- Internal C function to fetch table vacuum stats
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
    OUT rel_blks_hit bigint,
    OUT tuples_deleted bigint,
    OUT pages_scanned bigint,
    OUT pages_removed bigint,
    OUT tuples_frozen bigint,
    OUT recently_dead_tuples bigint,
    OUT missed_dead_pages bigint,
    OUT missed_dead_tuples bigint,
    OUT pages_frozen bigint,
    OUT pages_all_visible bigint,
    OUT total_file_segs bigint,
    OUT compacted_segments bigint,
    OUT tuples_moved bigint,
    OUT dead_pages bigint,
    OUT freeze_age_vacuum_count bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_tables'
LANGUAGE C STRICT STABLE;

-- Internal C function to fetch index vacuum stats
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
    OUT rel_blks_hit bigint,
    OUT tuples_deleted bigint,
    OUT pages_deleted bigint,
    OUT dead_pages bigint
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

-- View: vacuum statistics per table (heap and append-optimized)
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_tables AS
SELECT
  rel.oid AS relid,
  ns.nspname AS schema,
  rel.relname AS relname,
  db.datname AS dbname,
  stats.total_blks_read,
  stats.total_blks_hit,
  stats.total_blks_dirtied,
  stats.total_blks_written,
  stats.wal_records,
  stats.wal_fpi,
  stats.wal_bytes,
  stats.blk_read_time,
  stats.blk_write_time,
  stats.rel_blks_read,
  stats.rel_blks_hit,
  stats.tuples_deleted,
  stats.pages_scanned,
  stats.pages_removed,
  stats.tuples_frozen,
  stats.recently_dead_tuples,
  stats.missed_dead_pages,
  stats.missed_dead_tuples,
  stats.pages_frozen,
  stats.pages_all_visible,
  stats.total_file_segs,
  stats.compacted_segments,
  stats.tuples_moved,
  stats.dead_pages,
  stats.freeze_age_vacuum_count
FROM pg_database db,
     pg_class rel,
     pg_namespace ns,
     LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(db.oid, rel.oid) stats
WHERE db.datname = current_database()
  AND rel.relkind IN ('r', 'm', 't')
  AND rel.relnamespace = ns.oid
  AND rel.oid = stats.relid;

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_tables IS
  'Extended vacuum statistics per table (heap and append-optimized)';

-- View: vacuum statistics per index
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_indexes AS
SELECT
  rel.oid AS indexrelid,
  ns.nspname AS schema,
  rel.relname AS indexrelname,
  db.datname AS dbname,
  stats.total_blks_read,
  stats.total_blks_hit,
  stats.total_blks_dirtied,
  stats.total_blks_written,
  stats.wal_records,
  stats.wal_fpi,
  stats.wal_bytes,
  stats.blk_read_time,
  stats.blk_write_time,
  stats.rel_blks_read,
  stats.rel_blks_hit,
  stats.tuples_deleted,
  stats.pages_deleted,
  stats.dead_pages
FROM pg_database db,
     pg_class rel,
     pg_namespace ns,
     LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_indexes(db.oid, rel.oid) stats
WHERE db.datname = current_database()
  AND rel.relkind = 'i'
  AND rel.relnamespace = ns.oid
  AND rel.oid = stats.relid;

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
