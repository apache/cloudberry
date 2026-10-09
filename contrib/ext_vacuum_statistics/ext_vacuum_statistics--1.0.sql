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
    OUT rel_blks_hit bigint,
    OUT awaiting_drop_segments bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_tables'
LANGUAGE C STRICT STABLE;

-- Internal C function to fetch AO resource totals and phases from one statistics snapshot.
-- The AO view below selects parent AO/AOCS relations through pg_appendonly.
CREATE FUNCTION ext_vacuum_statistics.pg_stats_get_vacuum_ao_tables(
    IN dboid oid,
    IN reloid oid,
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
    OUT awaiting_drop_segments bigint,
    OUT ao_pre_cleanup_blks_read bigint,
    OUT ao_pre_cleanup_blks_hit bigint,
    OUT ao_pre_cleanup_blks_dirtied bigint,
    OUT ao_pre_cleanup_blks_written bigint,
    OUT ao_pre_cleanup_wal_records bigint,
    OUT ao_pre_cleanup_wal_fpi bigint,
    OUT ao_pre_cleanup_wal_bytes numeric,
    OUT ao_pre_cleanup_blk_read_time double precision,
    OUT ao_pre_cleanup_blk_write_time double precision,
    OUT ao_compaction_blks_read bigint,
    OUT ao_compaction_blks_hit bigint,
    OUT ao_compaction_blks_dirtied bigint,
    OUT ao_compaction_blks_written bigint,
    OUT ao_compaction_wal_records bigint,
    OUT ao_compaction_wal_fpi bigint,
    OUT ao_compaction_wal_bytes numeric,
    OUT ao_compaction_blk_read_time double precision,
    OUT ao_compaction_blk_write_time double precision,
    OUT ao_post_cleanup_blks_read bigint,
    OUT ao_post_cleanup_blks_hit bigint,
    OUT ao_post_cleanup_blks_dirtied bigint,
    OUT ao_post_cleanup_blks_written bigint,
    OUT ao_post_cleanup_wal_records bigint,
    OUT ao_post_cleanup_wal_fpi bigint,
    OUT ao_post_cleanup_wal_bytes numeric,
    OUT ao_post_cleanup_blk_read_time double precision,
    OUT ao_post_cleanup_blk_write_time double precision
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stats_get_vacuum_ao_tables'
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

-- Work counters come from native pgstat, even without an extension resource entry.
-- View: vacuum statistics per table (heap and append-optimized)
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
  work.pages_removed,
  work.tuples_frozen,
  work.recently_dead_tuples,
  work.missed_dead_pages,
  work.missed_dead_tuples,
  work.pages_frozen,
  work.pages_all_visible,
  work.total_file_segs,
  work.compacted_segments,
  work.tuples_moved,
  work.dead_pages,
  work.freeze_age_vacuum_count,
  COALESCE(stats.awaiting_drop_segments, 0) AS awaiting_drop_segments
FROM pg_class rel
JOIN pg_namespace ns ON ns.oid = rel.relnamespace
JOIN pg_database db ON db.datname = current_database()
CROSS JOIN LATERAL pg_catalog.pg_stat_get_vacuum_stats(rel.oid) work
LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(db.oid, rel.oid) stats ON true
WHERE rel.relkind IN ('r', 'm', 't', 'o', 'b', 'M');

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_tables IS
  'Extended vacuum statistics per table (heap and append-optimized)';

-- View: AO/AOCS parent tables, with phase resources and applicable work counters.
-- Read resource totals and phase counters together, including with stats_fetch_consistency = none.
CREATE VIEW ext_vacuum_statistics.pg_stats_vacuum_ao_tables AS
SELECT
  rel.oid AS relid,
  ns.nspname AS schema,
  rel.relname,
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
  work.pages_removed,
  work.recently_dead_tuples,
  work.total_file_segs,
  work.compacted_segments,
  work.tuples_moved,
  COALESCE(stats.awaiting_drop_segments, 0) AS awaiting_drop_segments,
  COALESCE(stats.ao_pre_cleanup_blks_read, 0) AS ao_pre_cleanup_blks_read,
  COALESCE(stats.ao_pre_cleanup_blks_hit, 0) AS ao_pre_cleanup_blks_hit,
  COALESCE(stats.ao_pre_cleanup_blks_dirtied, 0) AS ao_pre_cleanup_blks_dirtied,
  COALESCE(stats.ao_pre_cleanup_blks_written, 0) AS ao_pre_cleanup_blks_written,
  COALESCE(stats.ao_pre_cleanup_wal_records, 0) AS ao_pre_cleanup_wal_records,
  COALESCE(stats.ao_pre_cleanup_wal_fpi, 0) AS ao_pre_cleanup_wal_fpi,
  COALESCE(stats.ao_pre_cleanup_wal_bytes, 0) AS ao_pre_cleanup_wal_bytes,
  COALESCE(stats.ao_pre_cleanup_blk_read_time, 0) AS ao_pre_cleanup_blk_read_time,
  COALESCE(stats.ao_pre_cleanup_blk_write_time, 0) AS ao_pre_cleanup_blk_write_time,
  COALESCE(stats.ao_compaction_blks_read, 0) AS ao_compaction_blks_read,
  COALESCE(stats.ao_compaction_blks_hit, 0) AS ao_compaction_blks_hit,
  COALESCE(stats.ao_compaction_blks_dirtied, 0) AS ao_compaction_blks_dirtied,
  COALESCE(stats.ao_compaction_blks_written, 0) AS ao_compaction_blks_written,
  COALESCE(stats.ao_compaction_wal_records, 0) AS ao_compaction_wal_records,
  COALESCE(stats.ao_compaction_wal_fpi, 0) AS ao_compaction_wal_fpi,
  COALESCE(stats.ao_compaction_wal_bytes, 0) AS ao_compaction_wal_bytes,
  COALESCE(stats.ao_compaction_blk_read_time, 0) AS ao_compaction_blk_read_time,
  COALESCE(stats.ao_compaction_blk_write_time, 0) AS ao_compaction_blk_write_time,
  COALESCE(stats.ao_post_cleanup_blks_read, 0) AS ao_post_cleanup_blks_read,
  COALESCE(stats.ao_post_cleanup_blks_hit, 0) AS ao_post_cleanup_blks_hit,
  COALESCE(stats.ao_post_cleanup_blks_dirtied, 0) AS ao_post_cleanup_blks_dirtied,
  COALESCE(stats.ao_post_cleanup_blks_written, 0) AS ao_post_cleanup_blks_written,
  COALESCE(stats.ao_post_cleanup_wal_records, 0) AS ao_post_cleanup_wal_records,
  COALESCE(stats.ao_post_cleanup_wal_fpi, 0) AS ao_post_cleanup_wal_fpi,
  COALESCE(stats.ao_post_cleanup_wal_bytes, 0) AS ao_post_cleanup_wal_bytes,
  COALESCE(stats.ao_post_cleanup_blk_read_time, 0) AS ao_post_cleanup_blk_read_time,
  COALESCE(stats.ao_post_cleanup_blk_write_time, 0) AS ao_post_cleanup_blk_write_time
FROM pg_appendonly a
JOIN pg_class rel ON rel.oid = a.relid
JOIN pg_namespace ns ON ns.oid = rel.relnamespace
JOIN pg_database db ON db.datname = current_database()
CROSS JOIN LATERAL pg_catalog.pg_stat_get_vacuum_stats(rel.oid) work
LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_ao_tables(db.oid, rel.oid) stats ON true;

COMMENT ON VIEW ext_vacuum_statistics.pg_stats_vacuum_ao_tables IS
  'Extended vacuum statistics and phase resources for AO/AOCS parent tables on the connected instance';

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
  work.pages_deleted,
  work.dead_pages
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
