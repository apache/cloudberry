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

--
-- The extended vacuum statistics of append-optimized tables (AO row and
-- AOCS) and of their indexes.
--
-- This test runs against a Cloudberry cluster that has ext_vacuum_statistics
-- in shared_preload_libraries of every instance ("make installcheck-cluster").
--
CREATE EXTENSION IF NOT EXISTS ext_vacuum_statistics;
-- Missing optimizer statistics notices are unrelated to vacuum accounting.
SET optimizer_print_missing_stats = off;
SELECT ext_vacuum_statistics.gp_vacuum_statistics_reset();

CREATE TABLE gpvs_ao_row (id int, v text) WITH (appendonly = true)
  DISTRIBUTED BY (id);
CREATE TABLE gpvs_ao_col (id int, v text)
  WITH (appendonly = true, orientation = column) DISTRIBUTED BY (id);
CREATE INDEX gpvs_ao_row_idx ON gpvs_ao_row (id);
CREATE INDEX gpvs_ao_col_idx ON gpvs_ao_col (id);
INSERT INTO gpvs_ao_row SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
INSERT INTO gpvs_ao_col SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
DELETE FROM gpvs_ao_row WHERE id % 2 = 0;
DELETE FROM gpvs_ao_col WHERE id % 2 = 0;
VACUUM gpvs_ao_row, gpvs_ao_col;

-- The compaction discarded the deleted rows on the segments, and truncating
-- the compacted segment files released space. No hidden rows remain, and
-- the heap-only counters stay zero.
SELECT relname, tuples_deleted, pages_removed > 0 AS pages_removed,
       wal_records > 0 AS has_wal, pages_scanned > 0 AS pages_scanned, tuples_frozen,
       recently_dead_tuples, missed_dead_tuples, missed_dead_pages
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary
 WHERE relname LIKE 'gpvs_ao_%'
 ORDER BY relname;

-- Phase resources exclude index work and sum to the table totals on every
-- instance. The awaiting-drop snapshot is a subset of segment metadata.
SELECT relname,
       bool_and(awaiting_drop_segments >= 0 AND awaiting_drop_segments <= total_file_segs
                AND total_blks_read = ao_pre_cleanup_blks_read + ao_compaction_blks_read + ao_post_cleanup_blks_read
                AND total_blks_hit = ao_pre_cleanup_blks_hit + ao_compaction_blks_hit + ao_post_cleanup_blks_hit
                AND total_blks_dirtied = ao_pre_cleanup_blks_dirtied + ao_compaction_blks_dirtied + ao_post_cleanup_blks_dirtied
                AND total_blks_written = ao_pre_cleanup_blks_written + ao_compaction_blks_written + ao_post_cleanup_blks_written
                AND wal_records = ao_pre_cleanup_wal_records + ao_compaction_wal_records + ao_post_cleanup_wal_records
                AND wal_fpi = ao_pre_cleanup_wal_fpi + ao_compaction_wal_fpi + ao_post_cleanup_wal_fpi
                AND wal_bytes = ao_pre_cleanup_wal_bytes + ao_compaction_wal_bytes + ao_post_cleanup_wal_bytes
                AND abs(blk_read_time - (ao_pre_cleanup_blk_read_time + ao_compaction_blk_read_time + ao_post_cleanup_blk_read_time)) < 0.000001
                AND abs(blk_write_time - (ao_pre_cleanup_blk_write_time + ao_compaction_blk_write_time + ao_post_cleanup_blk_write_time)) < 0.000001) AS phase_totals_match
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col')
 GROUP BY relname ORDER BY relname;

-- AO compaction work is cumulative; the segment count is a snapshot.
SELECT relname, total_file_segs > 0 AS has_segments,
       compacted_segments > 0 AS has_compaction, tuples_moved = 1500 AS moved_live_rows
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname LIKE 'gpvs_ao_%' ORDER BY relname;
CREATE TEMP TABLE gpvs_ao_before AS
  SELECT relid, total_file_segs, compacted_segments, tuples_moved, pages_scanned
    FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
   WHERE relname LIKE 'gpvs_ao_%'
  DISTRIBUTED BY (relid);

-- One row per instance; the coordinator holds no rows.
SELECT relname, gp_segment_id = -1 AS coordinator,
       sum(tuples_deleted) AS tuples_deleted, count(*) AS instances
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables
 WHERE relname LIKE 'gpvs_ao_%'
 GROUP BY 1, 2 ORDER BY 1, 2;

-- The compaction moves the live rows to another segment file, so the index
-- pass removes the entries of all rows of the compacted file, the moved live
-- ones included: 3000, not 1500.
SELECT indexrelname, tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_indexes_summary
 WHERE indexrelname LIKE 'gpvs_ao_%'
 ORDER BY indexrelname;

-- Relation-local block counts never include the index's buffer accesses.
-- Check every instance so aggregation cannot hide negative counts.
SELECT relname, bool_and(rel_blks_read >= 0 AND rel_blks_hit >= 0) AS blocks_nonnegative
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col')
 GROUP BY relname ORDER BY relname;

-- The database aggregate covers the tables and the indexes.
SELECT db_wal_records >=
         (SELECT sum(wal_records) FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables
           WHERE relname LIKE 'gpvs_ao_%') +
         (SELECT sum(wal_records) FROM ext_vacuum_statistics.gp_stats_vacuum_indexes
           WHERE indexrelname LIKE 'gpvs_ao_%') AS database_covers_relations
  FROM ext_vacuum_statistics.gp_stats_vacuum_database_summary
 WHERE dbname = current_database();

-- A second vacuum, with nothing left to compact, adds no deleted tuples.
VACUUM gpvs_ao_row, gpvs_ao_col;
SELECT relname, tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname = 'gpvs_ao_row';

SELECT bool_and(s.total_file_segs = b.total_file_segs
                AND s.compacted_segments = b.compacted_segments
                AND s.tuples_moved = b.tuples_moved
                AND s.pages_scanned = b.pages_scanned) AS no_extra_compaction
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary s
  JOIN gpvs_ao_before b USING (relid);
DROP TABLE gpvs_ao_before;

-- Hidden rows left by each completed AO vacuum are accumulated, even
-- when the same rows remain below the compaction threshold.
DELETE FROM gpvs_ao_row WHERE id <= 200;
DELETE FROM gpvs_ao_col WHERE id <= 200;
SET gp_appendonly_compaction_threshold = 100;
VACUUM gpvs_ao_row, gpvs_ao_col;
SELECT relname, recently_dead_tuples
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col') ORDER BY relname;
SET gp_appendonly_compaction = off;
VACUUM gpvs_ao_row, gpvs_ao_col;
SELECT relname, recently_dead_tuples
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col') ORDER BY relname;
RESET gp_appendonly_compaction;
SET gp_appendonly_compaction_threshold = 1;
VACUUM gpvs_ao_row, gpvs_ao_col;
SELECT relname, recently_dead_tuples
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col') ORDER BY relname;
-- Extension reset preserves native AO work.
SELECT ext_vacuum_statistics.gp_vacuum_statistics_reset();
SELECT relname, recently_dead_tuples
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary
 WHERE relname IN ('gpvs_ao_row', 'gpvs_ao_col') ORDER BY relname;
RESET gp_appendonly_compaction_threshold;

-- The entries go away with the tables, on every instance.
SELECT 'gpvs_ao_row'::regclass::oid AS row_oid \gset
SELECT oid AS dboid FROM pg_database WHERE datname = current_database() \gset
CREATE VIEW gpvs_ao_entry AS
  SELECT s.relid
    FROM gp_id,
         LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(:dboid, :row_oid) s;
SELECT count(*) AS segment_entries FROM gp_dist_random('gpvs_ao_entry');
DROP TABLE gpvs_ao_row, gpvs_ao_col;
SELECT count(*) AS segment_entries FROM gp_dist_random('gpvs_ao_entry');
DROP VIEW gpvs_ao_entry;

-- Replicated AO summaries average phase resources and the segment snapshot.
CREATE TABLE gpvs_ao_repl (id int) WITH (appendonly = true) DISTRIBUTED REPLICATED;
INSERT INTO gpvs_ao_repl SELECT generate_series(1, 1000);
DELETE FROM gpvs_ao_repl WHERE id <= 500;
VACUUM gpvs_ao_repl;
SELECT s.tuples_deleted = 500 AND s.ao_compaction_wal_records > 0
       AND s.ao_pre_cleanup_blks_hit = round(d.pre_hits / p.numsegments)
       AND s.ao_compaction_wal_records = round(d.compact_wal / p.numsegments)
       AND s.ao_post_cleanup_blks_hit = round(d.post_hits / p.numsegments)
       AND s.awaiting_drop_segments = round(d.awaiting / p.numsegments)
         AS replicated_phases_match
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables_summary s
  JOIN gp_distribution_policy p ON p.localoid = s.relid
  JOIN (SELECT relid, sum(ao_pre_cleanup_blks_hit) AS pre_hits,
               sum(ao_compaction_wal_records) AS compact_wal,
               sum(ao_post_cleanup_blks_hit) AS post_hits,
               sum(awaiting_drop_segments) AS awaiting
          FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables
         WHERE relname = 'gpvs_ao_repl' AND gp_segment_id >= 0
         GROUP BY relid) d ON d.relid = s.relid
 WHERE s.relname = 'gpvs_ao_repl';
SELECT ext_vacuum_statistics.gp_extvac_reset_entry(
  (SELECT oid FROM pg_database WHERE datname = current_database()), 'gpvs_ao_repl'::regclass);
SELECT count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p')
       AND bool_and(awaiting_drop_segments = 0
                    AND ao_pre_cleanup_blks_hit = 0 AND ao_pre_cleanup_wal_records = 0
                    AND ao_compaction_blks_hit = 0 AND ao_compaction_wal_records = 0
                    AND ao_post_cleanup_blks_hit = 0 AND ao_post_cleanup_wal_records = 0)
         AS phase_counters_reset
  FROM ext_vacuum_statistics.gp_stats_vacuum_ao_tables WHERE relname = 'gpvs_ao_repl';
DROP TABLE gpvs_ao_repl;
RESET optimizer_print_missing_stats;
