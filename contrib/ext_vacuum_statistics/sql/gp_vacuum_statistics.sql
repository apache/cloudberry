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
-- Cluster-wide vacuum statistics: aggregation, collection control, reset,
-- and cleanup after DROP.  The final two cases cover core summary views
-- and local scope of track_cost_delay_timing.
--
-- This test runs against a Cloudberry cluster that has ext_vacuum_statistics
-- in shared_preload_libraries of every instance ("make installcheck-cluster").
--
CREATE EXTENSION IF NOT EXISTS ext_vacuum_statistics;
SELECT ext_vacuum_statistics.gp_vacuum_statistics_reset();

SELECT oid AS dboid FROM pg_database WHERE datname = current_database() \gset

-- Prepare dead tuples: 10000 in a distributed table and 300 copies on
-- each segment in a replicated table.  VACUUM also cleans the index.
CREATE TABLE gpvs_dist (id int, v text) WITH (autovacuum_enabled = off)
  DISTRIBUTED BY (id);
CREATE INDEX gpvs_dist_idx ON gpvs_dist (id);
INSERT INTO gpvs_dist SELECT g, 'x' FROM generate_series(1, 10000) g;
CREATE TABLE gpvs_repl (id int) WITH (autovacuum_enabled = off)
  DISTRIBUTED REPLICATED;
INSERT INTO gpvs_repl SELECT generate_series(1, 300);
DELETE FROM gpvs_dist;
DELETE FROM gpvs_repl;
VACUUM gpvs_dist, gpvs_repl;

-- Collection: expect an entry on every primary segment and the coordinator.
-- role = 'p' includes the coordinator and excludes mirrors.
SELECT count(*) = (SELECT count(*) FROM gp_segment_configuration
                    WHERE role = 'p') AS one_row_per_instance
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist';

-- Expect 10000 removed tuples across the segments and 0 on the coordinator.
SELECT gp_segment_id = -1 AS coordinator, sum(tuples_deleted) AS tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist'
 GROUP BY 1 ORDER BY 1;

-- Aggregation: expect 10000 for the distributed table and 300 for the
-- replicated table.  Replicated counts must be averaged across segments.
SELECT relname, tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary
 WHERE relname LIKE 'gpvs%'
 ORDER BY relname;

-- The index summary must also report 10000 removed tuples.
SELECT indexrelname, tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_indexes_summary
 WHERE indexrelname = 'gpvs_dist_idx';

-- WAL volume varies, so compare the summary with the segment counters.
SELECT s.wal_records = (SELECT sum(wal_records)
                          FROM ext_vacuum_statistics.gp_stats_vacuum_tables
                         WHERE relname = 'gpvs_dist' AND gp_segment_id >= 0)
         AS summary_matches_segments
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary s
 WHERE s.relname = 'gpvs_dist';

-- Database WAL includes these tables plus indexes and other relations;
-- it must be positive and at least as large as the table WAL total.
SELECT db_wal_records > 0 AS database_has_wal,
       db_wal_records >= (SELECT sum(wal_records)
                            FROM ext_vacuum_statistics.gp_stats_vacuum_tables
                           WHERE relname LIKE 'gpvs%') AS database_covers_tables
  FROM ext_vacuum_statistics.gp_stats_vacuum_database_summary
 WHERE dbname = current_database();

-- Core table summaries must also record a positive vacuum duration.
SELECT relname, total_vacuum_time > 0 AS vacuum_timed
  FROM gp_stat_all_tables_summary
 WHERE relname LIKE 'gpvs%'
 ORDER BY relname;

-- Collection control: disable collection on the coordinator, then vacuum
-- another 1000 dead tuples. Native work grows; resource counters stay unchanged.
CREATE TEMP TABLE gpvs_resources_before AS
  SELECT gp_segment_id AS segid, total_blks_read, total_blks_hit, wal_records
    FROM ext_vacuum_statistics.gp_stats_vacuum_tables
   WHERE relname = 'gpvs_dist'
  DISTRIBUTED BY (segid);
INSERT INTO gpvs_dist SELECT g, 'y' FROM generate_series(1, 1000) g;
DELETE FROM gpvs_dist;
SET vacuum_statistics.enabled = off;
VACUUM gpvs_dist;
RESET vacuum_statistics.enabled;
SELECT bool_and(s.total_blks_read = b.total_blks_read
                AND s.total_blks_hit = b.total_blks_hit
                AND s.wal_records = b.wal_records) AS resources_unchanged
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables s
  JOIN gpvs_resources_before b ON b.segid = s.gp_segment_id
 WHERE s.relname = 'gpvs_dist';
DROP TABLE gpvs_resources_before;
SELECT bool_and(e.tuples_deleted = n.tuples_deleted) AS native_work_matches
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables e
  JOIN gp_stat_vacuum_tables n USING (gp_segment_id, relid)
 WHERE e.relname = 'gpvs_dist';
SELECT sum(tuples_deleted) AS tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist';

-- Relation reset clears extension resources across the cluster, preserving
-- native work for gpvs_dist and gpvs_repl.
SELECT ext_vacuum_statistics.gp_extvac_reset_entry(:dboid, 'gpvs_dist'::regclass);
SELECT bool_and(total_blks_read = 0 AND total_blks_hit = 0 AND wal_records = 0)
         AS resources_reset
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist';
SELECT coalesce(sum(tuples_deleted), 0) AS tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist';
SELECT relname, tuples_deleted
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary
 WHERE relname LIKE 'gpvs%'
 ORDER BY relname;

-- DROP cleanup: save the OID and read the statistics directly.  The public
-- views join pg_class, so they would hide an orphaned entry after DROP.
-- gp_id makes this helper view executable on each segment via gp_dist_random.
-- Expect 3 segment entries and 1 coordinator entry in the regression cluster.
SELECT 'gpvs_repl'::regclass::oid AS repl_oid \gset
CREATE VIEW gpvs_repl_entry AS
  SELECT s.relid
    FROM gp_id,
         LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(:dboid, :repl_oid) s;
SELECT count(*) AS segment_entries FROM gp_dist_random('gpvs_repl_entry');
SELECT count(*) AS coordinator_entries FROM gpvs_repl_entry;
-- Rolling back DROP must preserve the 3 segment entries.
BEGIN;
DROP TABLE gpvs_repl;
ROLLBACK;
SELECT count(*) AS segment_entries FROM gp_dist_random('gpvs_repl_entry');
-- Committing DROP must remove entries from both segments and coordinator.
DROP TABLE gpvs_repl;
SELECT count(*) AS segment_entries FROM gp_dist_random('gpvs_repl_entry');
SELECT count(*) AS coordinator_entries FROM gpvs_repl_entry;

-- Core database timing is published asynchronously.  Allow pending stats
-- to be flushed before checking that the vacuum duration is positive.
SELECT pg_sleep(2);
SELECT total_vacuum_time > 0 AS database_vacuum_timed
  FROM gp_stat_vacuum_summary
 WHERE datname = current_database();

-- Cluster reset must preserve the remaining table's entries and zero
-- its resources on every instance, preserving native work.
SELECT ext_vacuum_statistics.gp_vacuum_statistics_reset();
SELECT count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p') AS rows_preserved,
       bool_and(total_blks_read = 0 AND total_blks_hit = 0 AND wal_records = 0) AS resources_reset,
       sum(tuples_deleted) = 11000 AS work_preserved
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname = 'gpvs_dist';

DROP VIEW gpvs_repl_entry;
DROP TABLE gpvs_dist;

-- Core GUC regression: reconnect with timing enabled at session startup.
-- Segment processes must keep the default off across coordinator startup
-- options, SET, and ROLLBACK.  All three checks expect t.
\connect -reuse-previous=on "options='-c track_cost_delay_timing=on'"
SELECT bool_and(setting = 'off') AS segment_timing_local
  FROM gp_dist_random('pg_settings') WHERE name = 'track_cost_delay_timing';
BEGIN;
SET track_cost_delay_timing = off;
SELECT bool_and(setting = 'off') AS segment_timing_local
  FROM gp_dist_random('pg_settings') WHERE name = 'track_cost_delay_timing';
ROLLBACK;
SELECT bool_and(setting = 'off') AS segment_timing_local
  FROM gp_dist_random('pg_settings') WHERE name = 'track_cost_delay_timing';
RESET track_cost_delay_timing;

-- Core index-summary regression: two indexes on the same table must give
-- exactly two summary rows.  Joining by table OID alone used to multiply
-- rows and mix index timings; compare each index with its own segment sum.
CREATE TABLE core_summary_test (a int, b int) DISTRIBUTED BY (a);
CREATE INDEX core_summary_a ON core_summary_test(a);
CREATE INDEX core_summary_b ON core_summary_test(b);
INSERT INTO core_summary_test SELECT g, g FROM generate_series(1, 10000) g;
DELETE FROM core_summary_test WHERE a % 2 = 0;
VACUUM core_summary_test;
SELECT count(*) AS summary_indexes,
       count(DISTINCT indexrelid) AS distinct_indexes
  FROM gp_stat_all_indexes_summary WHERE relname = 'core_summary_test';
SELECT bool_and(s.total_vacuum_time = d.total_vacuum_time) AS index_totals_match
  FROM gp_stat_all_indexes_summary s
  JOIN (SELECT indexrelid, sum(total_vacuum_time) AS total_vacuum_time
          FROM gp_dist_random('pg_stat_all_indexes')
         WHERE relname = 'core_summary_test' GROUP BY indexrelid) d
    USING (indexrelid)
 WHERE s.relname = 'core_summary_test';
DROP TABLE core_summary_test;

-- Heap page counters: distributed totals and replicated averages must
-- agree with per-segment work. Forced rescans must not count it again.
CREATE TABLE gpvs_pages_dist (id int) DISTRIBUTED BY (id);
CREATE TABLE gpvs_pages_repl (id int) DISTRIBUTED REPLICATED;
INSERT INTO gpvs_pages_dist SELECT generate_series(1, 10000);
INSERT INTO gpvs_pages_repl SELECT generate_series(1, 1000);
VACUUM (FREEZE) gpvs_pages_dist, gpvs_pages_repl;
SELECT count(*) = 2 AND
       bool_and(s.pages_frozen > 0 AND s.pages_all_visible > 0
                AND s.pages_frozen = d.pages_frozen /
                    CASE WHEN p.policytype = 'r' THEN p.numsegments ELSE 1 END
                AND s.pages_all_visible = d.pages_all_visible /
                    CASE WHEN p.policytype = 'r' THEN p.numsegments ELSE 1 END
                AND s.freeze_age_vacuum_count > 0 AND s.dead_pages = 0
                AND s.freeze_age_vacuum_count = d.freeze_age_vacuum_count /
                    CASE WHEN p.policytype = 'r' THEN p.numsegments ELSE 1 END)
         AS page_summaries_match
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary s
  JOIN (SELECT relid, sum(pages_frozen) AS pages_frozen,
                     sum(pages_all_visible) AS pages_all_visible,
                     sum(freeze_age_vacuum_count) AS freeze_age_vacuum_count
          FROM gp_dist_random('ext_vacuum_statistics.pg_stats_vacuum_tables')
         WHERE relname IN ('gpvs_pages_dist', 'gpvs_pages_repl')
         GROUP BY relid) d USING (relid)
  JOIN gp_distribution_policy p ON p.localoid = s.relid
 WHERE s.relname IN ('gpvs_pages_dist', 'gpvs_pages_repl');
CREATE TEMP TABLE gpvs_pages_before AS
  SELECT relid, pages_frozen, pages_all_visible
    FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary
   WHERE relname IN ('gpvs_pages_dist', 'gpvs_pages_repl')
  DISTRIBUTED BY (relid);
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) gpvs_pages_dist, gpvs_pages_repl;
SELECT bool_and(s.pages_frozen = b.pages_frozen
                AND s.pages_all_visible = b.pages_all_visible) AS no_new_page_work
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary s
  JOIN gpvs_pages_before b USING (relid);
SELECT gp_stat_reset_vacuum_stats('gpvs_pages_dist'::regclass);
SELECT gp_stat_reset_vacuum_stats('gpvs_pages_repl'::regclass);
SELECT bool_and(pages_frozen = 0 AND pages_all_visible = 0
                AND dead_pages = 0 AND freeze_age_vacuum_count = 0) AS page_counters_reset
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
 WHERE relname IN ('gpvs_pages_dist', 'gpvs_pages_repl');
DROP TABLE gpvs_pages_before, gpvs_pages_dist, gpvs_pages_repl;
