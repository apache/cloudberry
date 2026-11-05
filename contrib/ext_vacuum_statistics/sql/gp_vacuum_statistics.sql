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
-- and cleanup after DROP.
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

-- TOAST tables and their indexes inherit the parent's distribution policy.
CREATE TABLE gpvs_toast_repl (id int, payload text) DISTRIBUTED REPLICATED;
CREATE TABLE gpvs_toast_dist (id int, payload text) DISTRIBUTED BY (id);
ALTER TABLE gpvs_toast_repl ALTER COLUMN payload SET STORAGE EXTERNAL;
ALTER TABLE gpvs_toast_dist ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO gpvs_toast_repl
  SELECT g, repeat(md5(g::text), 500) FROM generate_series(1, 100) g;
INSERT INTO gpvs_toast_dist SELECT * FROM gpvs_toast_repl;
DELETE FROM gpvs_toast_repl WHERE id <= 50;
DELETE FROM gpvs_toast_dist WHERE id <= 50;
VACUUM gpvs_toast_repl, gpvs_toast_dist;

WITH raw AS (
  SELECT relid, sum(tuples_deleted) AS tuples_deleted, sum(wal_records) AS wal_records
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables
  WHERE gp_segment_id >= 0 GROUP BY relid
)
SELECT count(*) = 2 AND bool_and(
         s.tuples_deleted > 0 AND s.wal_records > 0 AND
         s.tuples_deleted = (r.tuples_deleted / CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END)::bigint AND
         s.wal_records = (r.wal_records / CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END)::bigint)
       AS toast_tables_normalized
FROM pg_class c
JOIN gp_distribution_policy d ON d.localoid = c.oid
JOIN ext_vacuum_statistics.gp_stats_vacuum_tables_summary s ON s.relid = c.reltoastrelid
JOIN raw r ON r.relid = s.relid
WHERE c.oid IN ('gpvs_toast_repl'::regclass, 'gpvs_toast_dist'::regclass);

WITH raw AS (
  SELECT indexrelid, sum(tuples_deleted) AS tuples_deleted, sum(wal_records) AS wal_records
  FROM ext_vacuum_statistics.gp_stats_vacuum_indexes
  WHERE gp_segment_id >= 0 GROUP BY indexrelid
)
SELECT count(*) = 2 AND bool_and(
         s.tuples_deleted > 0 AND s.wal_records > 0 AND
         s.tuples_deleted = (r.tuples_deleted / CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END)::bigint AND
         s.wal_records = (r.wal_records / CASE WHEN d.policytype = 'r' THEN d.numsegments ELSE 1 END)::bigint)
       AS toast_indexes_normalized
FROM pg_class c
JOIN gp_distribution_policy d ON d.localoid = c.oid
JOIN pg_index i ON i.indrelid = c.reltoastrelid
JOIN ext_vacuum_statistics.gp_stats_vacuum_indexes_summary s ON s.indexrelid = i.indexrelid
JOIN raw r ON r.indexrelid = s.indexrelid
WHERE c.oid IN ('gpvs_toast_repl'::regclass, 'gpvs_toast_dist'::regclass);

DROP TABLE gpvs_toast_repl, gpvs_toast_dist;
