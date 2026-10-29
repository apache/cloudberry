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

-- AO auxiliary tables and indexes are independently visible and follow the
-- parent's replication divisor. Save OIDs before DROP to detect orphaned
-- entries, which a view joined to pg_class would hide.
CREATE EXTENSION IF NOT EXISTS ext_vacuum_statistics;
SET optimizer_print_missing_stats = off;
CREATE TABLE gpvs_gc_row (id int, v text) WITH (appendonly = true) DISTRIBUTED BY (id);
CREATE TABLE gpvs_gc_col (id int, v text) WITH (appendonly = true, orientation = column) DISTRIBUTED REPLICATED;
CREATE INDEX gpvs_gc_row_idx ON gpvs_gc_row (id);
CREATE INDEX gpvs_gc_col_idx ON gpvs_gc_col (id);
INSERT INTO gpvs_gc_row SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
INSERT INTO gpvs_gc_col SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g;
DELETE FROM gpvs_gc_row WHERE id <= 1500;
DELETE FROM gpvs_gc_col WHERE id <= 500;
VACUUM gpvs_gc_row, gpvs_gc_col;

-- Store literal OIDs in a view so reads remain local even after DROP.
DO $$
DECLARE object_rows text; /* in func */
BEGIN
WITH relations AS (
  SELECT c.oid AS relid, c.oid AS parentrelid FROM pg_class c
   WHERE c.relname IN ('gpvs_gc_row', 'gpvs_gc_col')
  UNION ALL
  SELECT aux.relid, a.relid FROM pg_appendonly a,
       LATERAL unnest(ARRAY[a.segrelid, a.blkdirrelid, a.visimaprelid]) aux(relid)
   WHERE a.relid IN ('gpvs_gc_row'::regclass, 'gpvs_gc_col'::regclass) AND aux.relid <> 0
), objects AS (
  SELECT relid, parentrelid FROM relations
  UNION ALL
  SELECT i.indexrelid, r.parentrelid FROM relations r JOIN pg_index i ON i.indrelid = r.relid
)
SELECT string_agg(format('(%s::oid,%s::oid,%L::"char")', o.relid, o.parentrelid, c.relkind), ',') INTO object_rows
  FROM objects o JOIN pg_class c ON c.oid = o.relid; /* in func */
EXECUTE 'CREATE VIEW gpvs_gc_objects AS SELECT * FROM (VALUES ' || object_rows || ') AS objects(relid, parentrelid, relkind)'; /* in func */
END; /* in func */
$$;

-- All six auxiliary tables must be visible locally and in cluster summaries.
SELECT count(*) = 6 AS auxiliary_tables_visible
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables v
  JOIN gpvs_gc_objects o USING (relid) WHERE o.relkind IN ('o', 'b', 'M');
SELECT count(*) = 6 AS auxiliary_summaries_visible
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary v
  JOIN gpvs_gc_objects o USING (relid) WHERE o.relkind IN ('o', 'b', 'M');

-- Compare one physical resource counter against each object's segment sum.
SELECT count(*) = 6 AND bool_and(s.total_blks_hit = round(d.hits /
         CASE WHEN p.policytype = 'r' THEN p.numsegments ELSE 1 END)) AS auxiliary_totals_match
  FROM ext_vacuum_statistics.gp_stats_vacuum_tables_summary s
  JOIN gpvs_gc_objects o USING (relid)
  JOIN gp_distribution_policy p ON p.localoid = o.parentrelid
  JOIN (SELECT relid, sum(total_blks_hit) AS hits
          FROM ext_vacuum_statistics.gp_stats_vacuum_tables WHERE gp_segment_id >= 0
         GROUP BY relid) d USING (relid)
 WHERE o.relkind IN ('o', 'b', 'M');
SELECT count(*) > 2 AND bool_and(s.total_blks_hit = round(d.hits /
         CASE WHEN p.policytype = 'r' THEN p.numsegments ELSE 1 END)) AS index_totals_match
  FROM ext_vacuum_statistics.gp_stats_vacuum_indexes_summary s
  JOIN gpvs_gc_objects o ON o.relid = s.indexrelid
  JOIN gp_distribution_policy p ON p.localoid = o.parentrelid
  JOIN (SELECT indexrelid, sum(total_blks_hit) AS hits
          FROM ext_vacuum_statistics.gp_stats_vacuum_indexes WHERE gp_segment_id >= 0
         GROUP BY indexrelid) d USING (indexrelid);

CREATE VIEW gpvs_gc_entries AS
SELECT o.relid
  FROM gp_id CROSS JOIN gpvs_gc_objects o CROSS JOIN pg_database db
  LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_tables(db.oid, o.relid) t ON true
  LEFT JOIN LATERAL ext_vacuum_statistics.pg_stats_get_vacuum_indexes(db.oid, o.relid) i ON true
 WHERE db.datname = current_database() AND coalesce(t.relid, i.relid) IS NOT NULL;

SELECT count(*) = (SELECT count(*) FROM gpvs_gc_objects) AS all_local_entries
  FROM gpvs_gc_entries;
SELECT count(*) = (SELECT count(*) FROM gpvs_gc_objects) *
       (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0)
         AS all_segment_entries
  FROM gp_dist_random('gpvs_gc_entries');
BEGIN;
DROP TABLE gpvs_gc_row, gpvs_gc_col;
ROLLBACK;
SELECT count(*) = (SELECT count(*) FROM gpvs_gc_objects) AS rollback_keeps_local_entries
  FROM gpvs_gc_entries;
SELECT count(*) = (SELECT count(*) FROM gpvs_gc_objects) *
       (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0)
         AS rollback_keeps_segment_entries
  FROM gp_dist_random('gpvs_gc_entries');
BEGIN;
DROP TABLE gpvs_gc_row, gpvs_gc_col;
COMMIT;
SELECT count(*) = 0 AS commit_removes_local_entries FROM gpvs_gc_entries;
SELECT count(*) = 0 AS commit_removes_segment_entries FROM gp_dist_random('gpvs_gc_entries');
DROP VIEW gpvs_gc_entries;
DROP VIEW gpvs_gc_objects;
RESET optimizer_print_missing_stats;
