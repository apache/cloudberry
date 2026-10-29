CREATE TABLE vacstat_test (a int);
INSERT INTO vacstat_test SELECT i FROM generate_series(1,10) i ;
VACUUM vacstat_test;

-- Confirm that VACUUM has updated stats from all nodes
SELECT true FROM pg_class WHERE oid='vacstat_test'::regclass
AND relpages > 0
AND reltuples > 0
AND relallvisible > 0;

SELECT true FROM pg_class WHERE oid='vacstat_test'::regclass
AND relpages =
    (SELECT SUM(relpages) FROM gp_dist_random('pg_class')
     WHERE oid='vacstat_test'::regclass)
AND reltuples =
    (SELECT SUM(reltuples) FROM gp_dist_random('pg_class')
     WHERE oid='vacstat_test'::regclass)
AND relallvisible =
    (SELECT SUM(relallvisible) FROM gp_dist_random('pg_class')
     WHERE oid='vacstat_test'::regclass);

DROP TABLE vacstat_test;

-- relallfrozen follows segment catalogs for dist tables.
CREATE TABLE frozen_stats_dist (a int) DISTRIBUTED BY (a);
INSERT INTO frozen_stats_dist SELECT generate_series(1, 2000);
ANALYZE frozen_stats_dist;
VACUUM (FREEZE) frozen_stats_dist;
SELECT relallfrozen > 0 AND relallfrozen <= relallvisible
       AND relallfrozen = (SELECT sum(relallfrozen) FROM gp_dist_random('pg_class')
                          WHERE oid = 'frozen_stats_dist'::regclass)
       AND relallvisible = (SELECT sum(relallvisible) FROM gp_dist_random('pg_class')
                           WHERE oid = 'frozen_stats_dist'::regclass)
         AS frozen_stats_match
  FROM pg_class WHERE oid = 'frozen_stats_dist'::regclass;
UPDATE frozen_stats_dist SET a = a + 10000 WHERE a = 1;
ANALYZE frozen_stats_dist;
SELECT relallfrozen > 0 AND relallfrozen <= relallvisible
       AND relallfrozen = (SELECT sum(relallfrozen) FROM gp_dist_random('pg_class')
                          WHERE oid = 'frozen_stats_dist'::regclass)
       AND relallvisible = (SELECT sum(relallvisible) FROM gp_dist_random('pg_class')
                           WHERE oid = 'frozen_stats_dist'::regclass)
         AS frozen_stats_match
  FROM pg_class WHERE oid = 'frozen_stats_dist'::regclass;
VACUUM (FULL) frozen_stats_dist;
SELECT relallfrozen = 0 AND relallvisible = 0 AS rewrite_clears_map
  FROM pg_class WHERE oid = 'frozen_stats_dist'::regclass;
DROP TABLE frozen_stats_dist;

-- relallfrozen follows segment catalogs for repl tables.
CREATE TABLE frozen_stats_repl (a int) DISTRIBUTED REPLICATED;
INSERT INTO frozen_stats_repl SELECT generate_series(1, 2000);
ANALYZE frozen_stats_repl;
VACUUM (FREEZE) frozen_stats_repl;
SELECT relallfrozen > 0 AND relallfrozen <= relallvisible
       AND relallfrozen = (SELECT sum(relallfrozen) FROM gp_dist_random('pg_class')
                          WHERE oid = 'frozen_stats_repl'::regclass) / (SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'frozen_stats_repl'::regclass)
       AND relallvisible = (SELECT sum(relallvisible) FROM gp_dist_random('pg_class')
                           WHERE oid = 'frozen_stats_repl'::regclass) / (SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'frozen_stats_repl'::regclass)
         AS frozen_stats_match
  FROM pg_class WHERE oid = 'frozen_stats_repl'::regclass;
UPDATE frozen_stats_repl SET a = a + 10000 WHERE a = 1;
ANALYZE frozen_stats_repl;
SELECT relallfrozen > 0 AND relallfrozen <= relallvisible
       AND relallfrozen = (SELECT sum(relallfrozen) FROM gp_dist_random('pg_class')
                          WHERE oid = 'frozen_stats_repl'::regclass) / (SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'frozen_stats_repl'::regclass)
       AND relallvisible = (SELECT sum(relallvisible) FROM gp_dist_random('pg_class')
                           WHERE oid = 'frozen_stats_repl'::regclass) / (SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'frozen_stats_repl'::regclass)
         AS frozen_stats_match
  FROM pg_class WHERE oid = 'frozen_stats_repl'::regclass;
VACUUM (FULL) frozen_stats_repl;
SELECT relallfrozen = 0 AND relallvisible = 0 AS rewrite_clears_map
  FROM pg_class WHERE oid = 'frozen_stats_repl'::regclass;
DROP TABLE frozen_stats_repl;

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
CREATE TABLE core_timing_repl (a int) DISTRIBUTED REPLICATED;
INSERT INTO core_timing_repl SELECT generate_series(1, 1000);
DELETE FROM core_timing_repl;
VACUUM core_summary_test, core_timing_repl;
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
-- Native timing summaries work without a statistics extension.
SELECT count(*) = 2 AND bool_and(total_vacuum_time > 0) AS tables_vacuum_timed
  FROM gp_stat_all_tables_summary
 WHERE relname IN ('core_summary_test', 'core_timing_repl');
-- Flush database timing before checking its cluster aggregate.
SELECT gp_stat_force_next_flush();
SELECT total_vacuum_time > 0 AS database_vacuum_timed
  FROM gp_stat_vacuum_summary
 WHERE datname = current_database();
DROP TABLE core_summary_test, core_timing_repl;
