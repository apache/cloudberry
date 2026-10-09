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

-- Native work counters and cluster summaries need no statistics extension.
CREATE TABLE work_stats_dist (a int PRIMARY KEY) DISTRIBUTED BY (a);
CREATE TABLE work_stats_repl (a int) DISTRIBUTED REPLICATED;
INSERT INTO work_stats_dist SELECT generate_series(1, 2000);
ANALYZE work_stats_dist;
INSERT INTO work_stats_repl SELECT generate_series(1, 2000);
ANALYZE work_stats_repl;
VACUUM (FREEZE) work_stats_dist, work_stats_repl;
SELECT relname, freeze_age_vacuum_count =
       CASE WHEN relname = 'work_stats_repl' THEN 1
            ELSE (SELECT numsegments FROM gp_distribution_policy
                  WHERE localoid = 'work_stats_dist'::regclass) END AS freeze_count_matches,
       pages_frozen > 0 AND pages_all_visible > 0 AS page_work_recorded
  FROM gp_stat_vacuum_tables_summary
 WHERE relname IN ('work_stats_dist', 'work_stats_repl') ORDER BY relname;
SELECT count(*) = 2 * (SELECT numsegments FROM gp_distribution_policy
                      WHERE localoid = 'work_stats_dist'::regclass)
       AND bool_and(freeze_age_vacuum_count = 1) AS per_segment_counts
  FROM gp_stat_vacuum_tables
 WHERE relname IN ('work_stats_dist', 'work_stats_repl') AND gp_segment_id >= 0;
DELETE FROM work_stats_dist WHERE a <= 100;
DELETE FROM work_stats_repl WHERE a <= 100;
VACUUM work_stats_dist, work_stats_repl;
SELECT relname, tuples_deleted = 100 AS deleted_matches
  FROM gp_stat_vacuum_tables_summary
 WHERE relname IN ('work_stats_dist', 'work_stats_repl') ORDER BY relname;
SELECT tuples_deleted = 100 AS index_work_matches
  FROM gp_stat_vacuum_indexes_summary WHERE indexrelname = 'work_stats_dist_pkey';
DROP TABLE work_stats_dist, work_stats_repl;

CREATE TABLE work_stats_ao (a int) WITH (appendonly = true) DISTRIBUTED BY (a);
CREATE TABLE work_stats_aoco (a int) WITH (appendonly = true, orientation = column) DISTRIBUTED BY (a);
INSERT INTO work_stats_ao SELECT generate_series(1, 2000);
ANALYZE work_stats_ao;
INSERT INTO work_stats_aoco SELECT generate_series(1, 2000);
ANALYZE work_stats_aoco;
DELETE FROM work_stats_ao WHERE a <= 1000;
DELETE FROM work_stats_aoco WHERE a <= 1000;
VACUUM work_stats_ao, work_stats_aoco;
SELECT relname, tuples_deleted = 1000 AND tuples_moved = 1000
       AND compacted_segments > 0 AND pages_scanned > 0 AS compaction_matches
  FROM gp_stat_vacuum_tables_summary
 WHERE relname IN ('work_stats_ao', 'work_stats_aoco') ORDER BY relname;
DROP TABLE work_stats_ao, work_stats_aoco;
