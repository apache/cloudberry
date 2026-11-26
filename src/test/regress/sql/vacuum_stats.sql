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
       tuples_frozen = 2000 AS freeze_work_recorded
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

-- TOAST relations inherit the replicated owner's normalization policy.
CREATE TABLE work_stats_toast (a int, payload text) DISTRIBUTED REPLICATED;
ALTER TABLE work_stats_toast ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO work_stats_toast SELECT g, repeat(md5(g::text), 2048)
  FROM generate_series(1, 100) g;
DELETE FROM work_stats_toast WHERE a <= 50;
VACUUM work_stats_toast;
SELECT count(*) = 1 AND bool_and(s.tuples_deleted > 0 AND
       s.tuples_deleted = raw.total / d.numsegments) AS toast_work_normalized
  FROM gp_stat_vacuum_tables_summary s
  JOIN pg_class owner ON owner.reltoastrelid = s.relid
  JOIN gp_distribution_policy d ON d.localoid = owner.oid
  JOIN (SELECT relid, sum(tuples_deleted) AS total
          FROM gp_dist_random('pg_stat_vacuum_tables') GROUP BY relid) raw
    ON raw.relid = s.relid
 WHERE owner.oid = 'work_stats_toast'::regclass;
SELECT count(*) = 1 AND bool_and(s.tuples_deleted > 0 AND
       s.tuples_deleted = raw.total / d.numsegments) AS toast_index_normalized
  FROM gp_stat_vacuum_indexes_summary s
  JOIN pg_index i ON i.indexrelid = s.indexrelid
  JOIN pg_class owner ON owner.reltoastrelid = i.indrelid
  JOIN gp_distribution_policy d ON d.localoid = owner.oid
  JOIN (SELECT indexrelid, sum(tuples_deleted) AS total
          FROM gp_dist_random('pg_stat_vacuum_indexes') GROUP BY indexrelid) raw
    ON raw.indexrelid = s.indexrelid
 WHERE owner.oid = 'work_stats_toast'::regclass;
DROP TABLE work_stats_toast;
