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

-- A fresh backend's first heap write must clear VM bits without allocating
-- inside its critical section (also exercised by assertion builds).
CREATE TABLE core_vm_first_write (a int) DISTRIBUTED REPLICATED;
INSERT INTO core_vm_first_write VALUES (1);
VACUUM FREEZE core_vm_first_write;
\connect -reuse-previous=on
INSERT INTO core_vm_first_write VALUES (2);
SELECT gp_stat_force_next_flush();
SELECT bool_and(visible_page_marks_cleared > 0 AND frozen_page_marks_cleared > 0)
       AS vm_clears_counted
  FROM gp_stat_all_tables WHERE relname = 'core_vm_first_write' AND gp_segment_id > -1;
DROP TABLE core_vm_first_write;

-- Core GUC regression: reconnect with timing enabled at session startup.
-- Segment processes must follow coordinator startup options, SET, and
-- ROLLBACK.  All three checks expect t.
\connect -reuse-previous=on "options='-c track_cost_delay_timing=on'"
SELECT bool_and(setting = 'on') AS segment_timing_local
  FROM gp_dist_random('pg_settings') WHERE name = 'track_cost_delay_timing';
BEGIN;
SET track_cost_delay_timing = off;
SELECT bool_and(setting = 'off') AS segment_timing_local
  FROM gp_dist_random('pg_settings') WHERE name = 'track_cost_delay_timing';
ROLLBACK;
SELECT bool_and(setting = 'on') AS segment_timing_local
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
