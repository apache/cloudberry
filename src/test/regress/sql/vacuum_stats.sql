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
