-- Happy-path DDL, binding persistence, distributed catalog state, and drops.

SET client_min_messages = warning;
DROP TABLE IF EXISTS dlskel_t, dlskel_t2, dlskel_t3, dlskel_t_dump,
  dlskel_t_keep, dlskel_t_purge CASCADE;
DROP SERVER IF EXISTS dlskel_cat CASCADE;
DROP SERVER IF EXISTS dlskel_cat_rest CASCADE;
DROP SERVER IF EXISTS dlskel_vol CASCADE;
RESET client_min_messages;

-- Quiet, so that the output does not depend on whether another test in the
-- same database created the extension first.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER dlskel_cat
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083');
CREATE SERVER dlskel_cat_rest
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'rest', uri 'https://fake:443');
DROP SERVER dlskel_cat_rest;
CREATE SERVER dlskel_vol
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/prefix',
           endpoint 'http://fake:9000');

CREATE TABLE dlskel_t (a int, b text)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');

SELECT relname, amname, reloptions
FROM pg_class c
JOIN pg_am a ON a.oid = c.relam
WHERE relname = 'dlskel_t';

SELECT policytype, distkey
FROM gp_distribution_policy
WHERE localoid = 'dlskel_t'::regclass;

-- Exactly one pg_class row on each primary segment.  Counting rows in total
-- would accept one segment missing its row as long as another had two, which is
-- the very divergence this is here to catch; so compare the set of segments that
-- have exactly one row against the set of primaries.
SELECT count(*) = 0 AS every_segment_has_exactly_one
FROM (SELECT content FROM gp_segment_configuration
      WHERE content >= 0 AND role = 'p'
      EXCEPT
      SELECT gp_segment_id FROM gp_dist_random('pg_class')
      WHERE relname = 'dlskel_t'
      GROUP BY gp_segment_id HAVING count(*) = 1) missing_or_duplicated;

SELECT oid AS dlskel_cat_oid
FROM pg_foreign_server
WHERE srvname = 'dlskel_cat'
\gset
SELECT oid AS dlskel_vol_oid
FROM pg_foreign_server
WHERE srvname = 'dlskel_vol'
\gset
SELECT 'dlskel_t'::regclass::oid AS dlskel_t_oid
\gset

SELECT b.binding,
       NOT EXISTS (SELECT content FROM gp_segment_configuration
                   WHERE content >= 0 AND role = 'p'
                   EXCEPT
                   SELECT gp_segment_id FROM gp_dist_random('pg_depend')
                   WHERE classid = 'pg_class'::regclass
                     AND objid = :dlskel_t_oid
                     AND refclassid = 'pg_foreign_server'::regclass
                     AND refobjid = b.refobjid
                   GROUP BY gp_segment_id HAVING count(*) = 1)
       AS every_segment_has_exactly_one
FROM (VALUES ('catalog', :dlskel_cat_oid::oid),
             ('volume', :dlskel_vol_oid::oid)) AS b(binding, refobjid)
ORDER BY b.binding;

ANALYZE dlskel_t;
VACUUM dlskel_t;
SELECT reltuples IN (-1, 0) AS no_local_stats
FROM pg_class
WHERE oid = 'dlskel_t'::regclass;

-- pg_dump writes DISTRIBUTED RANDOMLY into the CREATE TABLE it emits, so this
-- is the statement a restore replays; refusing it would mean refusing to
-- restore a dump this module produced.  It has to yield the same policy as the
-- clause the module injects on its own.
CREATE TABLE dlskel_t_dump (a int, b text)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol')
  DISTRIBUTED RANDOMLY;
SELECT policytype, distkey
FROM gp_distribution_policy
WHERE localoid = 'dlskel_t_dump'::regclass;
DROP TABLE dlskel_t_dump;

-- Dropping the table drops this database's reference to it; the lake data stays
-- unless the table said otherwise.  The default and the explicit form both have
-- to be observable, which is why the stub reports which one it was asked for.
CREATE TABLE dlskel_t_keep (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
DROP TABLE dlskel_t_keep;
CREATE TABLE dlskel_t_purge (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol',
        purge_on_drop = true);
SELECT reloptions FROM pg_class WHERE relname = 'dlskel_t_purge';
DROP TABLE dlskel_t_purge;

SET iceberg.default_catalog = 'dlskel_cat';
SET iceberg.default_volume = 'dlskel_vol';
CREATE TABLE dlskel_t2 (a int) USING iceberg;
SELECT relname, amname, reloptions
FROM pg_class c
JOIN pg_am a ON a.oid = c.relam
WHERE relname = 'dlskel_t2';
RESET iceberg.default_catalog;
RESET iceberg.default_volume;

SET default_table_access_method = iceberg;
SET iceberg.default_catalog = 'dlskel_cat';
SET iceberg.default_volume = 'dlskel_vol';
CREATE TABLE dlskel_t3 (a int);
\set HIDE_TABLEAM off
\d+ dlskel_t3
RESET default_table_access_method;
RESET iceberg.default_catalog;
RESET iceberg.default_volume;

DROP SERVER dlskel_cat;
DROP SERVER dlskel_vol;

DROP TABLE dlskel_t;
SELECT b.binding,
       NOT EXISTS (SELECT 1 FROM gp_dist_random('pg_depend')
                   WHERE classid = 'pg_class'::regclass
                     AND objid = :dlskel_t_oid
                     AND refclassid = 'pg_foreign_server'::regclass
                     AND refobjid = b.refobjid)
       AS gone_from_every_segment
FROM (VALUES ('catalog', :dlskel_cat_oid::oid),
             ('volume', :dlskel_vol_oid::oid)) AS b(binding, refobjid)
ORDER BY b.binding;

DROP SERVER dlskel_cat CASCADE;
DROP SERVER dlskel_vol CASCADE;

SELECT gp_segment_id, relname
FROM gp_dist_random('pg_class')
WHERE relname LIKE 'dlskel\_%' ESCAPE '\'
ORDER BY 1, 2;

SET client_min_messages = warning;
DROP TABLE IF EXISTS dlskel_t, dlskel_t2, dlskel_t3, dlskel_t_dump,
  dlskel_t_keep, dlskel_t_purge CASCADE;
DROP SERVER IF EXISTS dlskel_cat CASCADE;
DROP SERVER IF EXISTS dlskel_cat_rest CASCADE;
DROP SERVER IF EXISTS dlskel_vol CASCADE;
RESET client_min_messages;
