-- Unsupported data paths, CREATE/ALTER guards, and binding validators.
-- Errors raised on a segment carry its address and pid, which vary per run.
-- start_matchsubs
-- m/  \(seg[0-9]+[^)]* pid=[0-9]+\)/
-- s/  \(seg[0-9]+[^)]* pid=[0-9]+\)//
-- end_matchsubs

SET client_min_messages = warning;
DROP MATERIALIZED VIEW IF EXISTS dlskel_mv CASCADE;
DROP TABLE IF EXISTS
  dlskel_r, dlskel_r2, dlskel_bad, dlskel_bad_dist,
  dlskel_bad_part, dlskel_bad_inherits, dlskel_bad_typed,
  dlskel_bad_temp, dlskel_bad_unlogged, dlskel_bad_oncommit,
  dlskel_bad_tablespace, dlskel_bad_missing_server,
  dlskel_bad_wrong_catalog, dlskel_bad_no_catalog,
  dlskel_bad_reloption, dlskel_bad_engine, dlskel_r_new, dlskel_heap,
  dlskel_bad_repl, dlskel_bad_purge
  CASCADE;
DROP SERVER IF EXISTS dlskel_cat CASCADE;
DROP SERVER IF EXISTS dlskel_cat2 CASCADE;
DROP SERVER IF EXISTS dlskel_vol CASCADE;
DROP SERVER IF EXISTS dlskel_free CASCADE;
DROP SERVER IF EXISTS dlskel_free2 CASCADE;
DROP SERVER IF EXISTS dlskel_bad_server CASCADE;
DROP SERVER IF EXISTS dlskel_bad_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_token CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_user CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_keytab CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_clientid CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_keyid CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_token CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_case CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_type CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_hadoop CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_builtin CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_nourl CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_realm CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_empty CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_bool CASCADE;
DROP SERVER IF EXISTS dlskel_bad_nopath CASCADE;
DROP SERVER IF EXISTS dlskel_bad_scheme CASCADE;
DROP SERVER IF EXISTS dlskel_bad_authority CASCADE;
DROP SERVER IF EXISTS dlskel_bad_query CASCADE;
DROP SERVER IF EXISTS dlskel_bad_userinfo CASCADE;
DROP SERVER IF EXISTS dlskel_bad_bucket CASCADE;
DROP SCHEMA IF EXISTS dlskel_sch CASCADE;
DROP SCHEMA IF EXISTS dlskel_sch2 CASCADE;
DROP SCHEMA IF EXISTS dlskel_plain CASCADE;
DROP SCHEMA IF EXISTS dlskel_plain2 CASCADE;
DROP TYPE IF EXISTS dlskel_type CASCADE;
DROP ROLE IF EXISTS dlskel_role;
RESET client_min_messages;

-- Quiet, so that the output does not depend on whether another test in the
-- same database created the extension first.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER dlskel_cat
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083');
CREATE SERVER dlskel_vol
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/reject',
           endpoint 'http://fake:9000');
CREATE SERVER dlskel_free
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://free:9083');
-- Which NOTICE CREATE ROLE emits depends on gp_resource_manager, and neither
-- is what this case is about.
SET client_min_messages = warning;
CREATE ROLE dlskel_role;
RESET client_min_messages;
CREATE TYPE dlskel_type AS (a int);

CREATE TABLE dlskel_r (a int, b text)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');

SELECT * FROM dlskel_r;
INSERT INTO dlskel_r VALUES (1, 'x');
UPDATE dlskel_r SET a = 1;
DELETE FROM dlskel_r;
COPY dlskel_r FROM stdin;
1	x
\.
COPY dlskel_r TO stdout;
CREATE INDEX ON dlskel_r (a);
SELECT * FROM dlskel_r TABLESAMPLE BERNOULLI (10);

-- A table from an earlier transaction takes the new-filelocator path rather
-- than the access method's truncate callback, so this is the case that would
-- silently report success if only the callback rejected it.
TRUNCATE dlskel_r;

-- Same for a table created in this transaction, which does reach the callback.
BEGIN;
CREATE TABLE dlskel_r_new (a int, b text)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
TRUNCATE dlskel_r_new;
ROLLBACK;

-- A multi-table TRUNCATE must refuse before truncating the heap beside it, so
-- the row below has to survive the attempt.
CREATE TABLE dlskel_heap (a int) DISTRIBUTED BY (a);
INSERT INTO dlskel_heap VALUES (1);
TRUNCATE dlskel_heap, dlskel_r;
SELECT count(*) AS heap_rows_kept FROM dlskel_heap;

VACUUM FULL dlskel_r;
SELECT * FROM dlskel_r FOR UPDATE;

CREATE TABLE dlskel_bad_dist (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol')
  DISTRIBUTED BY (a);
CREATE TABLE dlskel_bad_part (a int)
  PARTITION BY RANGE (a)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
CREATE TABLE dlskel_bad_inherits (b text)
  INHERITS (dlskel_r)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
CREATE TABLE dlskel_bad_typed OF dlskel_type
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
CREATE TEMP TABLE dlskel_bad_temp (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
CREATE UNLOGGED TABLE dlskel_bad_unlogged (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
BEGIN;
CREATE TABLE dlskel_bad_oncommit (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol')
  ON COMMIT DROP;
ROLLBACK;
CREATE TABLE dlskel_bad_tablespace (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol')
  TABLESPACE pg_default;

CREATE TABLE dlskel_bad USING iceberg AS SELECT 1;
CREATE MATERIALIZED VIEW dlskel_mv USING iceberg AS SELECT 1;

-- Converting a heap into a lake table has to be refused too: the relation is
-- still a heap when the statement arrives, so the guard above does not see it.
ALTER TABLE dlskel_heap SET ACCESS METHOD iceberg;
SET iceberg.default_catalog = 'dlskel_cat';
SET iceberg.default_volume = 'dlskel_vol';
ALTER TABLE dlskel_heap SET ACCESS METHOD iceberg;
RESET iceberg.default_catalog;
RESET iceberg.default_volume;

-- A column-bearing clause still cannot be honoured, and neither can a
-- replicated policy.
CREATE TABLE dlskel_bad_repl (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol')
  DISTRIBUTED REPLICATED;

-- Renaming a schema would repoint its lake tables at a different external
-- namespace, so a schema holding one is refused -- and, just as importantly, a
-- schema holding none is not: the guard has to be no wider than the problem.
CREATE SCHEMA dlskel_sch;
CREATE TABLE dlskel_sch.t (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol');
BEGIN;
ALTER SCHEMA dlskel_sch RENAME TO dlskel_sch2;
ROLLBACK;
CREATE SCHEMA dlskel_plain;
ALTER SCHEMA dlskel_plain RENAME TO dlskel_plain2;
DROP SCHEMA dlskel_plain2;

-- Renaming a wrapper breaks every mapping lookup at once, including the one
-- DROP needs, so it is refused whether or not a table exists yet.
-- Each attempt is rolled back: if the guard ever regresses, an accepted rename
-- would leave the extension's own wrapper under a different name, which nothing
-- in the cleanup below can undo and which breaks every later run.
BEGIN;
ALTER FOREIGN DATA WRAPPER iceberg_catalog_fdw RENAME TO dlskel_other_fdw;
ROLLBACK;
BEGIN;
ALTER FOREIGN DATA WRAPPER iceberg_volume_fdw RENAME TO dlskel_other_fdw;
ROLLBACK;

ALTER TABLE dlskel_r ADD COLUMN c int;
ALTER TABLE dlskel_r SET (fillfactor = 90);
ALTER TABLE dlskel_r SET ACCESS METHOD heap;
ALTER TABLE dlskel_r SET DISTRIBUTED BY (a);
ALTER TABLE dlskel_r RENAME TO dlskel_r2;
ALTER TABLE dlskel_r RENAME COLUMN a TO aa;
ALTER TABLE dlskel_r SET SCHEMA public;
-- OWNER TO is the one ALTER TABLE form that goes through: pg_dump writes it for
-- every table, and ownership cannot reach the external table.  Put it back
-- afterwards so the rest of the file still owns what it created.
ALTER TABLE dlskel_r OWNER TO dlskel_role;
SELECT relname, pg_get_userbyid(relowner) AS owner
FROM pg_class WHERE relname = 'dlskel_r';
ALTER TABLE dlskel_r OWNER TO CURRENT_USER;

ALTER SERVER dlskel_cat
  OPTIONS (SET uri 'thrift://other:9083');
ALTER SERVER dlskel_cat VERSION '2';
ALTER SERVER dlskel_cat RENAME TO dlskel_cat2;
ALTER SERVER dlskel_cat OWNER TO dlskel_role;

ALTER SERVER dlskel_free
  OPTIONS (SET uri 'thrift://other:9083');
ALTER SERVER dlskel_free VERSION '2';
ALTER SERVER dlskel_free OWNER TO dlskel_role;
ALTER SERVER dlskel_free RENAME TO dlskel_free2;

RESET iceberg.default_catalog;
RESET iceberg.default_volume;
CREATE TABLE dlskel_bad_missing_server (a int)
  USING iceberg
  WITH (catalog = 'dlskel_missing', volume = 'dlskel_vol');
CREATE TABLE dlskel_bad_wrong_catalog (a int)
  USING iceberg
  WITH (catalog = 'dlskel_vol', volume = 'dlskel_vol');
CREATE TABLE dlskel_bad_no_catalog (a int)
  USING iceberg
  WITH (volume = 'dlskel_vol');
CREATE TABLE dlskel_bad_purge (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol',
        purge_on_drop = 'perhaps');
CREATE TABLE dlskel_bad_reloption (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol', nonsense = 'x');

CREATE SERVER dlskel_bad_server
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083', nonsense 'x');
CREATE SERVER dlskel_bad_secret
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (secret_key 'x');
-- The catalog wrapper has its own allowlist, and its own credential keys.
CREATE SERVER dlskel_bad_cat_secret
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083', password 'x');
CREATE SERVER dlskel_bad_cat_token
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'polaris', uri 'https://fake:443', client_secret 'x');
-- Every key the credential list mirrors from an option module, so that a key
-- renamed in one place and not the other fails here instead of silently
-- ceasing to be caught.
CREATE SERVER dlskel_bad_cat_user
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083', username 'u');
CREATE SERVER dlskel_bad_cat_keytab
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083', krb_client_keytab '/k');
CREATE SERVER dlskel_bad_cat_clientid
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'rest', uri 'https://fake:443', client_id 'i');
CREATE SERVER dlskel_bad_vol_token
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/p', session_token 't');
CREATE SERVER dlskel_bad_vol_keyid
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/p', access_key_id 'k');
CREATE SERVER dlskel_bad_vol_secret
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/p', secret_access_key 's');
-- Option names are matched exactly, as the server itself matches them; a quoted
-- variant is a different, unknown option rather than a second spelling.
CREATE SERVER dlskel_bad_cat_case
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS ("TYPE" 'hive', uri 'thrift://fake:9083');
-- A catalog type outside the vocabulary, and one that is in it but has no
-- implementation behind it yet.
CREATE SERVER dlskel_bad_cat_type
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'nosuchcatalog', uri 'thrift://fake:9083');
CREATE SERVER dlskel_bad_cat_hadoop
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hadoop', uri 'hdfs://fake:8020');
-- builtin needs no url; supplying one means the two disagree.
CREATE SERVER dlskel_bad_cat_builtin
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'builtin', uri 'thrift://fake:9083');
-- hive does need one.
CREATE SERVER dlskel_bad_cat_nourl
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive');
-- The realm applies to one catalog type only.
CREATE SERVER dlskel_bad_cat_realm
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri 'thrift://fake:9083',
           polaris_server_realm 'OTHER');
-- An empty value is refused where it is written, not where it is first read.
CREATE SERVER dlskel_bad_cat_empty
  FOREIGN DATA WRAPPER iceberg_catalog_fdw
  OPTIONS (type 'hive', uri '');
-- Not a boolean.
CREATE SERVER dlskel_bad_vol_bool
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://dlskel-bucket/p', path_style_access 'perhaps');
CREATE SERVER dlskel_bad_nopath
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (endpoint 'http://fake:9000');
CREATE SERVER dlskel_bad_scheme
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 'ftp://x/y');
CREATE SERVER dlskel_bad_authority
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://');
CREATE SERVER dlskel_bad_query
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://b/p?versionId=3');
CREATE SERVER dlskel_bad_userinfo
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://user@b/p');
CREATE SERVER dlskel_bad_bucket
  FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (base_path 's3://UPPER_case/p');

-- The metadata engine is not selectable, so naming one is an unknown option.
CREATE TABLE dlskel_bad_engine (a int)
  USING iceberg
  WITH (catalog = 'dlskel_cat', volume = 'dlskel_vol',
        engine = 'agent');

SET client_min_messages = warning;
DROP MATERIALIZED VIEW IF EXISTS dlskel_mv CASCADE;
DROP TABLE IF EXISTS
  dlskel_r, dlskel_r2, dlskel_bad, dlskel_bad_dist,
  dlskel_bad_part, dlskel_bad_inherits, dlskel_bad_typed,
  dlskel_bad_temp, dlskel_bad_unlogged, dlskel_bad_oncommit,
  dlskel_bad_tablespace, dlskel_bad_missing_server,
  dlskel_bad_wrong_catalog, dlskel_bad_no_catalog,
  dlskel_bad_reloption, dlskel_bad_engine, dlskel_r_new, dlskel_heap,
  dlskel_bad_repl, dlskel_bad_purge
  CASCADE;
DROP SERVER IF EXISTS dlskel_cat CASCADE;
DROP SERVER IF EXISTS dlskel_cat2 CASCADE;
DROP SERVER IF EXISTS dlskel_vol CASCADE;
DROP SERVER IF EXISTS dlskel_free CASCADE;
DROP SERVER IF EXISTS dlskel_free2 CASCADE;
DROP SERVER IF EXISTS dlskel_bad_server CASCADE;
DROP SERVER IF EXISTS dlskel_bad_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_token CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_user CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_keytab CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_clientid CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_keyid CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_secret CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_token CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_case CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_type CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_hadoop CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_builtin CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_nourl CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_realm CASCADE;
DROP SERVER IF EXISTS dlskel_bad_cat_empty CASCADE;
DROP SERVER IF EXISTS dlskel_bad_vol_bool CASCADE;
DROP SERVER IF EXISTS dlskel_bad_nopath CASCADE;
DROP SERVER IF EXISTS dlskel_bad_scheme CASCADE;
DROP SERVER IF EXISTS dlskel_bad_authority CASCADE;
DROP SERVER IF EXISTS dlskel_bad_query CASCADE;
DROP SERVER IF EXISTS dlskel_bad_userinfo CASCADE;
DROP SERVER IF EXISTS dlskel_bad_bucket CASCADE;
DROP SCHEMA IF EXISTS dlskel_sch CASCADE;
DROP SCHEMA IF EXISTS dlskel_sch2 CASCADE;
DROP SCHEMA IF EXISTS dlskel_plain CASCADE;
DROP SCHEMA IF EXISTS dlskel_plain2 CASCADE;
DROP TYPE IF EXISTS dlskel_type CASCADE;
DROP ROLE IF EXISTS dlskel_role;
RESET client_min_messages;
