-- Full-width assignments must survive both dispatch formats and queue copies.
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX
\set regress_dll :libdir '/regress' :dlsuffix
CREATE FUNCTION test_attribute_version_qd() RETURNS SETOF boolean
AS :'regress_dll', 'test_attribute_version_assignments'
LANGUAGE C EXECUTE ON COORDINATOR;
CREATE FUNCTION test_attribute_version_qe() RETURNS SETOF boolean
AS :'regress_dll', 'test_attribute_version_assignments'
LANGUAGE C EXECUTE ON ALL SEGMENTS;
SELECT * FROM test_attribute_version_qd();
SELECT bool_and(ok) FROM test_attribute_version_qe() AS t(ok);
DROP FUNCTION test_attribute_version_qd();
DROP FUNCTION test_attribute_version_qe();

CREATE SCHEMA mpp_attribute_version;
SET search_path = mpp_attribute_version, public;
CREATE TYPE inner_type AS (a integer, b text[]);
CREATE TYPE outer_type AS (x inner_type, y inner_type[]);
CREATE TABLE nested_rows(id integer, v outer_type) DISTRIBUTED BY(id);
-- Parameter remapping must preserve the named row type's version header.
PREPARE nested_param(integer, outer_type) AS
INSERT INTO nested_rows VALUES ($1, $2);
EXECUTE nested_param(1, ROW(ROW(7, ARRAY['foo','bar'])::inner_type,
                          ARRAY[ROW(8, ARRAY['baz'])::inner_type])::outer_type);
EXECUTE nested_param(2, ROW(ROW(9, ARRAY['quux'])::inner_type,
                          ARRAY[ROW(10, ARRAY['bear','beer'])::inner_type])::outer_type);
SELECT id, ((v).x).a, ((v).x).b, ((v).y[1]).a, ((v).y[1]).b
FROM nested_rows ORDER BY id;
-- Include a transient record alongside the nested named composite over Motion.
SELECT id, v, ROW(v, id) FROM nested_rows ORDER BY id;
DEALLOCATE nested_param;

CREATE TABLE ddl_version(a integer) DISTRIBUTED RANDOMLY;
ALTER TABLE ddl_version ALTER COLUMN a TYPE bigint;
ALTER TABLE ddl_version ADD COLUMN b text;
ALTER TABLE ddl_version ALTER COLUMN a TYPE numeric;
ALTER TABLE ddl_version ALTER COLUMN a TYPE double precision;
CREATE TABLE parent_version(a integer) DISTRIBUTED RANDOMLY;
CREATE TABLE child_version(extra text) INHERITS(parent_version) DISTRIBUTED RANDOMLY;
ALTER TABLE parent_version ADD COLUMN added bigint;
BEGIN;
SAVEPOINT version_rollback;
ALTER TABLE ddl_version ADD COLUMN rolled_back integer;
ROLLBACK TO SAVEPOINT version_rollback;
ALTER TABLE ddl_version ADD COLUMN after_rollback integer;
COMMIT;

-- The same logical attribute must have exactly one version across all nodes.
SELECT count(*) = 0 AS versions_match
FROM (
  SELECT attrelid, attnum FROM (
    SELECT attrelid, attnum, attoptions, attisdropped FROM pg_attribute
    UNION ALL
    SELECT attrelid, attnum, attoptions, attisdropped FROM gp_dist_random('pg_attribute')
  ) attrs
  WHERE attrelid IN (SELECT oid FROM pg_class
                    WHERE relnamespace = 'mpp_attribute_version'::regnamespace)
    AND attnum > 0 AND NOT attisdropped
  GROUP BY attrelid, attnum
  HAVING count(*) <> (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p')
    OR count(attoptions) <> count(*)
    OR count(DISTINCT attoptions) <> 1
    OR bool_and(array_to_string(attoptions, ',') ~ '(^|,)pg_attver=[1-9][0-9]*($|,)') IS NOT TRUE
) differences;
RESET search_path;
DROP SCHEMA mpp_attribute_version CASCADE;

-- Cloudberry relation kinds and internal heaps do not map one-to-one to
-- rowtypes. Directory tables have one, bitmap auxiliary heaps do not.
CREATE DIRECTORY TABLE attver_directory;
SELECT bool_and(attoptions IS NOT NULL) AS directory_versioned
FROM pg_attribute
WHERE attrelid = 'attver_directory'::regclass AND attnum > 0;
CREATE TABLE attver_bitmap(a int) DISTRIBUTED BY (a);
CREATE INDEX attver_bitmap_idx ON attver_bitmap USING bitmap(a);
SELECT count(*) = 1 AND bool_and(reltype = 0) AS bitmap_aux_untyped
FROM pg_class
WHERE relnamespace = 'pg_bitmapindex'::regnamespace
  AND relname = 'pg_bm_' || 'attver_bitmap_idx'::regclass::oid;
DROP TABLE attver_bitmap;
DROP DIRECTORY TABLE attver_directory;
