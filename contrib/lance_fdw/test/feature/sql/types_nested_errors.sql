-- types_nested_errors: the refusal half of struct and map.
--
-- Everything here is a column that must not be read: a map whose keys are not
-- strings, a struct with one B-tier subfield, a Lance Blob v2 descriptor, and
-- declarations that do not match the struct they are pointed at.  The messages
-- are the point, so the statements go through report() below, which returns the
-- message and its detail: for a nested type the column's own Arrow type is
-- "struct" and says nothing, so the detail is where the subfield is named.
--
-- Both execution modes are exercised, because the two of them refuse from
-- different schemas.  Under all segments the coordinator checks the projection
-- against the dataset schema, which carries the Arrow extension name; under
-- mpp_execute 'coordinator' one process opens a stream and checks the schema
-- that comes back, which has the extension name stripped and is where the Blob
-- v2 rule has only the descriptor's five child names to go by.
--
-- Every object this suite creates is named nsterr_*, so an assertion that
-- aborts it part-way leaves nothing another suite can trip over; composite
-- types are database-wide, which is why the prefix matters here.
\pset format unaligned
CREATE FUNCTION lance_feature.nsterr_report(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  d text;
BEGIN
  EXECUTE stmt;
  RETURN 'no error';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS d = PG_EXCEPTION_DETAIL;
  RETURN regexp_replace(SQLERRM, '\s*\(seg\d+ [^)]*\)$', '') ||
         CASE WHEN d = '' THEN '' ELSE ' | ' || d END;
END;
$$;
DO $$
BEGIN
  EXECUTE format('CREATE SERVER nsterr_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
-- A map with int32 keys: a JSON object has string keys only, and printing the
-- key on the way in would be a silent distortion.  No declaration
-- makes it readable, and the refusal names the column and the key type.
CREATE FOREIGN TABLE lance_feature.nsterr_map (id integer, c_map_i32_utf8 jsonb)
  SERVER nsterr_files OPTIONS (uri 'maps.lance');
SELECT lance_feature.nsterr_report($$SELECT c_map_i32_utf8 FROM lance_feature.nsterr_map$$) AS map_i32_key;
CREATE FOREIGN TABLE lance_feature.nsterr_map_coord (id integer, c_map_i32_utf8 jsonb)
  SERVER nsterr_files OPTIONS (uri 'maps.lance', mpp_execute 'coordinator');
SELECT lance_feature.nsterr_report($$SELECT c_map_i32_utf8 FROM lance_feature.nsterr_map_coord$$) AS map_i32_key_coordinator;
-- The A-tier map beside it is unaffected: the refusal is per column.
CREATE FOREIGN TABLE lance_feature.nsterr_map_ok (id integer, c_map_utf8_i32 jsonb)
  SERVER nsterr_files OPTIONS (uri 'maps.lance');
SELECT count(c_map_utf8_i32) AS readable_maps FROM lance_feature.nsterr_map_ok;
-- A struct with one B-tier subfield is B-tier as a whole: there is no partial
-- projection of a struct, so "ok" is not on offer either.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("nested_btier.lance")
  FROM SERVER nsterr_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."nested_btier.lance" RENAME TO nsterr_btier_import;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.nsterr_btier_import'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
SELECT count(*) AS rows, count(id) AS ids FROM lance_feature.nsterr_btier_import;
-- Declaring the composite type by hand does not make it readable either, and
-- the message names the subfield that sank the column.
CREATE TYPE lance_feature.nsterr_btier AS (ok integer, bad bigint);
CREATE FOREIGN TABLE lance_feature.nsterr_btier_tbl
  (id integer, c_struct_b lance_feature.nsterr_btier)
  SERVER nsterr_files OPTIONS (uri 'nested_btier.lance');
SELECT lance_feature.nsterr_report($$SELECT c_struct_b FROM lance_feature.nsterr_btier_tbl$$) AS btier_subfield;
CREATE FOREIGN TABLE lance_feature.nsterr_btier_coord
  (id integer, c_struct_b lance_feature.nsterr_btier)
  SERVER nsterr_files OPTIONS (uri 'nested_btier.lance', mpp_execute 'coordinator');
SELECT lance_feature.nsterr_report($$SELECT c_struct_b FROM lance_feature.nsterr_btier_coord$$) AS btier_subfield_coordinator;
-- A Lance Blob v2 column is refused although all five of its children are
-- A-tier scalars: the descriptor is Lance's internal representation, not the
-- payload, and handing it out now would have to be taken back later.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("blobv2.lance")
  FROM SERVER nsterr_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."blobv2.lance" RENAME TO nsterr_blobv2_import;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.nsterr_blobv2_import'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- The two columns beside it still read.
SELECT id, note FROM lance_feature.nsterr_blobv2_import ORDER BY id;
-- The strongest form of the rule: a composite type that matches the descriptor
-- exactly, which is what the column would be readable as if the rule were gone.
-- Under all segments the coordinator refuses it from the dataset schema, which
-- carries the extension name; under mpp_execute 'coordinator' the refusal comes
-- from the stream schema, where only the five child names are left.
--
-- What each case is worth was measured by taking the two halves of the rule out
-- in turn: without the extension name the first case reads the descriptor as a
-- composite, so that case is the rule's own test.  The second one is refused
-- either way, because the descriptor's "position" child is uint64 - the detail
-- is what tells the two refusals apart, and it is the reason the second case
-- checks the detail rather than only the message.
CREATE TYPE lance_feature.nsterr_blobv2 AS (kind smallint, position bigint,
                                            size bigint, blob_id integer,
                                            blob_uri text);
CREATE FOREIGN TABLE lance_feature.nsterr_blobv2_tbl
  (id integer, c_blob_v2 lance_feature.nsterr_blobv2)
  SERVER nsterr_files OPTIONS (uri 'blobv2.lance');
SELECT lance_feature.nsterr_report($$SELECT c_blob_v2 FROM lance_feature.nsterr_blobv2_tbl$$) AS blobv2_dataset_schema;
CREATE FOREIGN TABLE lance_feature.nsterr_blobv2_coord
  (id integer, c_blob_v2 lance_feature.nsterr_blobv2)
  SERVER nsterr_files OPTIONS (uri 'blobv2.lance', mpp_execute 'coordinator');
SELECT lance_feature.nsterr_report($$SELECT c_blob_v2 FROM lance_feature.nsterr_blobv2_coord$$) AS blobv2_stream_schema;
-- And as the type it will have one day, once lance-c can fetch the payload.
CREATE FOREIGN TABLE lance_feature.nsterr_blobv2_bytea (id integer, c_blob_v2 bytea)
  SERVER nsterr_files OPTIONS (uri 'blobv2.lance');
SELECT lance_feature.nsterr_report($$SELECT c_blob_v2 FROM lance_feature.nsterr_blobv2_bytea$$) AS blobv2_as_bytea;
-- A readable struct read into a declaration that does not fit.  A composite
-- type of the wrong shape says so; a subfield whose type does not fit is named
-- by its path under the column.
CREATE TYPE lance_feature.nsterr_one AS (a integer);
CREATE FOREIGN TABLE lance_feature.nsterr_shape
  (id integer, c_struct lance_feature.nsterr_one)
  SERVER nsterr_files OPTIONS (uri 'nested.lance');
SELECT lance_feature.nsterr_report($$SELECT c_struct FROM lance_feature.nsterr_shape$$) AS wrong_field_count;
CREATE TYPE lance_feature.nsterr_texts AS (a text, b text);
CREATE FOREIGN TABLE lance_feature.nsterr_subtype
  (id integer, c_struct lance_feature.nsterr_texts)
  SERVER nsterr_files OPTIONS (uri 'nested.lance');
SELECT lance_feature.nsterr_report($$SELECT c_struct FROM lance_feature.nsterr_subtype$$) AS wrong_subfield_type;
-- A nested struct names the whole path, not just the field it failed on.  The
-- subfield is called "inner", which is a keyword and has to be quoted by hand
-- here; IMPORT FOREIGN SCHEMA quotes every field name it writes, which is what
-- lets a Lance schema use a name PostgreSQL reserves.
CREATE TYPE lance_feature.nsterr_inner AS (x integer, y integer);
CREATE TYPE lance_feature.nsterr_outer AS ("inner" lance_feature.nsterr_inner, z integer);
CREATE FOREIGN TABLE lance_feature.nsterr_deep
  (id integer, c_nested lance_feature.nsterr_outer)
  SERVER nsterr_files OPTIONS (uri 'nested.lance');
SELECT lance_feature.nsterr_report($$SELECT c_nested FROM lance_feature.nsterr_deep$$) AS wrong_nested_subfield;
-- A struct is not text, and a map is not text: neither has a declaration that
-- turns it into one.
CREATE FOREIGN TABLE lance_feature.nsterr_as_text (id integer, c_struct text)
  SERVER nsterr_files OPTIONS (uri 'nested.lance');
SELECT lance_feature.nsterr_report($$SELECT c_struct FROM lance_feature.nsterr_as_text$$) AS struct_as_text;
CREATE FOREIGN TABLE lance_feature.nsterr_map_text (id integer, c_map_utf8_i32 text)
  SERVER nsterr_files OPTIONS (uri 'maps.lance');
SELECT lance_feature.nsterr_report($$SELECT c_map_utf8_i32 FROM lance_feature.nsterr_map_text$$) AS map_as_text;
-- A list of structs is an array of the composite type and not the composite
-- type itself.
CREATE FOREIGN TABLE lance_feature.nsterr_list_scalar
  (id integer, c_list_struct lance_feature.nsterr_texts)
  SERVER nsterr_files OPTIONS (uri 'nested.lance');
SELECT lance_feature.nsterr_report($$SELECT c_list_struct FROM lance_feature.nsterr_list_scalar$$) AS list_struct_as_composite;
-- A composite type name that is taken by something else is an error rather
-- than a reuse: reusing it would read the column as a type that was never
-- built for it.  The import creates its types in the schema it
-- imports into, so a second schema with a type of that name in it is enough to
-- provoke the collision without disturbing the one types_nested built.
CREATE SCHEMA nsterr_schema;
CREATE TYPE nsterr_schema.lance_nested_lance_c_struct AS (wrong integer);
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("nested.lance")
  FROM SERVER nsterr_files INTO nsterr_schema;
-- After all of that the same backend still reads the readable columns of the
-- same datasets, under both execution modes.
CREATE FOREIGN TABLE lance_feature.nsterr_ok (id integer, note text)
  SERVER nsterr_files OPTIONS (uri 'blobv2.lance');
CREATE FOREIGN TABLE lance_feature.nsterr_ok_coord (id integer, note text)
  SERVER nsterr_files OPTIONS (uri 'blobv2.lance', mpp_execute 'coordinator');
SELECT count(*) AS rows, count(note) AS notes FROM lance_feature.nsterr_ok;
SELECT count(*) AS rows, count(note) AS notes FROM lance_feature.nsterr_ok_coord;
