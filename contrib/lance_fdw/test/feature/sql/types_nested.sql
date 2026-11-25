-- types_nested: the readable half of struct and map.
--
-- The tables are imported rather than written by hand, because the composite
-- type a struct column needs does not exist until IMPORT FOREIGN SCHEMA makes
-- one: the two halves of the bridge - the type it declares and the value it
-- reads - are checked against each other, as in the types suite.
--
-- The values are test/fixtures/expected/nested.jsonl and maps.jsonl, which is
-- what pylance reads out of the same datasets.  Two of those values are UTF-8
-- (a string subfield and a map key), and everything committed here is ASCII,
-- so those two are checked as hex and by lookup instead of being printed.
--
-- Text subfields go through to_json() because two fixture values carry a tab
-- and a newline, which unaligned output would otherwise spread over three lines.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER nst_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("nested.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."nested.lance" RENAME TO nst;
-- A struct column is declared with a composite type named after the foreign
-- table and the field path, a list of structs with an array of one, and a
-- struct holding a list keeps the list as an array of its element.
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.nst'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- The types themselves, field by field: this is the structure the fixture's
-- manifest records as composite_fields, and the naming rule - lower
-- case, everything outside [a-z0-9_] folded to _, a nested struct carrying its
-- parent's path.
SELECT format('%s(%s %s)', t.typname, a.attname,
              format_type(a.atttypid, a.atttypmod)) AS composite_field
  FROM pg_type t
  JOIN pg_class c ON c.oid = t.typrelid
  JOIN pg_attribute a ON a.attrelid = c.oid
  WHERE t.typnamespace = 'lance_feature'::regnamespace
    AND t.typname LIKE 'lance_nested_lance%'
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY t.typname, a.attnum;
-- A flat struct, subfield by subfield.  Row 4 holds a non-ASCII string and is
-- checked below instead.
SELECT id, (c_struct).a AS a, to_json((c_struct).b) AS b
  FROM lance_feature.nst WHERE id <> 4 ORDER BY id;
SELECT id, (c_struct).a AS a, encode(convert_to((c_struct).b, 'UTF8'), 'hex') AS b_utf8
  FROM lance_feature.nst WHERE id = 4;
-- A NULL struct and a struct whose subfields are all NULL are different values,
-- and only the second one has a composite to look inside.  IS NULL cannot tell
-- them apart - on a row value it is true when every field is null, which is SQL
-- row semantics and not this wrapper's doing - so the text form is what says
-- whether there is a composite there at all.
SELECT id, c_struct::text IS NULL AS whole_null, c_struct::text AS as_text,
       (c_struct).a IS NULL AS a_null, (c_struct).b IS NULL AS b_null
  FROM lance_feature.nst WHERE id IN (1, 2) ORDER BY id;
-- Two levels deep: the inner struct is a composite type of its own, and its
-- fields are reached through it.
SELECT id, ((c_nested).inner).x AS x, to_json(((c_nested).inner).y) AS y,
       (c_nested).z AS z
  FROM lance_feature.nst ORDER BY id;
-- The same distinction one level in: row 2 has no inner struct, row 3 has one
-- whose fields are both NULL.
SELECT id, ((c_nested).inner)::text IS NULL AS inner_null,
       ((c_nested).inner)::text AS inner_text
  FROM lance_feature.nst WHERE id IN (2, 3) ORDER BY id;
-- A list of structs is an array of the composite type: an empty list is an
-- empty array, a NULL list is NULL, and a NULL element stays NULL.  The three
-- empty first elements below are told apart by the count beside them: no array,
-- an array of nothing, and an array whose first element is NULL.
SELECT id, c_list_struct IS NULL AS null_list, cardinality(c_list_struct) AS n,
       c_list_struct[1]::text AS first,
       (c_list_struct[1]).a AS a1, to_json((c_list_struct[1]).b) AS b1,
       (c_list_struct[2]).a AS a2, to_json((c_list_struct[2]).b) AS b2
  FROM lance_feature.nst ORDER BY id;
-- A struct holding a list: the list is an array inside the composite, with its
-- own NULLs and its own empty case.
SELECT id, (c_struct_list).arr AS arr, to_json((c_struct_list).name) AS name
  FROM lance_feature.nst ORDER BY id;
-- The whole set of columns as text, which is what the two execution modes are
-- compared on below.
SELECT id, c_struct::text AS c_struct, c_nested::text AS c_nested
  FROM lance_feature.nst WHERE id IN (0, 3) ORDER BY id;
-- The same dataset imported a second time reuses the composite types rather
-- than making a second set of them: the definitions are identical,
-- so the names resolve to the types the first import created.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("nested.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."nested.lance" RENAME TO nst_coord;
ALTER FOREIGN TABLE lance_feature.nst_coord OPTIONS (ADD mpp_execute 'coordinator');
SELECT count(*) AS composite_types
  FROM pg_type t
  WHERE t.typnamespace = 'lance_feature'::regnamespace
    AND t.typname LIKE 'lance_nested_lance%'
    AND t.typtype = 'c';
SELECT (SELECT a.atttypid FROM pg_attribute a
          WHERE a.attrelid = 'lance_feature.nst'::regclass AND a.attname = 'c_nested')
     = (SELECT a.atttypid FROM pg_attribute a
          WHERE a.attrelid = 'lance_feature.nst_coord'::regclass AND a.attname = 'c_nested')
       AS reused_the_type;
-- The debugging execution mode reads every fragment in one process and has to
-- return exactly the same rows.  Composite and array-of-composite values
-- are compared as text, which is the same comparison without asking the planner
-- for an equality operator on a row type.
SELECT count(*) AS coordinator_only FROM (
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst_coord
  EXCEPT
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst) d;
SELECT count(*) AS all_segments_only FROM (
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst
  EXCEPT
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst_coord) d;
-- A batch size that straddles the batch and the fragment boundary must not
-- change a value: a struct's children carry the parent's offset, which is
-- exactly what a batch starting part-way into a fragment gets wrong.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("nested.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."nested.lance" RENAME TO nst_b2;
ALTER FOREIGN TABLE lance_feature.nst_b2 OPTIONS (ADD batch_size '2');
SELECT count(*) AS batch2_only FROM (
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst_b2
  EXCEPT
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst) d;
SELECT count(*) AS default_only FROM (
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst
  EXCEPT
  SELECT id, c_struct::text, c_nested::text, c_list_struct::text, c_struct_list::text
    FROM lance_feature.nst_b2) d;
-- map<utf8,int32> is jsonb; the map beside it has int32 keys and is skipped,
-- loudly and by name.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("maps.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."maps.lance" RENAME TO nst_maps;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.nst_maps'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- Row 4 holds a non-ASCII key and is checked below.
SELECT id, c_map_utf8_i32 AS m, c_map_utf8_i32 ->> 'a' AS a, c_map_utf8_i32 ->> 'k' AS k
  FROM lance_feature.nst_maps WHERE id <> 4 ORDER BY id;
SELECT id, jsonb_typeof(c_map_utf8_i32) AS kind,
       c_map_utf8_i32 ->> '' AS empty_key,
       c_map_utf8_i32 ->> convert_from('\xc3bc6e6963c3b86465'::bytea, 'UTF8') AS unicode_key
  FROM lance_feature.nst_maps WHERE id = 4;
-- An empty map is not a NULL one, and a key whose value is NULL is not an
-- absent key: jsonb keeps both distinctions.
SELECT id, c_map_utf8_i32 IS NULL AS null_map, c_map_utf8_i32 = '{}'::jsonb AS empty_map
  FROM lance_feature.nst_maps WHERE id IN (1, 2) ORDER BY id;
SELECT id, c_map_utf8_i32 ? 'k' AS has_k, c_map_utf8_i32 ->> 'k' IS NULL AS k_is_null,
       c_map_utf8_i32 ? 'absent' AS has_absent
  FROM lance_feature.nst_maps WHERE id = 3;
-- The same two execution modes over the map column.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("maps.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."maps.lance" RENAME TO nst_maps_coord;
ALTER FOREIGN TABLE lance_feature.nst_maps_coord OPTIONS (ADD mpp_execute 'coordinator');
SELECT count(*) AS coordinator_only FROM (
  SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps_coord
  EXCEPT SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps) d;
SELECT count(*) AS all_segments_only FROM (
  SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps
  EXCEPT SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps_coord) d;
-- And the same batch boundary over the map column, whose entry window is read
-- from an offsets buffer of its own.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("maps.lance")
  FROM SERVER nst_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."maps.lance" RENAME TO nst_maps_b2;
ALTER FOREIGN TABLE lance_feature.nst_maps_b2 OPTIONS (ADD batch_size '2');
SELECT count(*) AS batch2_only FROM (
  SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps_b2
  EXCEPT SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps) d;
SELECT count(*) AS default_only FROM (
  SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps
  EXCEPT SELECT id, c_map_utf8_i32 FROM lance_feature.nst_maps_b2) d;
-- After all of that both datasets still count their rows.
SELECT count(*) AS rows, count(c_struct) AS structs FROM lance_feature.nst;
SELECT count(*) AS rows, count(c_map_utf8_i32) AS maps FROM lance_feature.nst_maps;
