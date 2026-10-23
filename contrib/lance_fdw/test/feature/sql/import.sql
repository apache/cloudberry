-- import: IMPORT FOREIGN SCHEMA against the fixtures, over file:// and
-- s3://.  The fixture directory and the bucket differ per environment, so the
-- servers are built from the regress.* settings run.sh injects and no path
-- or credential is ever printed.
--
-- The dataset directories are named <name>.lance, so that is what LIMIT TO
-- names and what the imported table is called; they are renamed afterwards to
-- keep the rest of the suite readable.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER lance_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("types_all.lance")
  FROM SERVER lance_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."types_all.lance" RENAME TO types_all;
-- Every A-tier Arrow type, mapped as the type table in README.md says.
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.types_all'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- The generated table keeps the name it was imported under, relative to the
-- server's base_uri.
SELECT o.opt AS table_option
  FROM pg_foreign_table t, unnest(t.ftoptions) AS o(opt)
  WHERE t.ftrelid = 'lance_feature.types_all'::regclass ORDER BY 1;
-- B-tier columns are skipped, loudly, and what is left is still imported.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("types_b.lance")
  FROM SERVER lance_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."types_b.lance" RENAME TO types_b;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.types_b'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- A blob-encoded column is B-tier even though its Arrow type is large_binary,
-- because the scanner hands out a descriptor for it; the plain
-- large_binary column next to it imports as bytea.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("blob.lance")
  FROM SERVER lance_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."blob.lance" RENAME TO blobs;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.blobs'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
-- Two datasets in one statement.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("frag_1.lance", "frag_2.lance")
  FROM SERVER lance_files INTO lance_feature;
SELECT c.relname AS imported
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'lance_feature' AND c.relkind = 'f' AND c.relname LIKE 'frag%'
  ORDER BY 1;
-- Without LIMIT TO there is nothing to enumerate, and saying so is the whole
-- point.
IMPORT FOREIGN SCHEMA fixtures FROM SERVER lance_files INTO lance_feature;
IMPORT FOREIGN SCHEMA fixtures EXCEPT ("types_all.lance")
  FROM SERVER lance_files INTO lance_feature;
-- A relative name needs a base_uri to be relative to.
CREATE SERVER lance_nobase FOREIGN DATA WRAPPER lance_fdw;
IMPORT FOREIGN SCHEMA fixtures LIMIT TO (relative_name)
  FROM SERVER lance_nobase INTO lance_feature;
-- The same import over s3://, with the credentials coming from the user
-- mapping and never from the statement text.
DO $$
BEGIN
  EXECUTE format('CREATE SERVER lance_s3 FOREIGN DATA WRAPPER lance_fdw '
                 'OPTIONS (base_uri %L, aws_endpoint %L, aws_region %L, allow_http %L)',
                 's3://' || current_setting('regress.s3_bucket') || '/fixtures',
                 current_setting('regress.s3_endpoint'),
                 current_setting('regress.s3_region'),
                 'true');
  EXECUTE format('CREATE USER MAPPING FOR PUBLIC SERVER lance_s3 '
                 'OPTIONS (aws_access_key_id %L, aws_secret_access_key %L)',
                 current_setting('regress.s3_key'),
                 current_setting('regress.s3_secret'));
END
$$;
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("frag_3.lance")
  FROM SERVER lance_s3 INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."frag_3.lance" RENAME TO frag_3_s3;
SELECT format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) AS definition
  FROM pg_attribute a
  WHERE a.attrelid = 'lance_feature.frag_3_s3'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum;
