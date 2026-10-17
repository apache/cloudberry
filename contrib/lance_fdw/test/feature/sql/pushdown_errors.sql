-- pushdown_errors: what happens when a qualifier names a column Lance cannot
-- give.  The column a pushed qualifier reads is not projected, so without the
-- check added for that case it would meet no validation at all - and the refusals
-- this wrapper rests on ("B-tier is loud, never silent") would have a side
-- door.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER pde_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;

-- A column name the dataset does not have, reached only through a qualifier.
-- The error has to name the column and point at the column_name option, the
-- same quality the projection gets - not a raw lance: message from a segment.
CREATE FOREIGN TABLE lance_feature.pde_missing (id integer, nosuch integer)
  SERVER pde_files OPTIONS (uri 'types_all.lance');
SELECT * FROM lance_feature.report(
  'SELECT count(*) FROM lance_feature.pde_missing WHERE nosuch = 1');
-- The same column in the projection, for comparison: both paths must refuse.
SELECT * FROM lance_feature.report('SELECT nosuch FROM lance_feature.pde_missing');

-- column_name maps a table column onto a differently named Lance column.  The
-- qualifier must be rendered against the Lance name, not the table's.
CREATE FOREIGN TABLE lance_feature.pde_renamed (id integer, mine integer)
  SERVER pde_files OPTIONS (uri 'types_all.lance');
ALTER FOREIGN TABLE lance_feature.pde_renamed
  ALTER COLUMN mine OPTIONS (ADD column_name 'c_int32');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pde_renamed WHERE mine = 0');
SELECT count(*) AS renamed_eq0 FROM lance_feature.pde_renamed WHERE mine = 0;

-- A B-tier column declared as something readable.  uint64 is B-tier, and if a
-- pushed qualifier could reach it without the converter check it would be
-- handed to Lance to interpret by its physical type.  The declaration has to
-- be refused whether the column is projected or only filtered on: that is the
-- side door Must NOT 7 is about.
CREATE FOREIGN TABLE lance_feature.pde_btier (id integer, c_uint64 bigint)
  SERVER pde_files OPTIONS (uri 'types_b.lance');
-- Only in a qualifier: the column never travels, and must still be refused.
SELECT count(*) FROM lance_feature.pde_btier WHERE c_uint64 = 0::bigint;
-- In the projection, for comparison: the same refusal, from the same check.
SELECT c_uint64 FROM lance_feature.pde_btier;
-- An A-tier column of the same table still filters, so the refusal above is
-- about that column and not about the table.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pde_btier WHERE id = 1');
