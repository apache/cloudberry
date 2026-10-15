-- snapshot: which version of a dataset a statement reads.
--
-- The versions fixture was appended to twice, so it has 10, 20 and 35 rows at
-- versions 1, 2 and 3 - and one, two and three fragments.  Pinning a version
-- therefore shows up twice: in the rows and in the fragment count EXPLAIN
-- ANALYZE reports.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER snap_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
CREATE FOREIGN TABLE lance_feature.snap_v1 (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'versions.lance', version '1');
CREATE FOREIGN TABLE lance_feature.snap_v2 (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'versions.lance', version '2');
CREATE FOREIGN TABLE lance_feature.snap_latest (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'versions.lance');
-- version 0 is how the option spells "whatever is newest".
CREATE FOREIGN TABLE lance_feature.snap_v0 (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'versions.lance', version '0');
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_v1;
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_v2;
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_latest;
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_v0;
-- An older version is a prefix of the newest one, not a different reading of
-- the same rows.
SELECT count(*) AS v1_not_in_latest FROM (
  SELECT * FROM lance_feature.snap_v1 EXCEPT SELECT * FROM lance_feature.snap_latest) d;
-- What the plan says it will read.
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_v1');
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_v2');
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_latest');
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_v0');
-- And what it actually opened: the fragment count is the version, seen from
-- the other side.
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_v1',
                                          'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_v2',
                                          'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
SELECT * FROM lance_feature.explain_lance('SELECT count(*) FROM lance_feature.snap_latest',
                                          'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
-- Pinning has to hold on the segments too, and an append-only history cannot
-- tell: a segment that opened the latest version but read only the fragment
-- ids the coordinator published would still return version 1's rows.
-- deleted.lance changes the contents of existing fragments between its two
-- versions (deletion files on three of the four), so here the difference is
-- in the rows themselves: 20 at version 1, 16 at the latest.
CREATE FOREIGN TABLE lance_feature.snap_del_v1 (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'deleted.lance', version '1');
CREATE FOREIGN TABLE lance_feature.snap_del_latest (id integer, v text, n bigint)
  SERVER snap_files OPTIONS (uri 'deleted.lance');
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_del_v1;
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id, sum(n) AS sum_n
  FROM lance_feature.snap_del_latest;
SELECT array_agg(id ORDER BY id) AS deleted_after_v1 FROM (
  SELECT * FROM lance_feature.snap_del_v1 EXCEPT SELECT * FROM lance_feature.snap_del_latest) d;
