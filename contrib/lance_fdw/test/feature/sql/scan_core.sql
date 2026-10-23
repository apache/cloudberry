-- scan_core: reading rows.  The values here are the ones pylance read back out
-- of the same fixtures, in test/fixtures/expected/*.jsonl; every query below is
-- that file seen through the wrapper.
--
-- Text columns are printed through to_json() because two of the fixture values
-- carry a tab and a newline, which would otherwise break the expected file into
-- pieces, and because it tells an empty string from a NULL.  Binary columns are
-- printed as they come, which is bytea's hex output.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER scan_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
-- Written by hand rather than imported: this is also the case where a foreign
-- table declares a subset of the dataset's columns.
CREATE FOREIGN TABLE lance_feature.sc_types (
  id integer,
  c_bool boolean,
  c_int8 smallint,
  c_int16 smallint,
  c_int32 integer,
  c_int64 bigint,
  c_uint8 smallint,
  c_uint16 integer,
  c_uint32 bigint,
  c_float32 real,
  c_float64 double precision,
  c_utf8 text,
  c_large_utf8 text,
  c_binary bytea,
  c_large_binary bytea
) SERVER scan_files OPTIONS (uri 'types_all.lance');
SELECT id, c_bool, c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32
  FROM lance_feature.sc_types ORDER BY id;
SELECT id, c_float32, c_float64 FROM lance_feature.sc_types ORDER BY id;
SELECT id, to_json(c_utf8) AS utf8, to_json(c_large_utf8) AS large_utf8
  FROM lance_feature.sc_types ORDER BY id;
SELECT id, c_binary, c_large_binary FROM lance_feature.sc_types ORDER BY id;
-- count(*) reads no column at all: the scan projects nothing and counts the
-- length of each Arrow batch.
SELECT count(*) FROM lance_feature.sc_types;
-- A projection that does not start at the first column.
SELECT count(*), count(c_int16), min(c_int16), max(c_int16) FROM lance_feature.sc_types;
-- A column referenced only in WHERE still has to be read.
SELECT id FROM lance_feature.sc_types WHERE c_int32 = 424242 ORDER BY id;
-- Widening inside a family: the same dataset read into the widest types.
CREATE FOREIGN TABLE lance_feature.sc_wide (
  id bigint,
  c_int8 bigint,
  c_int16 integer,
  c_int32 bigint,
  c_uint8 integer,
  c_uint16 bigint,
  c_float32 double precision
) SERVER scan_files OPTIONS (uri 'types_all.lance');
SELECT id, c_int8, c_int16, c_int32, c_uint8, c_uint16, c_float32
  FROM lance_feature.sc_wide WHERE id IN (1, 3, 11) ORDER BY id;
-- A batch size that straddles both batch and fragment boundaries must not
-- change a single row.
CREATE FOREIGN TABLE lance_feature.sc_types_b3 (
  id integer,
  c_bool boolean,
  c_int8 smallint,
  c_int16 smallint,
  c_int32 integer,
  c_int64 bigint,
  c_uint8 smallint,
  c_uint16 integer,
  c_uint32 bigint,
  c_float32 real,
  c_float64 double precision,
  c_utf8 text,
  c_large_utf8 text,
  c_binary bytea,
  c_large_binary bytea
) SERVER scan_files OPTIONS (uri 'types_all.lance', batch_size '3');
SELECT count(*) AS batch3_only FROM (
  SELECT * FROM lance_feature.sc_types_b3
  EXCEPT SELECT * FROM lance_feature.sc_types) d;
SELECT count(*) AS default_only FROM (
  SELECT * FROM lance_feature.sc_types
  EXCEPT SELECT * FROM lance_feature.sc_types_b3) d;
SELECT count(*) AS rows_b3 FROM lance_feature.sc_types_b3;
-- A row count is a poor memory bound when rows vary in width, which is the
-- normal case for Lance.  The byte limit is the bound that does not, and like
-- the row limit it must not change a single row.  64 bytes is below one row
-- here, so lance falls back to the smallest batch it will build.
CREATE FOREIGN TABLE lance_feature.sc_types_bytes (
  id integer,
  c_bool boolean,
  c_int8 smallint,
  c_int16 smallint,
  c_int32 integer,
  c_int64 bigint,
  c_uint8 smallint,
  c_uint16 integer,
  c_uint32 bigint,
  c_float32 real,
  c_float64 double precision,
  c_utf8 text,
  c_large_utf8 text,
  c_binary bytea,
  c_large_binary bytea
) SERVER scan_files OPTIONS (uri 'types_all.lance', batch_size_bytes '64');
SELECT count(*) AS bytes_only FROM (
  SELECT * FROM lance_feature.sc_types_bytes
  EXCEPT SELECT * FROM lance_feature.sc_types) d;
SELECT count(*) AS default_only_bytes FROM (
  SELECT * FROM lance_feature.sc_types
  EXCEPT SELECT * FROM lance_feature.sc_types_bytes) d;
SELECT count(*) AS rows_bytes FROM lance_feature.sc_types_bytes;
-- The two scan-wide bounds are GUCs read per backend when a scan opens.  This
-- table runs on the coordinator so that the SET below and the read are the
-- same backend: a segment reads its own value, which a session SET is not
-- guaranteed to carry, and a test that silently skipped the setters would
-- prove nothing.  Smallest legal values, to be sure they are really applied.
CREATE FOREIGN TABLE lance_feature.sc_types_bounded (
  id integer,
  c_bool boolean,
  c_int8 smallint,
  c_int16 smallint,
  c_int32 integer,
  c_int64 bigint,
  c_uint8 smallint,
  c_uint16 integer,
  c_uint32 bigint,
  c_float32 real,
  c_float64 double precision,
  c_utf8 text,
  c_large_utf8 text,
  c_binary bytea,
  c_large_binary bytea
) SERVER scan_files
  OPTIONS (uri 'types_all.lance', mpp_execute 'coordinator');
SET lance_fdw.io_buffer_size_mb = 1;
SET lance_fdw.batch_readahead = 1;
SELECT count(*) AS bounded_only FROM (
  SELECT * FROM lance_feature.sc_types_bounded
  EXCEPT SELECT * FROM lance_feature.sc_types) d;
SELECT count(*) AS default_only_bounded FROM (
  SELECT * FROM lance_feature.sc_types
  EXCEPT SELECT * FROM lance_feature.sc_types_bounded) d;
SELECT count(*) AS rows_bounded FROM lance_feature.sc_types_bounded;
RESET lance_fdw.io_buffer_size_mb;
RESET lance_fdw.batch_readahead;
-- Arrow batches come from lance, not from palloc, so nothing in the database
-- sees them unless this wrapper says so.  What matters is that every reserve
-- is released: a leaked one is invisible until the ledger drifts far enough to
-- refuse an unrelated allocation.  So the ledger is read before, during and
-- after, including after an error, and it has to be square everywhere but
-- during.  The flag says whether the tracker is on at all, since with it off
-- the wrapper charges nothing and every number below is legitimately zero.
SELECT current_setting('gp_vmem_protect_limit') <> '0' AS vmem_tracker_on;
SELECT current_bytes = 0 AS square_before FROM lance_fdw_memory();
-- The peak is what can be observed from outside: the current value is zero
-- whenever anything asks, since a batch is held only between one
-- IterateForeignScan and the next.  Putting the function in the scan's own
-- target list does not help - naming no column of the table leaves the
-- projection empty, an empty projection reads no Arrow buffer at all, and the
-- planner drops a column the query above does not use, so the attempt lands
-- back at the same place.  Charging nothing there is projection pushdown
-- working, not the ledger failing.
SELECT count(c_utf8) AS rows_scanned FROM lance_feature.sc_types_bounded;
SELECT current_bytes = 0 AS square_after, peak_bytes > 0 AS charged_during
  FROM lance_fdw_memory();
-- The path a reservation is most likely to leak on: an error thrown while the
-- batch is still held, which never reaches EndForeignScan.
SELECT lance_feature.capture($$SELECT 1/0 FROM lance_feature.sc_types_bounded$$)
  AS midscan_error;
SELECT current_bytes = 0 AS square_after_error FROM lance_fdw_memory();
-- Sizing the two cache GUCs was guesswork without these.
SELECT index_cache_entries >= 0 AND metadata_cache_entries > 0 AS cache_stats_live
  FROM lance_fdw_cache_stats();
-- Deleted rows do not come back, and the batches that follow a deletion file
-- start at a non-zero offset.
CREATE FOREIGN TABLE lance_feature.sc_deleted (id integer, v text, n bigint)
  SERVER scan_files OPTIONS (uri 'deleted.lance');
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.sc_deleted;
SELECT id, v, n FROM lance_feature.sc_deleted ORDER BY id;
-- Nothing to read at all.
CREATE FOREIGN TABLE lance_feature.sc_empty (id integer, v text, n bigint)
  SERVER scan_files OPTIONS (uri 'empty.lance');
SELECT count(*) AS rows FROM lance_feature.sc_empty;
SELECT * FROM lance_feature.sc_empty;
-- Fragment ids with a hole in them: nothing may assume they run 0..N-1.
CREATE FOREIGN TABLE lance_feature.sc_gap (id integer, v text, n bigint)
  SERVER scan_files OPTIONS (uri 'frag_gap.lance');
SELECT id, v, n FROM lance_feature.sc_gap ORDER BY id;
-- Schema evolution: v10 and vup were added later and live in data files of
-- their own, so one fragment's columns come from three files.
CREATE FOREIGN TABLE lance_feature.sc_evolved
  (id integer, v text, n bigint, v10 integer, vup text)
  SERVER scan_files OPTIONS (uri 'evolved.lance');
SELECT id, v, n, v10, vup FROM lance_feature.sc_evolved ORDER BY id;
SELECT count(*) AS wrong_rows FROM lance_feature.sc_evolved
  WHERE v10 <> id * 10 OR vup <> upper(v);
-- Joins.  The local side is replicated so that the join needs no motion and
-- the planner is free to pick either shape.
CREATE FOREIGN TABLE lance_feature.sc_frag3 (id integer, v text, n bigint)
  SERVER scan_files OPTIONS (uri 'frag_3.lance');
CREATE TABLE lance_feature.sc_local (id integer, label text) DISTRIBUTED REPLICATED;
INSERT INTO lance_feature.sc_local VALUES (2, 'two'), (7, 'seven'), (11, 'eleven'), (99, 'none');
SET enable_hashjoin = on;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SELECT l.label, f.v, f.n FROM lance_feature.sc_frag3 f
  JOIN lance_feature.sc_local l USING (id) ORDER BY l.label;
SELECT lance_feature.plan_mentions(
  'SELECT l.label FROM lance_feature.sc_frag3 f JOIN lance_feature.sc_local l USING (id)',
  'COSTS OFF', 'Hash Join') AS hash_join;
-- The same join as a nested loop.  rows_hint is what makes the foreign side
-- look small enough for the planner to put it on the inside, which is the case
-- that rescans it; without the hint every foreign table is the same constant
-- size and the local four-row table wins the inside.
CREATE FOREIGN TABLE lance_feature.sc_frag3_tiny (id integer, v text, n bigint)
  SERVER scan_files OPTIONS (uri 'frag_3.lance', rows_hint '1');
SET enable_hashjoin = off;
SET enable_nestloop = on;
SET enable_material = off;
SELECT l.label, f.v, f.n FROM lance_feature.sc_frag3_tiny f
  JOIN lance_feature.sc_local l USING (id) ORDER BY l.label;
SELECT lance_feature.plan_mentions(
  'SELECT l.label FROM lance_feature.sc_frag3_tiny f JOIN lance_feature.sc_local l USING (id)',
  'COSTS OFF', 'Nested Loop') AS nested_loop;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET enable_material;
-- The same datasets over s3://, with the credentials coming from the user
-- mapping.  The rows are compared against the file:// tables that were just
-- checked value by value, in both directions.
DO $$
BEGIN
  EXECUTE format('CREATE SERVER scan_s3 FOREIGN DATA WRAPPER lance_fdw '
                 'OPTIONS (base_uri %L, aws_endpoint %L, aws_region %L, allow_http %L)',
                 's3://' || current_setting('regress.s3_bucket') || '/fixtures',
                 current_setting('regress.s3_endpoint'),
                 current_setting('regress.s3_region'),
                 'true');
  EXECUTE format('CREATE USER MAPPING FOR PUBLIC SERVER scan_s3 '
                 'OPTIONS (aws_access_key_id %L, aws_secret_access_key %L)',
                 current_setting('regress.s3_key'),
                 current_setting('regress.s3_secret'));
END
$$;
CREATE FOREIGN TABLE lance_feature.sc_types_s3 (
  id integer,
  c_bool boolean,
  c_int8 smallint,
  c_int16 smallint,
  c_int32 integer,
  c_int64 bigint,
  c_uint8 smallint,
  c_uint16 integer,
  c_uint32 bigint,
  c_float32 real,
  c_float64 double precision,
  c_utf8 text,
  c_large_utf8 text,
  c_binary bytea,
  c_large_binary bytea
) SERVER scan_s3 OPTIONS (uri 'types_all.lance');
SELECT count(*) AS rows_s3 FROM lance_feature.sc_types_s3;
SELECT count(*) AS s3_only FROM (
  SELECT * FROM lance_feature.sc_types_s3
  EXCEPT SELECT * FROM lance_feature.sc_types) d;
SELECT count(*) AS file_only FROM (
  SELECT * FROM lance_feature.sc_types
  EXCEPT SELECT * FROM lance_feature.sc_types_s3) d;
CREATE FOREIGN TABLE lance_feature.sc_frag3_s3 (id integer, v text, n bigint)
  SERVER scan_s3 OPTIONS (uri 'frag_3.lance');
SELECT id, v, n FROM lance_feature.sc_frag3_s3 ORDER BY id;
