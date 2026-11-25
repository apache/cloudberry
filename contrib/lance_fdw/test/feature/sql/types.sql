-- types: every A-tier type read through the wrapper, value by value against
-- what pylance read out of the same fixtures (test/fixtures/expected/
-- types_all.jsonl, large_text.jsonl and blob.jsonl).
--
-- The table is imported rather than written by hand, so the column types are
-- the ones IMPORT FOREIGN SCHEMA generates and the two halves of the type
-- bridge - what it declares and what it reads - are checked against each other.
--
-- pg_regress runs psql with PGDATESTYLE=Postgres,MDY and PGTZ=PST8PDT, while
-- the reference values are ISO and UTC.  UTC is also what an Arrow timestamp
-- means, zone name or not, so the suite sets both rather than depending on the
-- harness.  Text columns go through to_json() because two fixture values carry
-- a tab and a newline; binary columns print as bytea's hex.
\pset format unaligned
SET DateStyle = 'ISO, YMD';
SET timezone = 'UTC';
DO $$
BEGIN
  EXECUTE format('CREATE SERVER types_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("types_all.lance")
  FROM SERVER types_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."types_all.lance" RENAME TO ty_all;
SELECT id, c_bool, c_int8, c_int16, c_int32, c_int64, c_uint8, c_uint16, c_uint32
  FROM lance_feature.ty_all ORDER BY id;
-- float16 is decoded to the value its 16 bits denote; 6.1035156e-05 is 2^-14
-- and 65504 is the largest half float.
SELECT id, c_float16, c_float32, c_float64 FROM lance_feature.ty_all ORDER BY id;
SELECT id, to_json(c_utf8) AS utf8, to_json(c_large_utf8) AS large_utf8
  FROM lance_feature.ty_all ORDER BY id;
SELECT id, c_binary, c_large_binary FROM lance_feature.ty_all ORDER BY id;
-- date32 counts days from 1970-01-01 and PostgreSQL from 2000-01-01; the
-- fixture reaches both ends of the year range on purpose.
SELECT id, c_date32 FROM lance_feature.ty_all ORDER BY id;
-- All four Arrow units land in the same microseconds.  The seconds and
-- milliseconds columns hold the same instants as the microsecond one, so a
-- unit that was scaled wrongly would show up as a shifted instant here.
SELECT id, c_ts_s, c_ts_ms, c_ts_us, c_ts_ns FROM lance_feature.ty_all ORDER BY id;
-- The zoned columns carry UTC, UTC and Asia/Shanghai zone names; the value is
-- a UTC epoch in all three cases and the name is dropped, so all
-- of them read as the same instants as the unzoned columns above.
SELECT id, c_ts_s_tz, c_ts_ms_tz, c_ts_us_tz, c_ts_ns_tz
  FROM lance_feature.ty_all ORDER BY id;
-- The scale Lance stored survives: 0.0000000000 keeps its ten digits, and the
-- 28-digit integer part of decimal128(38,10) is not a double on the way.
SELECT id, c_dec128_38_10, c_dec128_10_2 FROM lance_feature.ty_all ORDER BY id;
SELECT id, c_fsl_f32_4, c_fsl_f64_4 FROM lance_feature.ty_all ORDER BY id;
-- Lists keep their length, including the empty one, and a NULL element stays
-- NULL rather than becoming a NULL array.
SELECT id, c_list_i64, c_list_utf8 FROM lance_feature.ty_all ORDER BY id;
-- The same rows read in batches of three, which straddles both the batch and
-- the fragment boundary: every type has to survive a batch that starts at a
-- non-zero offset, which is where a list's offsets buffer is easiest to get
-- wrong.  Compared in both directions over all 31 columns.
IMPORT FOREIGN SCHEMA fixtures LIMIT TO ("types_all.lance")
  FROM SERVER types_files INTO lance_feature;
ALTER FOREIGN TABLE lance_feature."types_all.lance" RENAME TO ty_all_b3;
ALTER FOREIGN TABLE lance_feature.ty_all_b3 OPTIONS (ADD batch_size '3');
SELECT count(*) AS batch3_only FROM (
  SELECT * FROM lance_feature.ty_all_b3
  EXCEPT SELECT * FROM lance_feature.ty_all) d;
SELECT count(*) AS default_only FROM (
  SELECT * FROM lance_feature.ty_all
  EXCEPT SELECT * FROM lance_feature.ty_all_b3) d;
-- large_utf8 up to a MiB.  Values over the fixture's digest threshold are
-- recorded as {chars, bytes, md5}, so that is what is compared; row 6 is
-- multi-byte, where the character count and the byte count differ.
CREATE FOREIGN TABLE lance_feature.ty_large_text (id integer, small text, txt text)
  SERVER types_files OPTIONS (uri 'large_text.lance');
SELECT id, small, length(txt) AS chars, octet_length(txt) AS bytes, md5(txt) AS md5
  FROM lance_feature.ty_large_text ORDER BY id;
-- A MiB-sized large_binary column.  The blob-encoded column next to it is
-- B-tier and is left out of the table; test/feature/sql/types_errors.sql is
-- where referencing it belongs.
CREATE FOREIGN TABLE lance_feature.ty_blob (id integer, note text, plain_lb bytea)
  SERVER types_files OPTIONS (uri 'blob.lance');
SELECT id, note, octet_length(plain_lb) AS bytes, md5(plain_lb) AS md5
  FROM lance_feature.ty_blob ORDER BY id;
