-- types_errors: the strictness half of the type bridge.
--
-- Every case here is a foreign table whose declaration would cost information
-- to honour, and every one of them has to fail in BeginForeignScan rather than
-- return a truncated, wrapped or NULL value.  The four rules are: no narrowing,
-- no loss of precision, no crossing between timestamp and timestamptz, and
-- nothing at all out of a B-tier column.
--
-- The messages are the point, so the statements go through message(), which
-- returns the wording without the segment suffix a QE error arrives with.  Each
-- one names the column, the Arrow format string and the PostgreSQL type: the
-- three things it takes to fix the table definition.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER tyerr_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
-- Narrowing an integer or a float: the value would wrap or round.
CREATE FOREIGN TABLE lance_feature.tyerr_int64 (c_int64 integer)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_int64$$) AS int64_into_integer;
CREATE FOREIGN TABLE lance_feature.tyerr_uint32 (c_uint32 integer)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_uint32$$) AS uint32_into_integer;
CREATE FOREIGN TABLE lance_feature.tyerr_f16_int (c_float16 smallint)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_f16_int$$) AS float16_into_smallint;
-- A numeric has to hold both halves of the decimal: the scale, and the digits
-- in front of the point.  numeric(5,2) has neither, numeric(30,10) has the
-- scale but not the 28 integer digits of decimal128(38,10).
CREATE FOREIGN TABLE lance_feature.tyerr_dec_small (c_dec128_38_10 numeric(5,2))
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_dec_small$$) AS dec38_into_numeric_5_2;
CREATE FOREIGN TABLE lance_feature.tyerr_dec_short (c_dec128_38_10 numeric(30,10))
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_dec_short$$) AS dec38_into_numeric_30_10;
-- A declared precision below six microseconds would truncate every value.
CREATE FOREIGN TABLE lance_feature.tyerr_ts3 (c_ts_us timestamp(3))
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_ts3$$) AS ts_us_into_timestamp3;
-- A zone or its absence decides which of the two PostgreSQL types the column
-- is; reading either as the other would reinterpret the instant, so both
-- crossings are refused.
CREATE FOREIGN TABLE lance_feature.tyerr_tz_into_ts (c_ts_us_tz timestamp)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_tz_into_ts$$) AS zoned_into_timestamp;
CREATE FOREIGN TABLE lance_feature.tyerr_ts_into_tz (c_ts_us timestamptz)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_ts_into_tz$$) AS unzoned_into_timestamptz;
-- A date is not a timestamp, however close the two look.
CREATE FOREIGN TABLE lance_feature.tyerr_date_ts (c_date32 timestamp)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_date_ts$$) AS date32_into_timestamp;
-- A list is read as an array of its element type, and the element gets exactly
-- the rules a column of that type would: float64 does not fit real, int64 does
-- not fit integer, and a length-limited element type would truncate.
CREATE FOREIGN TABLE lance_feature.tyerr_fsl_narrow (c_fsl_f64_4 real[])
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_fsl_narrow$$) AS fsl_f64_into_real_array;
CREATE FOREIGN TABLE lance_feature.tyerr_list_narrow (c_list_i64 integer[])
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_list_narrow$$) AS list_i64_into_integer_array;
CREATE FOREIGN TABLE lance_feature.tyerr_list_varchar (c_list_utf8 varchar(4)[])
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_list_varchar$$) AS list_utf8_into_varchar_array;
-- A list read as something that is not an array at all.
CREATE FOREIGN TABLE lance_feature.tyerr_fsl_text (c_fsl_f32_4 text)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_fsl_text$$) AS fsl_into_text;
CREATE FOREIGN TABLE lance_feature.tyerr_list_scalar (c_list_i64 bigint)
  SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT lance_feature.message($$SELECT * FROM lance_feature.tyerr_list_scalar$$) AS list_into_bigint;
-- A Lance blob column is B-tier, so no
-- declaration makes it readable.  The dataset schema calls it
-- large_binary while the scanner hands back a struct{position,size} descriptor
-- instead of the bytes, so which of the two the message names depends on the
-- schema the stream reports; that it is refused rather than read does not.
CREATE FOREIGN TABLE lance_feature.tyerr_blob (id integer, plain_lb bytea, blob bytea)
  SERVER tyerr_files OPTIONS (uri 'blob.lance');
SELECT lance_feature.message($$SELECT blob FROM lance_feature.tyerr_blob$$)
       LIKE 'lance_fdw: column "blob": %is not supported%' AS blob_refused;
-- The B-tier column only has to be refused when it is read: the plain
-- large_binary column next to it in the same table still comes back.
SELECT id, octet_length(plain_lb) AS bytes FROM lance_feature.tyerr_blob ORDER BY id;
-- Nothing above rejects a declaration that costs nothing: an unconstrained
-- numeric, an exact one, a timestamp without a stated precision, a timestamptz
-- over the zoned column, and both float widenings.
CREATE FOREIGN TABLE lance_feature.tyerr_ok (
  id integer,
  c_ts_s timestamp,
  c_ts_s_tz timestamptz,
  c_dec128_10_2 numeric,
  c_dec128_38_10 numeric(38,10),
  c_fsl_f32_4 double precision[],
  c_float16 double precision
) SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT count(c_ts_s) AS ts, count(c_ts_s_tz) AS ts_tz, count(c_dec128_10_2) AS dec_any,
       count(c_dec128_38_10) AS dec_exact, count(c_fsl_f32_4) AS fsl_wide,
       count(c_float16) AS f16_wide
  FROM lance_feature.tyerr_ok;
-- Widening keeps the value the narrow type held, digit for digit.
SELECT id, c_float16, c_fsl_f32_4 FROM lance_feature.tyerr_ok
  WHERE id IN (2, 4) ORDER BY id;
-- After all of that the same backend still reads a real dataset.
SELECT count(*) AS rows FROM lance_feature.tyerr_ok;
-- A declared precision only has to hold the digits the Arrow unit has: a
-- timestamp[s] cannot lose anything to timestamp(0), nor a timestamp[ms] to
-- timestamp(3).  Both are refused for microseconds, which is the case above.
-- Narrowing must also change no value, which is what the EXCEPT says.
CREATE FOREIGN TABLE lance_feature.tyerr_ts_narrow (
  id integer, c_ts_s timestamp(0), c_ts_ms timestamp(3), c_ts_s_tz timestamptz(0)
) SERVER tyerr_files OPTIONS (uri 'types_all.lance');
CREATE FOREIGN TABLE lance_feature.tyerr_ts_wide (
  id integer, c_ts_s timestamp, c_ts_ms timestamp(6), c_ts_s_tz timestamptz
) SERVER tyerr_files OPTIONS (uri 'types_all.lance');
SELECT count(c_ts_s) AS ts_s, count(c_ts_ms) AS ts_ms, count(c_ts_s_tz) AS ts_s_tz
  FROM lance_feature.tyerr_ts_narrow;
SELECT count(*) AS narrowing_changed_a_value FROM (
  SELECT * FROM lance_feature.tyerr_ts_narrow
  EXCEPT SELECT * FROM lance_feature.tyerr_ts_wide) d;
-- Values PostgreSQL cannot represent are refused per value, not rounded and
-- not truncated.  strict.lance holds a nanosecond timestamp that is not a
-- whole microsecond and a string with a zero byte in it, each next to an
-- ordinary value of the same type that has to keep working.
CREATE FOREIGN TABLE lance_feature.tyerr_strict (
  id integer,
  ts_ns_aligned timestamp,
  ts_ns_odd timestamp,
  s_plain text,
  s_nul text
) SERVER tyerr_files OPTIONS (uri 'strict.lance');
SELECT lance_feature.message($$SELECT ts_ns_odd FROM lance_feature.tyerr_strict$$) AS ns_not_whole_microseconds;
SELECT lance_feature.message($$SELECT s_nul FROM lance_feature.tyerr_strict$$) AS text_with_a_zero_byte;
-- Same as the types suite: pg_regress runs psql with PGDATESTYLE=Postgres,MDY
-- and PGTZ=PST8PDT, and a timestamp is easier to check against the fixture in
-- the form the reference file uses.
SET DateStyle = 'ISO, YMD';
SET timezone = 'UTC';
SELECT id, ts_ns_aligned, to_json(s_plain) AS s_plain
  FROM lance_feature.tyerr_strict ORDER BY id;
RESET timezone;
RESET DateStyle;
SELECT count(*) AS rows, count(ts_ns_aligned) AS aligned, count(s_plain) AS plain
  FROM lance_feature.tyerr_strict;
