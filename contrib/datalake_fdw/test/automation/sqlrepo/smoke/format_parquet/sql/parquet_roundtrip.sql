-- Parquet: every type a lake table can store, written to a file and read back;
-- the row group range that a scan will one day be divided at; and the field
-- ids that a table's columns are matched to a file's by.

SET client_min_messages = warning;
DROP VIEW IF EXISTS dlparq_roundtrip, dlparq_split;
DROP TABLE IF EXISTS dlparq_src, dlparq_pairs, dlparq_unsupported, dlparq_bounded CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

-- A timestamptz is printed in the session's zone, so without this the output
-- would depend on where the test ran rather than on what the file holds.
SET TimeZone = 'UTC';

-- Fixed file names, removed before the run rather than overwritten during it:
-- the writer refuses a path that already exists, so what the last run left has
-- to go first.  COPY TO PROGRAM runs on the coordinator, which is where these
-- functions read and write.
\set roundtrip_file '/tmp/datalake_fdw_regress_roundtrip.parquet'
\set split_file '/tmp/datalake_fdw_regress_split.parquet'
\set swapped_file '/tmp/datalake_fdw_regress_swapped.parquet'
\set empty_file '/tmp/datalake_fdw_regress_empty.parquet'
\set batch_file '/tmp/datalake_fdw_regress_batch.parquet'
\set gzip_file '/tmp/datalake_fdw_regress_gzip.parquet'
\set missing_file '/tmp/datalake_fdw_regress_does_not_exist.parquet'
COPY (SELECT 1) TO PROGRAM 'rm -f /tmp/datalake_fdw_regress_*.parquet';

-- varchar without a length, because a length is what a lake table cannot hold;
-- see the refusals at the end.
CREATE TABLE dlparq_src (
	c_bool		boolean,
	c_int2		smallint,
	c_int4		integer,
	c_int8		bigint,
	c_float4	real,
	c_float8	double precision,
	c_text		text,
	c_varchar	varchar,
	c_bytea		bytea,
	c_date		date,
	c_time		time,
	c_ts		timestamp,
	c_tstz		timestamptz,
	c_uuid		uuid
) DISTRIBUTED RANDOMLY;

-- The dates and timestamps are chosen around both epochs: PostgreSQL counts
-- from 2000-01-01 and Arrow from 1970-01-01, and a value on either side of
-- 1970 is what tells a wrong shift from a right one.  24:00:00 is the one time
-- PostgreSQL admits past the end of the day.  A row of nulls is here because a
-- validity bitmap that is never exercised is a bitmap that has not been tested.
INSERT INTO dlparq_src VALUES
	(true, 1, 100, 1000, 1.5, 2.5,
	 'hello', 'varchar', '\x0102'::bytea,
	 '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
	 '00000000-0000-0000-0000-000000000000'),
	(false, -2, -200, -2000, -1.5, -2.5,
	 'a longer string with 中文', 'x', '\x'::bytea,
	 '2000-01-01', '12:34:56.789012', '2000-01-01 12:34:56.789012', '2000-01-01 12:34:56.789012+00',
	 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
	(true, 32767, 2147483647, 9223372036854775807, 3.25, 1e300,
	 '', 'z', '\xdeadbeef'::bytea,
	 '1969-12-31', '24:00:00', '1969-12-31 23:59:59.999999', '1969-12-31 23:59:59.999999+00',
	 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
	(NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT datalake_parquet_write(:'roundtrip_file',
							  'SELECT * FROM dlparq_src') AS rows_written;

-- The column definition list is what the caller claims the file holds; it is
-- checked against the file's own schema, not assumed.  A view so that the list
-- is written once.
CREATE VIEW dlparq_roundtrip AS
	SELECT * FROM datalake_parquet_read(:'roundtrip_file') AS t (
		c_bool		boolean,
		c_int2		smallint,
		c_int4		integer,
		c_int8		bigint,
		c_float4	real,
		c_float8	double precision,
		c_text		text,
		c_varchar	varchar,
		c_bytea		bytea,
		c_date		date,
		c_time		time,
		c_ts		timestamp,
		c_tstz		timestamptz,
		c_uuid		uuid);

SELECT * FROM dlparq_roundtrip ORDER BY c_int4;

-- Both directions: one way only says what the file lost, and a file with a row
-- nobody wrote is just as wrong as one missing a row somebody did.
SELECT count(*) AS differences
FROM ((TABLE dlparq_src EXCEPT ALL TABLE dlparq_roundtrip)
	  UNION ALL
	  (TABLE dlparq_roundtrip EXCEPT ALL TABLE dlparq_src)) d;

-- The bytes as well as the values: a string that came back re-encoded would
-- still compare equal.
SELECT octet_length(c_text) AS text_bytes,
	   octet_length(c_bytea) AS bytea_bytes
FROM dlparq_roundtrip ORDER BY 1, 2;

-- Row groups are the unit a scan is divided at, so reading the parts has to add
-- up to reading the whole -- no row seen twice, none missed.
CREATE TABLE dlparq_pairs AS
	SELECT i AS k, 'v' || i AS v FROM generate_series(1, 6) i
	DISTRIBUTED RANDOMLY;

SELECT datalake_parquet_write(:'split_file',
							  'SELECT k, v FROM dlparq_pairs ORDER BY k',
							  2) AS rows_written;

CREATE VIEW dlparq_split AS
	SELECT * FROM datalake_parquet_read(:'split_file') AS t (k int, v text);

SELECT * FROM dlparq_split ORDER BY k;

SELECT 0 AS first_row_group, * FROM datalake_parquet_read(:'split_file', 0, 1)
	AS t (k int, v text)
UNION ALL
SELECT 1, * FROM datalake_parquet_read(:'split_file', 1, 1) AS t (k int, v text)
UNION ALL
SELECT 2, * FROM datalake_parquet_read(:'split_file', 2, 1) AS t (k int, v text)
ORDER BY 1, 2;

-- Reading from a row group on is the same as reading each of them.
SELECT count(*) AS rows_from_the_second_on
FROM datalake_parquet_read(:'split_file', 1) AS t (k int, v text);

-- A table's columns are matched to a file's by field id, never by position.
-- The writer numbered these 1 (k) and 2 (v); asking for them the other way
-- round gives them the other way round, and asking for an id the file does not
-- have -- a column added to the table after the file was written -- gives NULL.
SELECT * FROM datalake_parquet_read(:'split_file', 0, 0, '{2, 1, 3}')
	AS t (v text, k int, added int) ORDER BY k;
SELECT * FROM datalake_parquet_read(:'split_file', 0, 0, '{2}')
	AS t (v text) ORDER BY v;
-- Nothing the file has: as many rows as the file, all NULL.
SELECT count(*) AS rows_of_nothing, count(x) AS values_of_nothing
FROM datalake_parquet_read(:'split_file', 0, 0, '{9}') AS t (x int);

-- The ids follow the data that was written, not the names: a file written with
-- the columns the other way round holds v under 1 and k under 2.
SELECT datalake_parquet_write(:'swapped_file',
							  'SELECT v, k FROM dlparq_pairs ORDER BY k') AS rows_written;
SELECT * FROM datalake_parquet_read(:'swapped_file', 0, 0, '{2, 1}')
	AS t (k int, v text) ORDER BY k;

-- What a column can be read as without loss: its own type, and the wider one
-- Iceberg lets it be promoted to.
SELECT * FROM datalake_parquet_read(:'split_file') AS t (k bigint, v text) ORDER BY k;

-- A query that returns nothing still produces a file: an empty file is a fact
-- about the query, a missing one would be a fact about the writer.
SELECT datalake_parquet_write(:'empty_file',
							  'SELECT k, v FROM dlparq_pairs WHERE false') AS rows_written;
SELECT count(*) AS rows_read
FROM datalake_parquet_read(:'empty_file') AS t (k int, v text);

-- iceberg.batch_rows is how many rows cross the boundary at a time, and both
-- halves read it.  Everything above fits in one batch, which leaves the loops
-- on both sides running exactly once; at two rows a batch the reader iterates
-- and the writer accumulates several batches into the one row group.
SET iceberg.batch_rows = 2;
SELECT count(*) AS rows_in_batches_of_two FROM dlparq_split;
SELECT datalake_parquet_write(:'batch_file',
							  'SELECT k, v FROM dlparq_pairs ORDER BY k') AS rows_written;
SELECT * FROM datalake_parquet_read(:'batch_file') AS t (k int, v text) ORDER BY k;
RESET iceberg.batch_rows;

-- The compression is asked of Arrow, so the name is spelled the way Arrow
-- spells it, in whatever case the option was typed.
SELECT datalake_parquet_write(:'gzip_file',
							  'SELECT k, v FROM dlparq_pairs ORDER BY k',
							  0, 'GZIP') AS rows_written;
SELECT count(*) AS rows_read_gzip
FROM datalake_parquet_read(:'gzip_file') AS t (k int, v text);

-- Refusals.  Each of these would otherwise be a wrong answer rather than an
-- error: a column silently dropped, a value reinterpreted, a short read.
CREATE TABLE dlparq_unsupported (k int, n numeric) DISTRIBUTED RANDOMLY;
SELECT datalake_parquet_write(:'missing_file',
							  'SELECT * FROM dlparq_unsupported');

-- A length limit has nowhere to live in the file, and padding would be written
-- as data; both are refused on the way in and on the way out.
CREATE TABLE dlparq_bounded (k int, v varchar(16), c char(5)) DISTRIBUTED RANDOMLY;
SELECT datalake_parquet_write(:'missing_file',
							  'SELECT k, v FROM dlparq_bounded');
SELECT datalake_parquet_write(:'missing_file',
							  'SELECT k, c FROM dlparq_bounded');
SELECT * FROM datalake_parquet_read(:'split_file') AS t (k int, v varchar(3));
SELECT * FROM datalake_parquet_read(:'split_file') AS t (k int, v char(3));

SELECT * FROM datalake_parquet_read(:'split_file') AS t (k text, v text);
SELECT * FROM datalake_parquet_read(:'split_file') AS t (k int);
SELECT * FROM datalake_parquet_read(:'split_file', 0, 0, '{1}') AS t (k int, v text);
SELECT * FROM datalake_parquet_read(:'split_file', 9, 1) AS t (k int, v text);

-- A row group range that only overflowed arithmetic would let through: the
-- count is rejected rather than turned into a two-billion-entry list.
SELECT * FROM datalake_parquet_read(:'split_file', 1, 2147483647) AS t (k int, v text);

SELECT datalake_parquet_write(:'missing_file', 'SELECT 1', -1);
SELECT datalake_parquet_write(:'missing_file', 'SELECT 1', 2000000000);

-- Compression names Arrow does not know, and one it knows but Parquet cannot
-- use.  Both are refused before a file is created.
SELECT datalake_parquet_write(:'missing_file', 'SELECT 1', 0, 'deflate');
SELECT datalake_parquet_write(:'missing_file', 'SELECT 1', 0, 'lzo');

-- A path that already exists is refused whatever is there: the writer only
-- ever removes a file it created, so what was there is still there afterwards.
SELECT datalake_parquet_write(:'split_file', 'SELECT 1');
SELECT count(*) AS rows_still_there FROM dlparq_split;

-- PostgreSQL's timestamp range runs about 34 years past the last instant Arrow
-- can hold as microseconds from 1970.  Writing one of those has to be refused,
-- because the shift would wrap and the value would land 292000 years before the
-- epoch with the write reporting success.
SELECT datalake_parquet_write(:'missing_file',
							  $$SELECT '294250-01-01 00:00:00'::timestamp$$);

-- Arrow words this one, and its wording is not ours to depend on.
\set VERBOSITY terse
SELECT * FROM datalake_parquet_read(:'missing_file') AS t (k int, v text);
\set VERBOSITY default

SET client_min_messages = warning;
DROP VIEW dlparq_roundtrip, dlparq_split;
DROP TABLE dlparq_src, dlparq_pairs, dlparq_unsupported, dlparq_bounded;
RESET client_min_messages;
