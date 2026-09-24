-- The storage behaviour every backend owes the format layer, written once and
-- run against each of them.  Included by conformance_file, conformance_dltest
-- and conformance_s3, which differ only in where :prefix points, whether a
-- :volume supplies credentials, and the :kv the text helpers need.
--
-- What is asserted here is what changes with the storage underneath: that a
-- Parquet file written through a volume reads back, that a name in use is
-- never overwritten, and that a write which fails partway leaves nothing.
-- What a Parquet file holds is the business of the format_parquet cases.

SELECT datalake_parquet_write(:'prefix' || '/roundtrip.parquet',
	$q$SELECT i AS id, (i * 1.5)::float8 AS amount, 'row ' || i AS label,
			  (i % 2 = 0) AS flag, ('2024-01-01'::date + i) AS day,
			  ('2024-01-01 00:00:00+00'::timestamptz + i * interval '1 second') AS at,
			  decode(lpad(to_hex(i), 8, '0'), 'hex') AS raw
	   FROM generate_series(1, 2000) i$q$,
	500, 'snappy', :volume) AS rows_written;

SELECT count(*) AS rows_read,
	   sum(id) AS id_sum,
	   min(label) AS first_label,
	   count(*) FILTER (WHERE flag) AS flagged,
	   max(day) AS last_day,
	   max(at) AS last_at,
	   max(encode(raw, 'hex')) AS last_raw
FROM datalake_parquet_read(:'prefix' || '/roundtrip.parquet', 0, 0, '{}', :volume)
	AS t(id int, amount float8, label text, flag boolean, day date,
		 at timestamptz, raw bytea);

-- Columns are matched by the field id each one carries, not by where it sits
-- in the file, so a projection can name them in any order and leave some out.
SELECT count(*) AS projected_rows, min(label) AS first_label, sum(id) AS id_sum
FROM datalake_parquet_read(:'prefix' || '/roundtrip.parquet', 0, 0,
						   '{3,1}', :volume) AS t(label text, id int);

-- Written with 500-row groups, so a range of them is a range of the file.
SELECT count(*) AS rows_in_two_groups, min(id) AS first_id, max(id) AS last_id
FROM datalake_parquet_read(:'prefix' || '/roundtrip.parquet', 1, 2, '{1}', :volume)
	AS t(id int);

-- The file is there, and it is the only thing there.
SELECT replace(path, :'root', '') AS object
FROM datalake_storage_list(:'prefix', :kv) AS path
ORDER BY 1;

-- A name in use is refused, and what was there is untouched.
\set VERBOSITY sqlstate
SELECT datalake_parquet_write(:'prefix' || '/roundtrip.parquet',
							  'SELECT 1 AS id', 0, '', :volume);
\set VERBOSITY default
SELECT count(*) AS rows_still_there
FROM datalake_parquet_read(:'prefix' || '/roundtrip.parquet', 0, 0, '{1}', :volume)
	AS t(id int);

-- A write that fails partway leaves nothing behind: not a truncated file, not
-- an empty one, and nothing for the next attempt at that name to trip over.
\set VERBOSITY sqlstate
-- Each row carries a kilobyte, and the failure comes after twelve thousand of
-- them: past the point where object storage has begun a multipart upload, so
-- what this proves is that the upload is abandoned and not merely that no
-- object appears.
SELECT datalake_parquet_write(:'prefix' || '/aborted.parquet',
	$q$SELECT i AS id, repeat('x', 1024) AS padding,
			  1 / (i - 12000) AS boom
	   FROM generate_series(1, 20000) i$q$,
	2000, '', :volume);
\set VERBOSITY default

SELECT replace(path, :'root', '') AS object
FROM datalake_storage_list(:'prefix', :kv) AS path
ORDER BY 1;

-- And the name is free, so the next writer gets it.
SELECT datalake_parquet_write(:'prefix' || '/aborted.parquet',
							  'SELECT 7 AS id', 0, '', :volume) AS rows_written;
SELECT id FROM datalake_parquet_read(:'prefix' || '/aborted.parquet', 0, 0, '{}',
									 :volume) AS t(id int);

-- Reading something that is not there says so, whatever the storage is.
\set VERBOSITY sqlstate
SELECT count(*) FROM datalake_parquet_read(:'prefix' || '/missing.parquet', 0, 0,
										   '{}', :volume) AS t(id int);

-- So does listing a prefix nothing was ever written under.  Object storage has
-- no such thing as a directory and would answer an empty list; a filesystem
-- would answer that there is no such directory.  The rule is the facade's, so
-- both say the same thing here.
SELECT count(*) FROM datalake_storage_list(:'prefix' || '/never-written', :kv);
\set VERBOSITY default

SELECT datalake_storage_delete(:'prefix' || '/roundtrip.parquet', :kv) AS cleaned_roundtrip,
	   datalake_storage_delete(:'prefix' || '/aborted.parquet', :kv) AS cleaned_aborted;

-- And a prefix that held objects until a moment ago is no different from one
-- that never did: on a filesystem the directory is still there and empty, and
-- that has to read the same way.
\set VERBOSITY sqlstate
SELECT count(*) FROM datalake_storage_list(:'prefix', :kv);
\set VERBOSITY default
