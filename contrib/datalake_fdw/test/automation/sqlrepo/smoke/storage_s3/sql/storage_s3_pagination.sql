-- Listing more objects than one page holds.  Roughly four thousand requests
-- go over the wire here, which is minutes rather than seconds, so this is its
-- own case and runs where DATALAKE_TEST_S3_PAGINATION says to -- CI, not every
-- local build.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

SET log_statement = 'none';
SET log_min_duration_statement = -1;
SET log_min_error_statement = 'panic';

\set ECHO none
\set region ''
\set path_style ''
\getenv endpoint DATALAKE_TEST_S3_ENDPOINT
\getenv bucket DATALAKE_TEST_S3_BUCKET
\getenv access_key DATALAKE_TEST_S3_ACCESS_KEY
\getenv secret_key DATALAKE_TEST_S3_SECRET_KEY
\getenv region DATALAKE_TEST_S3_REGION
\getenv path_style DATALAKE_TEST_S3_PATH_STYLE

SELECT set_config('datalake.s3_endpoint', :'endpoint', false) AS endpoint,
	   set_config('datalake.s3_bucket', :'bucket', false) AS bucket,
	   set_config('datalake.s3_access_key', :'access_key', false) AS access_key,
	   set_config('datalake.s3_secret', :'secret_key', false) AS secret,
	   set_config('datalake.s3_region',
				  coalesce(nullif(:'region', ''), 'us-east-1'), false) AS region,
	   set_config('datalake.s3_path_style',
				  coalesce(nullif(:'path_style', ''), 'true'), false) AS path_style,
	   set_config('datalake.s3_run',
				  to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS') || '_' ||
				  pg_backend_pid(), false) AS run
\gset
\set ECHO all

CREATE FUNCTION s3_kv() RETURNS text[] LANGUAGE sql AS $$
	SELECT ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
				 'region=' || current_setting('datalake.s3_region'),
				 'path_style_access=' || current_setting('datalake.s3_path_style'),
				 'access_key_id=' || current_setting('datalake.s3_access_key'),
				 'secret_access_key=' || current_setting('datalake.s3_secret')]
$$;
CREATE FUNCTION s3_uri(name text) RETURNS text LANGUAGE sql AS $$
	SELECT format('s3://%s/datalake_regress/%s/%s',
				  current_setting('datalake.s3_bucket'),
				  current_setting('datalake.s3_run'), name)
$$;
CREATE FUNCTION s3_prefix() RETURNS text LANGUAGE sql AS $$
	SELECT format('s3://%s/datalake_regress/%s',
				  current_setting('datalake.s3_bucket'),
				  current_setting('datalake.s3_run'))
$$;

-- More objects than one listing page holds.  Each aggregate reads the value
-- the function returned rather than counting rows: a target list nothing
-- refers to can be optimised away, and then the writes never happen while the
-- count still looks right.
SELECT count(*) FILTER (WHERE bytes > 0) AS created FROM (
	SELECT datalake_storage_write_text(s3_uri('page/' || lpad(i::text, 5, '0')),
									   i::text, s3_kv()) AS bytes
	FROM generate_series(1, 1100) i) AS w;
SELECT count(*) AS listed FROM datalake_storage_list(s3_prefix() || '/page', s3_kv());
SELECT count(*) FILTER (WHERE gone) AS deleted FROM (
	SELECT datalake_storage_delete(s3_uri('page/' || lpad(i::text, 5, '0')),
								   s3_kv()) AS gone
	FROM generate_series(1, 1100) i) AS d;

DROP FUNCTION s3_prefix(), s3_uri(text), s3_kv();
