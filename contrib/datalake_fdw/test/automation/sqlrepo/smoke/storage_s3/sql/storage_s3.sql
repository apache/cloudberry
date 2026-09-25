-- The s3 backend against a real S3-compatible service.  Endpoint and
-- credentials come from the environment, so this category only runs where
-- DATALAKE_TEST_S3_ENDPOINT is set; the Makefile skips it otherwise.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

-- This file hands the server credentials, and psql puts them in the statement
-- it sends, so with statement logging on they would land in the server log
-- and no check could tell that apart from the extension leaking them.  With
-- it off, a credential in the log came from the code under test.
SET log_statement = 'none';
SET log_min_duration_statement = -1;
SET log_min_error_statement = 'panic';

-- Which service, which bucket and whose credentials differ from one machine
-- to the next, so they are read into settings with the echo off: what this
-- file asserts must not depend on where it ran.  Objects also go under a
-- prefix of this run's own, so two runs against one bucket cannot collide.
\set ECHO none
-- \getenv leaves the variable unset when the environment does not define it,
-- and an unset variable interpolates as its own name, which is a syntax error
-- rather than a default.  So the optional ones are given values first.
\set region ''
\set path_style ''
\set bad_secret ''
\getenv endpoint DATALAKE_TEST_S3_ENDPOINT
\getenv bucket DATALAKE_TEST_S3_BUCKET
\getenv access_key DATALAKE_TEST_S3_ACCESS_KEY
\getenv secret_key DATALAKE_TEST_S3_SECRET_KEY
\getenv region DATALAKE_TEST_S3_REGION
\getenv path_style DATALAKE_TEST_S3_PATH_STYLE
\getenv bad_secret DATALAKE_TEST_S3_BAD_SECRET

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
				  pg_backend_pid(), false) AS run,
	   -- A wrong secret, taken from the environment rather than written here:
	   -- whatever this file says ends up in the server log as statement text,
	   -- and a secret in a log is the very thing the harness watches for.
	   set_config('datalake.s3_bad_secret',
				  coalesce(nullif(:'bad_secret', ''), 'not-the-secret'),
				  false) AS bad_secret
\gset
\set ECHO all

SELECT current_setting('datalake.s3_endpoint') <> '' AS have_endpoint,
	   current_setting('datalake.s3_bucket') <> '' AS have_bucket,
	   current_setting('datalake.s3_run') <> '' AS have_run;

CREATE FUNCTION s3_kv(secret text DEFAULT NULL) RETURNS text[] LANGUAGE sql AS $$
	SELECT ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
				 'region=' || current_setting('datalake.s3_region'),
				 'path_style_access=' || current_setting('datalake.s3_path_style'),
				 'access_key_id=' || current_setting('datalake.s3_access_key'),
				 'secret_access_key=' ||
				 coalesce(secret, current_setting('datalake.s3_secret'))]
$$;
CREATE FUNCTION s3_prefix() RETURNS text LANGUAGE sql AS $$
	SELECT format('s3://%s/datalake_regress/%s',
				  current_setting('datalake.s3_bucket'),
				  current_setting('datalake.s3_run'))
$$;
CREATE FUNCTION s3_uri(name text) RETURNS text LANGUAGE sql AS $$
	SELECT s3_prefix() || '/' || name
$$;

CREATE FUNCTION s3_error(uri text, kv text[]) RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
	message text;
	detail text;
BEGIN
	PERFORM datalake_storage_read_text(uri, kv);
	RETURN 'no error';
EXCEPTION WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS message = MESSAGE_TEXT, detail = PG_EXCEPTION_DETAIL;
	RETURN message || ' | ' || detail;
END
$fn$;

-- Round trip.
SELECT datalake_storage_write_text(s3_uri('a.txt'), 'first-object', s3_kv());
SELECT datalake_storage_read_text(s3_uri('a.txt'), s3_kv());

-- Larger than the 8 MiB part size, so this goes out as a multipart upload and
-- has to come back byte for byte.
SELECT datalake_storage_write_text(s3_uri('big.bin'),
								   repeat('0123456789', 900000), s3_kv()) AS bytes_written;
SELECT length(datalake_storage_read_text(s3_uri('big.bin'), s3_kv())) AS bytes_read,
	   md5(datalake_storage_read_text(s3_uri('big.bin'), s3_kv()))
		 = md5(repeat('0123456789', 900000)) AS same_bytes;

-- Listing reports this run's objects by their native path.
SELECT replace(path,
			   format('%s/datalake_regress/%s/', current_setting('datalake.s3_bucket'),
					  current_setting('datalake.s3_run')), '') AS object
FROM datalake_storage_list(s3_prefix(), s3_kv()) AS path
ORDER BY 1;

\set VERBOSITY sqlstate
-- An object that exists is never replaced.
SELECT datalake_storage_write_text(s3_uri('a.txt'), 'replacement', s3_kv());
-- A key that is not there, and a bucket that is not there.
SELECT datalake_storage_read_text(s3_uri('missing.txt'), s3_kv());
SELECT datalake_storage_read_text('s3://datalake-no-such-bucket-9f2b/x.txt', s3_kv());
-- A wrong secret is refused.
SELECT datalake_storage_read_text(s3_uri('a.txt'),
								  s3_kv(current_setting('datalake.s3_bad_secret')));
\set VERBOSITY default

-- The refused write left the object as it was.
SELECT datalake_storage_read_text(s3_uri('a.txt'), s3_kv());

-- What the user is told names the object but not the secret.  The text of an
-- SDK message varies between services, so this asks what must and must not be
-- in it rather than pinning the whole string.

SELECT s3_error(s3_uri('a.txt'), s3_kv(current_setting('datalake.s3_bad_secret')))
		 LIKE '%' || current_setting('datalake.s3_bad_secret') || '%'
		 AS leaks_the_secret,
	   s3_error(s3_uri('a.txt'), s3_kv(current_setting('datalake.s3_bad_secret')))
		 LIKE '%a.txt%' AS names_the_object,
	   s3_error(s3_uri('a.txt'), s3_kv())
		 = 'no error' AS good_credentials_still_work;

-- Redaction, tested where the service really does echo the value back: a
-- bucket name appears in the error about it, so a run whose secret IS that
-- bucket name must come back with the name masked.  Remove the redaction and
-- this fails, which the wrong-secret case above cannot claim.
SELECT s3_error('s3://dl-redaction-probe-9f2b/x.txt',
				ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
					  'region=' || current_setting('datalake.s3_region'),
					  'path_style_access=' || current_setting('datalake.s3_path_style'),
					  'access_key_id=' || current_setting('datalake.s3_access_key'),
					  'secret_access_key=dl-redaction-probe-9f2b'])
		 LIKE '%dl-redaction-probe-9f2b%' AS leaks_the_secret,
	   s3_error('s3://dl-redaction-probe-9f2b/x.txt',
				ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
					  'region=' || current_setting('datalake.s3_region'),
					  'path_style_access=' || current_setting('datalake.s3_path_style'),
					  'access_key_id=' || current_setting('datalake.s3_access_key'),
					  'secret_access_key=dl-redaction-probe-9f2b'])
		 LIKE '%***%' AS masked_it;

-- Half a credential pair is a mistake, not a reason to fall back to whatever
-- identity the host happens to have.
\set VERBOSITY sqlstate
SELECT datalake_storage_read_text(s3_uri('a.txt'),
	ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
		  'region=us-east-1', 'path_style_access=true',
		  'access_key_id=' || current_setting('datalake.s3_access_key')]);
\set VERBOSITY default

-- Asking for host-style addressing against an endpoint that is an IP address
-- still works, because a bucket name cannot be prepended to an IP and the SDK
-- falls back to path style.  Worth pinning: it is the reason a wrong
-- path_style_access setting does not fail loudly in a lab.
SELECT datalake_storage_read_text(s3_uri('a.txt'),
	ARRAY['endpoint=' || current_setting('datalake.s3_endpoint'),
		  'region=us-east-1', 'path_style_access=false',
		  'access_key_id=' || current_setting('datalake.s3_access_key'),
		  'secret_access_key=' || current_setting('datalake.s3_secret')])
	AS host_style_against_an_ip;

-- An endpoint that answers nothing has to become an error while someone is
-- still waiting for it, rather than a session that cannot be cancelled.
SELECT s3_error(s3_uri('a.txt'),
				ARRAY['endpoint=http://10.255.255.1:9000',
					  'region=us-east-1', 'path_style_access=true',
					  'access_key_id=x', 'secret_access_key=y'])
		 <> 'no error' AS blackhole_reported,
	   clock_timestamp() - statement_timestamp() < interval '30 seconds'
		 AS within_the_bound;

-- The session still works afterwards.
SELECT datalake_storage_read_text(s3_uri('a.txt'), s3_kv());

-- Take this run's objects back out again.
SELECT datalake_storage_delete(s3_uri('a.txt'), s3_kv()) AS deleted_a,
	   datalake_storage_delete(s3_uri('big.bin'), s3_kv()) AS deleted_big;
\set VERBOSITY sqlstate
SELECT datalake_storage_delete(s3_uri('a.txt'), s3_kv());
\set VERBOSITY default

DROP FUNCTION s3_error(text, text[]), s3_uri(text), s3_prefix(), s3_kv(text);
