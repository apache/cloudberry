-- The s3 backend, reached the way a user reaches it: through a volume server
-- and a user mapping, rather than by handing credentials to a function.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

SET log_statement = 'none';
SET log_min_duration_statement = -1;
SET log_min_error_statement = 'panic';

-- Endpoint, bucket and credentials differ from one machine to the next, so
-- they are read with the echo off: what this file asserts must not depend on
-- where it ran.
\set ECHO none
-- An unset psql variable interpolates as its own name, so the optional ones
-- are given a value before \getenv has the chance to leave them undefined.
\set region ''
\set path_style ''
\getenv endpoint DATALAKE_TEST_S3_ENDPOINT
\getenv bucket DATALAKE_TEST_S3_BUCKET
\getenv access_key DATALAKE_TEST_S3_ACCESS_KEY
\getenv secret_key DATALAKE_TEST_S3_SECRET_KEY
\getenv region DATALAKE_TEST_S3_REGION
\getenv path_style DATALAKE_TEST_S3_PATH_STYLE

SELECT to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS') || '_' ||
	   pg_backend_pid() AS run \gset

-- Server options take literals, so the values are assembled first and
-- interpolated as literals afterwards.
SELECT format('s3://%s/datalake_conformance/%s', :'bucket', :'run') AS base_path,
	   format('s3://%s/datalake_conformance/%s', :'bucket', :'run') AS prefix,
	   format('%s/datalake_conformance/%s/', :'bucket', :'run') AS root,
	   format('ARRAY[%L,%L,%L,%L,%L]',
			  'endpoint=' || :'endpoint', 'region=' || :'region',
			  'path_style_access=' || coalesce(nullif(:'path_style', ''), 'true'),
			  'access_key_id=' || :'access_key',
			  'secret_access_key=' || :'secret_key') AS kv,
	   coalesce(nullif(:'region', ''), 'us-east-1') AS server_region,
	   coalesce(nullif(:'path_style', ''), 'true') AS server_path_style \gset

DROP SERVER IF EXISTS dlconf_volume CASCADE;
CREATE SERVER dlconf_volume FOREIGN DATA WRAPPER iceberg_volume_fdw OPTIONS (
	base_path :'base_path',
	endpoint :'endpoint',
	region :'server_region',
	path_style_access :'server_path_style');
CREATE USER MAPPING FOR CURRENT_USER SERVER dlconf_volume OPTIONS (
	access_key_id :'access_key',
	secret_access_key :'secret_key');

\set volume '''dlconf_volume'''
\set ECHO all

-- The volume exists and names this run's prefix.
SELECT count(*) = 1 AS volume_created
FROM pg_foreign_server WHERE srvname = 'dlconf_volume';

-- pg_regress feeds the script to psql on standard input, so there is no
-- script directory for \ir to resolve against; the path is relative to where
-- make runs, which is the module's own directory.
\i test/automation/sqlrepo/smoke/storage_conformance/sql/conformance_body.sql

\set ECHO none
DROP SERVER dlconf_volume CASCADE;
\set ECHO all
