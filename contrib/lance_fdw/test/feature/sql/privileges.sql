-- privileges: what a role that is not a superuser may make lance_fdw read.
--
-- The model is file_fdw's and postgres_fdw's.  A dataset outside the server's
-- base_uri, a base_uri on this host's file system and the options that choose
-- where lance-c connects are checked when they are set; a non-superuser that
-- reads from an object store has to bring keys of its own, which is checked
-- when the dataset is opened.  Errors go through message(), because the ones
-- a CREATE FOREIGN TABLE raises come back from a segment.
\pset format unaligned
-- Quietly: whether CREATE ROLE mentions a resource queue depends on the
-- cluster's resource manager.
SET client_min_messages = warning;
DROP ROLE IF EXISTS lance_priv_user;
DROP ROLE IF EXISTS lance_priv_files;
CREATE ROLE lance_priv_user;
CREATE ROLE lance_priv_files IN ROLE pg_read_server_files;
RESET client_min_messages;
GRANT USAGE ON FOREIGN DATA WRAPPER lance_fdw TO lance_priv_user, lance_priv_files;
GRANT USAGE, CREATE ON SCHEMA lance_feature TO lance_priv_user, lance_priv_files;
DO $$
BEGIN
  EXECUTE format('CREATE SERVER priv_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
  EXECUTE format('CREATE SERVER priv_s3 FOREIGN DATA WRAPPER lance_fdw '
                 'OPTIONS (base_uri %L, aws_endpoint %L, aws_region %L, allow_http %L)',
                 's3://' || current_setting('regress.s3_bucket') || '/fixtures',
                 current_setting('regress.s3_endpoint'),
                 current_setting('regress.s3_region'), 'true');
END
$$;
GRANT USAGE ON FOREIGN SERVER priv_files, priv_s3 TO lance_priv_user, lance_priv_files;
-- The absolute names a plain role tries below, built before SET ROLE because
-- reading the fixture directory's setting is fine for anyone.
SELECT current_setting('regress.fixture_dir') || '/frag_3.lance' AS abs_path \gset
SELECT 'file://' || current_setting('regress.fixture_dir') || '/frag_3.lance' AS abs_url \gset
SELECT 's3://' || current_setting('regress.s3_bucket') || '/fixtures/frag_3.lance' AS abs_s3 \gset
SET ROLE lance_priv_user;
-- Inside base_uri: a plain name, one with a "." component, and dots that are
-- part of a name rather than a component of their own.
CREATE FOREIGN TABLE lance_feature.priv_inside (id integer) SERVER priv_files
  OPTIONS (uri 'frag_3.lance');
SELECT count(*) AS inside FROM lance_feature.priv_inside;
CREATE FOREIGN TABLE lance_feature.priv_dot (id integer) SERVER priv_files
  OPTIONS (uri './frag_3.lance');
SELECT count(*) AS dot FROM lance_feature.priv_dot;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_dots (id integer)
  SERVER priv_files OPTIONS (uri 'a..b/...')$$) AS dots_in_a_name;
-- Out of it: absolute names of every spelling, and ".." however it is written.
SELECT lance_feature.message(format($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri %L)$$, :'abs_path')) AS absolute_path;
SELECT lance_feature.message(format($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri %L)$$, :'abs_url')) AS absolute_file_url;
SELECT lance_feature.message(format($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri %L)$$, :'abs_s3')) AS absolute_s3;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri '../data/frag_3.lance')$$) AS dot_dot;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri 'sub/../../frag_3.lance')$$) AS dot_dot_later;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri '%2e%2E/frag_3.lance')$$) AS encoded_dot_dot;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri '.%2e/frag_3.lance')$$) AS half_encoded_dot_dot;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri '..\frag_3.lance')$$) AS backslash;
SELECT lance_feature.message($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri '..%2ffrag_3.lance')$$) AS encoded_slash;
-- Changing the uri of a table goes through the same check.
SELECT lance_feature.message($$ALTER FOREIGN TABLE lance_feature.priv_inside
  OPTIONS (SET uri '../frag_3.lance')$$) AS alter_to_dot_dot;
-- IMPORT FOREIGN SCHEMA opens the dataset before the table it would create is
-- validated, so it checks the name itself.
SELECT lance_feature.message($$IMPORT FOREIGN SCHEMA x LIMIT TO ("../data/frag_3.lance")
  FROM SERVER priv_files INTO lance_feature$$) AS import_dot_dot;
-- A server of its own: a local base_uri reads this host's files, and the
-- endpoint and allow_http decide where and how lance-c connects.  An object
-- store base_uri is fine; what it may read is up to the credentials.
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri '/tmp')$$) AS own_local_base;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri 'file:///tmp')$$) AS own_file_url_base;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (aws_endpoint 'http://169.254.169.254')$$) AS own_endpoint;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (allow_http 'true')$$) AS own_allow_http;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri 's3://bucket/prefix')$$) AS own_s3_base;
DROP SERVER priv_own;
-- Reading an object store without keys of its own: no user mapping at all,
-- and one without keys.  Without them lance would fall back to the host's
-- environment, ~/.aws or instance role.
CREATE FOREIGN TABLE lance_feature.priv_s3 (id integer) SERVER priv_s3
  OPTIONS (uri 'frag_3.lance');
SELECT lance_feature.message($$SELECT count(*) FROM lance_feature.priv_s3$$) AS no_user_mapping;
RESET ROLE;
CREATE USER MAPPING FOR lance_priv_user SERVER priv_s3;
SET ROLE lance_priv_user;
SELECT lance_feature.message($$SELECT count(*) FROM lance_feature.priv_s3$$) AS mapping_without_keys;
SELECT lance_feature.message($$IMPORT FOREIGN SCHEMA x LIMIT TO ("frag_3.lance")
  FROM SERVER priv_s3 INTO lance_feature$$) AS import_without_keys;
-- Only a superuser may waive that.
SELECT lance_feature.message($$ALTER USER MAPPING FOR lance_priv_user SERVER priv_s3
  OPTIONS (ADD credentials_required 'false')$$) AS waive_as_plain_role;
SELECT lance_feature.message($$ALTER USER MAPPING FOR lance_priv_user SERVER priv_s3
  OPTIONS (ADD credentials_required 'maybe')$$) AS not_a_boolean;
RESET ROLE;
ALTER USER MAPPING FOR lance_priv_user SERVER priv_s3 OPTIONS (ADD credentials_required 'false');
ALTER USER MAPPING FOR lance_priv_user SERVER priv_s3 OPTIONS (SET credentials_required 'true');
-- With keys it reads.
DO $$
BEGIN
  EXECUTE format('ALTER USER MAPPING FOR lance_priv_user SERVER priv_s3 '
                 'OPTIONS (ADD aws_access_key_id %L, ADD aws_secret_access_key %L)',
                 current_setting('regress.s3_key'), current_setting('regress.s3_secret'));
END
$$;
SET ROLE lance_priv_user;
SELECT count(*) AS with_keys FROM lance_feature.priv_s3;
-- Keys only ever reach s3://, so any other store is the host's credentials.
RESET ROLE;
CREATE SERVER priv_gs FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri 'gs://bucket/prefix');
GRANT USAGE ON FOREIGN SERVER priv_gs TO lance_priv_user;
CREATE USER MAPPING FOR lance_priv_user SERVER priv_gs
  OPTIONS (aws_access_key_id 'k', aws_secret_access_key 's');
SET ROLE lance_priv_user;
CREATE FOREIGN TABLE lance_feature.priv_gs (id integer) SERVER priv_gs
  OPTIONS (uri 'frag_3.lance');
SELECT lance_feature.message($$SELECT count(*) FROM lance_feature.priv_gs$$) AS other_store;
RESET ROLE;
-- pg_read_server_files may read this host's files, so it may give a local
-- absolute path and a local base_uri - but not an absolute object store uri,
-- nor the connection options.
SET ROLE lance_priv_files;
CREATE FOREIGN TABLE lance_feature.priv_files_abs (id integer) SERVER priv_files
  OPTIONS (uri :'abs_path');
SELECT count(*) AS files_absolute FROM lance_feature.priv_files_abs;
SELECT lance_feature.message(format($$CREATE FOREIGN TABLE lance_feature.priv_x (id integer)
  SERVER priv_files OPTIONS (uri %L)$$, :'abs_s3')) AS files_absolute_s3;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri '/tmp')$$) AS files_local_base;
DROP SERVER priv_own;
SELECT lance_feature.message($$CREATE SERVER priv_own FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (aws_endpoint 'http://169.254.169.254')$$) AS files_endpoint;
RESET ROLE;
-- A superuser may name anything, and a table it creates may be granted to a
-- role that could not have created it, as with file_fdw.
CREATE FOREIGN TABLE lance_feature.priv_su_abs (id integer) SERVER priv_files
  OPTIONS (uri :'abs_path');
CREATE FOREIGN TABLE lance_feature.priv_su_dotdot (id integer) SERVER priv_files
  OPTIONS (uri '../data/frag_3.lance');
GRANT SELECT ON lance_feature.priv_su_abs TO lance_priv_user;
SET ROLE lance_priv_user;
SELECT count(*) AS granted_by_superuser FROM lance_feature.priv_su_abs;
RESET ROLE;
-- Leave nothing behind that the next suite, or the next run, would trip over.
DROP FOREIGN TABLE lance_feature.priv_inside, lance_feature.priv_dot,
  lance_feature.priv_dots, lance_feature.priv_s3, lance_feature.priv_gs,
  lance_feature.priv_files_abs, lance_feature.priv_su_abs,
  lance_feature.priv_su_dotdot;
DROP USER MAPPING FOR lance_priv_user SERVER priv_s3;
DROP USER MAPPING FOR lance_priv_user SERVER priv_gs;
DROP SERVER priv_s3, priv_gs, priv_files;
REVOKE ALL ON SCHEMA lance_feature FROM lance_priv_user, lance_priv_files;
REVOKE ALL ON FOREIGN DATA WRAPPER lance_fdw FROM lance_priv_user, lance_priv_files;
DROP ROLE lance_priv_user;
DROP ROLE lance_priv_files;
