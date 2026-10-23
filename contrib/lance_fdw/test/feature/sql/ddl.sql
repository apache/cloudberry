-- ddl: the option surface - what is accepted, what is
-- rejected, and where - plus a pg_dump round trip and a clean uninstall.
-- Every uri here is synthetic: the DDL itself never reaches an object store,
-- which is what shows that validation does no I/O, and the one scan
-- below is there to fail.
\pset format unaligned
CREATE SERVER lance_ddl_srv FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri 'file:///lance/base',
           aws_endpoint 'http://minio.invalid:9000',
           aws_region 'us-east-1',
           allow_http 'true',
           virtual_hosted_style_request 'false');
-- Not a credential: this suite never reaches an object store.
CREATE USER MAPPING FOR PUBLIC SERVER lance_ddl_srv
  OPTIONS (aws_access_key_id 'AKIA_EXAMPLE',
           aws_secret_access_key 'example-only',
           aws_session_token 'example-token');
CREATE FOREIGN TABLE lance_feature.ddl_all (
  id integer,
  v text OPTIONS (column_name 'renamed_v')
) SERVER lance_ddl_srv
  OPTIONS (uri 'ds.lance', version '3', batch_size '128', rows_hint '4200');
SELECT o.opt AS server_option
  FROM pg_foreign_server s, unnest(s.srvoptions) AS o(opt)
  WHERE s.srvname = 'lance_ddl_srv' ORDER BY 1;
-- Names only: the values are credentials, and credentials do not belong in an
-- expected file even when they are made up.
SELECT split_part(o.opt, '=', 1) AS user_mapping_option
  FROM pg_user_mappings u, unnest(u.umoptions) AS o(opt)
  WHERE u.srvname = 'lance_ddl_srv' ORDER BY 1;
SELECT o.opt AS table_option
  FROM pg_foreign_table t, unnest(t.ftoptions) AS o(opt)
  WHERE t.ftrelid = 'lance_feature.ddl_all'::regclass ORDER BY 1;
SELECT format('%s: %s', a.attname, o.opt) AS column_option
  FROM pg_attribute a, unnest(a.attfdwoptions) AS o(opt)
  WHERE a.attrelid = 'lance_feature.ddl_all'::regclass ORDER BY 1;
-- The byte limit on its own is accepted; ALTER has to see the combination too,
-- because the validator is handed the merged option list, not just the change.
CREATE FOREIGN TABLE lance_feature.ddl_bytes (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'ds.lance', batch_size_bytes '1048576');
SELECT o.opt AS bytes_option
  FROM pg_foreign_table t, unnest(t.ftoptions) AS o(opt)
  WHERE t.ftrelid = 'lance_feature.ddl_bytes'::regclass ORDER BY 1;
ALTER FOREIGN TABLE lance_feature.ddl_bytes OPTIONS (ADD batch_size '128');
-- mpp_execute may be overridden on a server and on a table.
CREATE SERVER lance_coord_srv FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri 'file:///lance/base', mpp_execute 'coordinator');
ALTER FOREIGN TABLE lance_feature.ddl_all OPTIONS (ADD mpp_execute 'any');
SELECT o.opt AS mpp_option
  FROM pg_foreign_table t, unnest(t.ftoptions) AS o(opt)
  WHERE t.ftrelid = 'lance_feature.ddl_all'::regclass AND o.opt LIKE 'mpp%';
-- Rejected: an option that belongs on another object, or a value that is not
-- what it claims to be.
CREATE SERVER lance_bad FOREIGN DATA WRAPPER lance_fdw OPTIONS (bogus 'x');
CREATE SERVER lance_bad FOREIGN DATA WRAPPER lance_fdw OPTIONS (uri 'x');
CREATE SERVER lance_bad FOREIGN DATA WRAPPER lance_fdw OPTIONS (allow_http 'maybe');
CREATE SERVER lance_bad FOREIGN DATA WRAPPER lance_fdw OPTIONS (mpp_execute 'sideways');
CREATE USER MAPPING FOR PUBLIC SERVER lance_coord_srv OPTIONS (password 'x');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (version '1');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', version 'HEAD');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', version '-1');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', batch_size '0');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', batch_size_bytes '0');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', batch_size_bytes 'lots');
-- lance lets a byte limit override a row limit, so taking both would ignore
-- one of them without saying so.
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', batch_size '128', batch_size_bytes '1048576');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', rows_hint 'lots');
CREATE FOREIGN TABLE lance_feature.bad (id integer) SERVER lance_ddl_srv
  OPTIONS (uri 'x', base_uri 'y');
CREATE FOREIGN TABLE lance_feature.bad (id integer OPTIONS (nope 'x')) SERVER lance_ddl_srv
  OPTIONS (uri 'x');
-- The one statement in this suite that does reach for a store, and the uri is
-- synthetic, so it comes back as a lance error rather than rows.
SELECT lance_feature.capture($$SELECT * FROM lance_feature.ddl_all$$) AS scan;
-- pg_dump round trip: the dumped definition rebuilds the same foreign table.
\! pg_dump --schema-only --no-owner --table=lance_feature.ddl_all contrib_regression > test/feature/results/ddl_dump.sql 2>&1
\! grep -c '^CREATE FOREIGN TABLE lance_feature.ddl_all (' test/feature/results/ddl_dump.sql
DROP FOREIGN TABLE lance_feature.ddl_all;
\! psql -X -q -o /dev/null -d contrib_regression -f test/feature/results/ddl_dump.sql 2>&1
SELECT o.opt AS table_option
  FROM pg_foreign_table t, unnest(t.ftoptions) AS o(opt)
  WHERE t.ftrelid = 'lance_feature.ddl_all'::regclass ORDER BY 1;
SELECT format('%s: %s', a.attname, o.opt) AS column_option
  FROM pg_attribute a, unnest(a.attfdwoptions) AS o(opt)
  WHERE a.attrelid = 'lance_feature.ddl_all'::regclass ORDER BY 1;
-- DROP EXTENSION CASCADE takes every lance object with it.  The notice lists
-- the cascaded objects in dependency order, which is not worth pinning down,
-- so it is silenced rather than compared.
SET client_min_messages = warning;
DROP EXTENSION lance_fdw CASCADE;
RESET client_min_messages;
SELECT count(*) AS lance_servers FROM pg_foreign_server WHERE srvname LIKE 'lance%';
SELECT count(*) AS lance_foreign_tables
  FROM pg_foreign_table t
  JOIN pg_class c ON c.oid = t.ftrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'lance_feature';
SELECT count(*) AS wrappers FROM pg_foreign_data_wrapper WHERE fdwname = 'lance_fdw';
-- and back, for the suites that follow
CREATE EXTENSION lance_fdw;
