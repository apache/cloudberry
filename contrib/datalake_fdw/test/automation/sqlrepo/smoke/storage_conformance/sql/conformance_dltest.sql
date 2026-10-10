-- A backend registered from outside the module through the public contract,
-- mounted under a subtree.  Its paths are relative rather than absolute, which
-- is exactly the difference a backend is allowed to have.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

COPY (SELECT 1) TO PROGRAM
	'rm -rf /tmp/datalake_fdw_conformance_dltest && mkdir -p /tmp/datalake_fdw_conformance_dltest';

\set prefix 'dltest:///tmp/datalake_fdw_conformance_dltest'
\set root ''
\set volume NULL
\set kv NULL

-- pg_regress feeds the script to psql on standard input, so there is no
-- script directory for \ir to resolve against; the path is relative to where
-- make runs, which is the module's own directory.
\i test/automation/sqlrepo/smoke/storage_conformance/sql/conformance_body.sql


-- A prefix holding only an empty directory.  Object storage has no such
-- thing, so the facade must answer for a filesystem the way it answers for
-- a bucket: nothing stored is not found, whichever backend is underneath.
COPY (SELECT 1) TO PROGRAM 'mkdir -p /tmp/datalake_fdw_conformance_dltest/onlydirs/empty';
\set VERBOSITY sqlstate
SELECT count(*) FROM datalake_storage_list('dltest:///tmp/datalake_fdw_conformance_dltest/onlydirs', :kv);
\set VERBOSITY default
COPY (SELECT 1) TO PROGRAM 'rm -rf /tmp/datalake_fdw_conformance_dltest';
