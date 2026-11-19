-- Storage servers are shared objects: ownership and comments must agree on
-- the coordinator and segments.
\pset format unaligned
\pset tuples_only on

CREATE ROLE regress_storage_owner;
CREATE ROLE regress_storage_newowner;
CREATE ROLE regress_storage_other;
CREATE ROLE regress_storage_admin;
-- test_setup supplies the mdb_admin role.
GRANT mdb_admin TO regress_storage_admin;

SET ROLE regress_storage_owner;
CREATE STORAGE SERVER regress_storage_server OPTIONS (endpoint 'example');
COMMENT ON STORAGE SERVER regress_storage_server IS 'initial comment';
RESET ROLE;

SELECT srvowner = 'regress_storage_owner'::regrole AS initial_owner,
       shobj_description(oid, 'gp_storage_server') = 'initial comment' AS initial_comment
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SELECT count(*) = 1 AS owner_dependency
FROM pg_shdepend d JOIN gp_storage_server s ON s.oid = d.objid
WHERE d.classid = 'gp_storage_server'::regclass AND d.dbid = 0
  AND d.refclassid = 'pg_authid'::regclass AND d.deptype = 'o'
  AND d.refobjid = 'regress_storage_owner'::regrole
  AND s.srvname = 'regress_storage_server';

-- An unrelated role cannot change either property.
SET ROLE regress_storage_other;
COMMENT ON STORAGE SERVER regress_storage_server IS 'not allowed';
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_newowner;
RESET ROLE;

-- The owner must be able to SET ROLE to the target, not just a member.
SET ROLE regress_storage_owner;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_newowner;
RESET ROLE;
GRANT regress_storage_newowner TO regress_storage_owner WITH INHERIT FALSE, SET FALSE;
SET ROLE regress_storage_owner;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_newowner;
RESET ROLE;
GRANT regress_storage_newowner TO regress_storage_owner WITH INHERIT FALSE, SET TRUE;
SET ROLE regress_storage_owner;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_newowner;
RESET ROLE;
REVOKE regress_storage_newowner FROM regress_storage_owner;

SELECT srvowner = 'regress_storage_newowner'::regrole AS new_owner,
       srvacl IS NULL AS default_acl,
       srvoptions = ARRAY['endpoint=example'] AS options_preserved
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SELECT bool_and(srvowner = 'regress_storage_newowner'::regrole) AS segment_owners
FROM gp_dist_random('gp_storage_server') WHERE srvname = 'regress_storage_server';
SELECT count(*) = 1 AS new_owner_dependency
FROM pg_shdepend d JOIN gp_storage_server s ON s.oid = d.objid
WHERE d.classid = 'gp_storage_server'::regclass AND d.dbid = 0
  AND d.refclassid = 'pg_authid'::regclass AND d.deptype = 'o'
  AND d.refobjid = 'regress_storage_newowner'::regrole
  AND s.srvname = 'regress_storage_server';

-- The previous owner no longer has owner privileges.
SET ROLE regress_storage_owner;
COMMENT ON STORAGE SERVER regress_storage_server IS 'not allowed';
RESET ROLE;
SET ROLE regress_storage_newowner;
ALTER STORAGE SERVER regress_storage_server OWNER TO CURRENT_USER;
COMMENT ON STORAGE SERVER regress_storage_server IS 'replacement comment';
RESET ROLE;
SELECT shobj_description(oid, 'gp_storage_server') = 'replacement comment' AS replaced
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SELECT bool_and(description = 'replacement comment') AS segment_comments
FROM gp_dist_random('pg_shdescription')
WHERE classoid = 'gp_storage_server'::regclass
  AND objoid = (SELECT oid FROM gp_storage_server WHERE srvname = 'regress_storage_server');

-- Both properties participate in transaction rollback.
BEGIN;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_other;
COMMENT ON STORAGE SERVER regress_storage_server IS 'rolled back';
ROLLBACK;
SELECT srvowner = 'regress_storage_newowner'::regrole AS owner_rolled_back,
       shobj_description(oid, 'gp_storage_server') = 'replacement comment' AS comment_rolled_back
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SELECT bool_and(srvowner = 'regress_storage_newowner'::regrole) AS segment_owners_rolled_back
FROM gp_dist_random('gp_storage_server') WHERE srvname = 'regress_storage_server';

-- NULL and the empty string both remove the shared comment.
SET ROLE regress_storage_newowner;
COMMENT ON STORAGE SERVER regress_storage_server IS NULL;
RESET ROLE;
SELECT shobj_description(oid, 'gp_storage_server') IS NULL AS removed
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SET ROLE regress_storage_newowner;
COMMENT ON STORAGE SERVER regress_storage_server IS 'temporary comment';
COMMENT ON STORAGE SERVER regress_storage_server IS '';
RESET ROLE;
SELECT shobj_description(oid, 'gp_storage_server') IS NULL AS removed_empty
FROM gp_storage_server WHERE srvname = 'regress_storage_server';

-- A shared owner dependency must also work with REASSIGN OWNED.
-- User mappings belong to their mapped user and must not be reassigned.
SET ROLE regress_storage_newowner;
CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER regress_storage_server;
RESET ROLE;
REASSIGN OWNED BY regress_storage_newowner TO regress_storage_other;
SELECT srvowner = 'regress_storage_other'::regrole AS reassigned
FROM gp_storage_server WHERE srvname = 'regress_storage_server';
SELECT umuser = 'regress_storage_newowner'::regrole AS mapping_preserved
FROM gp_storage_user_mapping m JOIN gp_storage_server s ON m.umserver = s.oid
WHERE s.srvname = 'regress_storage_server';
DROP STORAGE USER MAPPING FOR regress_storage_newowner STORAGE SERVER regress_storage_server;
DROP ROLE regress_storage_newowner;

-- Retain the Cloudberry administrator exception, including its restrictions.
SET ROLE regress_storage_admin;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_owner;
ALTER STORAGE SERVER regress_storage_server OWNER TO pg_read_all_data;
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_other;
RESET ROLE;
SELECT srvowner = 'regress_storage_other'::regrole AS admin_transfer
FROM gp_storage_server WHERE srvname = 'regress_storage_server';

SET ROLE regress_storage_other;
COMMENT ON STORAGE SERVER regress_storage_server IS 'drop me';
RESET ROLE;
SELECT oid AS storage_server_oid FROM gp_storage_server
WHERE srvname = 'regress_storage_server' \gset
DROP STORAGE SERVER regress_storage_server;
SELECT NOT EXISTS (SELECT FROM pg_shdescription
                   WHERE classoid = 'gp_storage_server'::regclass
                     AND objoid = :storage_server_oid) AS comment_cleaned_up;
SELECT NOT EXISTS (SELECT FROM pg_shdepend
                   WHERE classid = 'gp_storage_server'::regclass
                     AND objid = :storage_server_oid) AS dependencies_cleaned_up;

COMMENT ON STORAGE SERVER regress_storage_server IS 'missing';
ALTER STORAGE SERVER regress_storage_server OWNER TO regress_storage_other;
DROP ROLE regress_storage_owner;
DROP ROLE regress_storage_other;
DROP ROLE regress_storage_admin;

\pset tuples_only off
\pset format aligned
