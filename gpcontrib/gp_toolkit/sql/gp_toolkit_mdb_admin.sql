-- Tests for gp_toolkit.pg_create_mdb_admin_role(): creation of the
-- fixed-OID mdb_admin role and the resource-group permission gate it
-- enables.  gp_toolkit is installed in template1 by gpinitsystem, so the
-- extension is already present in this database.

-- ---------------------------------------------------------------------
-- pg_create_mdb_admin_role() creates the mdb_admin role with its fixed OID.
-- ---------------------------------------------------------------------
SELECT gp_toolkit.pg_create_mdb_admin_role() AS mdb_admin_oid;

-- The role exists with the fixed OID and is a non-login, non-superuser,
-- connection-limited role.
SELECT oid = 8067 AS has_fixed_oid, rolcanlogin, rolsuper,
       rolcreaterole, rolcreatedb, rolconnlimit
  FROM pg_authid WHERE rolname = 'mdb_admin';

-- Creating it a second time is rejected.
SELECT gp_toolkit.pg_create_mdb_admin_role();

-- ---------------------------------------------------------------------
-- Resource-group permission gate: a role that is not a member of mdb_admin
-- is rejected on every entry point.  These checks run before the "resource
-- group is enabled" check, so they are deterministic regardless of the
-- resource manager in use.
-- ---------------------------------------------------------------------
CREATE ROLE regress_rg_noadmin;
SET ROLE regress_rg_noadmin;
CREATE RESOURCE GROUP regress_rg_x WITH (concurrency=1, cpu_max_percent=5);
ALTER RESOURCE GROUP regress_rg_x SET cpu_max_percent 6;
DROP RESOURCE GROUP regress_rg_x;
RESET ROLE;
DROP ROLE regress_rg_noadmin;

-- ---------------------------------------------------------------------
-- Cleanup.
-- ---------------------------------------------------------------------
DROP ROLE mdb_admin;
