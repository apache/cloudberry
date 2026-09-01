-- Tests permission checks for pg_manage_resource_groups with
-- resource groups enabled.

-- start_matchsubs
-- m/ERROR:  cannot find process: \d+/
-- s/\d+/XXX/g
-- end_matchsubs

DROP ROLE IF EXISTS role_rg_admin;
DROP ROLE IF EXISTS role_rg_noadmin;
-- start_ignore
DROP RESOURCE GROUP rg_perm_admin1;
DROP RESOURCE GROUP rg_perm_admin2;
DROP RESOURCE GROUP rg_perm_revoke1;
DROP RESOURCE GROUP rg_perm_revoke2;
DROP RESOURCE GROUP rg_perm_test;
-- end_ignore

-- ---------------------------------------------------------------------
-- Setup.
-- ---------------------------------------------------------------------
CREATE RESOURCE GROUP rg_perm_test WITH (concurrency=2, cpu_max_percent=10);
CREATE ROLE role_rg_admin RESOURCE GROUP rg_perm_test;
CREATE ROLE role_rg_noadmin RESOURCE GROUP rg_perm_test;
GRANT pg_manage_resource_groups TO role_rg_admin;

-- The predefined role must be present after initdb.
SELECT 1 AS role_exists FROM pg_roles WHERE rolname = 'pg_manage_resource_groups';

-- ---------------------------------------------------------------------
-- 1. Member of pg_manage_resource_groups can CREATE/ALTER/DROP
--    resource groups (statements are dispatched to segments).
-- ---------------------------------------------------------------------
1: SET ROLE role_rg_admin;
1: CREATE RESOURCE GROUP rg_perm_admin1 WITH (concurrency=1, cpu_max_percent=5);
1: ALTER RESOURCE GROUP rg_perm_admin1 SET cpu_max_percent 6;
1: DROP RESOURCE GROUP rg_perm_admin1;

-- 2. Even a member cannot ALTER or DROP the system admin_group.
1: ALTER RESOURCE GROUP admin_group SET cpu_max_percent 99;
1: DROP RESOURCE GROUP admin_group;
1q:

-- 2b. A real superuser can still ALTER a system group (invariant preserved,
--     value restored to its default afterwards).
ALTER RESOURCE GROUP admin_group SET cpu_max_percent 9;
ALTER RESOURCE GROUP admin_group SET cpu_max_percent 10;

-- ---------------------------------------------------------------------
-- 3. A non-member is rejected on every entry point.
-- ---------------------------------------------------------------------
2: SET ROLE role_rg_noadmin;
2: CREATE RESOURCE GROUP rg_perm_admin2 WITH (concurrency=1, cpu_max_percent=5);
2: ALTER RESOURCE GROUP rg_perm_test SET cpu_max_percent 7;
2: DROP RESOURCE GROUP rg_perm_test;
2q:

-- ---------------------------------------------------------------------
-- 4. pg_resgroup_move_query() honours the same permission check.
--    The first call (non-member) must fail with "permission denied".
--    The second call (member) gets past the permission gate and
--    fails on the pid lookup (masked by start_matchsubs above).
-- ---------------------------------------------------------------------
3: SET ROLE role_rg_noadmin;
3: SELECT pg_resgroup_move_query(999999999, 'admin_group');
3: RESET ROLE;
3: SET ROLE role_rg_admin;
3: SELECT pg_resgroup_move_query(999999999, 'admin_group');
3q:

-- ---------------------------------------------------------------------
-- 5. Cross-session REVOKE takes effect on the granted session's
--    next statement (the privilege is re-checked per command, not
--    cached at SET ROLE time).
-- ---------------------------------------------------------------------
4: SET ROLE role_rg_admin;
4: CREATE RESOURCE GROUP rg_perm_revoke1 WITH (concurrency=1, cpu_max_percent=5);
5: REVOKE pg_manage_resource_groups FROM role_rg_admin;
4: CREATE RESOURCE GROUP rg_perm_revoke2 WITH (concurrency=1, cpu_max_percent=5);
4: DROP RESOURCE GROUP rg_perm_revoke1;
4q:
5q:

-- ---------------------------------------------------------------------
-- Cleanup. Roles must be dropped before the resource group they
-- reference, otherwise DROP RESOURCE GROUP fails with
-- "resource group is used by at least one role".
-- ---------------------------------------------------------------------
RESET ROLE;
DROP ROLE role_rg_admin;
DROP ROLE role_rg_noadmin;
DROP RESOURCE GROUP rg_perm_revoke1;
DROP RESOURCE GROUP rg_perm_test;
