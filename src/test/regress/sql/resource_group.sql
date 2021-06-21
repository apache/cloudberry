--
-- Test: create objects for pg_dumpall/pg_upgrade
--
-- this test creates resource group objects and roles associated with
-- resource groups so pg_dumpall/pg_upgrade can dump those objects at
-- the end of ICW.
-- 
-- NOTE: please always put this test in the end of this file and do not
-- drop them.

-- start_ignore
DROP ROLE IF EXISTS role_dump_test1;
DROP ROLE IF EXISTS role_dump_test2;
DROP ROLE IF EXISTS role_dump_test3;

DROP RESOURCE GROUP rg_dump_test1;
DROP RESOURCE GROUP rg_dump_test2;
DROP RESOURCE GROUP rg_dump_test3;
-- end_ignore

CREATE RESOURCE GROUP rg_dump_test1 WITH (concurrency=2, cpu_max_percent=5);
CREATE RESOURCE GROUP rg_dump_test2 WITH (concurrency=2, cpu_max_percent=5);
CREATE RESOURCE GROUP rg_dump_test3 WITH (concurrency=2, cpu_max_percent=5);

CREATE ROLE role_dump_test1 RESOURCE GROUP rg_dump_test1;
CREATE ROLE role_dump_test2 RESOURCE GROUP rg_dump_test2;
CREATE ROLE role_dump_test3 RESOURCE GROUP rg_dump_test3;

CREATE ROLE mdb_admin RESOURCE GROUP rg_dump_test1;
CREATE ROLE not_mdb_admin RESOURCE GROUP rg_dump_test1;

SET ROLE mdb_admin;

CREATE RESOURCE GROUP mdb_admin_test1 WITH (concurrency=2, cpu_max_percent=5, memory_quota=5);
CREATE RESOURCE GROUP mdb_admin_test2 WITH (concurrency=2, cpu_max_percent=5, memory_quota=5);
ALTER RESOURCE GROUP mdb_admin_test1 SET cpu_max_percent 2;
DROP RESOURCE GROUP mdb_admin_test1;
ALTER RESOURCE GROUP admin_group SET cpu_max_percent 2;
DROP RESOURCE GROUP admin_group;

SET ROLE not_mdb_admin;

CREATE RESOURCE GROUP mdb_admin_test1 WITH (concurrency=2, cpu_max_percent=5, memory_quota=5);
ALTER RESOURCE GROUP mdb_admin_test2 SET cpu_max_percent 2;
DROP RESOURCE GROUP mdb_admin_test2;
DROP RESOURCE GROUP admin_group;

RESET ROLE;
DROP RESOURCE GROUP mdb_admin_test2;
DROP ROLE mdb_admin;
DROP ROLE not_mdb_admin;
