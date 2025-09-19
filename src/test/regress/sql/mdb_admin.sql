CREATE ROLE regress_mdb_admin_user1;
CREATE ROLE regress_mdb_admin_user2;
CREATE ROLE regress_mdb_admin_user3;
CREATE ROLE mdb_admin;

CREATE ROLE regress_superuser WITH SUPERUSER;

GRANT mdb_admin TO regress_mdb_admin_user1;
GRANT CREATE ON DATABASE regression TO regress_mdb_admin_user2;
GRANT CREATE ON DATABASE regression TO regress_mdb_admin_user3;

-- mdb admin trasfers ownership to another role

SET ROLE regress_mdb_admin_user2;
CREATE FUNCTION regress_mdb_admin_add(integer, integer) RETURNS integer
    AS 'SELECT $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE SCHEMA regress_mdb_admin_schema;
GRANT CREATE ON SCHEMA regress_mdb_admin_schema TO regress_mdb_admin_user3;
CREATE TABLE regress_mdb_admin_schema.regress_mdb_admin_table();
CREATE TABLE regress_mdb_admin_table();
CREATE VIEW regress_mdb_admin_view as SELECT 1;
SET ROLE regress_mdb_admin_user1;

ALTER FUNCTION regress_mdb_admin_add (integer, integer) OWNER TO regress_mdb_admin_user3;
ALTER VIEW regress_mdb_admin_view OWNER TO regress_mdb_admin_user3;
ALTER TABLE regress_mdb_admin_schema.regress_mdb_admin_table OWNER TO regress_mdb_admin_user3;
ALTER TABLE regress_mdb_admin_table OWNER TO regress_mdb_admin_user3;
ALTER SCHEMA regress_mdb_admin_schema OWNER TO regress_mdb_admin_user3;


-- mdb admin fails to transfer ownership to superusers and system roles

ALTER FUNCTION regress_mdb_admin_add (integer, integer) OWNER TO regress_superuser;
ALTER VIEW regress_mdb_admin_view OWNER TO regress_superuser;
ALTER TABLE regress_mdb_admin_schema.regress_mdb_admin_table OWNER TO regress_superuser;
ALTER TABLE regress_mdb_admin_table OWNER TO regress_superuser;
ALTER SCHEMA regress_mdb_admin_schema OWNER TO regress_superuser;

ALTER FUNCTION regress_mdb_admin_add (integer, integer) OWNER TO pg_execute_server_program;
ALTER VIEW regress_mdb_admin_view OWNER TO pg_execute_server_program;
ALTER TABLE regress_mdb_admin_schema.regress_mdb_admin_table OWNER TO pg_execute_server_program;
ALTER TABLE regress_mdb_admin_table OWNER TO pg_execute_server_program;
ALTER SCHEMA regress_mdb_admin_schema OWNER TO pg_execute_server_program;

ALTER FUNCTION regress_mdb_admin_add (integer, integer) OWNER TO pg_write_server_files;
ALTER VIEW regress_mdb_admin_view OWNER TO pg_write_server_files;
ALTER TABLE regress_mdb_admin_schema.regress_mdb_admin_table OWNER TO pg_write_server_files;
ALTER TABLE regress_mdb_admin_table OWNER TO pg_write_server_files;
ALTER SCHEMA regress_mdb_admin_schema OWNER TO pg_write_server_files;

ALTER FUNCTION regress_mdb_admin_add (integer, integer) OWNER TO pg_read_server_files;
ALTER VIEW regress_mdb_admin_view OWNER TO pg_read_server_files;
ALTER TABLE regress_mdb_admin_schema.regress_mdb_admin_table OWNER TO pg_read_server_files;
ALTER TABLE regress_mdb_admin_table OWNER TO pg_read_server_files;
ALTER SCHEMA regress_mdb_admin_schema OWNER TO pg_read_server_files;


-- end tests

RESET SESSION AUTHORIZATION;
--
REVOKE CREATE ON DATABASE regression FROM regress_mdb_admin_user2;
REVOKE CREATE ON DATABASE regression FROM regress_mdb_admin_user3;

DROP VIEW regress_mdb_admin_view;
DROP FUNCTION regress_mdb_admin_add;
DROP TABLE regress_mdb_admin_schema.regress_mdb_admin_table;
DROP TABLE regress_mdb_admin_table;
DROP SCHEMA regress_mdb_admin_schema;
DROP ROLE regress_mdb_admin_user1;
DROP ROLE regress_mdb_admin_user2;
DROP ROLE regress_mdb_admin_user3;
DROP ROLE mdb_admin;
