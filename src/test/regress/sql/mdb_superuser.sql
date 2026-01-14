CREATE ROLE regress_mdb_superuser_user1;
CREATE ROLE regress_mdb_superuser_user2;
CREATE ROLE regress_mdb_superuser_user3;

GRANT mdb_admin TO mdb_superuser;

CREATE ROLE regress_superuser WITH SUPERUSER;

GRANT mdb_superuser TO regress_mdb_superuser_user1;

GRANT CREATE ON DATABASE regression TO regress_mdb_superuser_user2;
GRANT CREATE ON DATABASE regression TO regress_mdb_superuser_user3;


SET ROLE regress_mdb_superuser_user2;

CREATE FUNCTION regress_mdb_superuser_add(integer, integer) RETURNS integer
    AS 'SELECT $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE SCHEMA regress_mdb_superuser_schema;
CREATE TABLE regress_mdb_superuser_schema.regress_mdb_superuser_table();
CREATE TABLE regress_mdb_superuser_table();
CREATE VIEW regress_mdb_superuser_view as SELECT 1;

SET ROLE regress_mdb_superuser_user3;
INSERT INTO regress_mdb_superuser_table SELECT * FROM regress_mdb_superuser_table;

SET ROLE regress_mdb_superuser_user1;

-- mdb_superuser can grant to other role
GRANT USAGE, CREATE ON SCHEMA regress_mdb_superuser_schema TO regress_mdb_superuser_user3;
GRANT ALL PRIVILEGES  ON TABLE regress_mdb_superuser_table TO regress_mdb_superuser_user3;
REVOKE ALL PRIVILEGES  ON TABLE regress_mdb_superuser_table FROM regress_mdb_superuser_user3;

GRANT INSERT, SELECT ON TABLE regress_mdb_superuser_table TO regress_mdb_superuser_user3;

-- grant works
SET ROLE regress_mdb_superuser_user3;
INSERT INTO regress_mdb_superuser_table SELECT * FROM regress_mdb_superuser_table;

SET ROLE mdb_superuser;

-- mdb_superuser drop object of other role
DROP TABLE regress_mdb_superuser_table;
-- mdb admin fails to transfer ownership to superusers and system roles

RESET SESSION AUTHORIZATION;

CREATE TABLE regress_superuser_table();

SET ROLE pg_read_server_files;

CREATE TABLE regress_pgrsf_table();

SET ROLE pg_write_server_files;

CREATE TABLE regress_pgwsf_table();

SET ROLE pg_execute_server_program;

CREATE TABLE regress_pgxsp_table();

SET ROLE pg_read_all_data;

CREATE TABLE regress_pgrad_table();

SET ROLE pg_write_all_data;

CREATE TABLE regress_pgrwd_table();

SET ROLE mdb_superuser;

-- cannot read all data (fail)
SELECT * FROM pg_authid;

-- can not drop superuser objects, because does not has_privs_of pg_database_owner 
DROP TABLE regress_superuser_table;
DROP TABLE regress_pgrsf_table;
DROP TABLE regress_pgwsf_table;
DROP TABLE regress_pgxsp_table;
DROP TABLE regress_pgrad_table;
DROP TABLE regress_pgrwd_table;


-- does allowed to creare database, role or extension
-- or grant such priviledge 

CREATE DATABASE regress_db_fail;
CREATE ROLE regress_role_fail;

ALTER ROLE mdb_superuser WITH CREATEROLE;
ALTER ROLE mdb_superuser WITH CREATEDB;

ALTER ROLE regress_mdb_superuser_user2 WITH CREATEROLE;
ALTER ROLE regress_mdb_superuser_user2 WITH CREATEDB;

-- mdb_superuser more powerfull than pg_database_owner

RESET SESSION AUTHORIZATION;
CREATE DATABASE regress_check_owner OWNER regress_mdb_superuser_user2;

\c regress_check_owner;

SET ROLE regress_mdb_superuser_user2;
CREATE SCHEMA regtest;
CREATE TABLE regtest.regtest();

-- this should fail

SET ROLE regress_mdb_superuser_user3;
GRANT ALL ON TABLE regtest.regtest TO regress_mdb_superuser_user3;
ALTER TABLE regtest.regtest OWNER TO regress_mdb_superuser_user3;

SET ROLE regress_mdb_superuser_user1;
GRANT ALL ON TABLE regtest.regtest TO regress_mdb_superuser_user1;
ALTER TABLE regtest.regtest OWNER TO regress_mdb_superuser_user1;

\c regression
DROP DATABASE regress_check_owner;

-- end tests

RESET SESSION AUTHORIZATION;
--
REVOKE CREATE ON DATABASE regression FROM regress_mdb_superuser_user2;
REVOKE CREATE ON DATABASE regression FROM regress_mdb_superuser_user3;

DROP VIEW regress_mdb_superuser_view;
DROP FUNCTION regress_mdb_superuser_add;
DROP TABLE regress_mdb_superuser_schema.regress_mdb_superuser_table;
DROP TABLE regress_mdb_superuser_table;
DROP SCHEMA regress_mdb_superuser_schema;
DROP ROLE regress_mdb_superuser_user1;
DROP ROLE regress_mdb_superuser_user2;
DROP ROLE regress_mdb_superuser_user3;
DROP TABLE regress_superuser_table;
DROP TABLE regress_pgrsf_table;
DROP TABLE regress_pgwsf_table;
DROP TABLE regress_pgxsp_table;
DROP TABLE regress_pgrad_table;
DROP TABLE regress_pgrwd_table;
