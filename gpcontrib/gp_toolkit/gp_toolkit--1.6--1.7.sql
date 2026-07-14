/* gpcontrib/gp_toolkit/gp_toolkit--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.7'" to load this file. \quit

-- Create the mdb_admin privilege role with its fixed OID (8067).
--
-- The server lets members of mdb_admin CREATE/ALTER/DROP resource groups and
-- run pg_resgroup_move_query() without superuser.  The role is identified by
-- a fixed, well-known OID rather than by name, so it must be created through
-- this function (a plain CREATE ROLE would assign an ordinary OID and the
-- permission checks would not recognise its members).  The OID assignment is
-- dispatched to the segments, so the role has the same OID cluster-wide.
--
-- Typical usage (once per cluster, as superuser):
--   SELECT gp_toolkit.pg_create_mdb_admin_role();
--   GRANT mdb_admin TO cloud_admin;
CREATE FUNCTION gp_toolkit.pg_create_mdb_admin_role()
RETURNS OID
AS 'gp_toolkit.so', 'pg_create_mdb_admin_role'
LANGUAGE C STRICT;
