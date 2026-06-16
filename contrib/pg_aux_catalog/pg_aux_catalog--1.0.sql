/* contrib/pg_aux_catalog/pg_aux_catalog--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_aux_catalog" to load this file. \quit

-- Create the mdb_admin role with fixed OID 8067
CREATE FUNCTION pg_create_mdb_admin_role()
RETURNS OID
AS 'MODULE_PATHNAME', 'pg_create_mdb_admin_role'
LANGUAGE C PARALLEL SAFE STRICT;
