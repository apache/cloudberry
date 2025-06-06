/* contrib/cloudberry_fdw/cloudberry_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cloudberry_fdw" to load this file. \quit

CREATE FUNCTION cloudberry_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION cloudberry_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER cloudberry_fdw
  HANDLER cloudberry_fdw_handler
  VALIDATOR cloudberry_fdw_validator;
