/* gp_url_tools--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_url_tools" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS url_tools_schema;

CREATE FUNCTION url_tools_schema.encode_url(text)
RETURNS text
AS 'MODULE_PATHNAME', 'encode_url'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION url_tools_schema.decode_url(text)
RETURNS text
AS 'MODULE_PATHNAME', 'decode_url'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION url_tools_schema.encode_uri(text)
RETURNS text
AS 'MODULE_PATHNAME', 'encode_uri'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION url_tools_schema.decode_uri(text)
RETURNS text
AS 'MODULE_PATHNAME', 'decode_uri'
LANGUAGE C IMMUTABLE STRICT;

