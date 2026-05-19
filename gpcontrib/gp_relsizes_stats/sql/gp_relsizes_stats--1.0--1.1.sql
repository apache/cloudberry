/* gp_relsizes_stats--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit


DROP FUNCTION relsizes_stats_schema.get_stats_for_database(dboid INTEGER);

CREATE FUNCTION relsizes_stats_schema.get_stats_for_database(dboid OID, fast BOOL)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_stats_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;
