-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit

DO $$
BEGIN
    EXECUTE 'GRANT USAGE ON SCHEMA relsizes_stats_schema TO "' || session_user || '" WITH GRANT OPTION';
    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA relsizes_stats_schema TO "' || session_user || '" WITH GRANT OPTION';
END
$$;
