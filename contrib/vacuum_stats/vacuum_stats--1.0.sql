/* contrib/vacuum_stats/vacuum_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vacuum_stats" to load this file. \quit

--
-- Per-relation accessor functions (tables and indexes).
--
CREATE FUNCTION pg_stat_get_vacuum_tuples_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_tuples_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_dead_tuples(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_dead_tuples'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_bytes_removed(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_bytes_removed'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_total_file_segs(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_total_file_segs'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_dead_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_dead_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_frozen(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_frozen'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_pages_all_visible(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_pages_all_visible'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_vacuum_freeze_age_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_freeze_age_count'
LANGUAGE C STABLE STRICT;

--
-- Per-database accessor functions.
--
CREATE FUNCTION pg_stat_get_db_vacuum_tuples_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_tuples_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_dead_tuples(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_dead_tuples'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_deleted(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_deleted'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_bytes_removed(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_bytes_removed'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_dead_pages(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_dead_pages'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_frozen(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_frozen'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_pages_all_visible(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_pages_all_visible'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_stat_get_db_vacuum_freeze_age_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_freeze_age_count'
LANGUAGE C STABLE STRICT;

-- Access to counters stored in ordinary relation and database statistics.
CREATE FUNCTION pg_stat_get_frozen_page_marks_cleared(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_frozen_page_marks_cleared'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_visible_page_marks_cleared(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_visible_page_marks_cleared'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_vacuum_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_vacuum_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_autovacuum_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_autovacuum_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_vacuum_delay_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_vacuum_delay_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_autovacuum_delay_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_autovacuum_delay_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_analyze_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_analyze_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_total_autoanalyze_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_total_autoanalyze_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_vacuum_failsafe_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_vacuum_failsafe_count'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_frozen_page_marks_cleared(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_frozen_page_marks_cleared'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_visible_page_marks_cleared(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_visible_page_marks_cleared'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_total_vacuum_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_db_total_vacuum_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_total_autovacuum_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_db_total_autovacuum_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_total_vacuum_delay_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_db_total_vacuum_delay_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_total_autovacuum_delay_time(oid) RETURNS double precision
AS 'MODULE_PATHNAME', 'pg_stat_get_db_total_autovacuum_delay_time'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_vacuum_failsafe_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_failsafe_count'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE FUNCTION pg_stat_get_db_vacuum_interrupt_count(oid) RETURNS int8
AS 'MODULE_PATHNAME', 'pg_stat_get_db_vacuum_interrupt_count'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

-- Local statistics, in milliseconds for timing columns.
CREATE VIEW pg_stat_vacuum_tables AS
    SELECT
        c.oid AS relid,
        n.nspname AS schemaname,
        c.relname AS relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(c.oid) AS tuples_deleted,
        @extschema@.pg_stat_get_vacuum_dead_tuples(c.oid) AS dead_tuples,
        @extschema@.pg_stat_get_vacuum_pages_deleted(c.oid) AS pages_deleted,
        @extschema@.pg_stat_get_vacuum_bytes_removed(c.oid) AS bytes_removed,
        @extschema@.pg_stat_get_vacuum_dead_pages(c.oid) AS dead_pages,
        @extschema@.pg_stat_get_vacuum_pages_frozen(c.oid) AS pages_frozen,
        @extschema@.pg_stat_get_vacuum_pages_all_visible(c.oid) AS pages_all_visible,
        @extschema@.pg_stat_get_frozen_page_marks_cleared(c.oid) AS frozen_page_marks_cleared,
        @extschema@.pg_stat_get_visible_page_marks_cleared(c.oid) AS visible_page_marks_cleared,
        @extschema@.pg_stat_get_vacuum_freeze_age_count(c.oid) AS freeze_age_vacuum_count,
        @extschema@.pg_stat_get_vacuum_failsafe_count(c.oid) AS vacuum_failsafe_count,
        @extschema@.pg_stat_get_total_vacuum_time(c.oid) AS total_vacuum_time,
        @extschema@.pg_stat_get_total_autovacuum_time(c.oid) AS total_autovacuum_time,
        @extschema@.pg_stat_get_total_vacuum_delay_time(c.oid) AS total_vacuum_delay_time,
        @extschema@.pg_stat_get_total_autovacuum_delay_time(c.oid) AS total_autovacuum_delay_time,
        @extschema@.pg_stat_get_total_analyze_time(c.oid) AS total_analyze_time,
        @extschema@.pg_stat_get_total_autoanalyze_time(c.oid) AS total_autoanalyze_time,
        @extschema@.pg_stat_get_vacuum_total_file_segs(c.oid) AS total_file_segs
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm');

CREATE VIEW pg_stat_vacuum_indexes AS
    SELECT
        c.oid AS relid,
        i.oid AS indexrelid,
        n.nspname AS schemaname,
        c.relname AS relname,
        i.relname AS indexrelname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(i.oid) AS tuples_deleted,
        @extschema@.pg_stat_get_vacuum_dead_tuples(i.oid) AS dead_tuples,
        @extschema@.pg_stat_get_vacuum_pages_deleted(i.oid) AS pages_deleted,
        @extschema@.pg_stat_get_vacuum_bytes_removed(i.oid) AS bytes_removed,
        @extschema@.pg_stat_get_vacuum_dead_pages(i.oid) AS dead_pages,
        @extschema@.pg_stat_get_vacuum_pages_frozen(i.oid) AS pages_frozen,
        @extschema@.pg_stat_get_vacuum_pages_all_visible(i.oid) AS pages_all_visible,
        @extschema@.pg_stat_get_frozen_page_marks_cleared(i.oid) AS frozen_page_marks_cleared,
        @extschema@.pg_stat_get_visible_page_marks_cleared(i.oid) AS visible_page_marks_cleared,
        @extschema@.pg_stat_get_vacuum_freeze_age_count(i.oid) AS freeze_age_vacuum_count,
        @extschema@.pg_stat_get_vacuum_failsafe_count(i.oid) AS vacuum_failsafe_count,
        @extschema@.pg_stat_get_total_vacuum_time(i.oid) AS total_vacuum_time,
        @extschema@.pg_stat_get_total_autovacuum_time(i.oid) AS total_autovacuum_time,
        @extschema@.pg_stat_get_total_vacuum_delay_time(i.oid) AS total_vacuum_delay_time,
        @extschema@.pg_stat_get_total_autovacuum_delay_time(i.oid) AS total_autovacuum_delay_time
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i';

CREATE VIEW pg_stat_vacuum_database AS
    SELECT
        d.oid AS datid,
        d.datname AS datname,
        @extschema@.pg_stat_get_db_vacuum_tuples_deleted(d.oid) AS tuples_deleted,
        @extschema@.pg_stat_get_db_vacuum_dead_tuples(d.oid) AS dead_tuples,
        @extschema@.pg_stat_get_db_vacuum_pages_deleted(d.oid) AS pages_deleted,
        @extschema@.pg_stat_get_db_vacuum_bytes_removed(d.oid) AS bytes_removed,
        @extschema@.pg_stat_get_db_vacuum_dead_pages(d.oid) AS dead_pages,
        @extschema@.pg_stat_get_db_vacuum_pages_frozen(d.oid) AS pages_frozen,
        @extschema@.pg_stat_get_db_vacuum_pages_all_visible(d.oid) AS pages_all_visible,
        @extschema@.pg_stat_get_db_frozen_page_marks_cleared(d.oid) AS frozen_page_marks_cleared,
        @extschema@.pg_stat_get_db_visible_page_marks_cleared(d.oid) AS visible_page_marks_cleared,
        @extschema@.pg_stat_get_db_vacuum_freeze_age_count(d.oid) AS freeze_age_vacuum_count,
        @extschema@.pg_stat_get_db_vacuum_failsafe_count(d.oid) AS vacuum_failsafe_count,
        @extschema@.pg_stat_get_db_total_vacuum_time(d.oid) AS total_vacuum_time,
        @extschema@.pg_stat_get_db_total_autovacuum_time(d.oid) AS total_autovacuum_time,
        @extschema@.pg_stat_get_db_total_vacuum_delay_time(d.oid) AS total_vacuum_delay_time,
        @extschema@.pg_stat_get_db_total_autovacuum_delay_time(d.oid) AS total_autovacuum_delay_time,
        @extschema@.pg_stat_get_db_vacuum_interrupt_count(d.oid) AS vacuum_interrupt_count
    FROM (
        SELECT 0::oid AS oid, NULL::name AS datname
        UNION ALL
        SELECT oid, datname FROM pg_catalog.pg_database
    ) d;

-- Cluster views execute on the coordinator and all segments. The bodies
-- access catalogs directly because segment functions cannot scan local views.
-- Utility sessions return their local rows once, without a segment branch.
CREATE FUNCTION gp_stat_get_coordinator_vacuum_tables() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid,
        n.nspname,
        c.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_bytes_removed(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(c.oid),
        @extschema@.pg_stat_get_frozen_page_marks_cleared(c.oid),
        @extschema@.pg_stat_get_visible_page_marks_cleared(c.oid),
        @extschema@.pg_stat_get_vacuum_freeze_age_count(c.oid),
        @extschema@.pg_stat_get_vacuum_failsafe_count(c.oid),
        @extschema@.pg_stat_get_total_vacuum_time(c.oid),
        @extschema@.pg_stat_get_total_autovacuum_time(c.oid),
        @extschema@.pg_stat_get_total_vacuum_delay_time(c.oid),
        @extschema@.pg_stat_get_total_autovacuum_delay_time(c.oid),
        @extschema@.pg_stat_get_total_analyze_time(c.oid),
        @extschema@.pg_stat_get_total_autoanalyze_time(c.oid),
        @extschema@.pg_stat_get_vacuum_total_file_segs(c.oid)
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm')
$$
LANGUAGE SQL EXECUTE ON COORDINATOR;

CREATE FUNCTION gp_stat_get_segment_vacuum_tables() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid,
        n.nspname,
        c.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(c.oid),
        @extschema@.pg_stat_get_vacuum_bytes_removed(c.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(c.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(c.oid),
        @extschema@.pg_stat_get_frozen_page_marks_cleared(c.oid),
        @extschema@.pg_stat_get_visible_page_marks_cleared(c.oid),
        @extschema@.pg_stat_get_vacuum_freeze_age_count(c.oid),
        @extschema@.pg_stat_get_vacuum_failsafe_count(c.oid),
        @extschema@.pg_stat_get_total_vacuum_time(c.oid),
        @extschema@.pg_stat_get_total_autovacuum_time(c.oid),
        @extschema@.pg_stat_get_total_vacuum_delay_time(c.oid),
        @extschema@.pg_stat_get_total_autovacuum_delay_time(c.oid),
        @extschema@.pg_stat_get_total_analyze_time(c.oid),
        @extschema@.pg_stat_get_total_autoanalyze_time(c.oid),
        @extschema@.pg_stat_get_vacuum_total_file_segs(c.oid)
    FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm')
        AND pg_catalog.current_setting('gp_role') <> 'utility'
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_tables AS
    SELECT * FROM gp_stat_get_coordinator_vacuum_tables() AS T
        (gp_segment_id int,
         relid oid,
         schemaname name,
         relname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision,
         total_analyze_time double precision,
         total_autoanalyze_time double precision,
         total_file_segs int8)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_tables() AS T
        (gp_segment_id int,
         relid oid,
         schemaname name,
         relname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision,
         total_analyze_time double precision,
         total_autoanalyze_time double precision,
         total_file_segs int8);

CREATE FUNCTION gp_stat_get_coordinator_vacuum_indexes() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid,
        i.oid,
        n.nspname,
        c.relname,
        i.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_bytes_removed(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(i.oid),
        @extschema@.pg_stat_get_frozen_page_marks_cleared(i.oid),
        @extschema@.pg_stat_get_visible_page_marks_cleared(i.oid),
        @extschema@.pg_stat_get_vacuum_freeze_age_count(i.oid),
        @extschema@.pg_stat_get_vacuum_failsafe_count(i.oid),
        @extschema@.pg_stat_get_total_vacuum_time(i.oid),
        @extschema@.pg_stat_get_total_autovacuum_time(i.oid),
        @extschema@.pg_stat_get_total_vacuum_delay_time(i.oid),
        @extschema@.pg_stat_get_total_autovacuum_delay_time(i.oid)
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i'
$$
LANGUAGE SQL EXECUTE ON COORDINATOR;

CREATE FUNCTION gp_stat_get_segment_vacuum_indexes() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        c.oid,
        i.oid,
        n.nspname,
        c.relname,
        i.relname,
        @extschema@.pg_stat_get_vacuum_tuples_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_tuples(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_deleted(i.oid),
        @extschema@.pg_stat_get_vacuum_bytes_removed(i.oid),
        @extschema@.pg_stat_get_vacuum_dead_pages(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_frozen(i.oid),
        @extschema@.pg_stat_get_vacuum_pages_all_visible(i.oid),
        @extschema@.pg_stat_get_frozen_page_marks_cleared(i.oid),
        @extschema@.pg_stat_get_visible_page_marks_cleared(i.oid),
        @extschema@.pg_stat_get_vacuum_freeze_age_count(i.oid),
        @extschema@.pg_stat_get_vacuum_failsafe_count(i.oid),
        @extschema@.pg_stat_get_total_vacuum_time(i.oid),
        @extschema@.pg_stat_get_total_autovacuum_time(i.oid),
        @extschema@.pg_stat_get_total_vacuum_delay_time(i.oid),
        @extschema@.pg_stat_get_total_autovacuum_delay_time(i.oid)
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index x ON c.oid = x.indrelid
        JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 't', 'm') AND i.relkind = 'i'
        AND pg_catalog.current_setting('gp_role') <> 'utility'
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_indexes AS
    SELECT * FROM gp_stat_get_coordinator_vacuum_indexes() AS T
        (gp_segment_id int,
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_indexes() AS T
        (gp_segment_id int,
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision);

CREATE FUNCTION gp_stat_get_coordinator_vacuum_database() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        d.oid,
        d.datname,
        @extschema@.pg_stat_get_db_vacuum_tuples_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_tuples(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_bytes_removed(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_frozen(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_all_visible(d.oid),
        @extschema@.pg_stat_get_db_frozen_page_marks_cleared(d.oid),
        @extschema@.pg_stat_get_db_visible_page_marks_cleared(d.oid),
        @extschema@.pg_stat_get_db_vacuum_freeze_age_count(d.oid),
        @extschema@.pg_stat_get_db_vacuum_failsafe_count(d.oid),
        @extschema@.pg_stat_get_db_total_vacuum_time(d.oid),
        @extschema@.pg_stat_get_db_total_autovacuum_time(d.oid),
        @extschema@.pg_stat_get_db_total_vacuum_delay_time(d.oid),
        @extschema@.pg_stat_get_db_total_autovacuum_delay_time(d.oid),
        @extschema@.pg_stat_get_db_vacuum_interrupt_count(d.oid)
    FROM (
        SELECT 0::oid AS oid, NULL::name AS datname
        UNION ALL
        SELECT oid, datname FROM pg_catalog.pg_database
    ) d
$$
LANGUAGE SQL EXECUTE ON COORDINATOR;

CREATE FUNCTION gp_stat_get_segment_vacuum_database() RETURNS SETOF RECORD AS
$$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id,
        d.oid,
        d.datname,
        @extschema@.pg_stat_get_db_vacuum_tuples_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_tuples(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_deleted(d.oid),
        @extschema@.pg_stat_get_db_vacuum_bytes_removed(d.oid),
        @extschema@.pg_stat_get_db_vacuum_dead_pages(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_frozen(d.oid),
        @extschema@.pg_stat_get_db_vacuum_pages_all_visible(d.oid),
        @extschema@.pg_stat_get_db_frozen_page_marks_cleared(d.oid),
        @extschema@.pg_stat_get_db_visible_page_marks_cleared(d.oid),
        @extschema@.pg_stat_get_db_vacuum_freeze_age_count(d.oid),
        @extschema@.pg_stat_get_db_vacuum_failsafe_count(d.oid),
        @extschema@.pg_stat_get_db_total_vacuum_time(d.oid),
        @extschema@.pg_stat_get_db_total_autovacuum_time(d.oid),
        @extschema@.pg_stat_get_db_total_vacuum_delay_time(d.oid),
        @extschema@.pg_stat_get_db_total_autovacuum_delay_time(d.oid),
        @extschema@.pg_stat_get_db_vacuum_interrupt_count(d.oid)
    FROM (
        SELECT 0::oid AS oid, NULL::name AS datname
        UNION ALL
        SELECT oid, datname FROM pg_catalog.pg_database
    ) d
        WHERE pg_catalog.current_setting('gp_role') <> 'utility'
$$
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE VIEW gp_stat_vacuum_database AS
    SELECT * FROM gp_stat_get_coordinator_vacuum_database() AS T
        (gp_segment_id int,
         datid oid,
         datname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision,
         vacuum_interrupt_count int8)
    UNION ALL
    SELECT * FROM gp_stat_get_segment_vacuum_database() AS T
        (gp_segment_id int,
         datid oid,
         datname name,
         tuples_deleted int8,
         dead_tuples int8,
         pages_deleted int8,
         bytes_removed int8,
         dead_pages int8,
         pages_frozen int8,
         pages_all_visible int8,
         frozen_page_marks_cleared int8,
         visible_page_marks_cleared int8,
         freeze_age_vacuum_count int8,
         vacuum_failsafe_count int8,
         total_vacuum_time double precision,
         total_autovacuum_time double precision,
         total_vacuum_delay_time double precision,
         total_autovacuum_delay_time double precision,
         vacuum_interrupt_count int8);

GRANT SELECT ON pg_stat_vacuum_tables, pg_stat_vacuum_indexes,
    pg_stat_vacuum_database, gp_stat_vacuum_tables,
    gp_stat_vacuum_indexes, gp_stat_vacuum_database TO PUBLIC;
