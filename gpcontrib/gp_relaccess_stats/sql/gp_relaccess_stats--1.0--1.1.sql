/* gp_relaccess_stats--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION gp_relaccess_stats UPDATE TO '1.1'" to load this file. \quit

DROP VIEW relaccess.relaccess_stats_root_tables_aggregated;

ALTER TABLE relaccess.relaccess_stats ALTER COLUMN n_select_queries TYPE int8;
ALTER TABLE relaccess.relaccess_stats ALTER COLUMN n_insert_queries TYPE int8;
ALTER TABLE relaccess.relaccess_stats ALTER COLUMN n_update_queries TYPE int8;
ALTER TABLE relaccess.relaccess_stats ALTER COLUMN n_delete_queries TYPE int8;
ALTER TABLE relaccess.relaccess_stats ALTER COLUMN n_truncate_queries TYPE int8;

-- This utility view shows **ONLY** stats on **EXISTING** partitioned tables in aggregated form
CREATE VIEW relaccess.relaccess_stats_root_tables_aggregated AS (
    WITH RECURSIVE parents AS (
        SELECT inhrelid AS child, inhparent AS parent FROM pg_inherits
        UNION ALL
        SELECT prev.child, next.inhparent AS parent FROM parents AS prev JOIN pg_inherits AS next ON prev.parent = next.inhrelid
    ), part_to_root_mapping AS (
        SELECT DISTINCT child AS partid, min(parent) OVER (partition BY child) AS rootid FROM parents
    ), parts_including_roots AS (
        SELECT rootid as partid, rootid FROM (SELECT DISTINCT rootid FROM part_to_root_mapping) AS p
        UNION
        SELECT * FROM part_to_root_mapping
    ), with_root_id AS (
        SELECT part_tbl.rootid, stats.* FROM relaccess.relaccess_stats stats JOIN parts_including_roots part_tbl ON (stats.relid = part_tbl.partid)
    ), without_last_user AS (
        SELECT rootid AS relid,
            rootid::regclass::text AS relname,
            max(last_read) AS last_read,
            max(last_write) AS last_write,
            sum(n_select_queries) AS n_select_queries,
            sum(n_insert_queries) AS n_insert_queries,
            sum(n_update_queries) AS n_update_queries,
            sum(n_delete_queries) AS n_delete_queries,
            sum(n_truncate_queries) AS n_truncate_queries
        FROM with_root_id outer_tbl GROUP BY rootid
    )
    SELECT relid,
        relname,
        (SELECT last_reader_id FROM with_root_id w WHERE w.rootid = wo.relid AND wo.last_read = w.last_read LIMIT 1) AS last_reader_id,
        (SELECT last_writer_id FROM with_root_id w WHERE w.rootid = wo.relid AND wo.last_write = w.last_write LIMIT 1) AS last_writer_id,
        last_read,
        last_write,
        n_select_queries,
        n_insert_queries,
        n_update_queries,
        n_delete_queries,
        n_truncate_queries
    FROM without_last_user wo
);
