/* gp_relsizes_stats--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_relsizes_stats" to load this file. \quit

CREATE OR REPLACE VIEW relsizes_stats_schema.table_files AS
    WITH part_oids AS (
        SELECT n.nspname, c1.relname, c1.oid, true own_oid
        FROM pg_class c1
        JOIN pg_namespace n ON c1.relnamespace = n.oid
        WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')
        UNION ALL
        SELECT n.nspname, c1.relname, c2.oid, false own_oid
        FROM pg_class c1
        JOIN pg_namespace n ON c1.relnamespace = n.oid
        JOIN pg_partition pp ON c1.oid = pp.parrelid
        JOIN pg_partition_rule pr ON pp.oid = pr.paroid
        JOIN pg_class c2 ON pr.parchildrelid = c2.oid
        WHERE c1.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')
    ),
    table_oids AS (
        SELECT po.nspname, po.relname, po.oid, po.own_oid, 'main' AS kind
            FROM part_oids po
        UNION ALL
        SELECT po.nspname, po.relname, t.reltoastrelid, po.own_oid, 'toast' AS kind
            FROM part_oids po
            JOIN pg_class t ON po.oid = t.oid
            WHERE t.reltoastrelid > 0
        UNION ALL
        SELECT po.nspname, po.relname, ti.indexrelid, po.own_oid, 'toast_idx' AS kind
            FROM part_oids po
            JOIN pg_class t ON po.oid = t.oid
            JOIN pg_index ti ON t.reltoastrelid = ti.indrelid
            WHERE t.reltoastrelid > 0
        UNION ALL
        SELECT po.nspname, po.relname, ao.segrelid, po.own_oid, 'ao' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
        UNION ALL
        SELECT po.nspname, po.relname, ao.visimaprelid, po.own_oid, 'ao_vm' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
        UNION ALL
        SELECT po.nspname, po.relname, ao.visimapidxid, po.own_oid, 'ao_vm_idx' AS kind
            FROM part_oids po
            JOIN pg_appendonly ao ON po.oid = ao.relid
    )
    SELECT table_oids.nspname, table_oids.relname, m.segment, m.relfilenode, fs.filepath, kind, size, mtime, table_oids.own_oid own_file
    FROM table_oids
    JOIN relsizes_stats_schema.segment_file_map m ON table_oids.oid = m.reloid
    JOIN relsizes_stats_schema.segment_file_sizes fs ON m.segment = fs.segment AND m.relfilenode = fs.relfilenode;

CREATE OR REPLACE VIEW relsizes_stats_schema.namespace_sizes AS
    SELECT nspname, sum(size) AS size FROM relsizes_stats_schema.table_files
    WHERE own_file
    GROUP BY nspname;
