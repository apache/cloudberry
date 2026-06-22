-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relsizes_stats" to load this file. \quit


-- CREATE TABLE IF NOT EXISTS ... (....) DISTRIBUTED BY ...
CREATE SCHEMA IF NOT EXISTS relsizes_stats_schema;

-- create table
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.segment_file_map
    (segment INTEGER, reloid OID, relfilenode OID)
    WITH (appendonly=true) DISTRIBUTED RANDOMLY;
-- create table
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.segment_file_sizes
    (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
    WITH (appendonly=true) DISTRIBUTED RANDOMLY;
-- create table for backup info
CREATE TABLE IF NOT EXISTS relsizes_stats_schema.table_sizes_history
    (insert_date date NOT NULL, nspname text NOT NULL, relname text NOT NULL, size bigint NOT NULL, mtime timestamp NOT NULL)
    DISTRIBUTED RANDOMLY;


CREATE OR REPLACE VIEW relsizes_stats_schema.table_files AS
    /*
     * Recursive enumeration of all relations together with their partition
     * trees, preserving the original GP6 view semantics:
     *
     *   - Every relation appears as a "self" row with own_oid = true
     *     (relname carries its own name).
     *   - Additionally, for every relation that is a "root" of a partition
     *     tree (has no parent in pg_inherits), we walk the entire tree of
     *     descendants and add a row per descendant with own_oid = false;
     *     the descendant inherits the relname of the root (so SUMs in
     *     table_sizes / namespace_sizes attribute leaf data to the root
     *     partitioned table).
     *
     *   - Intermediate partitioned tables (which are themselves children
     *     of another partitioned table) only contribute a self-row; they
     *     do NOT roll up their own leaves — those are already counted
     *     under the root.
     *
     * On Cloudberry / PG14 partition root and intermediate partitioned
     * tables have relfilenode = 0 — no physical storage. To keep them
     * visible in the view (with size = 0) we LEFT JOIN segment_file_sizes
     * instead of using INNER JOIN.
     */
    WITH RECURSIVE all_rels AS (
        SELECT n.nspname, c.relname, c.oid
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r', 'p')
          AND c.reltablespace != (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_global')
    ),
    /* "Roots" — relations that do not inherit from any table/partitioned table. */
    roots AS (
        SELECT ar.nspname, ar.relname, ar.oid
        FROM all_rels ar
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_inherits pi
            JOIN pg_class pc ON pi.inhparent = pc.oid
            WHERE pi.inhrelid = ar.oid
              AND pc.relkind IN ('r', 'p')
        )
    ),
    /* Walk the entire partition tree starting from each root. */
    descendants AS (
        SELECT r.nspname, r.relname AS root_relname, r.oid AS cur_oid, 0 AS depth
        FROM roots r
        UNION ALL
        SELECT d.nspname, d.root_relname, c2.oid, d.depth + 1
        FROM descendants d
        JOIN pg_inherits pi ON d.cur_oid = pi.inhparent
        JOIN pg_class c2 ON pi.inhrelid = c2.oid
        WHERE c2.relkind IN ('r', 'p')
    ),
    part_oids AS (
        /* Self-row for every relation. */
        SELECT nspname, relname, oid, true AS own_oid
        FROM all_rels
        UNION ALL
        /* Descendant rows for every root (excluding the root itself). */
        SELECT nspname, root_relname AS relname, cur_oid AS oid, false AS own_oid
        FROM descendants
        WHERE depth > 0
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
    SELECT table_oids.nspname,
           table_oids.relname,
           m.segment,
           m.relfilenode,
           fs.filepath,
           kind,
           COALESCE(fs.size,  0) AS size,
           COALESCE(fs.mtime, 0) AS mtime,
           table_oids.own_oid    AS own_file
    FROM table_oids
    JOIN relsizes_stats_schema.segment_file_map m
        ON table_oids.oid = m.reloid
    /*
     * LEFT JOIN, not INNER JOIN: partitioned (root + intermediate) tables
     * on Cloudberry have no physical file, so segment_file_sizes does not contain
     * a matching row.  We still want to surface them with size = 0.
     */
    LEFT JOIN relsizes_stats_schema.segment_file_sizes fs
        ON m.segment = fs.segment AND m.relfilenode = fs.relfilenode;
CREATE OR REPLACE VIEW relsizes_stats_schema.table_sizes AS
    SELECT nspname, relname, sum(size) AS size, to_timestamp(MAX(mtime)) AS mtime FROM relsizes_stats_schema.table_files
    GROUP BY nspname, relname;
CREATE OR REPLACE VIEW relsizes_stats_schema.namespace_sizes AS
    SELECT nspname, sum(size) AS size FROM relsizes_stats_schema.table_files
    WHERE own_file
    GROUP BY nspname;
-- Here go any C or PL/SQL functions, table or view definitions etc
-- for example:

CREATE FUNCTION relsizes_stats_schema.get_stats_for_database(dboid OID, fast BOOL)
RETURNS TABLE (segment INTEGER, relfilenode OID, filepath TEXT, size BIGINT, mtime BIGINT)
AS 'MODULE_PATHNAME', 'get_stats_for_database'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION relsizes_stats_schema.relsizes_collect_stats_once()
RETURNS void
AS 'MODULE_PATHNAME', 'relsizes_collect_stats_once'
LANGUAGE C STRICT;


DO $$
BEGIN
    EXECUTE 'GRANT USAGE ON SCHEMA relsizes_stats_schema TO "' || session_user || '" WITH GRANT OPTION';
    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA relsizes_stats_schema TO "' || session_user || '" WITH GRANT OPTION';
END
$$;
