-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_check_functions" to load this file. \quit

CREATE OR REPLACE FUNCTION get_tablespace_version_directory_name()
RETURNS text
AS '$libdir/gp_check_functions'
LANGUAGE C;

--------------------------------------------------------------------------------
-- @function:
--        __get_ao_segno_list
--
-- @in:
--
-- @out:
--        oid - relation oid
--        int - segment number
--        eof - eof of the segment file
--
-- @doc:
--        UDF to retrieve AO segment file numbers for each ao_row table
--
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION __get_ao_segno_list()
RETURNS TABLE (relid oid, segno int, eof bigint) AS
$$
DECLARE
  table_name text;
  rec record;
  cur refcursor;
  row record;
BEGIN
  -- iterate over the aoseg relations
  FOR rec IN SELECT tc.oid tableoid, tc.relname, ns.nspname
             FROM pg_appendonly a 
             JOIN pg_class tc ON a.relid = tc.oid 
             JOIN pg_namespace ns ON tc.relnamespace = ns.oid
             WHERE tc.relstorage = 'a' 
  LOOP
    table_name := rec.relname;
    -- Fetch and return each row from the aoseg table
    BEGIN
      OPEN cur FOR EXECUTE format('SELECT segno, eof '
                                  'FROM gp_toolkit.__gp_aoseg(''%I.%I'') ',
                                   rec.nspname, rec.relname);
      SELECT rec.tableoid INTO relid;
      LOOP
        FETCH cur INTO row;
        EXIT WHEN NOT FOUND;
        segno := row.segno;
        eof := row.eof;
        IF segno <> 0 THEN -- there's no '.0' file, it means the file w/o extension
          RETURN NEXT;
        END IF;
      END LOOP;
      CLOSE cur;
    EXCEPTION
      -- If failed to open the aoseg table (e.g. the table itself is missing), continue
      WHEN OTHERS THEN
      RAISE WARNING 'Failed to get aoseg info for %: %', table_name, SQLERRM;
    END;
  END LOOP;
  RETURN;
END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION __get_ao_segno_list() TO public;

--------------------------------------------------------------------------------
-- @function:
--        __get_aoco_segno_list
--
-- @in:
--
-- @out:
--        oid - relation oid
--        int - segment number
--        eof - eof of the segment file
--
-- @doc:
--        UDF to retrieve AOCO segment file numbers for each ao_column table
--
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION __get_aoco_segno_list()
RETURNS TABLE (relid oid, segno int, eof bigint) AS
$$
DECLARE
  table_name text;
  rec record;
  cur refcursor;
  row record;
BEGIN
  -- iterate over the aocoseg relations
  FOR rec IN SELECT tc.oid tableoid, tc.relname, ns.nspname
             FROM pg_appendonly a
             JOIN pg_class tc ON a.relid = tc.oid
             JOIN pg_namespace ns ON tc.relnamespace = ns.oid
             WHERE tc.relstorage = 'c'
  LOOP
    table_name := rec.relname;
    -- Fetch and return each extended segno corresponding to attnum and segno in the aocoseg table
    BEGIN
      OPEN cur FOR EXECUTE format('SELECT physical_segno as segno, eof '
                                  'FROM gp_toolkit.__gp_aocsseg(''%I.%I'') ',
                                   rec.nspname, rec.relname);
      SELECT rec.tableoid INTO relid;
      LOOP
        FETCH cur INTO row;
        EXIT WHEN NOT FOUND;
        segno := row.segno;
        eof := row.eof;
        IF segno <> 0 THEN -- there's no '.0' file, it means the file w/o extension
          RETURN NEXT;
        END IF;
      END LOOP;
      CLOSE cur;
    EXCEPTION
      -- If failed to open the aocoseg table (e.g. the table itself is missing), continue
      WHEN OTHERS THEN
      RAISE WARNING 'Failed to get aocsseg info for %: %', table_name, SQLERRM;
    END;
  END LOOP;
  RETURN;
END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION __get_aoco_segno_list() TO public;

--------------------------------------------------------------------------------
-- @view:
--        __get_exist_files
--
-- @doc:
--        Retrieve a list of all existing data files in the default
--        and user tablespaces.
--
--------------------------------------------------------------------------------
-- return the list of existing files in the database
CREATE VIEW __get_exist_files AS
WITH Tablespaces AS (
-- 1. The default tablespace
    SELECT t.oid AS tablespace, 'base/' || d.oid::text AS dirname
    FROM pg_tablespace t, pg_database d 
    WHERE t.spcname = 'pg_default' AND d.datname = current_database()
    UNION
-- 2. The global tablespace
    SELECT oid AS tablespace, 'global' AS dirname
    FROM pg_tablespace
    WHERE spcname = 'pg_global'
    UNION
-- 3. The user-defined tablespaces
    SELECT ts.oid AS tablespace,
       'pg_tblspc/' || ts.oid::text || '/' || get_tablespace_version_directory_name() || '/' ||
         (SELECT d.oid::text FROM pg_database d WHERE d.datname = current_database()) AS dirname
    FROM pg_tablespace ts
    WHERE ts.spcname NOT IN ('pg_default', 'pg_global')
)
SELECT tablespace, files.filename, dirname || '/' || files.filename AS filepath
FROM Tablespaces, pg_ls_dir(dirname, true, false) AS files(filename);

GRANT SELECT ON __get_exist_files TO public;

--------------------------------------------------------------------------------
-- @view:
--        __get_expect_files
--
-- @doc:
--        Retrieve a list of expected data files in the database,
--        using the knowledge from catalogs. This does not include
--        any extended data files, nor does it include external,
--        foreign or virtual tables.
--
--------------------------------------------------------------------------------
CREATE VIEW __get_expect_files AS
SELECT CASE WHEN s.reltablespace = 0 THEN
            (SELECT dattablespace FROM pg_database WHERE datname = current_database())
            ELSE s.reltablespace END AS tablespace, 
       s.relname, s.relstorage,
       (CASE WHEN s.relfilenode != 0 THEN s.relfilenode ELSE pg_relation_filenode(s.oid) END)::text AS filename
FROM pg_class s
WHERE s.relstorage NOT IN ('x', 'v', 'f');

GRANT SELECT ON __get_expect_files TO public;

--------------------------------------------------------------------------------
-- @view:
--        __get_expect_files_ext
--
-- @doc:
--        Retrieve a list of expected data files in the database,
--        using the knowledge from catalogs. This includes all
--        the extended data files for AO/CO tables, nor does it
--        include external, foreign or virtual tables.
--        Also ignore AO segments w/ eof=0. They might be created just for
--        modcount whereas no data has ever been inserted to the seg.
--        Or, they could be created when a seg has only aborted rows.
--        In both cases, we can ignore these segs, because no matter
--        whether the data files exist or not, the rest of the system
--        can handle them gracefully.
--
--------------------------------------------------------------------------------
CREATE VIEW __get_expect_files_ext AS
WITH class_info AS (
  SELECT oid, relname, relstorage, relfilenode,
    CASE WHEN reltablespace = 0 THEN 
      (SELECT dattablespace FROM pg_database WHERE datname = current_database())
      ELSE reltablespace END AS tablespace
  FROM pg_class
)
SELECT tablespace, relname, relstorage,
       (CASE WHEN relfilenode != 0 THEN relfilenode ELSE pg_relation_filenode(oid) END)::text AS filename
FROM class_info
WHERE relstorage NOT IN ('x', 'v', 'f')
UNION
-- AO extended files
SELECT c.tablespace, c.relname, c.relstorage,
       format(c.relfilenode::text || '.' || s.segno::text) AS filename
FROM __get_ao_segno_list() s
JOIN class_info c ON s.relid = c.oid
WHERE s.eof >0 AND c.relstorage NOT IN ('x', 'v', 'f')
UNION
-- CO extended files
SELECT c.tablespace, c.relname, c.relstorage,
       format(c.relfilenode::text || '.' || s.segno::text) AS filename
FROM __get_aoco_segno_list() s
JOIN class_info c ON s.relid = c.oid
WHERE s.eof > 0 AND c.relstorage NOT IN ('x', 'v', 'f');

GRANT SELECT ON __get_expect_files_ext TO public;

--------------------------------------------------------------------------------
-- @view:
--        __check_orphaned_files
--
-- @doc:
--        Check orphaned data files on default and user tablespaces.
--        A file is considered orphaned if its main relfilenode is not expected
--        to exist. For example, '12345.1' is an orphaned file if there is no
--        table has relfilenode=12345, but not otherwise.
--        Therefore, this view counts for file extension as well and we do not
--        need a "_ext" view like the missing file view.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW __check_orphaned_files AS
SELECT f1.tablespace, f1.filename, f1.filepath, pg_catalog.gp_execution_segment() AS gp_segment_id
from __get_exist_files f1
LEFT JOIN __get_expect_files f2
ON f1.tablespace = f2.tablespace AND substring(f1.filename from '[0-9]+') = f2.filename
WHERE f2.tablespace IS NULL
  AND f1.filename SIMILAR TO '[0-9]+(\.)?(\_)?%';


GRANT SELECT ON __check_orphaned_files TO public;

--------------------------------------------------------------------------------
-- @function:
--        __gp_check_orphaned_files_func
--
-- @in:
--
-- @out:
--        gp_segment_id int - segment content ID
--        tablespace oid - tablespace OID
--        filename text - name of the orphaned file
--        filepath text - relative path of the orphaned file in data directory
--
-- @doc:
--        (Internal UDF, shouldn't be exposed)
--        UDF to retrieve orphaned files and their paths.
--
--        Locks pg_class in shared mode and ensures no transactions are running.
--        May pause and wait if pg_class is already locked by another process.
--
--------------------------------------------------------------------------------
CREATE FUNCTION __gp_check_orphaned_files_func()
RETURNS TABLE (
    gp_segment_id int,
    tablespace oid,
    filename text,
    filepath text
)
SET search_path = pg_catalog
LANGUAGE plpgsql AS $$
BEGIN
    -- lock pg_class so that no one will be adding/altering relfilenodes
    -- NOTE: This operation may pause and wait if pg_class is already locked
    -- by another transaction! We intentionally do not use NOWAIT here.
    -- NOWAIT inside PL/pgSQL does not behave as expected: instead of throwing
    -- exception when the lock is unavailable, it silently waits.
    LOCK TABLE pg_class IN SHARE MODE;

    -- make sure no other active/idle transaction is running
    IF EXISTS (
        SELECT 1
        FROM (SELECT * from pg_stat_activity UNION ALL SELECT * FROM gp_dist_random('pg_stat_activity'))q
        WHERE
        sess_id <> -1
        AND sess_id <> current_setting('gp_session_id')::int -- Exclude the current session
        AND state <> 'idle' -- Exclude idle session like GDD
    ) THEN
        RAISE EXCEPTION 'There is a client session running on one or more segment. Aborting...';
    END IF;

    -- force checkpoint to make sure we do not include files that are normally pending delete
    CHECKPOINT;

    RETURN QUERY 
    SELECT v.gp_segment_id, v.tablespace, v.filename, v.filepath
    FROM gp_dist_random('@extschema@.__check_orphaned_files') v
    UNION ALL
    SELECT -1 AS gp_segment_id, v.tablespace, v.filename, v.filepath
    FROM @extschema@.__check_orphaned_files v;
END;
$$;

GRANT EXECUTE ON FUNCTION __gp_check_orphaned_files_func() TO public;

CREATE TYPE __gp_move_orphaned_files_pairs AS (
    spc_oid        oid,        -- tablespace oid
    target_path    text        -- directory to move orphaned files to
);

--------------------------------------------------------------------------------
-- @function:
--        __gp_move_orphaned_files
--
-- @in:
--        tablespace_location_pairs __gp_move_orphaned_files_pairs
--                array of pairs {tablespace_oid, target_path}. When
--                process_all_tablespaces = TRUE the array must contain
--                exactly one element; its tablespace_oid component is
--                ignored and its target_path component designates the
--                directory into which all orphaned files are moved.
--
--        process_all_tablespaces boolean default false
--                if TRUE, operate on orphaned files belonging to every
--                tablespace found on the segment; otherwise move files only
--                for the OIDs explicitly listed in tablespace_location_pairs.
--
-- @out:
--        gp_segment_id int - segment content ID
--        move_success bool - TRUE if the move succeeded
--        oldpath text - absolute pathname before the move
--        newpath text - absolute pathname after the move
--
-- @doc:
--        Internal helper invoked by the user‑facing wrappers.
--
--        Because the move relies on the rename syscall, the target
--        directory must reside on the same filesystem as the source file or
--        the operation will fail with "Invalid cross‑device link".
--
--        Locks pg_class in shared mode and ensures no transactions are running.
--        May pause and wait if pg_class is already locked by another process.
--
--------------------------------------------------------------------------------
CREATE FUNCTION __gp_move_orphaned_files(
        tablespace_location_pairs @extschema@.__gp_move_orphaned_files_pairs[],
        process_all_tablespaces bool DEFAULT false)
RETURNS TABLE (
    gp_segment_id int,
    move_success bool,
    oldpath text,
    newpath text
)
SET search_path = pg_catalog
LANGUAGE plpgsql AS $$
DECLARE
    pair @extschema@.__gp_move_orphaned_files_pairs;
BEGIN
    -- if process_all_tablespaces is true, tablespace_location_pairs should contain only one element
    -- with directory where we move the orphaned files from all tablespaces.
    IF process_all_tablespaces THEN
        IF array_length(tablespace_location_pairs, 1) <> 1 THEN
            RAISE EXCEPTION 'When process_all_tablespaces is true, tablespace_location_pairs must contain exactly one element.';
        END IF;
    END IF;

    -- lock pg_class so that no one will be adding/altering relfilenodes
    -- NOTE: This operation may pause and wait if pg_class is already locked
    -- by another transaction! We intentionally do not use NOWAIT here.
    -- NOWAIT inside PL/pgSQL does not behave as expected: instead of throwing
    -- exception when the lock is unavailable, it silently waits.
    LOCK TABLE pg_class IN SHARE MODE;

    -- make sure no other active/idle transaction is running
    IF EXISTS (
        SELECT 1
        FROM (SELECT * from pg_stat_activity UNION ALL SELECT * FROM gp_dist_random('pg_stat_activity'))q
        WHERE
        sess_id <> -1
        AND sess_id <> current_setting('gp_session_id')::int -- Exclude the current session
        AND state <> 'idle' -- Exclude idle session like GDD
    ) THEN
        RAISE EXCEPTION 'There is a client session running on one or more segment. Aborting...';
    END IF;

    -- force checkpoint to make sure we do not include files that are normally pending delete
    CHECKPOINT;

    -- process each (tablespace oid -> target_path) pair
    FOREACH pair IN ARRAY tablespace_location_pairs LOOP
        RETURN QUERY
        SELECT
            q.gp_segment_id,
            q.move_success,
            q.oldpath,
            q.newpath
        FROM 
        (
            SELECT s1.gp_segment_id, s1.oldpath, s1.newpath, pg_file_rename(s1.oldpath, s1.newpath, NULL) AS move_success
            FROM
            (
                SELECT
                    o.gp_segment_id,
                    s.setting || '/' || o.filepath AS oldpath,
                    pair.target_path || '/seg' || o.gp_segment_id::text || '_' || REPLACE(o.filepath, '/', '_') AS newpath
                FROM @extschema@.__check_orphaned_files o, pg_settings s
                WHERE s.name = 'data_directory' AND (o.tablespace = pair.spc_oid OR process_all_tablespaces)
            ) s1
            UNION ALL
            -- segments
            SELECT s2.gp_segment_id, s2.oldpath, s2.newpath, pg_file_rename(s2.oldpath, s2.newpath, NULL) AS move_success
            FROM
            (
                SELECT
                    o.gp_segment_id,
                    s.setting || '/' || o.filepath AS oldpath,
                    pair.target_path || '/seg' || o.gp_segment_id::text || '_' || REPLACE(o.filepath, '/', '_') AS newpath
                FROM gp_dist_random('@extschema@.__check_orphaned_files') o
                JOIN (SELECT gp_execution_segment() AS gp_segment_id, * FROM gp_dist_random('pg_settings')) s ON o.gp_segment_id = s.gp_segment_id
                WHERE s.name = 'data_directory' AND (o.tablespace = pair.spc_oid OR process_all_tablespaces)
            ) s2
        ) AS q
        ORDER BY q.gp_segment_id, q.oldpath;
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        DECLARE
            original_msg text;
            original_hint text;
            original_detail text;
            original_code text;
        BEGIN
            GET STACKED DIAGNOSTICS
                original_msg    = MESSAGE_TEXT,
                original_hint   = PG_EXCEPTION_HINT,
                original_detail = PG_EXCEPTION_DETAIL,
                original_code   = RETURNED_SQLSTATE;

            RAISE EXCEPTION '%', original_msg
            USING
                ERRCODE = original_code,
                DETAIL  = original_detail,
                HINT    = CASE WHEN original_hint IS NOT NULL AND original_hint <> ''
                              THEN original_hint || E'\n'
                              ELSE ''
                          END ||
                            'Operation was interrupted with an error. Only some or no files were renamed. ' ||
                            'Use gp_check_orphaned_files to see remaining files. Then call ' ||
                            'gp_move_orphaned_files_by_tablespace_location(tablespace_oid, /path) or ' ||
                            'gp_move_orphaned_files_by_tablespace_location(ARRAY[''{tablespace_oid, /path}'', ...]) ' ||
                            'to move files for specific tablespaces.';
        END;
END;
$$;

--------------------------------------------------------------------------------
-- @function:
--        gp_move_orphaned_files
--
-- @in:
--        target_location text
--                Absolute path of the directory to which all orphaned files
--                should be moved. The directory must be located on the same
--                filesystem as each orphaned file on the segment; otherwise
--                rename will fail.
--
-- @out:
--        gp_segment_id int - segment content ID
--        move_success bool - TRUE if the move succeeded
--        oldpath text - absolute pathname before the move
--        newpath text - absolute pathname after the move
--
-- @doc:
--        Moves all orphaned files detected on a segment
--        to a single target directory. If tablespaces reside on
--        multiple filesystems, use
--        gp_move_orphaned_files_by_tablespace_location instead.
--
--        Locks pg_class in shared mode and ensures no transactions are running.
--        May pause and wait if pg_class is already locked by another process.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_move_orphaned_files(target_location text) RETURNS TABLE (
    gp_segment_id int,
    move_success bool,
    oldpath text,
    newpath text
)
SET search_path = pg_catalog
LANGUAGE SQL AS $$
SELECT * FROM @extschema@.__gp_move_orphaned_files(
    ARRAY[ROW(0, target_location)::@extschema@.__gp_move_orphaned_files_pairs],
    process_all_tablespaces := TRUE::bool
);
$$;

--------------------------------------------------------------------------------
-- @function:
--        gp_move_orphaned_files_by_tablespace_location
--
-- @in:
--        tablespace_oid oid
--                OID of the tablespace whose orphaned files should be moved.
--        target_location text
--                Absolute path of the directory that will receive the files.
--                Must be on the same filesystem as the tablespace location.
--
-- @out:
--        gp_segment_id int - segment content ID
--        move_success bool - TRUE if the move succeeded
--        oldpath text - absolute pathname before the move
--        newpath text - absolute pathname after the move
--
-- @doc:
--        Moves orphaned files belonging to a specific tablespace
--        to the supplied target directory.
--
--        Locks pg_class in shared mode and ensures no transactions are running.
--        May pause and wait if pg_class is already locked by another process.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_move_orphaned_files_by_tablespace_location(
        tablespace_oid  oid,
        target_location text
)
RETURNS TABLE (
        gp_segment_id  int,
        move_success   bool,
        oldpath        text,
        newpath        text
)
SET search_path = pg_catalog
LANGUAGE plpgsql AS $$
BEGIN
    IF target_location IS NULL OR target_location = '' THEN
        RAISE EXCEPTION 'target_location cannot be NULL or empty';
    ELSIF NOT target_location LIKE '/%' THEN
        RAISE EXCEPTION 'target_location must be an absolute path, got: "%"', target_location;
    END IF;

    RETURN QUERY
    SELECT * FROM @extschema@.__gp_move_orphaned_files(
        ARRAY[ROW(tablespace_oid, target_location)::@extschema@.__gp_move_orphaned_files_pairs],
        process_all_tablespaces := FALSE::bool
    );
END;
$$;

--------------------------------------------------------------------------------
-- @function:
--        gp_move_orphaned_files_by_tablespace_location
--        (overloaded variant)
--
-- @in:
--        tablespace_location_pairs text[]
--                Array of text pairs formatted as
--                ARRAY['{tablespace_oid, /absolute/target/path}', ...]. Each
--                pair identifies a tablespace and the directory on the same
--                filesystem where its orphaned files will be moved.
--
-- @out:
--        gp_segment_id int - segment content ID
--        move_success bool - TRUE if the move succeeded
--        oldpath text - absolute pathname before the move
--        newpath text - absolute pathname after the move
--
-- @doc:
--        Moves orphaned files from multiple tablespaces to their
--        specified target directories.
--
--        Locks pg_class in shared mode and ensures no transactions are running.
--        May pause and wait if pg_class is already locked by another process.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_move_orphaned_files_by_tablespace_location(
        tablespace_location_pairs text[]
)
RETURNS TABLE (
        gp_segment_id  int,
        move_success   bool,
        oldpath        text,
        newpath        text
)
SET search_path = pg_catalog
LANGUAGE plpgsql AS $$
DECLARE
    raw_entry text;
    m text[];
    parsed @extschema@.__gp_move_orphaned_files_pairs[] :=
        ARRAY[]::@extschema@.__gp_move_orphaned_files_pairs[];
BEGIN
    FOREACH raw_entry IN ARRAY tablespace_location_pairs LOOP
        -- Use SELECT INTO since regexp_matches() returns SETOF text[]
        SELECT regexp_matches(raw_entry, '^\{\s*(\d+)\s*,\s*(/.+?)\s*\}$') INTO m;

        IF m IS NULL THEN
            RAISE EXCEPTION 'invalid format: "%". Expected format: {oid, /path}', raw_entry;
        END IF;

        parsed := parsed || ROW(m[1]::oid, m[2])::@extschema@.__gp_move_orphaned_files_pairs;
    END LOOP;
    RETURN QUERY
    SELECT * FROM @extschema@.__gp_move_orphaned_files(parsed, process_all_tablespaces := FALSE::bool);
END;
$$;

--------------------------------------------------------------------------------
-- @view:
--        __check_missing_files
--
-- @doc:
--        Check missing data files on default and user tablespaces,
--        not including extended files.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW __check_missing_files AS
SELECT f1.tablespace, f1.relname, f1.filename
from __get_expect_files f1
LEFT JOIN __get_exist_files f2
ON f1.tablespace = f2.tablespace AND f1.filename = f2.filename
WHERE f2.tablespace IS NULL
  AND f1.filename SIMILAR TO '[0-9]+';

GRANT SELECT ON __check_missing_files TO public;

--------------------------------------------------------------------------------
-- @view:
--        __check_missing_files_ext
--
-- @doc:
--        Check missing data files on default and user tablespaces,
--        including extended files.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW __check_missing_files_ext AS
SELECT f1.tablespace, f1.relname, f1.filename
FROM __get_expect_files_ext f1
LEFT JOIN __get_exist_files f2
ON f1.tablespace = f2.tablespace AND f1.filename = f2.filename
WHERE f2.tablespace IS NULL
  AND f1.filename SIMILAR TO '[0-9]+(\.[0-9]+)?';

GRANT SELECT ON __check_missing_files_ext TO public;

--------------------------------------------------------------------------------
-- @view:
--        gp_check_orphaned_files
--
-- @doc:
--        User-facing view of __check_orphaned_files. 
--        Gather results from coordinator and all segments.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gp_check_orphaned_files AS 
SELECT * FROM __gp_check_orphaned_files_func();

GRANT SELECT ON gp_check_orphaned_files TO public;

--------------------------------------------------------------------------------
-- @view:
--        gp_check_missing_files
--
-- @doc:
--        User-facing view of __check_missing_files. 
--        Gather results from coordinator and all segments.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gp_check_missing_files AS 
SELECT pg_catalog.gp_execution_segment() AS gp_segment_id, *
FROM gp_dist_random('__check_missing_files')
UNION ALL 
SELECT -1 AS gp_segment_id, *
FROM __check_missing_files;

GRANT SELECT ON gp_check_missing_files TO public;

--------------------------------------------------------------------------------
-- @view:
--        gp_check_missing_files_ext
--
-- @doc:
--        User-facing view of __check_missing_files_ext.
--        Gather results from coordinator and all segments.
--
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gp_check_missing_files_ext AS 
SELECT pg_catalog.gp_execution_segment() AS gp_segment_id, *
FROM gp_dist_random('__check_missing_files_ext')
UNION ALL 
SELECT -1 AS gp_segment_id, *
FROM __check_missing_files; -- not checking ext on coordinator

GRANT SELECT ON gp_check_missing_files_ext TO public;

