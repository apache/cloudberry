--
-- This file contains tests for dispatching blackmap and canceling
-- queries in smgrextend hook by relation's relfilenode.
--

-- this function return valid tablespaceoid.
-- For role/namespace quota, return as it is.
-- For namespace_tablespace/role_tablespace quota, return non-zero tablespaceoid.
CREATE OR REPLACE FUNCTION get_real_tablespace_oid(block_type text, tablespaceoid oid)         /*in func*/
  RETURNS oid AS                                                                               /*in func*/
$$                                                                                             /*in func*/
BEGIN                                                                                          /*in func*/
                                                                                               /*in func*/
  CASE                                                                                         /*in func*/
    WHEN (block_type = 'NAMESPACE') OR (block_type = 'ROLE') THEN RETURN tablespaceoid;        /*in func*/
    ELSE RETURN (                                                                              /*in func*/
      CASE tablespaceoid                                                                       /*in func*/
        WHEN 0 THEN (SELECT dattablespace FROM pg_database WHERE datname = CURRENT_DATABASE()) /*in func*/
        ELSE                                                                                   /*in func*/
          tablespaceoid                                                                        /*in func*/
        END                                                                                    /*in func*/
      );                                                                                       /*in func*/
    END CASE;                                                                                  /*in func*/
END;                                                                                           /*in func*/
$$ LANGUAGE plpgsql;                                                                           /*in func*/

CREATE OR REPLACE FUNCTION block_relation_on_seg0(rel regclass, block_type text, segexceeded boolean)
  RETURNS void AS $$                                                                                                   /*in func*/
  DECLARE                                                                                                              /*in func*/
    bt          int;                                                                                                   /*in func*/
    targetoid   oid;                                                                                                   /*in func*/
  BEGIN                                                                                                                /*in func*/
    CASE block_type                                                                                                    /*in func*/
      WHEN 'NAMESPACE' THEN                                                                                            /*in func*/
        bt = 0;                                                                                                        /*in func*/
        SELECT relnamespace INTO targetoid                                                                             /*in func*/
          FROM pg_class WHERE relname=rel::text;                                                                       /*in func*/
      WHEN 'ROLE'      THEN                                                                                            /*in func*/
        bt = 1;                                                                                                        /*in func*/
        SELECT relowner INTO targetoid                                                                                 /*in func*/
          FROM pg_class WHERE relname=rel::text;                                                                       /*in func*/
      WHEN 'NAMESPACE_TABLESPACE' THEN                                                                                 /*in func*/
        bt = 2;                                                                                                        /*in func*/
        SELECT relnamespace INTO targetoid                                                                             /*in func*/
          FROM pg_class WHERE relname=rel::text;                                                                       /*in func*/
      WHEN 'ROLE_TABLESPACE' THEN                                                                                      /*in func*/
        bt = 3;                                                                                                        /*in func*/
        SELECT relowner INTO targetoid                                                                                 /*in func*/
          FROM pg_class WHERE relname=rel::text;                                                                       /*in func*/
    END CASE;                                                                                                          /*in func*/
    PERFORM diskquota.refresh_blackmap(                                                                                /*in func*/
    ARRAY[                                                                                                             /*in func*/
          ROW (targetoid,                                                                                              /*in func*/
               (SELECT oid FROM pg_database WHERE datname = CURRENT_DATABASE()),                                       /*in func*/
               (SELECT get_real_tablespace_oid(                                                                        /*in func*/
                                               block_type,                                                             /*in func*/
                                               (SELECT pg_class.reltablespace FROM pg_class WHERE relname = rel::TEXT) /*in func*/
               )),                                                                                                     /*in func*/
               bt,                                                                                                     /*in func*/
               segexceeded)                                                                                            /*in func*/
      ]::diskquota.blackmap_entry[],                                                                                   /*in func*/
    ARRAY[rel]::oid[])                                                                                                 /*in func*/
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;                                                                  /*in func*/
  END; $$                                                                                                              /*in func*/
LANGUAGE 'plpgsql';


-- Enable check quota by relfilenode on seg0.
SELECT gp_inject_fault_infinite('enable_check_quota_by_relfilenode', 'skip', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- 1. Test canceling the extending of an ordinary table.
CREATE TABLE blocked_t1(i int) DISTRIBUTED BY (i);
INSERT INTO blocked_t1 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Insert a small amount of data into blocked_t1. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t1 SELECT generate_series(1, 10000);

-- Dispatch blackmap to seg0.
SELECT block_relation_on_seg0('blocked_t1'::regclass, 'NAMESPACE'::text, false);

SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Session 1 will return and emit an error message saying that the quota limit is exceeded on seg0.
1<:

-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 2. Test canceling the extending of a toast relation.
CREATE TABLE blocked_t2(i text) DISTRIBUTED BY (i);
INSERT INTO blocked_t2 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Insert a small amount of data into blocked_t2. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t2 SELECT generate_series(1, 10000);

-- Dispatch blackmap to seg0.
SELECT block_relation_on_seg0('blocked_t2'::regclass, 'NAMESPACE'::text, false);

SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Session 1 will return and emit an error message saying that the quota limit is exceeded on seg0.
1<:

-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 3. Test canceling the extending of an appendonly relation.
CREATE TABLE blocked_t3(i int) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO blocked_t3 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Insert a small amount of data into blocked_t3. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t3 SELECT generate_series(1, 10000);

-- Dispatch blackmap to seg0.
SELECT block_relation_on_seg0('blocked_t3'::regclass, 'NAMESPACE'::text, false);

SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Session 1 will return and emit an error message saying that the quota limit is exceeded on seg0.
1<:

-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 4. Test canceling the extending of an index relation.
CREATE TABLE blocked_t4(i int) DISTRIBUTED BY (i);
CREATE INDEX blocked_t4_index ON blocked_t4(i);
INSERT INTO blocked_t4 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Insert a small amount of data into blocked_t4. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t4 SELECT generate_series(1, 10000);

-- Dispatch blackmap to seg0.
SELECT block_relation_on_seg0('blocked_t4_index'::regclass, 'NAMESPACE'::text, false);

SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Session 1 will return and emit an error message saying that the quota limit is exceeded on seg0.
1<:

-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 5. Test error message for NAMESPACE_TABLESPACE_QUOTA when the quota limit is exceeded on segments.
CREATE TABLE blocked_t5(i int) DISTRIBUTED BY (i);
INSERT INTO blocked_t5 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1&: INSERT INTO blocked_t5 SELECT generate_series(1, 10000);
SELECT block_relation_on_seg0('blocked_t5'::regclass, 'NAMESPACE_TABLESPACE'::text, true);
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 6. Test error message for ROLE_TABLESPACE_QUOTA when the quota limit is exceeded on segments.
CREATE TABLE blocked_t6(i int) DISTRIBUTED BY (i);
INSERT INTO blocked_t6 SELECT generate_series(1, 100);
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1&: INSERT INTO blocked_t6 SELECT generate_series(1, 10000);
SELECT block_relation_on_seg0('blocked_t6'::regclass, 'ROLE_TABLESPACE'::text, true);
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- Do some clean-ups.
DROP TABLE blocked_t1;
DROP TABLE blocked_t2;
DROP TABLE blocked_t3;
DROP TABLE blocked_t4;
DROP TABLE blocked_t5;
DROP TABLE blocked_t6;

--
-- Below are helper functions for testing adding uncommitted relations to blackmap.
--
-- start_ignore
CREATE OR REPLACE LANGUAGE plpythonu;
-- end_ignore
CREATE TYPE cached_relation_entry AS (
  reloid        oid,
  relname       text,
  relowner      oid,
  relnamespace  oid,
  reltablespace oid,
  relfilenode   oid,
  segid         int);

-- This function dumps given relation_cache entries to the given file.
CREATE OR REPLACE FUNCTION dump_relation_cache_to_file(filename text)
  RETURNS void
AS $$
    rv = plpy.execute("""
                      SELECT (oid, relname, relowner,
                              relnamespace, reltablespace,
                              relfilenode, gp_segment_id)::cached_relation_entry
                      FROM gp_dist_random('pg_class')
                      """)
    with open(filename, 'wt') as f:
        for v in rv:
            f.write(v['row'][1:-1] + '\n')
$$ LANGUAGE plpythonu;

-- This function reads relation_cache entries from the given file.
CREATE OR REPLACE FUNCTION read_relation_cache_from_file(filename text)
  RETURNS SETOF cached_relation_entry
AS $$
   with open(filename) as f:
       for l in f:
           r = l.split(',')
	   yield (r[0], r[1], r[2], r[3], r[4], r[5], r[6])
$$ LANGUAGE plpythonu;

-- This function replaces the oid appears in the auxiliary relation's name
-- with the corresponding relname of that oid.
CREATE OR REPLACE FUNCTION replace_oid_with_relname(given_name text, filename text)
  RETURNS text AS $$                                                                      /*in func*/
  BEGIN                                                                                   /*in func*/
    RETURN COALESCE(                                                                      /*in func*/
      REGEXP_REPLACE(given_name,                                                          /*in func*/
         '^(pg_toast_|pg_aoseg_|pg_aovisimap_|pg_aoblkdir_|pg_aocsseg_)\d+',              /*in func*/
         '\1' ||                                                                          /*in func*/
	 (SELECT DISTINCT relname FROM read_relation_cache_from_file(filename)            /*in func*/
          WHERE  REGEXP_REPLACE(given_name, '\D', '', 'g') <> ''
          AND reloid=REGEXP_REPLACE(given_name, '\D', '', 'g')::oid), 'g'), given_name);/*in func*/
  END;                                                                                    /*in func*/
$$ LANGUAGE plpgsql;

-- This function helps dispatch blackmap for the given relation to seg0.
CREATE OR REPLACE FUNCTION block_uncommitted_relation_on_seg0(rel text, block_type text, segexceeded boolean, filename text)
  RETURNS void AS $$                                                                           /*in func*/
  DECLARE                                                                                      /*in func*/
    bt          int;                                                                           /*in func*/
    targetoid   oid;                                                                           /*in func*/
  BEGIN                                                                                        /*in func*/
    CASE block_type                                                                            /*in func*/
      WHEN 'NAMESPACE' THEN                                                                    /*in func*/
        bt = 0;                                                                                /*in func*/
        SELECT relnamespace INTO targetoid                                                     /*in func*/
          FROM read_relation_cache_from_file(filename)                                         /*in func*/
    WHERE relname=rel::text AND segid=0;                                                       /*in func*/
      WHEN 'ROLE'      THEN                                                                    /*in func*/
        bt = 1;                                                                                /*in func*/
        SELECT relowner INTO targetoid                                                         /*in func*/
          FROM read_relation_cache_from_file(filename)                                         /*in func*/
    WHERE relname=rel::text AND segid=0;                                                       /*in func*/
      WHEN 'NAMESPACE_TABLESPACE' THEN                                                         /*in func*/
        bt = 2;                                                                                /*in func*/
        SELECT relnamespace INTO targetoid                                                     /*in func*/
          FROM read_relation_cache_from_file(filename)                                         /*in func*/
    WHERE relname=rel::text AND segid=0;                                                       /*in func*/
      WHEN 'ROLE_TABLESPACE' THEN                                                              /*in func*/
        bt = 3;                                                                                /*in func*/
        SELECT relowner INTO targetoid                                                         /*in func*/
          FROM read_relation_cache_from_file(filename)                                         /*in func*/
    WHERE relname=rel::text AND segid=0;                                                       /*in func*/
    END CASE;                                                                                  /*in func*/
    PERFORM diskquota.refresh_blackmap(                                                        /*in func*/
    ARRAY[                                                                                     /*in func*/
          ROW (targetoid,                                                                      /*in func*/
               (SELECT oid FROM pg_database WHERE datname = CURRENT_DATABASE()),               /*in func*/
               (SELECT get_real_tablespace_oid(                                                /*in func*/
                                               block_type,                                     /*in func*/
                                               (SELECT reltablespace                           /*in func*/
                                                  FROM read_relation_cache_from_file(filename) /*in func*/
                                                 WHERE relname = rel::text                     /*in func*/
                                                   AND segid = 0)                              /*in func*/
               )),                                                                             /*in func*/
               bt,                                                                             /*in func*/
               segexceeded)                                                                    /*in func*/
      ]::diskquota.blackmap_entry[],                                                           /*in func*/
    ARRAY[(SELECT reloid FROM read_relation_cache_from_file(filename)                          /*in func*/
             WHERE relname=rel::text AND segid=0)::regclass]::oid[])                           /*in func*/
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;                                          /*in func*/
  END; $$                                                                                      /*in func*/
LANGUAGE 'plpgsql';

-- 7. Test that we are able to block an ordinary relation on seg0 by its relnamespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE'::text, false, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 8. Test that we are able to block an ordinary relation on seg0 by its relowner.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'ROLE'::text, false, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 9. Test that we are able to block an ordinary relation on seg0 by its relnamespace and reltablespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE_TABLESPACE'::text, false, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 10. Test that we are able to block an ordinary relation on seg0 by its relowner and reltablespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'ROLE_TABLESPACE'::text, false, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 11. Test that we are able to block an ordinary relation on seg0 by its relnamespace and reltablespace (segexceeded=true).
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE_TABLESPACE'::text, true, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 12. Test that we are able to block an ordinary relation on seg0 by its relowner and reltablespace (segexceeded=true).
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'ROLE_TABLESPACE'::text, true, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner, replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text),
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 13. Test that we are able to block a toast relation on seg0 by its namespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i text) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE'::text, true, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner,
          replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text) AS relname,
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0
     ORDER BY relname DESC;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 14. Test that we are able to block an appendonly relation on seg0 by its namespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) WITH (appendonly=true) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE'::text, true, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner,
          replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text) AS relname,
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0
     ORDER BY relname DESC;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- 15. Test that we are able to block an appendonly (column oriented) relation on seg0 by its namespace.
1: BEGIN;
1: CREATE TABLE blocked_t7(i int) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
1: SELECT dump_relation_cache_to_file('/tmp/test_blackmap.csv');
-- Inject 'suspension' to check_blackmap_by_relfilenode on seg0.
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
-- Insert a small amount of data into blocked_t7. It will hang up at check_blackmap_by_relfilenode().
1&: INSERT INTO blocked_t7 SELECT generate_series(1, 10000);
SELECT block_uncommitted_relation_on_seg0('blocked_t7'::text, 'NAMESPACE'::text, true, '/tmp/test_blackmap.csv'::text);
-- Show that blocked_t7 is blocked on seg0.
2: SELECT rel.segid, rel.relnamespace, rel.reltablespace, rel.relowner,
          replace_oid_with_relname(rel.relname, '/tmp/test_blackmap.csv'::text) AS relname,
          be.target_type, be.target_oid
     FROM gp_dist_random('diskquota.blackmap') AS be,
          read_relation_cache_from_file('/tmp/test_blackmap.csv') AS rel
     WHERE be.segid=rel.segid AND be.relnode=rel.relfilenode AND rel.relfilenode<>0
     ORDER BY relname DESC;
SELECT gp_inject_fault_infinite('check_blackmap_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
1: ABORT;
-- Clean up the blackmap on seg0.
SELECT diskquota.refresh_blackmap(
  ARRAY[]::diskquota.blackmap_entry[], ARRAY[]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;

-- Disable check quota by relfilenode on seg0.
SELECT gp_inject_fault_infinite('enable_check_quota_by_relfilenode', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=0;
