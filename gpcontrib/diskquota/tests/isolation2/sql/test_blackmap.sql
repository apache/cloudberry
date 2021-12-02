--
-- This file contains tests for dispatching blackmap and canceling
-- queries in smgrextend hook by relation's relfilenode.
--

CREATE OR REPLACE FUNCTION block_relation_on_seg0(rel regclass, block_type text, segexceeded boolean)
  RETURNS void AS $$                                                      /*in func*/
  DECLARE                                                                 /*in func*/
    bt          int;                                                      /*in func*/
    targetoid   oid;                                                      /*in func*/
  BEGIN                                                                   /*in func*/
    CASE block_type                                                       /*in func*/
      WHEN 'NAMESPACE' THEN                                               /*in func*/
        bt = 0;                                                           /*in func*/
        SELECT relnamespace INTO targetoid                                /*in func*/
          FROM pg_class WHERE relname=rel::text;                          /*in func*/
      WHEN 'ROLE'      THEN                                               /*in func*/
        bt = 1;                                                           /*in func*/
        SELECT relowner INTO targetoid                                    /*in func*/
          FROM pg_class WHERE relname=rel::text;                          /*in func*/
      WHEN 'NAMESPACE_TABLESPACE' THEN                                    /*in func*/
        bt = 2;                                                           /*in func*/
        SELECT relnamespace INTO targetoid                                /*in func*/
          FROM pg_class WHERE relname=rel::text;                          /*in func*/
      WHEN 'ROLE_TABLESPACE' THEN                                         /*in func*/
        bt = 3;                                                           /*in func*/
        SELECT relowner INTO targetoid                                    /*in func*/
          FROM pg_class WHERE relname=rel::text;                          /*in func*/
    END CASE;                                                             /*in func*/
    PERFORM diskquota.refresh_blackmap(                                   /*in func*/
    ARRAY[                                                                /*in func*/
      ROW(targetoid,                                                      /*in func*/
          (SELECT oid FROM pg_database WHERE datname=current_database()), /*in func*/
          (SELECT reltablespace FROM pg_class WHERE relname=rel::text),   /*in func*/
          bt,                                                             /*in func*/
          segexceeded)                                                    /*in func*/
      ]::diskquota.blackmap_entry[],                                      /*in func*/
    ARRAY[rel]::oid[])                                                    /*in func*/
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;                     /*in func*/
  END; $$                                                                 /*in func*/
LANGUAGE 'plpgsql';

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
