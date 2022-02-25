--
-- This file contains tests for dispatching and quering blackmap.
--

CREATE SCHEMA s_blackmap;
SET search_path TO s_blackmap;

-- This function replaces the oid appears in the auxiliary relation's name
-- with the corresponding relname of that oid.
CREATE OR REPLACE FUNCTION replace_oid_with_relname(given_name text)
  RETURNS text AS $$
  BEGIN
    RETURN COALESCE(
      REGEXP_REPLACE(given_name,
         '^(pg_toast_|pg_aoseg_|pg_aovisimap_|pg_aoblkdir_|pg_aocsseg_)\d+',
         '\1' ||
	 (SELECT relname FROM pg_class
          WHERE oid=REGEXP_REPLACE(given_name, '\D', '', 'g')::oid), 'g'), given_name);
  END;
$$ LANGUAGE plpgsql;

-- this function return valid tablespaceoid.
-- For role/namespace quota, return as it is.
-- For namespace_tablespace/role_tablespace quota, return non-zero tablespaceoid.
CREATE OR REPLACE FUNCTION get_real_tablespace_oid(block_type text, tablespaceoid oid)
	RETURNS oid AS
$$
BEGIN
	CASE
		WHEN (block_type = 'NAMESPACE') OR (block_type = 'ROLE') THEN RETURN tablespaceoid;
		ELSE RETURN (
			CASE tablespaceoid
				WHEN 0 THEN (SELECT dattablespace FROM pg_database WHERE datname = CURRENT_DATABASE())
				ELSE
					tablespaceoid
				END
			);
		END CASE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION block_relation_on_seg0(rel regclass, block_type text)
  RETURNS void AS $$
  DECLARE
    bt        int;
    targetoid oid;
    tablespaceoid oid;
  BEGIN
    SELECT reltablespace INTO tablespaceoid FROM pg_class WHERE relname=rel::text;
    CASE block_type
      WHEN 'NAMESPACE' THEN
        bt = 0;
        SELECT relnamespace INTO targetoid
          FROM pg_class WHERE relname=rel::text;
      WHEN 'ROLE'      THEN
        bt = 1;
        SELECT relowner INTO targetoid
          FROM pg_class WHERE relname=rel::text;
      WHEN 'NAMESPACE_TABLESPACE' THEN
        bt = 2;
        SELECT relnamespace INTO targetoid
          FROM pg_class WHERE relname=rel::text;
      WHEN 'ROLE_TABLESPACE' THEN
        bt = 3;
        SELECT relowner INTO targetoid
          FROM pg_class WHERE relname=rel::text;
	END CASE;
    PERFORM diskquota.refresh_blackmap(
    ARRAY[
      ROW(targetoid,
          (SELECT oid FROM pg_database WHERE datname=current_database()),
          (SELECT get_real_tablespace_oid(block_type, tablespaceoid)),
          bt,
          false)
      ]::diskquota.blackmap_entry[],
    ARRAY[rel]::oid[])
  FROM gp_dist_random('gp_id') WHERE gp_segment_id=0;
  END; $$
LANGUAGE 'plpgsql';

--
-- 1. Create an ordinary table and add its oid to blackmap on seg0.
--    Check that it's relfilenode is blocked on seg0 by various conditions.
--
CREATE TABLE blocked_t1(i int) DISTRIBUTED BY (i);

-- Insert an entry for blocked_t1 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t1'::regclass, 'NAMESPACE'::text);

-- Shows that the relfilenode of blocked_t1 is blocked on seg0 by its namespace.
SELECT rel.relname, be.target_type, (be.target_oid=rel.relnamespace) AS namespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid;

-- Insert an entry for blocked_t1 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t1'::regclass, 'ROLE'::text);

-- Shows that the relfilenode of blocked_t1 is blocked on seg0 by its owner.
SELECT rel.relname, be.target_type, (be.target_oid=rel.relowner) AS owner_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid;

-- Create a tablespace to test the rest of blocking types.
\! mkdir -p /tmp/blocked_space
CREATE TABLESPACE blocked_space LOCATION '/tmp/blocked_space';
ALTER TABLE blocked_t1 SET TABLESPACE blocked_space;

-- Insert an entry for blocked_t1 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t1'::regclass, 'NAMESPACE_TABLESPACE'::text);

-- Shows that the relfilenode of blocked_t1 is blocked on seg0 by its namespace and tablespace.
SELECT rel.relname, be.target_type,
                    (be.target_oid=rel.relnamespace) AS namespace_matched,
                    (be.tablespace_oid=rel.reltablespace) AS tablespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid;

-- Insert an entry for blocked_t1 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t1'::regclass, 'ROLE_TABLESPACE'::text);

-- Shows that the relfilenode of blocked_t1 is blocked on seg0 by its owner and tablespace.
SELECT rel.relname, be.target_type,
                    (be.target_oid=rel.relowner) AS owner_matched,
                    (be.tablespace_oid=rel.reltablespace) AS tablespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid;

--
-- 2. Test that the relfilenodes of toast relation together with its
--    index are blocked on seg0.
--
CREATE TABLE blocked_t2(i text) DISTRIBUTED BY (i);
-- Insert an entry for blocked_t2 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t2'::regclass, 'NAMESPACE'::text);

-- Shows that the relfilenodes of blocked_t2 together with its toast relation and toast
-- index relation are blocked on seg0 by its namespace.
SELECT replace_oid_with_relname(rel.relname),
       rel.relkind, be.target_type,
       (be.target_oid=rel.relnamespace) AS namespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid
  ORDER BY rel.relname DESC;

--
-- 3. Test that the relfilenodes of appendonly relation (row oriented) together with its
--    auxiliary relations are blocked on seg0.
--
CREATE TABLE blocked_t3(i int) WITH (appendonly=true) DISTRIBUTED BY (i);
CREATE INDEX blocked_t3_index ON blocked_t3(i);
-- Insert an entry for blocked_t3 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t3'::regclass, 'NAMESPACE'::text);

-- Shows that the relfilenodes of blocked_t3 together with its appendonly relation and appendonly
-- index relations are blocked on seg0 by its namespace.
SELECT replace_oid_with_relname(rel.relname),
       rel.relkind, be.target_type,
       (be.target_oid=rel.relnamespace) AS namespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid
  ORDER BY rel.relname DESC;

--
-- 4. Test that the relfilenodes of appendonly relation (column oriented) together with its
--    auxiliary relations are blocked on seg0.
--
CREATE TABLE blocked_t4(i int) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
CREATE INDEX blocked_t4_index ON blocked_t4(i);
-- Insert an entry for blocked_t4 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t4'::regclass, 'NAMESPACE'::text);

-- Shows that the relfilenodes of blocked_t4 together with its appendonly relation and appendonly
-- index relation are blocked on seg0 by its namespace.
SELECT replace_oid_with_relname(rel.relname),
       rel.relkind, be.target_type,
       (be.target_oid=rel.relnamespace) AS namespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid
  ORDER BY rel.relname DESC;

--
-- 5. Test that the relfilenodes of toast appendonly relation (row oriented) together with its
--    auxiliary relations are blocked on seg0.
--
CREATE TABLE blocked_t5(i text) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
CREATE INDEX blocked_t5_index ON blocked_t5(i);
-- Insert an entry for blocked_t5 to blackmap on seg0.
SELECT block_relation_on_seg0('blocked_t5'::regclass, 'NAMESPACE'::text);

-- Shows that the relfilenodes of blocked_t5 together with its toast relation, toast
-- index relation and appendonly relations are blocked on seg0 by its namespace.
SELECT replace_oid_with_relname(rel.relname),
       rel.relkind, be.target_type,
       (be.target_oid=rel.relnamespace) AS namespace_matched
  FROM gp_dist_random('pg_class') AS rel,
       gp_dist_random('diskquota.blackmap') AS be
  WHERE rel.relfilenode=be.relnode AND be.relnode<>0 AND rel.gp_segment_id=be.segid
  ORDER BY rel.relname DESC;

-- Do some clean-ups.
DROP FUNCTION replace_oid_with_relname(text);
DROP FUNCTION block_relation_on_seg0(regclass, text);
DROP FUNCTION get_real_tablespace_oid(text, oid);
DROP TABLE blocked_t1;
DROP TABLE blocked_t2;
DROP TABLE blocked_t3;
DROP TABLE blocked_t4;
DROP TABLE blocked_t5;
DROP TABLESPACE blocked_space;
SET search_path TO DEFAULT;
DROP SCHEMA s_blackmap;
