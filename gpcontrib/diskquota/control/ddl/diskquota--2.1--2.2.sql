-- TODO check if worker should not refresh, current lib should be diskquota-2.2.so

-- TYPE
ALTER TYPE diskquota.relation_cache_detail ADD ATTRIBUTE RELAM OID;
-- TYPE END

-- UDF
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_schema_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_role_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.init_table_size_table() RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type AS '$libdir/diskquota-2.2.so', 'diskquota_fetch_table_stat' LANGUAGE C VOLATILE;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_schema_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_role_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_per_segment_quota(text, float4) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.refresh_rejectmap(diskquota.rejectmap_entry[], oid[]) RETURNS void STRICT AS '$libdir/diskquota-2.2.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_rejectmap() RETURNS setof diskquota.rejectmap_entry_detail AS '$libdir/diskquota-2.2.so', 'show_rejectmap' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.pause() RETURNS void STRICT AS '$libdir/diskquota-2.2.so', 'diskquota_pause' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.resume() RETURNS void STRICT AS '$libdir/diskquota-2.2.so', 'diskquota_resume' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_worker_epoch() RETURNS bigint STRICT AS '$libdir/diskquota-2.2.so', 'show_worker_epoch' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.wait_for_worker_new_epoch() RETURNS boolean STRICT AS '$libdir/diskquota-2.2.so', 'wait_for_worker_new_epoch' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.status() RETURNS TABLE ("name" text, "status" text) STRICT AS '$libdir/diskquota-2.2.so', 'diskquota_status' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_relation_cache() RETURNS setof diskquota.relation_cache_detail AS '$libdir/diskquota-2.2.so', 'show_relation_cache' LANGUAGE C;

DROP FUNCTION IF EXISTS diskquota.relation_size(relation regclass);
DROP FUNCTION IF EXISTS diskquota.relation_size_local(reltablespace oid, relfilenode oid, relpersistence "char", relstorage "char");
CREATE FUNCTION diskquota.relation_size_local(reltablespace oid, relfilenode oid, relpersistence "char", relstorage "char", relam oid) RETURNS bigint STRICT AS '$libdir/diskquota-2.2.so', 'relation_size_local' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.pull_all_table_size(OUT tableid oid, OUT size bigint, OUT segid smallint) RETURNS SETOF RECORD AS '$libdir/diskquota-2.2.so', 'pull_all_table_size' LANGUAGE C;

CREATE FUNCTION diskquota.relation_size(relation regclass) RETURNS bigint STRICT AS $$
       SELECT SUM(size)::bigint FROM (
               SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence,
		CASE WHEN EXISTS
	(SELECT FROM pg_catalog.pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relstorage') THEN relstorage::"char" ELSE ''::"char" END,
		relam) AS size
               FROM gp_dist_random('pg_class') as relstorage WHERE oid = relation
               UNION ALL
               SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence,
		CASE WHEN EXISTS
	(SELECT FROM pg_catalog.pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relstorage') THEN relstorage::"char" ELSE ''::"char" END,
		relam) AS size
               FROM pg_class as relstorage WHERE oid = relation
       ) AS t $$ LANGUAGE SQL;

/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_relation_cache_all_seg() RETURNS setof diskquota.relation_cache_detail AS $$
	WITH relation_cache AS (
		SELECT diskquota.show_relation_cache() AS a
		FROM  gp_dist_random('gp_id')
	)
	SELECT (a).* FROM relation_cache; $$ LANGUAGE SQL;
-- UDF end

-- VIEW
CREATE OR REPLACE VIEW diskquota.show_all_relation_view AS
WITH
  relation_cache AS (
    SELECT (f).* FROM diskquota.show_relation_cache() as f
  )
SELECT DISTINCT(oid), relowner, relnamespace, reltablespace from (
  SELECT relid as oid, owneroid as relowner, namespaceoid as relnamespace, spcnode as reltablespace FROM relation_cache
  UNION
  SELECT oid, relowner, relnamespace, reltablespace from pg_class
) as union_relation;
-- VIEW
