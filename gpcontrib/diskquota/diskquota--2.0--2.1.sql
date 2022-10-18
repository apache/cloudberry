-- TODO check if worker should not refresh, current lib should be diskquota-2.1.so

-- UDF
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_schema_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_role_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.init_table_size_table() RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type AS '$libdir/diskquota-2.1.so', 'diskquota_fetch_table_stat' LANGUAGE C VOLATILE;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_schema_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_role_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.set_per_segment_quota(text, float4) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.refresh_rejectmap(diskquota.rejectmap_entry[], oid[]) RETURNS void STRICT AS '$libdir/diskquota-2.1.so' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_rejectmap() RETURNS setof diskquota.rejectmap_entry_detail AS '$libdir/diskquota-2.1.so', 'show_rejectmap' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.pause() RETURNS void STRICT AS '$libdir/diskquota-2.1.so', 'diskquota_pause' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.resume() RETURNS void STRICT AS '$libdir/diskquota-2.1.so', 'diskquota_resume' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_worker_epoch() RETURNS bigint STRICT AS '$libdir/diskquota-2.1.so', 'show_worker_epoch' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.wait_for_worker_new_epoch() RETURNS boolean STRICT AS '$libdir/diskquota-2.1.so', 'wait_for_worker_new_epoch' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.status() RETURNS TABLE ("name" text, "status" text) STRICT AS '$libdir/diskquota-2.1.so', 'diskquota_status' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_relation_cache() RETURNS setof diskquota.relation_cache_detail AS '$libdir/diskquota-2.1.so', 'show_relation_cache' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.relation_size_local(reltablespace oid, relfilenode oid, relpersistence "char", relstorage "char") RETURNS bigint STRICT AS '$libdir/diskquota-2.1.so', 'relation_size_local' LANGUAGE C;
/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.pull_all_table_size(OUT tableid oid, OUT size bigint, OUT segid smallint) RETURNS SETOF RECORD AS '$libdir/diskquota-2.1.so', 'pull_all_table_size' LANGUAGE C;

/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.relation_size(relation regclass) RETURNS bigint STRICT AS $$
	SELECT SUM(size)::bigint FROM (
		SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence, relstorage) AS size
		FROM gp_dist_random('pg_class') WHERE oid = relation
		UNION ALL
		SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence, relstorage) AS size
		FROM pg_class WHERE oid = relation
	) AS t $$ LANGUAGE SQL;

/* ALTER */ CREATE OR REPLACE FUNCTION diskquota.show_relation_cache_all_seg() RETURNS setof diskquota.relation_cache_detail AS $$
	WITH relation_cache AS (
		SELECT diskquota.show_relation_cache() AS a
		FROM  gp_dist_random('gp_id')
	)
	SELECT (a).* FROM relation_cache; $$ LANGUAGE SQL;
-- UDF end


-- views
CREATE VIEW diskquota.show_all_relation_view AS
WITH
  relation_cache AS (
    SELECT (f).* FROM diskquota.show_relation_cache() as f
  )
SELECT DISTINCT(oid), relowner, relnamespace, reltablespace from (
  SELECT relid as oid, owneroid as relowner, namespaceoid as relnamespace, spcnode as reltablespace FROM relation_cache
  UNION
  SELECT oid, relowner, relnamespace, reltablespace from pg_class
) as union_relation;

/* ALTER */ CREATE OR REPLACE VIEW diskquota.show_fast_schema_quota_view AS
WITH
  quota_usage AS (
    SELECT
      relnamespace,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      diskquota.show_all_relation_view
    WHERE
      tableid = diskquota.show_all_relation_view.oid AND
      segid = -1
    GROUP BY
      relnamespace
  )
SELECT
  nspname AS schema_name,
  targetoid AS schema_oid,
  quotalimitMB AS quota_in_mb,
  COALESCE(total_size, 0) AS nspsize_in_bytes
FROM
  diskquota.quota_config JOIN
  pg_namespace ON targetoid = pg_namespace.oid LEFT OUTER JOIN
  quota_usage ON pg_namespace.oid = relnamespace
WHERE
  quotaType = 0; -- NAMESPACE_QUOTA

/* ALTER */ CREATE OR REPLACE VIEW diskquota.show_fast_role_quota_view AS
WITH
  quota_usage AS (
    SELECT
      relowner,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      diskquota.show_all_relation_view
    WHERE
      tableid = diskquota.show_all_relation_view.oid AND
      segid = -1
    GROUP BY
      relowner
  )
SELECT
  rolname AS role_name,
  targetoid AS role_oid,
  quotalimitMB AS quota_in_mb,
  COALESCE(total_size, 0) AS rolsize_in_bytes
FROM
  diskquota.quota_config JOIN
  pg_roles ON targetoid = pg_roles.oid LEFT OUTER JOIN
  quota_usage ON pg_roles.oid = relowner
WHERE
  quotaType = 1; -- ROLE_QUOTA

/* ALTER */ CREATE OR REPLACE VIEW diskquota.show_fast_schema_tablespace_quota_view AS
WITH
  default_tablespace AS (
    SELECT dattablespace FROM pg_database
    WHERE datname = current_database()
  ),
  quota_usage AS (
    SELECT
      relnamespace,
      CASE
        WHEN reltablespace = 0 THEN dattablespace
        ELSE reltablespace
      END AS reltablespace,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      diskquota.show_all_relation_view,
      default_tablespace
    WHERE
      tableid = diskquota.show_all_relation_view.oid AND
      segid = -1
    GROUP BY
      relnamespace,
      reltablespace,
      dattablespace
  ),
  full_quota_config AS (
    SELECT
      primaryOid,
      tablespaceoid,
      quotalimitMB
    FROM
      diskquota.quota_config AS config,
      diskquota.target AS target
    WHERE
      config.targetOid = target.rowId AND
      config.quotaType = target.quotaType AND
      config.quotaType = 2 -- NAMESPACE_TABLESPACE_QUOTA
  )
SELECT
  nspname AS schema_name,
  primaryoid AS schema_oid,
  spcname AS tablespace_name,
  tablespaceoid AS tablespace_oid,
  quotalimitMB AS quota_in_mb,
  COALESCE(total_size, 0) AS nspsize_tablespace_in_bytes
FROM
  full_quota_config JOIN
  pg_namespace ON primaryOid = pg_namespace.oid JOIN
  pg_tablespace ON tablespaceoid = pg_tablespace.oid LEFT OUTER JOIN
  quota_usage ON pg_namespace.oid = relnamespace AND pg_tablespace.oid = reltablespace;

/* ALTER */ CREATE OR REPLACE VIEW diskquota.show_fast_role_tablespace_quota_view AS
WITH
  default_tablespace AS (
    SELECT dattablespace FROM pg_database
    WHERE datname = current_database()
  ),
  quota_usage AS (
    SELECT
      relowner,
      CASE
        WHEN reltablespace = 0 THEN dattablespace
        ELSE reltablespace
      END AS reltablespace,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      diskquota.show_all_relation_view,
      default_tablespace
    WHERE
      tableid = diskquota.show_all_relation_view.oid AND
      segid = -1
    GROUP BY
      relowner,
      reltablespace,
      dattablespace
  ),
  full_quota_config AS (
    SELECT
      primaryOid,
      tablespaceoid,
      quotalimitMB
    FROM
      diskquota.quota_config AS config,
      diskquota.target AS target
    WHERE
      config.targetOid = target.rowId AND
      config.quotaType = target.quotaType AND
      config.quotaType = 3 -- ROLE_TABLESPACE_QUOTA
  )
SELECT
  rolname AS role_name,
  primaryoid AS role_oid,
  spcname AS tablespace_name,
  tablespaceoid AS tablespace_oid,
  quotalimitMB AS quota_in_mb,
  COALESCE(total_size, 0) AS rolsize_tablespace_in_bytes
FROM
  full_quota_config JOIN
  pg_roles ON primaryoid = pg_roles.oid JOIN
  pg_tablespace ON tablespaceoid = pg_tablespace.oid LEFT OUTER JOIN
  quota_usage ON pg_roles.oid = relowner AND pg_tablespace.oid = reltablespace;

-- view end