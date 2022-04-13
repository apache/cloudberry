-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION diskquota" to load this file. \quit

CREATE SCHEMA diskquota;

-- when (quotatype == NAMESPACE_QUOTA/ROLE_QUOTA) then targetOid = role_oid/schema_oid;
-- when (quotatype == NAMESPACE_TABLESPACE_QUOTA/ROLE_TABLESPACE_QUOTA) then targetOid = diskquota.target.rowId;
CREATE TABLE diskquota.quota_config(
	targetOid oid,
	quotatype int,
	quotalimitMB int8,
	segratio float4 DEFAULT 0,
	PRIMARY KEY(targetOid, quotatype)
) DISTRIBUTED BY (targetOid, quotatype);

CREATE TABLE diskquota.target (
	rowId serial,
	quotatype int, --REFERENCES disquota.quota_config.quotatype,
	primaryOid oid,
	tablespaceOid oid, --REFERENCES pg_tablespace.oid,
	PRIMARY KEY (primaryOid, tablespaceOid, quotatype)
);

CREATE TABLE diskquota.table_size(
	tableid oid,
	size bigint,
	segid smallint,
	PRIMARY KEY(tableid, segid)
) DISTRIBUTED BY (tableid, segid);

CREATE TABLE diskquota.state(
	state int,
	PRIMARY KEY(state)
) DISTRIBUTED BY (state);

-- diskquota.quota_config AND diskquota.target is dump-able, other table can be generate on fly
SELECT pg_catalog.pg_extension_config_dump('diskquota.quota_config', '');
SELECT gp_segment_id, pg_catalog.pg_extension_config_dump('diskquota.quota_config', '') FROM gp_dist_random('gp_id');
SELECT pg_catalog.pg_extension_config_dump('diskquota.target', '');
SELECT gp_segment_id, pg_catalog.pg_extension_config_dump('diskquota.target', '') FROM gp_dist_random('gp_id');

CREATE TYPE diskquota.diskquota_active_table_type AS (
	"TABLE_OID" oid,
	"TABLE_SIZE" int8,
	"GP_SEGMENT_ID" smallint
);

CREATE TYPE diskquota.blackmap_entry AS (
	target_oid oid,
	database_oid oid,
	tablespace_oid oid,
	target_type integer,
	seg_exceeded boolean
);

CREATE TYPE diskquota.blackmap_entry_detail AS (
	target_type text,
	target_oid oid,
	database_oid oid,
	tablespace_oid oid,
	seg_exceeded boolean,
	dbnode oid,
	spcnode oid,
	relnode oid,
	segid int
);

CREATE TYPE diskquota.relation_cache_detail AS (
	RELID oid,
	PRIMARY_TABLE_OID oid,
	AUXREL_NUM int,
	OWNEROID oid,
	NAMESPACEOID oid,
	BACKENDID int,
	SPCNODE oid,
	DBNODE oid,
	RELNODE oid,
	RELSTORAGE "char",
	AUXREL_OID oid[]
);

CREATE FUNCTION diskquota.set_schema_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.set_role_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.init_table_size_table() RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type AS '$libdir/diskquota-2.0.so', 'diskquota_fetch_table_stat' LANGUAGE C VOLATILE;
CREATE FUNCTION diskquota.set_schema_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.set_role_tablespace_quota(text, text, text) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.set_per_segment_quota(text, float4) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.refresh_blackmap(diskquota.blackmap_entry[], oid[]) RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
CREATE FUNCTION diskquota.show_blackmap() RETURNS setof diskquota.blackmap_entry_detail AS '$libdir/diskquota-2.0.so', 'show_blackmap' LANGUAGE C;
CREATE FUNCTION diskquota.pause() RETURNS void STRICT AS '$libdir/diskquota-2.0.so', 'diskquota_pause' LANGUAGE C;
CREATE FUNCTION diskquota.resume() RETURNS void STRICT AS '$libdir/diskquota-2.0.so', 'diskquota_resume' LANGUAGE C;
CREATE FUNCTION diskquota.show_worker_epoch() RETURNS bigint STRICT AS '$libdir/diskquota-2.0.so', 'show_worker_epoch' LANGUAGE C;
CREATE FUNCTION diskquota.wait_for_worker_new_epoch() RETURNS boolean STRICT AS '$libdir/diskquota-2.0.so', 'wait_for_worker_new_epoch' LANGUAGE C;
CREATE FUNCTION diskquota.status() RETURNS TABLE ("name" text, "status" text) STRICT AS '$libdir/diskquota-2.0.so', 'diskquota_status' LANGUAGE C;
CREATE FUNCTION diskquota.show_relation_cache() RETURNS setof diskquota.relation_cache_detail AS '$libdir/diskquota-2.0.so', 'show_relation_cache' LANGUAGE C;
CREATE FUNCTION diskquota.relation_size_local(
        reltablespace oid,
        relfilenode oid,
        relpersistence "char",
        relstorage "char")
RETURNS bigint STRICT AS '$libdir/diskquota-2.0.so', 'relation_size_local' LANGUAGE C;
CREATE FUNCTION diskquota.pull_all_table_size(OUT tableid oid, OUT size bigint, OUT segid smallint) RETURNS SETOF RECORD AS '$libdir/diskquota-2.0.so', 'pull_all_table_size' LANGUAGE C;

CREATE FUNCTION diskquota.relation_size(relation regclass) RETURNS bigint STRICT AS $$
	SELECT SUM(size)::bigint FROM (
		SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence, relstorage) AS size
		FROM gp_dist_random('pg_class') WHERE oid = relation
		UNION ALL
		SELECT diskquota.relation_size_local(reltablespace, relfilenode, relpersistence, relstorage) AS size
		FROM pg_class WHERE oid = relation
	) AS t $$ LANGUAGE SQL;


CREATE FUNCTION diskquota.show_relation_cache_all_seg() RETURNS setof diskquota.relation_cache_detail AS $$
	WITH relation_cache AS (
		SELECT diskquota.show_relation_cache() AS a
		FROM  gp_dist_random('gp_id')
	)
	SELECT (a).* FROM relation_cache; $$ LANGUAGE SQL;

-- view part
CREATE VIEW diskquota.show_fast_schema_quota_view AS
WITH
  quota_usage AS (
    SELECT
      relnamespace,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      pg_class
    WHERE
      tableid = pg_class.oid AND
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

CREATE VIEW diskquota.show_fast_role_quota_view AS
WITH
  quota_usage AS (
    SELECT
      relowner,
      SUM(size) AS total_size
    FROM
      diskquota.table_size,
      pg_class
    WHERE
      tableid = pg_class.oid AND
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

CREATE VIEW diskquota.show_fast_database_size_view AS
SELECT (
    (SELECT SUM(pg_relation_size(oid)) FROM pg_class WHERE oid <= 16384)
        +
    (SELECT SUM(size) FROM diskquota.table_size WHERE segid = -1)
) AS dbsize;

CREATE VIEW diskquota.blackmap AS SELECT * FROM diskquota.show_blackmap() AS BM;

CREATE VIEW diskquota.show_fast_schema_tablespace_quota_view AS
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
      pg_class,
      default_tablespace
    WHERE
      tableid = pg_class.oid AND
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

CREATE VIEW diskquota.show_fast_role_tablespace_quota_view AS
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
      pg_class,
      default_tablespace
    WHERE
      tableid = pg_class.oid AND
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

CREATE VIEW diskquota.show_segment_ratio_quota_view AS
SELECT 
  spcname as tablespace_name,
  pg_tablespace.oid as tablespace_oid,
  segratio as per_seg_quota_ratio
FROM
  diskquota.quota_config JOIN
  pg_tablespace ON targetOid = pg_tablespace.oid
  AND quotatype = 4;

-- view end

-- prepare to boot
INSERT INTO diskquota.state SELECT (count(relname) = 0)::int FROM pg_class AS c, pg_namespace AS n WHERE c.oid > 16384 AND relnamespace = n.oid AND nspname != 'diskquota';

-- re-dispatch pause status to false. in case user pause-drop-recreate.
-- refer to see test case 'test_drop_after_pause'
SELECT FROM diskquota.resume();


-- Starting the worker has to be the last step.
CREATE FUNCTION diskquota.diskquota_start_worker() RETURNS void STRICT AS '$libdir/diskquota-2.0.so' LANGUAGE C;
SELECT diskquota.diskquota_start_worker();
DROP FUNCTION diskquota.diskquota_start_worker();
