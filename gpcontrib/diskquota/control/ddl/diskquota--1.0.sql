-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION diskquota" to load this file. \quit

CREATE SCHEMA diskquota;

-- Configuration table
CREATE TABLE diskquota.quota_config(
    targetOid oid,
    quotatype int,
    quotalimitMB int8,
    PRIMARY KEY(targetOid, quotatype)
);

CREATE TABLE diskquota.table_size(
    tableid oid,
    size bigint,
    PRIMARY KEY(tableid)
);

CREATE TABLE diskquota.state(
    state int,
    PRIMARY KEY(state)
);

-- only diskquota.quota_config is dump-able, other table can be generate on fly
SELECT pg_catalog.pg_extension_config_dump('diskquota.quota_config', '');
SELECT gp_segment_id, pg_catalog.pg_extension_config_dump('diskquota.quota_config', '') FROM gp_dist_random('gp_id');

CREATE TYPE diskquota.diskquota_active_table_type AS (
	"TABLE_OID" oid,
	"TABLE_SIZE" int8
);

CREATE FUNCTION diskquota.set_schema_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota.so' LANGUAGE C;
CREATE FUNCTION diskquota.set_role_quota(text, text) RETURNS void STRICT AS '$libdir/diskquota.so' LANGUAGE C;
CREATE FUNCTION diskquota.update_diskquota_db_list(oid, int4) RETURNS void STRICT AS '$libdir/diskquota.so' LANGUAGE C;
CREATE FUNCTION diskquota.init_table_size_table() RETURNS void STRICT AS '$libdir/diskquota.so' LANGUAGE C;
CREATE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type AS '$libdir/diskquota.so', 'diskquota_fetch_table_stat' LANGUAGE C VOLATILE;

CREATE VIEW diskquota.show_fast_schema_quota_view AS
SELECT pgns.nspname AS schema_name, pgc.relnamespace AS schema_oid, qc.quotalimitMB AS quota_in_mb, SUM(ts.size) AS nspsize_in_bytes
FROM diskquota.table_size AS ts,
	pg_class AS pgc,
	diskquota.quota_config AS qc,
	pg_namespace AS pgns
WHERE ts.tableid = pgc.oid AND qc.targetoid = pgc.relnamespace AND pgns.oid = pgc.relnamespace
GROUP BY relnamespace, qc.quotalimitMB, pgns.nspname
ORDER BY pgns.nspname;

CREATE VIEW diskquota.show_fast_role_quota_view AS
SELECT pgr.rolname AS role_name, pgc.relowner AS role_oid, qc.quotalimitMB AS quota_in_mb, SUM(ts.size) AS rolsize_in_bytes
FROM diskquota.table_size AS ts,
	pg_class AS pgc,
	diskquota.quota_config AS qc,
	pg_roles AS pgr
WHERE pgc.relowner = qc.targetoid AND pgc.relowner = pgr.oid AND ts.tableid = pgc.oid
GROUP BY pgc.relowner, pgr.rolname, qc.quotalimitMB;

CREATE VIEW diskquota.show_fast_database_size_view AS
SELECT (
	(SELECT SUM(pg_relation_size(oid)) FROM pg_class WHERE oid <= 16384)
		+
	(SELECT SUM(size) FROM diskquota.table_size)
) AS dbsize;

-- prepare to boot
INSERT INTO diskquota.state SELECT (count(relname) = 0)::int FROM pg_class AS c, pg_namespace AS n WHERE c.oid > 16384 AND relnamespace = n.oid AND nspname != 'diskquota';

CREATE FUNCTION diskquota.diskquota_start_worker() RETURNS void STRICT AS '$libdir/diskquota.so' LANGUAGE C;
SELECT diskquota.diskquota_start_worker();
DROP FUNCTION diskquota.diskquota_start_worker();
