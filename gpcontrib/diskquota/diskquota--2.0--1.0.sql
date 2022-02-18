DROP FUNCTION IF EXISTS diskquota.set_schema_tablespace_quota(text, text, text);

DROP FUNCTION IF EXISTS diskquota.set_role_tablespace_quota(text, text, text);

DROP FUNCTION IF EXISTS diskquota.set_per_segment_quota(text, float4);

DROP FUNCTION IF EXISTS diskquota.pause();

DROP FUNCTION IF EXISTS diskquota.resume();

DROP FUNCTION IF EXISTS diskquota.refresh_blackmap(diskquota.blackmap_entry[], oid[]);

DROP FUNCTION IF EXISTS diskquota.status();

DROP TYPE IF EXISTS diskquota.blackmap_entry;

DROP VIEW IF EXISTS diskquota.blackmap;

DROP FUNCTION IF EXISTS diskquota.show_blackmap();

DROP TYPE IF EXISTS diskquota.blackmap_entry_detail;

CREATE OR REPLACE VIEW diskquota.show_fast_schema_quota_view AS
select pgns.nspname as schema_name, pgc.relnamespace as schema_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as nspsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_namespace as pgns
where ts.tableid = pgc.oid and qc.targetoid = pgc.relnamespace and pgns.oid = pgc.relnamespace
group by relnamespace, qc.quotalimitMB, pgns.nspname
order by pgns.nspname;

CREATE OR REPLACE VIEW diskquota.show_fast_role_quota_view AS
select pgr.rolname as role_name, pgc.relowner as role_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as rolsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_roles as pgr
WHERE pgc.relowner = qc.targetoid and pgc.relowner = pgr.oid and ts.tableid = pgc.oid
GROUP BY pgc.relowner, pgr.rolname, qc.quotalimitMB;

DROP VIEW IF EXISTS diskquota.show_fast_schema_tablespace_quota_view;
DROP VIEW IF EXISTS diskquota.show_fast_role_tablespace_quota_view;

CREATE OR REPLACE VIEW diskquota.show_fast_database_size_view AS
SELECT ((SELECT SUM(pg_relation_size(oid)) FROM pg_class WHERE oid <= 16384)+ (SELECT SUM(size) FROM diskquota.table_size)) AS dbsize;

-- Need to drop the old type and functions, then recreate them to make the gpdb to reload the new functions
DROP FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]);
DROP TYPE diskquota.diskquota_active_table_type;
CREATE TYPE diskquota.diskquota_active_table_type AS ("TABLE_OID" oid,  "TABLE_SIZE" int8);
CREATE OR REPLACE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type
AS 'MODULE_PATHNAME', 'diskquota_fetch_table_stat'
LANGUAGE C VOLATILE;

DROP TABLE IF EXISTS diskquota.target;
ALTER TABLE diskquota.quota_config DROP COLUMN segratio;
-- clean table_size and frop segid column
-- delete segments table size
DELETE FROM diskquota.table_size WHERE segid != -1;
-- delete tablespace quota config
DELETE FROM diskquota.quota_config WHERE quotatype=2 or quotatype=3;
ALTER TABLE diskquota.table_size DROP CONSTRAINT table_size_pkey;
ALTER TABLE diskquota.table_size SET DISTRIBUTED RANDOMLY;
ALTER TABLE diskquota.table_size DROP COLUMN segid;
ALTER TABLE diskquota.table_size SET DISTRIBUTED BY (tableid);
ALTER TABLE diskquota.table_size ADD PRIMARY KEY (tableid);
