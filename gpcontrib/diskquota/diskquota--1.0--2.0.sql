ALTER TABLE diskquota.quota_config ADD COLUMN segratio float4 DEFAULT -1;

CREATE TABLE diskquota.target (
        quotatype int, --REFERENCES disquota.quota_config.quotatype,
        primaryOid oid,
        tablespaceOid oid, --REFERENCES pg_tablespace.oid,
        PRIMARY KEY (primaryOid, tablespaceOid, quotatype)
);

CREATE OR REPLACE FUNCTION diskquota.set_schema_tablespace_quota(text, text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.set_role_tablespace_quota(text, text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.set_per_segment_quota(text, float4)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.pause()
RETURNS void STRICT
AS 'MODULE_PATHNAME', 'diskquota_pause'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.resume()
RETURNS void STRICT
AS 'MODULE_PATHNAME', 'diskquota_resume'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.enable_hardlimit()
RETURNS void STRICT
AS 'MODULE_PATHNAME', 'diskquota_enable_hardlimit'
LANGUAGE C;

CREATE OR REPLACE FUNCTION diskquota.disable_hardlimit()
RETURNS void STRICT
AS 'MODULE_PATHNAME', 'diskquota_disable_hardlimit'
LANGUAGE C;

CREATE TYPE diskquota.blackmap_entry AS
  (target_oid oid, database_oid oid, tablespace_oid oid, target_type integer, seg_exceeded boolean);
CREATE FUNCTION diskquota.refresh_blackmap(diskquota.blackmap_entry[], oid[])
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE TYPE diskquota.blackmap_entry_detail AS
  (target_type text, target_oid oid, database_oid oid,
   tablespace_oid oid, seg_exceeded boolean, dbnode oid, spcnode oid, relnode oid, segid int);

CREATE FUNCTION diskquota.show_blackmap()
RETURNS setof diskquota.blackmap_entry_detail
AS 'MODULE_PATHNAME', 'show_blackmap'
LANGUAGE C;

CREATE VIEW diskquota.blackmap AS
  SELECT * FROM diskquota.show_blackmap() AS BM;

ALTER TABLE diskquota.table_size ADD COLUMN segid smallint DEFAULT -1;
ALTER TABLE diskquota.table_size DROP CONSTRAINT table_size_pkey;
ALTER TABLE diskquota.table_size ADD PRIMARY KEY (tableid,segid);

CREATE OR REPLACE VIEW diskquota.show_fast_schema_quota_view AS
select pgns.nspname as schema_name, pgc.relnamespace as schema_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as nspsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_namespace as pgns
where ts.tableid = pgc.oid and qc.targetoid = pgc.relnamespace and pgns.oid = pgc.relnamespace and qc.quotatype=0 and ts.segid=-1
group by relnamespace, qc.quotalimitMB, pgns.nspname
order by pgns.nspname;

CREATE OR REPLACE VIEW diskquota.show_fast_role_quota_view AS
select pgr.rolname as role_name, pgc.relowner as role_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as rolsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_roles as pgr
WHERE pgc.relowner = qc.targetoid and pgc.relowner = pgr.oid and ts.tableid = pgc.oid and qc.quotatype=1 and ts.segid=-1
GROUP BY pgc.relowner, pgr.rolname, qc.quotalimitMB;

CREATE OR REPLACE VIEW diskquota.show_fast_schema_tablespace_quota_view AS
select pgns.nspname as schema_name, pgc.relnamespace as schema_oid, pgsp.spcname as tablespace_name, pgc.reltablespace as tablespace_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as nspsize_tablespace_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_namespace as pgns,
	pg_tablespace as pgsp,
	diskquota.target as t
where ts.tableid = pgc.oid and qc.targetoid = pgc.relnamespace and pgns.oid = pgc.relnamespace and pgsp.oid = pgc.reltablespace and qc.quotatype=2 and qc.targetoid=t.primaryoid and t.tablespaceoid=pgc.reltablespace and ts.segid=-1
group by relnamespace, reltablespace, qc.quotalimitMB, pgns.nspname, pgsp.spcname
order by pgns.nspname, pgsp.spcname;

CREATE OR REPLACE VIEW diskquota.show_fast_role_tablespace_quota_view AS
select pgr.rolname as role_name, pgc.relowner as role_oid, pgsp.spcname as tablespace_name, pgc.reltablespace as tablespace_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as rolsize_tablespace_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_roles as pgr,
	pg_tablespace as pgsp,
        diskquota.target as t
WHERE pgc.relowner = qc.targetoid and pgc.relowner = pgr.oid and ts.tableid = pgc.oid and pgsp.oid = pgc.reltablespace and qc.quotatype=3 and qc.targetoid=t.primaryoid and t.tablespaceoid=pgc.reltablespace and ts.segid=-1
GROUP BY pgc.relowner, reltablespace, pgr.rolname, pgsp.spcname, qc.quotalimitMB;

CREATE OR REPLACE VIEW diskquota.show_fast_database_size_view AS
SELECT ((SELECT SUM(pg_relation_size(oid)) FROM pg_class WHERE oid <= 16384)+ (SELECT SUM(size) FROM diskquota.table_size WHERE segid = -1)) AS dbsize;

-- Need to drop the old type and functions, then recreate them to make the gpdb to reload the new functions
DROP FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]);
DROP TYPE diskquota.diskquota_active_table_type;
CREATE TYPE diskquota.diskquota_active_table_type AS ("TABLE_OID" oid,  "TABLE_SIZE" int8, "GP_SEGMENT_ID" smallint);
CREATE OR REPLACE FUNCTION diskquota.diskquota_fetch_table_stat(int4, oid[]) RETURNS setof diskquota.diskquota_active_table_type
AS 'MODULE_PATHNAME', 'diskquota_fetch_table_stat'
LANGUAGE C VOLATILE;

-- returns the current status in current database
CREATE OR REPLACE FUNCTION diskquota.status()
RETURNS TABLE ("name" text, "status" text) STRICT
AS 'MODULE_PATHNAME', 'diskquota_status'
LANGUAGE C;
