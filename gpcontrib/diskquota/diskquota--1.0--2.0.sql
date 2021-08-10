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

CREATE OR REPLACE VIEW diskquota.show_fast_schema_quota_view AS
select pgns.nspname as schema_name, pgc.relnamespace as schema_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as nspsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_namespace as pgns
where ts.tableid = pgc.oid and qc.targetoid = pgc.relnamespace and pgns.oid = pgc.relnamespace and qc.quotatype=0
group by relnamespace, qc.quotalimitMB, pgns.nspname
order by pgns.nspname;

CREATE OR REPLACE VIEW diskquota.show_fast_role_quota_view AS
select pgr.rolname as role_name, pgc.relowner as role_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as rolsize_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_roles as pgr
WHERE pgc.relowner = qc.targetoid and pgc.relowner = pgr.oid and ts.tableid = pgc.oid and qc.quotatype=1
GROUP BY pgc.relowner, pgr.rolname, qc.quotalimitMB;

CREATE OR REPLACE VIEW diskquota.show_fast_schema_tablespace_quota_view AS
select pgns.nspname as schema_name, pgc.relnamespace as schema_oid, pgsp.spcname as tablespace_name, pgc.reltablespace as tablespace_oid, qc.quotalimitMB as quota_in_mb, sum(ts.size) as nspsize_tablespcae_in_bytes
from diskquota.table_size as ts,
        pg_class as pgc,
        diskquota.quota_config as qc,
        pg_namespace as pgns,
	pg_tablespace as pgsp,
	diskquota.target as t
where ts.tableid = pgc.oid and qc.targetoid = pgc.relnamespace and pgns.oid = pgc.relnamespace and pgsp.oid = pgc.reltablespace and qc.quotatype=2 and qc.targetoid=t.primaryoid and t.tablespaceoid=pgc.reltablespace
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
WHERE pgc.relowner = qc.targetoid and pgc.relowner = pgr.oid and ts.tableid = pgc.oid and pgsp.oid = pgc.reltablespace and qc.quotatype=3 and qc.targetoid=t.primaryoid and t.tablespaceoid=pgc.reltablespace
GROUP BY pgc.relowner, reltablespace, pgr.rolname, pgsp.spcname, qc.quotalimitMB;
