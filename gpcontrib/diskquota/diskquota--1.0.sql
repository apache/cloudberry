/* contrib/diskquota/diskquota--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION diskquota" to load this file. \quit

CREATE SCHEMA diskquota;

set search_path='diskquota';

-- Configuration table
create table diskquota.quota_config (targetOid oid, quotatype int, quotalimitMB int8, PRIMARY KEY(targetOid, quotatype));

SELECT pg_catalog.pg_extension_config_dump('diskquota.quota_config', '');

CREATE FUNCTION set_schema_quota(text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION set_role_quota(text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE VIEW show_schema_quota_view AS
SELECT pg_namespace.nspname as schema_name, pg_class.relnamespace as schema_oid, quota.quotalimitMB as quota_in_mb, sum(pg_total_relation_size(pg_class.oid)) as nspsize_in_bytes
FROM pg_namespace, pg_class, diskquota.quota_config as quota
WHERE pg_class.relnamespace = quota.targetoid and pg_class.relnamespace = pg_namespace.oid and quota.quotatype=0
GROUP BY pg_class.relnamespace, pg_namespace.nspname, quota.quotalimitMB;

CREATE VIEW show_role_quota_view AS
SELECT pg_roles.rolname as role_name, pg_class.relowner as role_oid, quota.quotalimitMB as quota_in_mb, sum(pg_total_relation_size(pg_class.oid)) as rolsize_in_bytes
FROM pg_roles, pg_class, diskquota.quota_config as quota
WHERE pg_class.relowner = quota.targetoid and pg_class.relowner = pg_roles.oid and quota.quotatype=1
GROUP BY pg_class.relowner, pg_roles.rolname, quota.quotalimitMB;

reset search_path;
