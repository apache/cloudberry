-- Test one session read tablespace segratio,
-- and at the same time, another session
-- update or insert the segratio

-- start_ignore
!\retcode mkdir -p /tmp/spc101;
-- end_ignore
CREATE SCHEMA s101;
DROP TABLESPACE IF EXISTS spc101;
CREATE TABLESPACE spc101 LOCATION '/tmp/spc101';

--
-- There is no tablesapce per segment quota configed yet
--

-- Read commited, first set_per_segment_quota, then set_schema_tablespace_quota
1: BEGIN;
1: SELECT diskquota.set_per_segment_quota('spc101', 1);
2: BEGIN;
2&: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;

-- Read commited, first set_schema_tablespace_quota, then set_per_segment_quota,
1: BEGIN;
1: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
2: BEGIN;
2&: SELECT diskquota.set_per_segment_quota('spc101', 1);
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;

--
-- There is already a tablesapce per segment quota configed
--

-- Read commited, first set_per_segment_quota, then set_schema_tablespace_quota
SELECT diskquota.set_per_segment_quota('spc101', 2);
1: BEGIN;
1: SELECT diskquota.set_per_segment_quota('spc101', 1);
2: BEGIN;
2&: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;

-- Read commited, first set_schema_tablespace_quota, then set_per_segment_quota,
SELECT diskquota.set_per_segment_quota('spc101', 2);
1: BEGIN;
1: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
2: BEGIN;
2&: SELECT diskquota.set_per_segment_quota('spc101', 1);
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;

-- Read commited, first delete per_segment_quota, then set_schema_tablespace_quota
SELECT diskquota.set_per_segment_quota('spc101', 2);
1: BEGIN;
1: SELECT diskquota.set_per_segment_quota('spc101', -1);
2: BEGIN;
2&: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;

-- Read commited, first set_schema_tablespace_quota, then delete tablespace per segment ratio
SELECT diskquota.set_per_segment_quota('spc101', 2);
1: BEGIN;
1: SELECT diskquota.set_schema_tablespace_quota('s101', 'spc101','1 MB');
2: BEGIN;
2&: SELECT diskquota.set_per_segment_quota('spc101', -1);
1: COMMIT;
2<:
2: COMMIT;

SELECT segratio FROM diskquota.quota_config, pg_namespace, diskquota.target
 WHERE targetoid = diskquota.target.rowId AND diskquota.target.primaryOid = oid AND nspname = 's101';
SELECT segratio from diskquota.quota_config, pg_tablespace where targetoid = oid and spcname = 'spc101';
-- cleanup
truncate table diskquota.quota_config;
truncate table diskquota.target;
DROP SCHEMA s101;
DROP TABLESPACE spc101;
