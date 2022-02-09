-- Test schema
-- start_ignore
\! mkdir -p /tmp/schemaspc_perseg
-- end_ignore
-- Test tablespace quota perseg
CREATE SCHEMA spcs1_perseg;
DROP TABLESPACE  IF EXISTS schemaspc_perseg;
CREATE TABLESPACE schemaspc_perseg LOCATION '/tmp/schemaspc_perseg';
SELECT diskquota.set_schema_tablespace_quota('spcs1_perseg', 'schemaspc_perseg','1 MB');
SET search_path TO spcs1_perseg;

CREATE TABLE a(i int) TABLESPACE schemaspc_perseg;
INSERT INTO a SELECT generate_series(1,100);
-- expect insert success
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail by tablespace schema diskquota
INSERT INTO a SELECT generate_series(1,100);
-- change tablespace schema quota
SELECT diskquota.set_schema_tablespace_quota('spcs1_perseg', 'schemaspc_perseg', '10 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);
SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name = 'spcs1_perseg' and tablespace_name ='schemaspc_perseg';

SELECT diskquota.set_per_segment_quota('schemaspc_perseg', 0.1);
SELECT diskquota.wait_for_worker_new_epoch();
---- expect insert fail by tablespace schema perseg quota
INSERT INTO a SELECT generate_series(1,100);

-- Test alter table set schema
CREATE SCHEMA spcs2_perseg;
ALTER TABLE spcs1_perseg.a SET SCHEMA spcs2_perseg;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO spcs2_perseg.a SELECT generate_series(1,200);
ALTER TABLE spcs2_perseg.a SET SCHEMA spcs1_perseg;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,200);
SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name = 'spcs1_perseg' and tablespace_name ='schemaspc_perseg';

-- Test alter tablespace
-- start_ignore
\! mkdir -p /tmp/schemaspc_perseg2
-- end_ignore
DROP TABLESPACE  IF EXISTS schemaspc_perseg2;
CREATE TABLESPACE schemaspc_perseg2 LOCATION '/tmp/schemaspc_perseg2';
ALTER TABLE a SET TABLESPACE schemaspc_perseg2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,200);
ALTER TABLE a SET TABLESPACE schemaspc_perseg;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,200);

-- Test update per segment ratio
SELECT diskquota.set_per_segment_quota('schemaspc_perseg', 3.1);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);
SELECT diskquota.set_per_segment_quota('schemaspc_perseg', 0.123);
SELECT diskquota.wait_for_worker_new_epoch();
---- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- Test delete per segment ratio
SELECT diskquota.set_per_segment_quota('schemaspc_perseg', -1);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);
SELECT diskquota.set_per_segment_quota('schemaspc_perseg', 0.123);
SELECT diskquota.wait_for_worker_new_epoch();
---- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- Test delete tablespace schema quota
SELECT diskquota.set_per_segment_quota('schemaspc_perseg', 2);
SELECT diskquota.set_schema_tablespace_quota('spcs1_perseg', 'schemaspc_perseg','-1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);
SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name = 'spcs1_perseg' and tablespace_name ='schemaspc_perseg';

RESET search_path;
DROP TABLE spcs1_perseg.a;
DROP SCHEMA spcs1_perseg;
DROP TABLESPACE schemaspc_perseg;
DROP TABLESPACE schemaspc_perseg2;

