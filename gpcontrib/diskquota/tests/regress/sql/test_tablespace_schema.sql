-- Test schema
-- start_ignore
\! mkdir -p /tmp/schemaspc
-- end_ignore
CREATE SCHEMA spcs1;
DROP TABLESPACE  IF EXISTS schemaspc;
CREATE TABLESPACE schemaspc LOCATION '/tmp/schemaspc';
SET search_path TO spcs1;

CREATE TABLE a(i int) TABLESPACE schemaspc DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_tablespace_quota('spcs1', 'schemaspc','1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);
CREATE TABLE a2(i int) TABLESPACE schemaspc DISTRIBUTED BY (i);
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,100);

-- Test alter table set schema
CREATE SCHEMA spcs2;
ALTER TABLE spcs1.a SET SCHEMA spcs2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO a2 SELECT generate_series(1,200);
-- expect insert succeed
INSERT INTO spcs2.a SELECT generate_series(1,200);
ALTER TABLE spcs2.a SET SCHEMA spcs1;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,200);
SELECT schema_name, tablespace_name, quota_in_mb, nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name = 'spcs1' and tablespace_name ='schemaspc';

-- Test alter tablespace
-- start_ignore
\! mkdir -p /tmp/schemaspc2
-- end_ignore
DROP TABLESPACE  IF EXISTS schemaspc2;
CREATE TABLESPACE schemaspc2 LOCATION '/tmp/schemaspc2';
ALTER TABLE a SET TABLESPACE schemaspc2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,200);
ALTER TABLE a SET TABLESPACE schemaspc;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,200);

-- Test update quota config
SELECT diskquota.set_schema_tablespace_quota('spcs1', 'schemaspc', '10 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);
-- expect insert success
INSERT INTO a SELECT generate_series(1,1000000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- Test delete quota config
SELECT diskquota.set_schema_tablespace_quota('spcs1', 'schemaspc', '-1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO a SELECT generate_series(1,100);

RESET search_path;
DROP TABLE spcs1.a2, spcs1.a;
DROP SCHEMA spcs1, spcs2;
DROP TABLESPACE schemaspc;
DROP TABLESPACE schemaspc2;

