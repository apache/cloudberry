-- Test schema
-- start_ignore
\! mkdir -p /tmp/indexspc
-- end_ignore
CREATE SCHEMA indexschema1;
DROP TABLESPACE  IF EXISTS indexspc;
CREATE TABLESPACE indexspc LOCATION '/tmp/indexspc';
SET search_path TO indexschema1;

CREATE TABLE test_index_a(i int) TABLESPACE indexspc;
INSERT INTO test_index_a SELECT generate_series(1,20000);
SELECT diskquota.set_schema_tablespace_quota('indexschema1', 'indexspc','2 MB');
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name,tablespace_name,quota_in_mb,nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name='indexschema1' and tablespace_name='indexspc';
SELECT size, segid FROM diskquota.table_size , pg_class where tableid=oid and relname='test_index_a' and segid=-1;
-- create index for the table, index in default tablespace
CREATE INDEX a_index ON test_index_a(i);
INSERT INTO test_index_a SELECT generate_series(1,10000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO test_index_a SELECT generate_series(1,100);
SELECT schema_name,tablespace_name,quota_in_mb,nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name ='indexschema1' and tablespace_name='indexspc';
SELECT size, segid FROM diskquota.table_size , pg_class where tableid=oid and (relname='test_index_a' or relname='a_index') and segid=-1;
-- add index to tablespace indexspc
ALTER index a_index SET TABLESPACE indexspc;
SELECT diskquota.wait_for_worker_new_epoch();
SELECT schema_name,tablespace_name,quota_in_mb,nspsize_tablespace_in_bytes FROM diskquota.show_fast_schema_tablespace_quota_view WHERE schema_name ='indexschema1' and tablespace_name='indexspc';
SELECT size, segid FROM diskquota.table_size , pg_class where tableid=oid and (relname='test_index_a' or relname='a_index') and segid=-1;
-- expect insert fail
INSERT INTO test_index_a SELECT generate_series(1,100);

-- index tablespace quota exceeded 
ALTER table test_index_a SET TABLESPACE pg_default;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO test_index_a SELECT generate_series(1,100);
INSERT INTO test_index_a SELECT generate_series(1,200000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO test_index_a SELECT generate_series(1,100);
RESET search_path;
DROP INDEX indexschema1.a_index;
DROP TABLE indexschema1.test_index_a;
DROP SCHEMA indexschema1;
DROP TABLESPACE indexspc;
