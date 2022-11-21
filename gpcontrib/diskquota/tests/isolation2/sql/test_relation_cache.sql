CREATE DATABASE tempdb1;
CREATE DATABASE tempdb2;

-- perpare extension
1:@db_name tempdb1: CREATE EXTENSION diskquota;
1:@db_name tempdb1: SELECT diskquota.wait_for_worker_new_epoch();
2:@db_name tempdb2: CREATE EXTENSION diskquota;
2:@db_name tempdb2: SELECT diskquota.wait_for_worker_new_epoch();

-- create a table in tempdb1
1:@db_name tempdb1: BEGIN;
1:@db_name tempdb1: CREATE TABLE t(i int);
1:@db_name tempdb1: INSERT INTO t select generate_series(1, 10000);

-- query relation_cache in tempdb2
2:@db_name tempdb2: SELECT count(*) from diskquota.show_relation_cache();

1:@db_name tempdb1: ABORT;

1:@db_name tempdb1: SELECT diskquota.pause();
1:@db_name tempdb1: SELECT diskquota.wait_for_worker_new_epoch();
1:@db_name tempdb1: DROP EXTENSION diskquota;
2:@db_name tempdb2: SELECT diskquota.pause();
2:@db_name tempdb2: SELECT diskquota.wait_for_worker_new_epoch();
2:@db_name tempdb2: DROP EXTENSION diskquota;
1q:
2q:

DROP DATABASE tempdb1;
DROP DATABASE tempdb2;
