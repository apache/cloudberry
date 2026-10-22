CREATE EXTENSION gp_relaccess_stats;

-- get rid of NOTICEs
SET client_min_messages TO WARNING;
SET search_path TO relaccess;
DROP TABLE IF EXISTS tbl1 CASCADE;
DROP TABLE IF EXISTS tbl2 CASCADE;
DROP TABLE IF EXISTS tbl3 CASCADE;
DROP TABLE IF EXISTS tbl4 CASCADE;
DROP TABLE IF EXISTS new_tbl1 CASCADE;
DROP TABLE IF EXISTS p3_sales CASCADE;
DROP TABLE IF EXISTS public.last_usr_checks CASCADE;
DROP USER IF EXISTS select_usr;
DROP USER IF EXISTS update_usr;
DROP USER IF EXISTS insert_usr;
DROP USER IF EXISTS delete_usr;
DROP USER IF EXISTS truncate_usr;

-- make sure tracking is ON
SET gp_relaccess_stats.enabled TO 'on';
SELECT relaccess_stats_init();
SELECT relaccess_stats_update();
TRUNCATE relaccess_stats;

-- test simple actions one by one in separate transactions
CREATE TABLE tbl1 (a INTEGER);

INSERT INTO tbl1 VALUES(1);
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1';

SELECT * FROM tbl1;
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1';

UPDATE tbl1 SET a = -a;
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1';

DELETE FROM tbl1 WHERE a < 0;
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1';

TRUNCATE tbl1;
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1';

-- verify that rename table works
ALTER TABLE tbl1 RENAME TO new_tbl1;
INSERT INTO new_tbl1 VALUES(1);
SELECT relaccess_stats_update();
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relname = 'tbl1';
SELECT n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats WHERE relid = 'new_tbl1'::regclass::oid AND relname = 'new_tbl1';

TRUNCATE relaccess_stats; 
-- multitable truncate
CREATE TABLE tbl1 (a integer);
CREATE TABLE tbl2 (a integer);
TRUNCATE tbl1, tbl2;
SELECT relaccess_stats_update();
SELECT relname, n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries FROM relaccess_stats
    WHERE relid = 'tbl1'::regclass::oid AND relname = 'tbl1' OR relid = 'tbl2'::regclass::oid AND relname = 'tbl2' ORDER BY relname;

TRUNCATE relaccess_stats; 
-- test a more complicated statement
CREATE TABLE tbl3 (a integer);
CREATE TABLE tbl4 (a integer);

BEGIN;
-- should give +1 insert for tbl1 and +1 select for other tables
INSERT INTO tbl1 SELECT * FROM tbl2 UNION SELECT * FROM tbl3 UNION SELECT * FROM tbl4;
-- nothing in there before we commit
SELECT relaccess_stats_update();
SELECT COUNT(*) FROM relaccess_stats WHERE relname LIKE ('tbl_');
COMMIT;
SELECT relaccess_stats_update();
SELECT relname, n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries 
    FROM relaccess_stats WHERE relname LIKE ('tbl_') AND relname::regclass::oid = relid ORDER BY relname;

TRUNCATE relaccess_stats; 
-- test views
CREATE VIEW v1_2_3 AS (SELECT * FROM tbl2 UNION SELECT * FROM tbl3 UNION SELECT * FROM tbl4);
INSERT INTO tbl1 SELECT * FROM v1_2_3;
SELECT relaccess_stats_update();
SELECT relname, n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries 
    FROM relaccess_stats WHERE relname = 'v1_2_3' OR relname LIKE ('tbl_') ORDER BY relname;

TRUNCATE relaccess_stats;
-- test timestamps difference
BEGIN;
INSERT INTO tbl1 VALUES (1);
SELECT pg_sleep(1);
SELECT COUNT(*) FROM tbl1;
COMMIT;
SELECT relaccess_stats_update();
SELECT EXTRACT(EPOCH FROM (last_read - last_write)) >= 1 FROM relaccess_stats WHERE relname = 'tbl1' AND relid = 'tbl1'::regclass::oid;
TRUNCATE relaccess_stats;

-- test nested partitions lookup
BEGIN;
CREATE TABLE p3_sales (id int, year int, month int, day int, 
                       region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (1) END (13) EVERY (1), 
        DEFAULT SUBPARTITION other_months )
           SUBPARTITION BY LIST (region)
             SUBPARTITION TEMPLATE (
               SUBPARTITION usa VALUES ('usa'),
               SUBPARTITION europe VALUES ('europe'),
               SUBPARTITION asia VALUES ('asia'),
               DEFAULT SUBPARTITION other_regions )
( START (2002) END (2012) EVERY (1), 
  DEFAULT PARTITION outlying_years );
-- 3 inserts into p3_sales root table
INSERT INTO p3_sales SELECT i, i%43+1980, i%12, i%25, 'asia' FROM generate_series(1, 100)i;
INSERT INTO p3_sales SELECT i, i%43+1980, i%12, i%25, 'europe' FROM generate_series(1, 100)i;
INSERT INTO p3_sales SELECT i, i%43+1980, i%12, i%25, 'usa' FROM generate_series(1, 100)i;
-- insert and select to/from specific leaf level partition
INSERT INTO p3_sales_1_prt_11_2_prt_12_3_prt_usa SELECT * FROM p3_sales_1_prt_11_2_prt_12_3_prt_usa;
COMMIT;
SELECT relaccess_stats_update();
SELECT relname, n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries 
FROM relaccess_stats WHERE relname LIKE 'p3_sales%' ORDER BY relname;
SELECT relname, n_select_queries, n_insert_queries, n_update_queries, n_delete_queries, n_truncate_queries 
FROM relaccess_stats_root_tables_aggregated WHERE relname LIKE 'p3_sales%' ORDER BY relname;

-- test last_reader and last_writer
CREATE USER select_usr;
CREATE USER update_usr;
CREATE USER insert_usr;
CREATE USER delete_usr;
CREATE USER truncate_usr;
CREATE TABLE public.last_usr_checks(a integer);
GRANT ALL ON TABLE public.last_usr_checks TO select_usr, update_usr, insert_usr, delete_usr, truncate_usr;
SET ROLE select_usr;
SELECT COUNT(*) FROM public.last_usr_checks;
RESET ROLE;
SELECT relaccess_stats_update();
SELECT (SELECT last_reader_id FROM relaccess_stats WHERE RELNAME = 'last_usr_checks') = (SELECT oid FROM pg_roles WHERE rolname = 'select_usr');
SET ROLE insert_usr;
INSERT INTO public.last_usr_checks VALUES (-1), (0), (1);
RESET ROLE;
SELECT relaccess_stats_update();
SELECT (SELECT last_writer_id FROM relaccess_stats WHERE RELNAME = 'last_usr_checks') = (SELECT oid FROM pg_roles WHERE rolname = 'insert_usr');
SET ROLE update_usr;
UPDATE public.last_usr_checks SET a = a*10 WHERE a < 0;
RESET ROLE;
SELECT relaccess_stats_update();
SELECT (SELECT last_writer_id FROM relaccess_stats WHERE RELNAME = 'last_usr_checks') = (SELECT oid FROM pg_roles WHERE rolname = 'update_usr');
SET ROLE delete_usr;
DELETE FROM public.last_usr_checks WHERE a >= 0;
RESET ROLE;
SELECT relaccess_stats_update();
SELECT (SELECT last_writer_id FROM relaccess_stats WHERE RELNAME = 'last_usr_checks') = (SELECT oid FROM pg_roles WHERE rolname = 'delete_usr');
SET ROLE truncate_usr;
TRUNCATE public.last_usr_checks;
RESET ROLE;
SELECT relaccess_stats_update();
SELECT (SELECT last_writer_id FROM relaccess_stats WHERE RELNAME = 'last_usr_checks') = (SELECT oid FROM pg_roles WHERE rolname = 'truncate_usr');
RESET ROLE;

-- make sure we can turn it OFF
SET gp_relaccess_stats.enabled TO 'off';
SELECT relaccess_stats_update();
TRUNCATE relaccess_stats;
SELECT * FROM tbl1;
SELECT relaccess_stats_update();
SELECT count(*) FROM relaccess_stats;
RESET gp_relaccess_stats.enabled;

DROP TABLE tbl1 CASCADE;
DROP TABLE tbl2 CASCADE;
DROP TABLE tbl3 CASCADE;
DROP TABLE tbl4 CASCADE;
DROP TABLE new_tbl1 CASCADE;
DROP TABLE p3_sales CASCADE;
DROP TABLE public.last_usr_checks CASCADE;
DROP USER select_usr;
DROP USER update_usr;
DROP USER insert_usr;
DROP USER delete_usr;
DROP USER truncate_usr;

