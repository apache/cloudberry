-- start_ignore
\c

DROP DATABASE IF EXISTS t1;
DROP DATABASE IF EXISTS t2;
DROP DATABASE IF EXISTS t3;
DROP DATABASE IF EXISTS t4;
DROP DATABASE IF EXISTS t5;
DROP DATABASE IF EXISTS t6;
DROP DATABASE IF EXISTS t7;
DROP DATABASE IF EXISTS t8;
DROP DATABASE IF EXISTS t9;
DROP DATABASE IF EXISTS t10;
DROP DATABASE IF EXISTS t11;
DROP DATABASE IF EXISTS t12;
CREATE DATABASE t1;
CREATE DATABASE t2;
CREATE DATABASE t3;
CREATE DATABASE t4;
CREATE DATABASE t5;
CREATE DATABASE t6;
CREATE DATABASE t7;
CREATE DATABASE t8;
CREATE DATABASE t9;
CREATE DATABASE t10;
CREATE DATABASE t11;
CREATE DATABASE t12;
--end_ignore
\c t1
CREATE EXTENSION diskquota;
CREATE TABLE f1(a int);
INSERT into f1 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f1'::regclass and segid = -1;

--start_ignore
\! gpconfig -c diskquota.max_workers -v 1;
\! gpstop -arf;
--end_ignore

\c 
SHOW diskquota.max_workers;

\c t2
CREATE EXTENSION diskquota;
CREATE TABLE f2(a int);
INSERT into f2 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f2'::regclass and segid = -1;

\c t3
CREATE EXTENSION diskquota;
CREATE TABLE f3(a int);
INSERT into f3 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f3'::regclass and segid = -1;

--start_ignore
\! gpconfig -c diskquota.max_workers -v 11;
\! gpstop -arf;
--end_ignore

\c 
SHOW diskquota.max_workers;

\c t4
CREATE EXTENSION diskquota;
CREATE TABLE f4(a int);
INSERT into f4 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f4'::regclass and segid = -1;

\c t5
CREATE EXTENSION diskquota;
CREATE TABLE f5(a int);
INSERT into f5 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f5'::regclass and segid = -1;

\c t6
CREATE EXTENSION diskquota;
CREATE TABLE f6(a int);
INSERT into f6 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f6'::regclass and segid = -1;

\c t7
CREATE EXTENSION diskquota;
CREATE TABLE f7(a int);
INSERT into f7 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f7'::regclass and segid = -1;

\c t8
CREATE EXTENSION diskquota;
CREATE TABLE f8(a int);
INSERT into f8 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f8'::regclass and segid = -1;

\c t9
CREATE EXTENSION diskquota;
CREATE TABLE f9(a int);
INSERT into f9 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f9'::regclass and segid = -1;

\c t10
CREATE EXTENSION diskquota;
CREATE TABLE f10(a int);
INSERT into f10 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f10'::regclass and segid = -1;

\c t11
CREATE EXTENSION diskquota;
CREATE TABLE f11(a int);
INSERT into f11 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f11'::regclass and segid = -1;

\c t1
INSERT into f1 SELECT generate_series(0,100000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f1'::regclass and segid = -1;

\c t7
INSERT into f7 SELECT generate_series(0,100000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f7'::regclass and segid = -1;

\c t1
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
DROP TABLE f1;
CREATE EXTENSION diskquota;
CREATE TABLE f1(a int);
INSERT into f1 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f1'::regclass and segid = -1;

\c t2
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
DROP TABLE f2;
CREATE EXTENSION diskquota;
CREATE TABLE f2(a int);
INSERT into f2 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f2'::regclass and segid = -1;

\c t3
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t4
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t5
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t6
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t7
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t8
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t9
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t10
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t11
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c t12
CREATE EXTENSION diskquota;
CREATE TABLE f12(a int);
INSERT into f12 SELECT generate_series(0,1000);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'f12'::regclass and segid = -1;
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c t1
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\c t2
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
--start_ignore
\c contrib_regression
DROP DATABASE t1;
DROP DATABASE t2;
DROP DATABASE t3;
DROP DATABASE t4;
DROP DATABASE t5;
DROP DATABASE t6;
DROP DATABASE t7;
DROP DATABASE t8;
DROP DATABASE t9;
DROP DATABASE t10;
DROP DATABASE t11;
DROP DATABASE t12;
\! gpconfig -r diskquota.worker_timeout;
\! gpconfig -r diskquota.max_workers;
\! gpstop -arf;
--end_ignore
