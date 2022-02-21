-- NOTE: when test this script, you must make sure that there is no diskquota
-- worker process.
CREATE DATABASE dbx0 ;
CREATE DATABASE dbx1 ;
CREATE DATABASE dbx2 ;
CREATE DATABASE dbx3 ;
CREATE DATABASE dbx4 ;
CREATE DATABASE dbx5 ;
CREATE DATABASE dbx6 ;
CREATE DATABASE dbx7 ;
CREATE DATABASE dbx8 ;
CREATE DATABASE dbx9 ;
CREATE DATABASE dbx10 ;

show max_worker_processes;

\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

-- FIXME: We need to sleep for a while each time after CREATE EXTENSION and
-- DROP EXTENSION to wait for the bgworker to start or to exit.

\c dbx0
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx1
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
INSERT INTO SX.a values(generate_series(0, 100000));
CREATE EXTENSION diskquota;
SELECT diskquota.init_table_size_table();
SELECT diskquota.wait_for_worker_new_epoch();
SELECT diskquota.set_schema_quota('SX', '1MB');
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx2
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx3
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx4
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx5
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx6
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx7
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx8
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int) DISTRIBUTED BY (i);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000));
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx9
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();

\c dbx10
CREATE EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
SELECT diskquota.wait_for_worker_new_epoch();

\c dbx0
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx1
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx2
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx3
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx4
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx5
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx6
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx7
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx8
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx9
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx10
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
\! sleep 0.5; ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c contrib_regression

DROP DATABASE dbx0 ;
DROP DATABASE dbx1 ;
DROP DATABASE dbx2 ;
DROP DATABASE dbx3 ;
DROP DATABASE dbx4 ;
DROP DATABASE dbx5 ;
DROP DATABASE dbx6 ;
DROP DATABASE dbx7 ;
DROP DATABASE dbx8 ;
DROP DATABASE dbx9 ;
DROP DATABASE dbx10 ;
