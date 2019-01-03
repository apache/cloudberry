-- NOTE: when test this script, you must make sure that there is no diskquota launcher
-- process or diskquota worker process. i.e. `ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l`
-- returns 0
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

\! sleep 4
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx0
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx1
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
INSERT INTO SX.a values(generate_series(0, 100000));
CREATE EXTENSION diskquota;
SELECT diskquota.init_table_size_table();
SELECT diskquota.set_schema_quota('SX', '1MB');
\! sleep 5
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx2
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx3
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx4
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx5
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx6
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx7
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx8
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l
CREATE SCHEMA SX;
CREATE TABLE SX.a(i int);
SELECT diskquota.set_schema_quota('SX', '1MB');
INSERT INTO SX.a values(generate_series(0, 100000000));
INSERT INTO SX.a values(generate_series(0, 10));
DROP TABLE SX.a;

\c dbx9
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx10
CREATE EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx0
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx1
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx2
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx3
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx4
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx5
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx6
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx7
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx8
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx9
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c dbx10
DROP EXTENSION diskquota;
\! sleep 2
\! ps -ef | grep postgres | grep "\[diskquota]" | grep -v grep | wc -l

\c postgres

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
