-- need 'contrib_regression' as test database
\c

CREATE SCHEMA s1;
SET search_path TO s1;
CREATE DATABASE test_pause_and_resume;
CREATE DATABASE test_new_create_database;

\c test_pause_and_resume
CREATE SCHEMA s1;
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch();

\c contrib_regression
CREATE TABLE s1.a(i int);
INSERT INTO s1.a SELECT generate_series(1,100000); -- expect insert succeed

\c test_pause_and_resume
CREATE TABLE s1.a(i int);
INSERT INTO s1.a SELECT generate_series(1,100000); -- expect insert succeed

\c contrib_regression
SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert fail

\c test_pause_and_resume
SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert fail

\c contrib_regression
SELECT diskquota.pause(); -- pause extension, onle effect current database
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 's1.a'::regclass AND segid = -1;
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert succeed

\c test_pause_and_resume
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 's1.a'::regclass AND segid = -1;
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert fail
SELECT diskquota.pause(); -- pause extension, onle effect current database
SELECT diskquota.wait_for_worker_new_epoch();
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 's1.a'::regclass AND segid = -1;
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert succeed

\c test_new_create_database;
CREATE SCHEMA s1;
CREATE EXTENSION diskquota;
SELECT diskquota.wait_for_worker_new_epoch(); -- new database should be active although other database is paused
CREATE TABLE s1.a(i int);
INSERT INTO s1.a SELECT generate_series(1,100000); -- expect insert succeed
SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100000); -- expect insert fail
SELECT diskquota.pause(); -- pause extension, onle effect current database
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert succeed

-- resume should onle effect current database
SELECT diskquota.resume();
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert fail

\c contrib_regression
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert succeed
SELECT diskquota.resume();
SELECT diskquota.wait_for_worker_new_epoch();
INSERT INTO s1.a SELECT generate_series(1,100); -- expect insert fail

\c test_pause_and_resume
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c test_new_create_database
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;

\c contrib_regression
DROP SCHEMA s1 CASCADE;
DROP DATABASE test_pause_and_resume;
DROP DATABASE test_new_create_database;
