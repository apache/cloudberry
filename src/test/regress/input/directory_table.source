--
-- Test for directory table
--

-- Display pg_tablespace, pg_directory_table, gp_storage_server, gp_storage_user_mapping catalog
\d+ pg_tablespace;
\d+ pg_directory_table;
\d+ gp_storage_server;
\d+ gp_storage_user_mapping;

SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'pg_directory_table';
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'gp_storage_server';
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'gp_storage_user_mapping';

-- CREATE TABLESPACE
CREATE TABLESPACE directory_tblspc LOCATION '@testtablespace@';

-- CREATE DATABASE
CREATE DATABASE dirtable_db;
\c dirtable_db
\d+ pg_directory_table;
\d+ gp_storage_server;
\d+ gp_storage_user_mapping;

SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'pg_directory_table';
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'gp_storage_server';
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname = 'gp_storage_user_mapping';
\c regression

-- CREATE USER for directory table
CREATE USER test_dirtable1;
CREATE USER test_dirtable2;
CREATE USER test_dirtable3;
CREATE USER test_dirtable4;

-- Test CREATE STORAGE SERVER
SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
CREATE STORAGE SERVER oss_server1;
CREATE STORAGE SERVER oss_server2 OPTIONS(protocal 'localhost');
CREATE STORAGE SERVER oss_server3 OPTIONS(endpoint '127.0.0.1:9000');
CREATE STORAGE SERVER oss_server4 OPTIONS(https 'true');
CREATE STORAGE SERVER oss_server5 OPTIONS(virtual_host 'false');
CREATE STORAGE SERVER oss_server6 OPTIONS(protocol 'qingstor', endpoint 'pek3b,qingstor.com');
CREATE STORAGE SERVER oss_server7 OPTIONS(https 'false', virtual_host 'true');
CREATE STORAGE SERVER oss_server8 OPTIONS(protocol 'hdfs', namenode '127.0.0.1:8020');
CREATE STORAGE SERVER oss_server9 OWNER TO postgres;    -- fail
CREATE STORAGE SERVER IF NOT EXISTS oss_server10;
CREATE STORAGE SERVER IF NOT EXISTS oss_server11 OPTIONS(protocol 's3av2');
CREATE STORAGE SERVER IF NOT EXISTS oss_server12 OPTIONS(protocol 's3av2', endpoint '127.0.0.1:9000', https 'false');
CREATE STORAGE SERVER IF NOT EXISTS oss_server13 OWNER TO postgres;   -- fail

SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c dirtable_db
SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c regression

-- Test ALTER STORAGE SERVER
ALTER STORAGE SERVER oss_server1 OPTIONS(protocol 'aws');
ALTER STORAGE SERVER oss_server1 OPTIONS(protocol 'test');  -- fail
ALTER STORAGE SERVER oss_server2 OPTIONS(https 'true');
ALTER STORAGE SERVER oss_server2 OPTIONS(https 'false', virtual_host 'true');   -- fail
ALTER STORAGE SERVER oss_server2 OPTIONS(virtual_host 'true');
ALTER STORAGE SERVER oss_server3 OPTIONS(endpoint '192.168.0.1');   -- fail
ALTER STORAGE SERVER oss_server4 OPTIONS(protocol 'localhost', virtual_host 'true');
ALTER STORAGE SERVER oss_server4 OPTIONS(protocol 'qingstor');  -- fail
ALTER STORAGE SERVER oss_server5;   -- fail
ALTER STORAGE SERVER oss_server6 OWNER TO postgres; -- fail
ALTER STORAGE SERVER IF EXISTS oss_server7 OPTIONS(endpoint '127.0.0.1:6555');    -- fail
ALTER STORAGE SERVER IF NOT EXISTS oss_server8 OPTIONS(virtual_host 'true');    -- fail

SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c dirtable_db
SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c regression

-- Test CREATE STORAGE USER MAPPING
CREATE STORAGE USER MAPPING FOR CURRENT_USER;   -- fail

CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server1;

CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server1
OPTIONS (accesskey 'KGFQWEFQQEFXVAEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');   -- fail

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR CURRENT_USER STORAGE SERVER oss_server1;

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR CURRENT_USER STORAGE SERVER oss_server1
OPTIONS (auth_method 'simple');

CREATE STORAGE USER MAPPING FOR CURRENT_ROLE STORAGE SERVER oss_server2
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '0ADQiAxcaUJ2lMHipis80hsUEhdiqui82JhduOKE');

CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server2
OPTIONS (accesskey 'KGFQWEFQQEFXVAEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');   -- fail

CREATE STORAGE USER MAPPING FOR CURRENT_ROLE STORAGE SERVER oss_server3
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '0ADQiAxcaUJ2lMHipis80hsUEhdiqui82JhduOKE');

CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_not_exits;  -- fail

CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_not_exits
OPTIONS (accesskey 'KGFQWEFQQEFXVAEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');   -- fail

CREATE STORAGE USER MAPPING FOR test_dirtable1 STORAGE SERVER oss_server1;

CREATE STORAGE USER MAPPING FOR test_dirtable1 STORAGE SERVER oss_server1
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '7KieQKdu02jHEUEjsqUYE83mx9OE2kdj2SJ72h6H');   -- fail

CREATE STORAGE USER MAPPING FOR test_dirtable1 STORAGE SERVER oss_server2
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '7KieQKdu02jHEUEjsqUYE83mx9OE2kdj2SJ72h6H');

CREATE STORAGE USER MAPPING FOR test_dirtable1 STORAGE SERVER oss_server3
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '7KieQKdu02jHEUEjsqUYE83mx9OE2kdj2SJ72h6H');

CREATE STORAGE USER MAPPING FOR no_exist_user STORAGE SERVER oss_server1;   -- fail

CREATE STORAGE USER MAPPING FOR no_exist_user STORAGE SERVER oss_server1
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '7KieQKdu02jHEUEjsqUYE83mx9OE2kdj2SJ72h6H');   -- fail

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR CURRENT_USER STORAGE SERVER oss_server1
OPTIONS (accesskey 'KGFQWEFQQEFXVAEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');   -- skip

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR test_dirtable2 STORAGE SERVER oss_server3
OPTIONS (endpoint '127.0.0.1:6555');

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR test_dirtable3 STORAGE SERVER oss_server8
OPTIONS (auth_method 'simple');

CREATE STORAGE USER MAPPING IF NOT EXISTS FOR no_exist_user STORAGE SERVER oss_server1;   -- fail

SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c dirtable_db
SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c regression

-- Test ALTER STORAGE USER MAPPING
ALTER STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server1; -- fail

ALTER STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server1
OPTIONS (accesskey 'KGFQWEFQQEFXVAEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');

ALTER STORAGE USER MAPPING FOR CURRENT_ROLE STORAGE SERVER oss_server1
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ'); -- fail

ALTER STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server1
OPTIONS (auth_method 'simple');

ALTER STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server2
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '0ADQiAxcaUJ2lMHipis80hsUEhdiqui82JhduOKE');   -- fail

ALTER STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER server_not_exists
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '0ADQiAxcaUJ2lMHipis80hsUEhdiqui82JhduOKE');   -- fail

ALTER STORAGE USER MAPPING IF EXISTS FOR no_exist_user STORAGE SERVER oss_server1
OPTIONS (accesskey 'EQEJIOJFAKQWESQEJIWQ', secretkey '0ADQiAxcaUJ2lMHipis80hsUEhdiqui82JhduOKE');   -- fail

ALTER STORAGE USER MAPPING IF NOT EXISTS FOR test_dirtable1 STORAGE SERVER oss_server3
OPTIONS (auth_method 'simple'); -- fail

SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c dirtable_db
SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c regression

-- Test DROP STORAGE USER MAPPING
DROP STORAGE USER MAPPING FOR CURRENT_USER; -- fail
DROP STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER oss_server2;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable1 STORAGE SERVER oss_server1;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable2 STORAGE SERVER no_exist_server;
DROP STORAGE USER MAPPING FOR test_dirtable3 STORAGE SERVER no_exist_server;    -- fail
DROP STORAGE USER MAPPING FOR no_exist_user STORAGE SERVER oss_server1; -- fail
DROP STORAGE USER MAPPING IF EXISTS FOR no_exist_user STORAGE SERVER oss_server1;   -- skip

SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c dirtable_db
SELECT umoptions FROM gp_storage_user_mapping ORDER BY 1;
\c regression

-- Test DROP STOARGE SERVER
DROP STORAGE SERVER oss_server1;    -- fail
DROP STORAGE SERVER oss_server2;    -- fail
DROP STORAGE SERVER oss_server3;    -- fail
DROP STORAGE SERVER oss_server4;    -- fail
DROP STORAGE SERVER oss_server8;    -- fail
DROP STORAGE SERVER oss_server9;    -- fail
DROP STAROGE SERVER IF EXISTS oss_server9;  -- fail
DROP STORAGE SERVER IF NOT EXISTS oss_server9;  --fail
DROP STORAGE SERVER IF EXISTS oss_server10;

SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c dirtable_db
SELECT srvname, srvacl, srvoptions from gp_storage_server ORDER BY 1;
\c regression

-- Test directory table
-- Test CREATE DIRECTORY TABLE
SELECT count(*) FROM pg_directory_table;
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname like '%dir_table%' ORDER BY 1;

CREATE DIRECTORY TABLE dir_table1;
CREATE DIRECTORY TABLE dir_table2 TABLESPACE directory_tblspc;
CREATE DIRECTORY TABLE dir_table3 TABLESPACE directory_tblspc DISTRIBUTED BY(relative_path);    -- fail
CREATE DIRECTORY TABLE dir_table3 TABLESPACE directory_tblspc DISTRIBUTED RANDOMLY; -- fail
CREATE DIRECTORY TABLE dir_table3 TABLESPACE directory_tblspc DISTRIBUTED REPLICATED;   -- fail
CREATE DIRECTORY TABLE dir_table3 TABLESPACE directory_tblspc;
CREATE DIRECTORY TABLE IF NOT EXISTS dir_table4 TABLESPACE directory_tblspc;
CREATE DIRECTORY TABLE IF NOT EXISTS dir_table2 TABLESPACE directory_tblspc;    -- fail
CREATE DIRECTORY TABLE dir_table5 TABLESPACE directory_tblspc;
CREATE DIRECTORY TABLE dir_table6 TABLESPACE pg_default;
CREATE DIRECTORY TABLE "abs.dir_table";

SELECT count(*) FROM pg_directory_table;
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname like '%dir_table%' ORDER BY 1;
\d+ dir_table1;
\d+ dir_table2;
\d+ dir_table3;
\c dirtable_db
SELECT count(*) FROM pg_directory_table;
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname like '%dir_table%' ORDER BY 1;
\c regression

-- Test create table inherits directory table
CREATE TABLE test_inherits1 INHERITS(dir_table1);   -- fail
CREATE TABLE test_inherits2(a int) INHERITS(dir_table2);    -- fail
CREATE TABLE test_inherits3 INHERITS(dir_table2, dir_table3);   -- fail
CREATE TABLE test_inherits4(b text) INHERITS(dir_table1, dir_table2);   -- fail

-- Test DROP DIRECTORY TABLE
DROP DIRECTORY TABLE dir_table4;
DROP DIRECTORY TABLE dir_table4;
DROP DIRECTORY TABLE IF EXISTS dir_table5;
DROP DIRECTORY TABLE IF EXISTS dir_table5;  -- skip

SELECT count(*) FROM pg_directory_table;
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname like '%dir_table%' ORDER BY 1;
\c dirtable_db
SELECT count(*) FROM pg_directory_table;
SELECT relname, relisshared, relpersistence, relkind FROM pg_class WHERE relname like '%dir_table%' ORDER BY 1;
\c regression

--Test Lock directory table
BEGIN;
LOCK TABLE dir_table1 IN ACCESS SHARE MODE;
LOCK TABLE dir_table1 IN ACCESS EXCLUSIVE MODE;
COMMIT;

BEGIN;
LOCK TABLE dir_table1 IN ACCESS SHARE MODE;
LOCK TABLE dir_table1 IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Test CREATE/DROP/REINDEX on DIRECTORY SCHEMA TABLE
-- Test CREATE INDEX on DIRECTORY SCHEMA TABLE
CREATE INDEX dirtable1_relative_path_idx on dir_table1(relative_path);
CREATE INDEX dirtable1_size_idx on dir_table1(size);
CREATE INDEX dirtable1_last_modified_idx on dir_table1(last_modified);
CREATE INDEX dirtable1_md5_idx on dir_table1(md5);
CREATE INDEX dirtable1_tag_idx on dir_table1(tag);
\d+ dir_table1;

-- Test DROP INDEX on DIRECTORY SCHEMA TABLE
DROP INDEX dir_table1_pkey; -- fail
DROP INDEX dir_table2_pkey; -- fail
DROP INDEX dir_table3_pkey; -- fail
DROP INDEX dir_table4_pkey; -- fail
DROP INDEX dir_table5_pkey; -- fail
DROP INDEX dir_table6_pkey; -- fail
DROP INDEX dirtable1_relative_path_idx; -- OK
DROP INDEX dirtable1_size_idx;  -- OK
DROP INDEX dirtable1_last_modified_idx; -- OK
DROP INDEX dirtable1_md5_idx;   -- OK
DROP INDEX dirtable1_tag_idx;   -- OK
\d+ dir_table1;

-- Test REINDEX on DIRECTORY SCHEMA TABLE
REINDEX INDEX dir_table1_pkey;
REINDEX INDEX dir_table2_pkey;
REINDEX INDEX dir_table3_pkey;
REINDEX INDEX dir_table4_pkey;
REINDEX INDEX dir_table5_pkey;
REINDEX INDEX dir_table6_pkey;

REINDEX TABLE dir_table1;
REINDEX TABLE dir_table2;
REINDEX TABLE dir_table3;
REINDEX TABLE dir_table4;
REINDEX TABLE dir_table5;
REINDEX TABLE dir_table6;

-- Test triggers
create function triggertest() returns trigger as $$
begin
	raise notice '% % % %', TG_TABLE_NAME, TG_OP, TG_WHEN, TG_LEVEL;
	return new;
end;$$ language plpgsql;

create trigger trigtest_b_row_tg_dirtable_1 before insert or update or delete on dir_table1
for each row execute procedure triggertest();
create trigger trigtest_a_row_tg_dirtable_1 after insert or update or delete on dir_table1
for each row execute procedure triggertest();
create trigger trigtest_b_stmt_tg_dirtable_1 before insert or update or delete on dir_table1
for each statement execute procedure triggertest();
create trigger trigtest_a_stmt_tg_dirtable_1 after insert or update or delete on dir_table1
for each statement execute procedure triggertest();

-- Test COPY DIRECTORY TABLE syntax
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

\COPY dir_table1 FROM '@abs_srcdir@/data/nation.csv';    -- fail
\COPY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation'; -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv';    -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation1';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation1'; -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation2' 'nation2'; -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation2';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation';    -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation2';    -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation4' WITH TAG 'nation';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation5' WITH TAG 'nation' WITH TAG 'nation2';    -- fail
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation6';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation7';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation8';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation9';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation10';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation11';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation12';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation13';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation14';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation15';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation16';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation17';
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation18';
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, content FROM directory_table('dir_table1') ORDER BY 1;

COPY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv';     -- fail
COPY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation';  -- fail
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation1';
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation1'; -- fail
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation2';
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation';
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation';    -- fail
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation3' WITH TAG 'nation2';    -- fail
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation4' WITH TAG 'nation';
COPY BINARY dir_table2 FROM PROGRAM 'cat @abs_srcdir@/data/nation.csv' 'nation5' WITH TAG 'nation' WITH TAG 'nation2';    -- fail
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;
SELECT relative_path, content FROM directory_table('dir_table2') ORDER BY 1;

\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation.txt';   -- OK
COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation2.txt';   -- OK
\COPY BINARY "abs.dir_table" FROM '@abs_srcdir@/data/nation.csv' 'aa.bb';    -- OK
COPY BINARY "abs.dir_table" FROM '@abs_srcdir@/data/nation.csv' 'cc.dd';    -- OK

-- Test copy binary from directory table
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (format CSV);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (freeze off);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (freeze on);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (delimiter ',');
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (null ' ');
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (header off);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (header on);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (quote ':');
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (escape ':');
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_quote (a));
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_quote *);
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_not_null (a));
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_null (a));
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (convert_selectively (a));
\COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (encoding 'sql_ascii');

COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (format CSV);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (freeze off);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (freeze on);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (delimiter ',');
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (null ' ');
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (header off);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (header on);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (quote ':');
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (escape ':');
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_quote (a));
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_quote *);
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_not_null (a));
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (force_null (a));
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (convert_selectively (a));
COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation_failed' (encoding 'sql_ascii');

-- Test copy file content md5
CREATE OR REPLACE FUNCTION file_content(text, text) RETURNS BYTEA LANGUAGE SQL AS
'select content from directory_table($1) where relative_path = $2';

CREATE OR REPLACE FUNCTION file_md5(text, text) RETURNS TEXT LANGUAGE SQL AS
'select md5 from directory_table($1) where relative_path = $2';

CREATE OR REPLACE FUNCTION md5_equal(text, text) RETURNS BOOL LANGUAGE SQL AS
'SELECT md5(file_content($1, $2)) = (SELECT file_md5($1, $2))';

SELECT md5_equal('dir_table1', 'nation1');
SELECT md5_equal('dir_table1', 'nation2');
SELECT md5_equal('dir_table1', 'nation3');
SELECT md5_equal('dir_table1', 'nation4');

SELECT md5_equal('dir_table2', 'nation1');
SELECT md5_equal('dir_table2', 'nation2');
SELECT md5_equal('dir_table2', 'nation3');
SELECT md5_equal('dir_table2', 'nation4');

-- Test Copy To directory table
\COPY dir_table1 TO '@abs_srcdir@/data/dir_table1';  -- fail
\COPY BINARY dir_table1 TO '@abs_srcdir@/data/dir_table1';  -- fail
COPY dir_table1 TO '@abs_srcdir@/data/dir_table1';  -- fail
COPY BINARY dir_table1 TO '@abs_srcdir@/data/dir_table1';  -- fail
\COPY dir_table2 TO '@abs_srcdir@/data/dir_table2';  -- fail
\COPY BINARY dir_table2 TO '@abs_srcdir@/data/dir_table2';  -- fail
COPY dir_table2 TO '@abs_srcdir@/data/dir_table2';  -- fail
COPY BINARY dir_table2 TO '@abs_srcdir@/data/dir_table2';  -- fail
\COPY BINARY dir_table1 TO '@abs_srcdir@/data/dir_table1';   -- fail
COPY BINARY dir_table1 TO '@abs_srcdir@/data/dir_table1';   -- fail
\COPY BINARY DIRECTORY TABLE dir_table1 'nation1' TO '@abs_srcdir@/data/nation1';   -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'nation1' TO '@abs_srcdir@/data/nation1';   -- OK
\COPY BINARY DIRECTORY TABLE dir_table1 'unknown' TO '@abs_srcdir@/data/unknown';   -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'unknown' TO '@abs_srcdir@/data/unknown';    -- OK
\COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO stdin; -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO stdin; -- OK
\COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO stdout; -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO stdout; -- OK
\COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO PROGRAM 'gzip -c -1 > @abs_srcdir@/data/nation2.gz';   -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'nation2' TO PROGRAM 'gzip -c -1 > @abs_srcdir@/data/nation2.gz';   -- OK

\COPY BINARY DIRECTORY TABLE "abs.dir_table" 'aa.bb' TO '@abs_srcdir@/data/aa.bb';   -- OK
COPY BINARY DIRECTORY TABLE "abs.dir_table" 'cc.dd' TO '@abs_srcdir@/data/cc.dd';    -- OK
\COPY BINARY DIRECTORY TABLE dir_table1 'nation.txt' TO '@abs_srcdir@/data/nation.txt'; -- OK
COPY BINARY DIRECTORY TABLE dir_table1 'nation2.txt' TO '@abs_srcdir@/data/nation2.txt'; -- OK
\COPY BINARY DIRECTORY TABLE public.dir_table1 'nation.txt' TO '@abs_srcdir@/data/nation3.txt'; -- OK
COPY BINARY DIRECTORY TABLE public.dir_table1 'nation2.txt' TO '@abs_srcdir@/data/nation4.txt'; -- OK


SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

-- Test join between two directory schema tables
ANALYZE dir_table1;
ANALYZE dir_table2;
EXPLAIN (COSTS OFF) SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.relative_path = dir_table2.relative_path ORDER BY 1;
SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.relative_path = dir_table2.relative_path ORDER BY 1;

ANALYZE dir_table1;
ANALYZE dir_table2;
EXPLAIN (COSTS OFF) SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.size = dir_table2.size ORDER BY 1 LIMIT 1;
SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.size = dir_table2.size ORDER BY 1 LIMIT 1;

ANALYZE dir_table1;
ANALYZE dir_table2;
EXPLAIN (COSTS OFF) SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.md5 = dir_table2.md5 ORDER BY 1 LIMIT 1;
SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.md5 = dir_table2.md5 ORDER BY 1 LIMIT 1;

ANALYZE dir_table1;
ANALYZE dir_table2;
EXPLAIN (COSTS OFF) SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.tag = dir_table2.tag ORDER BY 1;
SELECT dir_table1.relative_path FROM dir_table1, dir_table2
WHERE dir_table1.tag = dir_table2.tag ORDER BY 1;

-- Test DML directory schema table, only allow to update tag
INSERT INTO dir_table1 VALUES('insert');    -- fail
INSERT INTO dir_table2 VALUES('insert', 512, '2000-03-21 17:13:27+08', '70f09140d1b83eb3ecf9a0e28494d2a4', 'insert');   -- fail
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

DELETE FROM dir_table1; -- fail
DELETE FROM dir_table2 WHERE relative_path = 'nation1'; -- fail
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

UPDATE dir_table1 SET relative_path = 'nation_updated'; -- fail
UPDATE dir_table2 SET relative_path = 'nation_updated' WHERE relative_path = 'nation2'; -- fail
UPDATE dir_table1 SET size = 512;   -- fail
UPDATE dir_table2 SET size = 1024 WHERE relative_path = 'nation1';  -- fail
UPDATE dir_table1 SET last_modified = '2000-03-21 16:55:07+08'; -- fail
UPDATE dir_table2 SET last_modified = '2000-03-21 16:55:07+08' WHERE relative_path = 'nation3'; -- fail
UPDATE dir_table1 SET md5 = '70f09140d1b83eb3ecf9a0e28494d2a4'; -- fail
UPDATE dir_table2 SET md5 = '70f09140d1b83eb3ecf9a0e28494d2a4' WHERE relative_path = 'nation4'; -- fail
UPDATE dir_table1 SET tag = 'nation_new_tag';  -- ok
UPDATE dir_table1 SET tag = 'nation2_new_tag' WHERE relative_path = 'nation2';  -- ok
UPDATE dir_table2 SET tag = 'nation4_new_tag' WHERE relative_path = 'nation3';  -- ok
UPDATE dir_table1 SET tag = 'failed_tag' WHERE relative_path = 'not_exist_path';
UPDATE dir_table2 SET tag = 'no_tag' WHERE relative_path = 'not_exist_path';
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

-- Test alter table directory schema table
ALTER TABLE dir_table1 ADD COLUMN a int;    -- fail
ALTER DIRECTORY TABLE dir_table1 ADD COLUMN a int;  -- fail
ALTER TABLE dir_table2 DROP COLUMN relative_path;   -- fail
ALTER DIRECTORY TABLE dir_table2 DROP COLUMN relative_path; -- fail
ALTER TABLE dir_table1 RENAME TO dir_table_new; -- fail
ALTER DIRECTORY TABLE dir_table1 RENAME TO dir_table_new;   -- fail
ALTER TABLE dir_table2 ADD CONSTRAINT dirtable_constraint UNIQUE (tag); -- fail
ALTER DIRECTORY TABLE dir_table2 ADD CONSTRAINT dirtable_constraint UNIQUE (tag);   -- fail
ALTER TABLE dir_table1 DROP CONSTRAINT DROP CONSTRAINT test_pkey;   -- fail
ALTER DIRECTORY TABLE dir_table1 DROP CONSTRAINT DROP CONSTRAINT test_pkey; -- fail

-- Test remove_table
SELECT remove_file('dir_table1', 'nation5');    -- fail
SELECT remove_file('dir_table1', 'nation1');
SELECT remove_file('dir_table2', 'nation1', 'nation2'); -- fail
SELECT remove_file('dir_table1', 'nation2');
SELECT remove_file('dir_table3', 'nation1');    -- fail
SELECT remove_file('dir_table2', 'nation3');
SELECT remove_file('dir_table1', 'nation1');    -- fail
SELECT relative_path, size, tag FROM dir_table1 ORDER BY 1;
SELECT relative_path, size, tag FROM dir_table2 ORDER BY 1;

-- Test transaction commit of directory table manipulation
CREATE DIRECTORY TABLE dir_table4 TABLESPACE directory_tblspc;

BEGIN;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_commit';
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_commit2' WITH TAG 'nation';

COMMIT;
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;

BEGIN;
SELECT remove_file('dir_table4', 'nation_commit');
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;
COMMIT;
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;

BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
UPDATE dir_table4 SET tag = 'nation_updated' WHERE relative_path = 'nation_commit2';
COMMIT;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;

-- Test transaction rollback of directory table manipulation

BEGIN;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_rollback';
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;
ROLLBACK;
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;

BEGIN;
SELECT remove_file('dir_table4', 'nation_commit2');
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;
ROLLBACK;
SELECT relative_path, content FROM directory_table('dir_table4') ORDER BY 1;

BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_rollback2' WITH TAG 'nation';
UPDATE dir_table4 SET tag = 'nation_updated' WHERE relative_path = 'nation_rollback2';
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
ROLLBACK;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;

-- Test subtransaction commit of directory table manipulation
BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subcommit' WITH TAG 'nation';
SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subcommit2';
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subcommit3';
RELEASE SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COMMIT;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;

BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SELECT remove_file('dir_table4', 'nation_subcommit');
SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subcommit';
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
RELEASE SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COMMIT;

BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SELECT remove_file('dir_table4', 'nation_subcommit');
SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SELECT remove_file('dir_table4', 'nation_subcommit2');
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
RELEASE SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COMMIT;

BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SELECT remove_file('dir_table4', 'nation_subcommit2');
SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subcommit4';
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
ROLLBACK TO SAVEPOINT s1;
COMMIT;

-- Test subtransaction rollback of directory table manipulation
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback1';
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback2';
BEGIN;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback3';
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SAVEPOINT s1;
SELECT remove_file('dir_table4', 'nation_subrollback1');
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
ROLLBACK;

BEGIN;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback4';
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SAVEPOINT s1;
SELECT remove_file('dir_table4', 'nation_subrollback4');
SAVEPOINT s2;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
RELEASE SAVEPOINT s1;
ROLLBACK;

BEGIN;
SELECT remove_file('dir_table4', 'nation_subrollback2');
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback5';
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SAVEPOINT s1;
SELECT remove_file('dir_table4', 'nation_subrollback5');
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
ROLLBACK TO SAVEPOINT s1;
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
COPY BINARY dir_table4 FROM '@abs_srcdir@/data/nation.csv' 'nation_subrollback6';
SELECT relative_path, tag FROM dir_table4 ORDER BY 1;
SAVEPOINT s2;
ROLLBACK;

-- test create directory table with location
CREATE DIRECTORY TABLE dir_table_with_location TABLESPACE directory_tblspc WITH LOCATION '/test_dirtable';
CREATE DIRECTORY TABLE dir_table_with_location2 TABLESPACE directory_tblspc WITH LOCATION '/test_dirtable'; -- error
CREATE DIRECTORY TABLE dir_table_with_location3 WITH LOCATION '/test_dirtable';
SELECT count(*) FROM pg_directory_table;
DROP DIRECTORY TABLE dir_table_with_location WITH CONTENT;
DROP DIRECTORY TABLE dir_table_with_location3 WITH CONTENT;

-- clean up
DROP DIRECTORY TABLE IF EXISTS dir_table1;
DROP DIRECTORY TABLE IF EXISTS dir_table2;
DROP DIRECTORY TABLE IF EXISTS dir_table3;
DROP DIRECTORY TABLE IF EXISTS dir_table4;
DROP DIRECTORY TABLE IF EXISTS dir_table5;
DROP DIRECTORY TABLE IF EXISTS dir_table6;
DROP DIRECTORY TABLE IF EXISTS "abs.dir_table";

DROP FUNCTION IF EXISTS triggertest;

DROP STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER oss_server1;
DROP STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER oss_server2;
DROP STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER oss_server3;
DROP STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER oss_server4;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable1 STORAGE SERVER oss_server1;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable1 STORAGE SERVER oss_server2;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable1 STORAGE SERVER oss_server3;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable2 STORAGE SERVER oss_server3;
DROP STORAGE USER MAPPING IF EXISTS FOR test_dirtable3 STORAGE SERVER oss_server8;

DROP STORAGE SERVER IF EXISTS oss_server1;
DROP STORAGE SERVER IF EXISTS oss_server2;
DROP STORAGE SERVER IF EXISTS oss_server3;
DROP STORAGE SERVER IF EXISTS oss_server4;
DROP STORAGE SERVER IF EXISTS oss_server5;
DROP STORAGE SERVER IF EXISTS oss_server6;
DROP STORAGE SERVER IF EXISTS oss_server7;
DROP STORAGE SERVER IF EXISTS oss_server8;
DROP STORAGE SERVER IF EXISTS oss_server9;
DROP STORAGE SERVER IF EXISTS oss_server10;
DROP STORAGE SERVER IF EXISTS oss_server11;
DROP STORAGE SERVER IF EXISTS oss_server12;
DROP STORAGE SERVER IF EXISTS oss_server13;

SELECT srvname, srvacl, srvoptions FROM gp_storage_server;

DROP USER test_dirtable1;
DROP USER test_dirtable2;
DROP USER test_dirtable3;
DROP USER test_dirtable4;

DROP FUNCTION IF EXISTS file_content;
DROP FUNCTION IF EXISTS file_md5;
DROP FUNCTION IF EXISTS md5_equal;

DROP DATABASE dirtable_db;

DROP TRIGGER IF EXISTS trigtest_b_row_tg_dirtable_1 ON dir_table1;
DROP TRIGGER IF EXISTS trigtest_a_row_tg_dirtable_1 ON dir_table1;
DROP TRIGGER IF EXISTS trigtest_b_stmt_tg_dirtable_1 ON dir_table1;
DROP TRIGGER IF EXISTS trigtest_a_stmt_tg_dirtable_1 ON dir_table1;

\!rm -rf @testtablespace@;

DROP TABLESPACE directory_tblspc;