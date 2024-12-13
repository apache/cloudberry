-- start_matchsubs
-- m/ERROR:  can not drop a internal task "gp_dynamic_table_refresh_.*/
-- s/ERROR:  can not drop a internal task "gp_dynamic_table_refresh_.*/ERROR:  can not drop a internal task "gp_dynamic_table_refresh_xxx"/g
-- m/WARNING:  relation of oid "\d+" is not dynamic table/
-- s/WARNING:  relation of oid "\d+" is not dynamic table/WARNING:  relation of oid "XXX" is not dynamic table/g
-- end_matchsubs
CREATE SCHEMA dynamic_table_schema;
SET search_path TO dynamic_table_schema;
SET optimizer = OFF;

CREATE TABLE t1(a int, b int, c int) DISTRIBUTED BY (b);
INSERT INTO t1 SELECT i, i + 1, i + 2 FROM GENERATE_SERIES(1, 10) i;
INSERT INTO t1 SELECT i, i + 1, i + 2 FROM GENERATE_SERIES(1, 5) i;
ANALYZE t1;
CREATE DYNAMIC TABLE dt0 SCHEDULE '5 * * * *' AS
  SELECT a, b, sum(c) FROM t1 GROUP BY a, b DISTRIBUTED BY(b);
\d+ dt0
ANALYZE dt0;
-- test backgroud auto-refresh
SELECT schedule, command FROM pg_task WHERE jobname LIKE 'gp_dynamic_table_refresh%';

EXPLAIN(COSTS OFF, VERBOSE)
SELECT a, b, sum(c) FROM t1 GROUP BY a, b;
SELECT a, b, sum(c) FROM t1 GROUP BY a, b;
SELECT * FROM dt0;

-- test join on distributed keys
EXPLAIN(COSTS OFF, VERBOSE)
SELECT * FROM dt0 JOIN t1 USING(b);

-- Create Dynamic Table without SCHEDULE.
CREATE DYNAMIC TABLE dt1  AS
  SELECT * FROM t1 WHERE a = 1 DISTRIBUTED BY(b);
ANALYZE dt1;

-- Create Dynamic Table without DISTRIBUTION KEYS. 
CREATE DYNAMIC TABLE dt2  AS
  SELECT * FROM t1 WHERE a = 2 WITH NO DATA;

-- Refresh Dynamic Table WITH NO DATA
REFRESH DYNAMIC TABLE dt0 WITH NO DATA;

-- Refresh Dynamic Table
REFRESH DYNAMIC TABLE dt2;
ANALYZE dt2;

-- Test Answer Query using Dynamic Tables.
SET enable_answer_query_using_materialized_views = ON;
EXPLAIN(COSTS OFF, VERBOSE)
SELECT * FROM t1 WHERE a = 1;
SELECT * FROM t1 WHERE a = 1;
EXPLAIN(COSTS OFF, VERBOSE)
SELECT * FROM t1 WHERE a = 2;
SELECT * FROM t1 WHERE a = 2;

-- test DROP DYNAMIC TABLE
SELECT schedule, command FROM pg_task WHERE jobname LIKE 'gp_dynamic_table_refresh%' AND command LIKE '%dt0%';
DROP DYNAMIC TABLE dt0;
SELECT schedule, command FROM pg_task WHERE jobname LIKE 'gp_dynamic_table_refresh%' AND command LIKE '%dt0%';

-- drop base tables will drop DYNAMIC TABLEs too.
SELECT schedule, command FROM pg_task WHERE jobname LIKE 'gp_dynamic_table_refresh%';
DROP TABLE t1 CASCADE;
SELECT schedule, command FROM pg_task WHERE jobname LIKE 'gp_dynamic_table_refresh%';

-- construct dynamic table
CREATE TABLE t2(a int, b int, c int) DISTRIBUTED BY (b);
CREATE MATERIALIZED VIEW mv_t2 AS
  SELECT * FROM t2 WHERE a > 1;

-- construct dynamic table from materialized view
CREATE DYNAMIC TABLE dt3  AS
  SELECT * FROM mv_t2 WHERE a = 2;

-- construct dynamic table from dynamic table
CREATE DYNAMIC TABLE dt4  AS
  SELECT * FROM dt3 WHERE b = 3;

-- construct dynamic table from joins
CREATE DYNAMIC TABLE dt5  AS
  SELECT * FROM dt3 natural join t2 natural join mv_t2;

-- construct dynamic table from external table 
begin;

--start_ignore
CREATE OR REPLACE FUNCTION write_to_file() RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_export' LANGUAGE C STABLE NO SQL;
CREATE OR REPLACE FUNCTION read_from_file() RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_import' LANGUAGE C STABLE NO SQL;
DROP PROTOCOL IF EXISTS demoprot;
--end_ignore
CREATE TRUSTED PROTOCOL demoprot (readfunc = 'read_from_file', writefunc = 'write_to_file'); -- should succeed

CREATE WRITABLE EXTERNAL TABLE ext_w(id int)
    LOCATION('demoprot://dynamic_table_text_file.txt') 
FORMAT 'text'
DISTRIBUTED BY (id);

INSERT INTO ext_w SELECT * FROM generate_series(1, 10);

CREATE READABLE EXTERNAL TABLE ext_r(id int)
    LOCATION('demoprot://dynamic_table_text_file.txt') 
FORMAT 'text';

EXPLAIN(COSTS OFF, VERBOSE)
SELECT sum(id) FROM ext_r where id > 5;
SELECT sum(id) FROM ext_r where id > 5;

CREATE DYNAMIC TABLE dt_external  AS
  SELECT * FROM ext_r where id > 5;

ANALYZE dt_external;

SHOW optimizer;
SET LOCAL enable_answer_query_using_materialized_views = ON;
SET LOCAL aqumv_allow_foreign_table = ON;

EXPLAIN(COSTS OFF, VERBOSE)
SELECT sum(id) FROM ext_r where id > 5;
SELECT sum(id) FROM ext_r where id > 5;
DROP FOREIGN TABLE ext_r CASCADE;
DROP FOREIGN TABLE ext_w;
ABORT;

-- Test resevered job name for Dynamic Tables.
SELECT 'dt5'::regclass::oid AS dtoid \gset

-- should fail
CREATE TASK gp_dynamic_table_refresh_xxx SCHEDULE '1 second' AS 'REFRESH DYNAMIC TABLE dt5';

-- can not alter the REFRESH SQL of Dynamic Tables.
ALTER TASK gp_dynamic_table_refresh_:dtoid AS '* * * * *';
ALTER TASK gp_dynamic_table_refresh_:dtoid AS '';

-- should fail
DROP TASK gp_dynamic_table_refresh_:dtoid;

\unset dtoid

CREATE DYNAMIC TABLE dt_schedule SCHEDULE '1 2 3 4 5' AS SELECT * FROM t2;
SELECT pg_catalog.pg_get_dynamic_table_schedule('dt_schedule'::regclass::oid);
-- not a dynamic table
SELECT pg_catalog.pg_get_dynamic_table_schedule('t2'::regclass::oid);

SELECT * FROM pg_dynamic_tables;

CREATE TABLE t3(a int);
CREATE DYNAMIC TABLE dt_1_min SCHEDULE '* * * * *' AS SELECT * FROM t3 WITH NO DATA;
INSERT INTO T3 VALUES(1);
-- wait for backgroud refresh
SELECT pg_sleep(80);
SELECT * FROM dt_1_min;

RESET enable_answer_query_using_materialized_views;
RESET optimizer;
--start_ignore
DROP SCHEMA dynamic_table_schema cascade;
--end_ignore
