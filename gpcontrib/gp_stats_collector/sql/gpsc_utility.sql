CREATE EXTENSION gp_stats_collector;

CREATE OR REPLACE FUNCTION gpsc_status_order(status text)
RETURNS integer
AS $$
BEGIN
    RETURN CASE status
        WHEN 'QUERY_STATUS_SUBMIT' THEN 1
        WHEN 'QUERY_STATUS_START' THEN 2
        WHEN 'QUERY_STATUS_END' THEN 3
        WHEN 'QUERY_STATUS_DONE' THEN 4
        ELSE 999
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

SET gpsc.ignored_users_list TO '';
SET gpsc.enable TO TRUE;
SET gpsc.enable_utility TO TRUE;
SET gpsc.report_nested_queries TO TRUE;

SET gpsc.logging_mode to 'TBL';

CREATE TABLE test_table (a int, b text);
CREATE INDEX test_idx ON test_table(a);
ALTER TABLE test_table ADD COLUMN c int DEFAULT 1;
DROP TABLE test_table;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Partitioning
SET gpsc.logging_mode to 'TBL';

CREATE TABLE pt_test (a int, b int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (a)
(START (0) END (100) EVERY (50));
DROP TABLE pt_test;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Views and Functions
SET gpsc.logging_mode to 'TBL';

CREATE VIEW test_view AS SELECT 1 AS a;
CREATE FUNCTION test_func(i int) RETURNS int AS $$ SELECT $1 + 1; $$ LANGUAGE SQL;
DROP VIEW test_view;
DROP FUNCTION test_func(int);

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Transaction Operations
SET gpsc.logging_mode to 'TBL';

BEGIN;
SAVEPOINT sp1;
ROLLBACK TO sp1;
COMMIT;

BEGIN;
SAVEPOINT sp2;
ABORT;

BEGIN;
ROLLBACK;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- DML Operations
SET gpsc.logging_mode to 'TBL';

CREATE TABLE dml_test (a int, b text);
INSERT INTO dml_test VALUES (1, 'test');
UPDATE dml_test SET b = 'updated' WHERE a = 1;
DELETE FROM dml_test WHERE a = 1;
DROP TABLE dml_test;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- COPY Operations
SET gpsc.logging_mode to 'TBL';

CREATE TABLE copy_test (a int);
COPY (SELECT 1) TO STDOUT;
DROP TABLE copy_test;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Prepared Statements and error during execute
SET gpsc.logging_mode to 'TBL';

PREPARE test_prep(int) AS SELECT $1/0 AS value;
EXECUTE test_prep(0::int);
DEALLOCATE test_prep;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- GUC Settings
SET gpsc.logging_mode to 'TBL';

SET gpsc.report_nested_queries TO FALSE;
RESET gpsc.report_nested_queries;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION gpsc_status_order(text);
DROP EXTENSION gp_stats_collector;
RESET gpsc.enable;
RESET gpsc.report_nested_queries;
RESET gpsc.enable_utility;
RESET gpsc.ignored_users_list;
