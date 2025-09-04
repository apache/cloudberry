CREATE EXTENSION yagp_hooks_collector;

CREATE OR REPLACE FUNCTION yagp_status_order(status text)
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

SET yagpcc.enable TO TRUE;
SET yagpcc.enable_utility TO TRUE;
SET yagpcc.report_nested_queries TO TRUE;

SET yagpcc.logging_mode to 'TBL';

CREATE TABLE test_table (a int, b text);
CREATE INDEX test_idx ON test_table(a);
ALTER TABLE test_table ADD COLUMN c int DEFAULT 1;
DROP TABLE test_table;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Partitioning
SET yagpcc.logging_mode to 'TBL';

CREATE TABLE pt_test (a int, b int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (a)
(START (0) END (100) EVERY (50));
DROP TABLE pt_test;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Views and Functions
SET yagpcc.logging_mode to 'TBL';

CREATE VIEW test_view AS SELECT 1 AS a;
CREATE FUNCTION test_func(i int) RETURNS int AS $$ SELECT $1 + 1; $$ LANGUAGE SQL;
DROP VIEW test_view;
DROP FUNCTION test_func(int);

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Transaction Operations
SET yagpcc.logging_mode to 'TBL';

BEGIN;
SAVEPOINT sp1;
ROLLBACK TO sp1;
COMMIT;

BEGIN;
SAVEPOINT sp2;
ABORT;

BEGIN;
ROLLBACK;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- DML Operations
SET yagpcc.logging_mode to 'TBL';

CREATE TABLE dml_test (a int, b text);
INSERT INTO dml_test VALUES (1, 'test');
UPDATE dml_test SET b = 'updated' WHERE a = 1;
DELETE FROM dml_test WHERE a = 1;
DROP TABLE dml_test;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- COPY Operations
SET yagpcc.logging_mode to 'TBL';

CREATE TABLE copy_test (a int);
COPY (SELECT 1) TO STDOUT;
DROP TABLE copy_test;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Prepared Statements and error during execute
SET yagpcc.logging_mode to 'TBL';

PREPARE test_prep(int) AS SELECT $1/0 AS value;
EXECUTE test_prep(0::int);
DEALLOCATE test_prep;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- GUC Settings
SET yagpcc.logging_mode to 'TBL';

SET yagpcc.report_nested_queries TO FALSE;
RESET yagpcc.report_nested_queries;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION yagp_status_order(text);
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.enable;
RESET yagpcc.report_nested_queries;
RESET yagpcc.enable_utility;
