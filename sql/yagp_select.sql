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

SET yagpcc.ignored_users_list TO '';
SET yagpcc.enable TO TRUE;
SET yagpcc.report_nested_queries TO TRUE;
SET yagpcc.enable_utility TO FALSE;

-- Basic SELECT tests
SET yagpcc.logging_mode to 'TBL';

SELECT 1;
SELECT COUNT(*) FROM generate_series(1,10);

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Transaction test
SET yagpcc.logging_mode to 'TBL';

BEGIN;
SELECT 1;
COMMIT;

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- CTE test
SET yagpcc.logging_mode to 'TBL';

WITH t AS (VALUES (1), (2))
SELECT * FROM t;

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Prepared statement test
SET yagpcc.logging_mode to 'TBL';

PREPARE test_stmt AS SELECT 1;
EXECUTE test_stmt;
DEALLOCATE test_stmt;

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION yagp_status_order(text);
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.enable;
RESET yagpcc.report_nested_queries;
RESET yagpcc.enable_utility;
RESET yagpcc.ignored_users_list;
