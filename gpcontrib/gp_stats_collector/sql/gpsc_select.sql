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
SET gpsc.report_nested_queries TO TRUE;
SET gpsc.enable_utility TO FALSE;

-- Basic SELECT tests
SET gpsc.logging_mode to 'TBL';

SELECT 1;
SELECT COUNT(*) FROM generate_series(1,10);

RESET gpsc.logging_mode;
SELECT segid, query_text, query_status FROM gpsc.log ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Transaction test
SET gpsc.logging_mode to 'TBL';

BEGIN;
SELECT 1;
COMMIT;

RESET gpsc.logging_mode;
SELECT segid, query_text, query_status FROM gpsc.log ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- CTE test
SET gpsc.logging_mode to 'TBL';

WITH t AS (VALUES (1), (2))
SELECT * FROM t;

RESET gpsc.logging_mode;
SELECT segid, query_text, query_status FROM gpsc.log ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- Prepared statement test
SET gpsc.logging_mode to 'TBL';

PREPARE test_stmt AS SELECT 1;
EXECUTE test_stmt;
DEALLOCATE test_stmt;

RESET gpsc.logging_mode;
SELECT segid, query_text, query_status FROM gpsc.log ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION gpsc_status_order(text);
DROP EXTENSION gp_stats_collector;
RESET gpsc.enable;
RESET gpsc.report_nested_queries;
RESET gpsc.enable_utility;
RESET gpsc.ignored_users_list;
