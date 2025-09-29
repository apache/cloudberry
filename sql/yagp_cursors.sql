CREATE EXTENSION yagp_hooks_collector;

CREATE FUNCTION yagp_status_order(status text)
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

-- DECLARE
SET yagpcc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_0 CURSOR FOR SELECT 0;
CLOSE cursor_stats_0;
COMMIT;

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- DECLARE WITH HOLD
SET yagpcc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_1 CURSOR WITH HOLD FOR SELECT 1;
CLOSE cursor_stats_1;
DECLARE cursor_stats_2 CURSOR WITH HOLD FOR SELECT 2;
CLOSE cursor_stats_2;
COMMIT;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- ROLLBACK
SET yagpcc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_3 CURSOR FOR SELECT 1;
CLOSE cursor_stats_3;
DECLARE cursor_stats_4 CURSOR FOR SELECT 1;
ROLLBACK;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- FETCH
SET yagpcc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_5 CURSOR WITH HOLD FOR SELECT 2;
DECLARE cursor_stats_6 CURSOR WITH HOLD FOR SELECT 3;
FETCH 1 IN cursor_stats_5;
FETCH 1 IN cursor_stats_6;
CLOSE cursor_stats_5;
CLOSE cursor_stats_6;
COMMIT;

RESET yagpcc.logging_mode;

SELECT segid, query_text, query_status FROM yagpcc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION yagp_status_order(text);
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.enable;
RESET yagpcc.report_nested_queries;
RESET yagpcc.enable_utility;
