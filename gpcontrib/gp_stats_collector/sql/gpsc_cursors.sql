CREATE EXTENSION gp_stats_collector;

CREATE FUNCTION gpsc_status_order(status text)
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

-- DECLARE
SET gpsc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_0 CURSOR FOR SELECT 0;
CLOSE cursor_stats_0;
COMMIT;

RESET gpsc.logging_mode;
SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- DECLARE WITH HOLD
SET gpsc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_1 CURSOR WITH HOLD FOR SELECT 1;
CLOSE cursor_stats_1;
DECLARE cursor_stats_2 CURSOR WITH HOLD FOR SELECT 2;
CLOSE cursor_stats_2;
COMMIT;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- ROLLBACK
SET gpsc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_3 CURSOR FOR SELECT 1;
CLOSE cursor_stats_3;
DECLARE cursor_stats_4 CURSOR FOR SELECT 1;
ROLLBACK;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

-- FETCH
SET gpsc.logging_mode to 'TBL';

BEGIN;
DECLARE cursor_stats_5 CURSOR WITH HOLD FOR SELECT 2;
DECLARE cursor_stats_6 CURSOR WITH HOLD FOR SELECT 3;
FETCH 1 IN cursor_stats_5;
FETCH 1 IN cursor_stats_6;
CLOSE cursor_stats_5;
CLOSE cursor_stats_6;
COMMIT;

RESET gpsc.logging_mode;

SELECT segid, query_text, query_status FROM gpsc.log WHERE segid = -1 AND utility = true ORDER BY segid, ccnt, gpsc_status_order(query_status) ASC;
SELECT gpsc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION gpsc_status_order(text);
DROP EXTENSION gp_stats_collector;
RESET gpsc.enable;
RESET gpsc.report_nested_queries;
RESET gpsc.enable_utility;
RESET gpsc.ignored_users_list;
