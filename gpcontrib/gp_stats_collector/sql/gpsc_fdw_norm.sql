--
-- Test that a failure to normalize plan/query text does not interrupt
-- query execution.
--
-- A foreign table with an empty schema_name option makes postgres_fdw
-- deparse Remote SQL containing a zero-length delimited identifier ("").
-- The gpsc normalizer runs the core SQL lexer over the plan text, and the
-- lexer error used to be escalated to the user query as
-- "Unexpected exception in gpsc ...". Now a warning must be logged instead,
-- and the query must continue. LIMIT 0 keeps the (invalid) Remote SQL from
-- being sent to the loopback server.
--
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_stats_collector;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
SELECT gpsc.truncate_log();
-- end_ignore

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
SET gpsc.enable_utility TO FALSE;
SET gpsc.logging_mode TO 'TBL';

DO $$
BEGIN
    EXECUTE format(
        'CREATE SERVER gpsc_loopback FOREIGN DATA WRAPPER postgres_fdw '
        'OPTIONS (host %L, port %L, dbname %L)',
        split_part(current_setting('unix_socket_directories'), ',', 1),
        current_setting('port'),
        current_database());
END
$$;
CREATE USER MAPPING FOR CURRENT_USER SERVER gpsc_loopback;
CREATE FOREIGN TABLE gpsc_ft (x text) SERVER gpsc_loopback
    OPTIONS (schema_name '', table_name 'pg_class');

-- Before the fix this failed with:
-- ERROR: Unexpected exception in gpsc zero-length delimited identifier at or near """"
SELECT * FROM gpsc_ft LIMIT 0;

RESET gpsc.logging_mode;

SELECT query_status
FROM gpsc.log
WHERE segid = -1 AND query_text = 'SELECT * FROM gpsc_ft LIMIT 0;'
ORDER BY ccnt, gpsc_status_order(query_status);

-- plan_id is still calculated (hash of the raw plan text), while the
-- template fields stay unset because normalization failed.
SELECT plan_id IS NOT NULL AS has_plan_id,
       template_plan_text IS NULL AS no_template_plan,
       plan_text LIKE '%Remote SQL%' AS has_remote_sql
FROM gpsc.log
WHERE segid = -1 AND query_text = 'SELECT * FROM gpsc_ft LIMIT 0;'
  AND query_status = 'QUERY_STATUS_START';

DROP FOREIGN TABLE gpsc_ft;
DROP USER MAPPING FOR CURRENT_USER SERVER gpsc_loopback;
DROP SERVER gpsc_loopback;
DROP EXTENSION postgres_fdw;
DROP FUNCTION gpsc_status_order(text);
DROP EXTENSION gp_stats_collector;
RESET gpsc.enable;
RESET gpsc.enable_utility;
RESET gpsc.ignored_users_list;
RESET gpsc.logging_mode;
