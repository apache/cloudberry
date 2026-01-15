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
SET yagpcc.report_nested_queries TO TRUE;
SET yagpcc.enable_utility TO FALSE;

-- Hash distributed table

CREATE TABLE test_hash_dist (id int) DISTRIBUTED BY (id);
INSERT INTO test_hash_dist SELECT 1;

SET yagpcc.logging_mode to 'TBL';
SET optimizer_enable_direct_dispatch TO TRUE;
-- Direct dispatch is used here, only one segment is scanned.
select * from test_hash_dist where id = 1;
RESET optimizer_enable_direct_dispatch;

RESET yagpcc.logging_mode;
-- Should see 8 rows.
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

SET yagpcc.logging_mode to 'TBL';

-- Scan all segments.
select * from test_hash_dist;

DROP TABLE test_hash_dist;
RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Replicated table
CREATE FUNCTION force_segments() RETURNS SETOF text AS $$
BEGIN
  RETURN NEXT 'seg';
END;
$$ LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE TABLE test_replicated (id int) DISTRIBUTED REPLICATED;
INSERT INTO test_replicated SELECT 1;

SET yagpcc.logging_mode to 'TBL';
SELECT COUNT(*) FROM test_replicated, force_segments();
DROP TABLE test_replicated;
DROP FUNCTION force_segments();

RESET yagpcc.logging_mode;
SELECT segid, query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

-- Partially distributed table (2 numsegments)
SET allow_system_table_mods = ON;
CREATE TABLE test_partial_dist (id int, data text) DISTRIBUTED BY (id);
UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'test_partial_dist'::regclass;
INSERT INTO test_partial_dist SELECT * FROM generate_series(1, 100);

SET yagpcc.logging_mode to 'TBL';
SELECT COUNT(*) FROM test_partial_dist;
RESET yagpcc.logging_mode;

DROP TABLE test_partial_dist;
RESET allow_system_table_mods;
-- Should see 12 rows.
SELECT query_text, query_status FROM yagpcc.log ORDER BY segid, ccnt, yagp_status_order(query_status) ASC;
SELECT yagpcc.truncate_log() IS NOT NULL AS t;

DROP FUNCTION yagp_status_order(text);
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.enable;
RESET yagpcc.report_nested_queries;
RESET yagpcc.enable_utility;
