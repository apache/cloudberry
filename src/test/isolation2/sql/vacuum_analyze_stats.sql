-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed
-- with this work for additional information regarding copyright
-- ownership. The ASF licenses this file to You under the Apache
-- License, Version 2.0 (the "License"); you may not use this file
-- except in compliance with the License. You may obtain a copy of the
-- License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
-- implied. See the License for the specific language governing
-- permissions and limitations under the License.

-- Counts and elapsed times must use the same coordinator ANALYZE record.
-- QE sampling records auto-analyze as manual ANALYZE.
SET gp_autostats_mode = none;
CREATE TABLE analyze_time_hash (a int)
  WITH (autovacuum_enabled = false) DISTRIBUTED BY (a);
CREATE TABLE analyze_time_repl (a int)
  WITH (autovacuum_enabled = false) DISTRIBUTED REPLICATED;
INSERT INTO analyze_time_hash SELECT generate_series(1, 50000);
INSERT INTO analyze_time_repl SELECT generate_series(1, 50000);
ANALYZE analyze_time_hash;
ANALYZE analyze_time_repl;
SELECT count(*) = 2 AND bool_and(s.analyze_count = 1 AND s.autoanalyze_count = 0
                AND s.total_analyze_time > 0 AND s.total_autoanalyze_time = 0
                AND s.total_analyze_time = q.total_analyze_time) AS manual_times_match
  FROM gp_stat_all_tables_summary s JOIN pg_stat_all_tables q USING (relid)
 WHERE s.relname IN ('analyze_time_hash', 'analyze_time_repl');

-- Start a fresh interval, then let an actual coordinator autovacuum worker
-- analyze both tables. No explicit ANALYZE may contribute to this interval.
SELECT pg_stat_reset_single_table_counters('analyze_time_hash'::regclass);
SELECT pg_stat_reset_single_table_counters('analyze_time_repl'::regclass);
ALTER TABLE analyze_time_hash SET (autovacuum_enabled = true,
  autovacuum_analyze_threshold = 100, autovacuum_analyze_scale_factor = 0);
ALTER TABLE analyze_time_repl SET (autovacuum_enabled = true,
  autovacuum_analyze_threshold = 100, autovacuum_analyze_scale_factor = 0);
ALTER SYSTEM SET autovacuum = on;
ALTER SYSTEM SET autovacuum_naptime = '1s';
SELECT pg_reload_conf();
INSERT INTO analyze_time_hash SELECT generate_series(50001, 60000);
INSERT INTO analyze_time_repl SELECT generate_series(50001, 60000);
SELECT gp_stat_force_next_flush();
CREATE FUNCTION wait_for_analyze_times() RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..600 LOOP
    PERFORM pg_stat_clear_snapshot(); /* in func */
    IF (SELECT count(*) = 2 FROM pg_stat_all_tables
         WHERE relname IN ('analyze_time_hash', 'analyze_time_repl')
           AND autoanalyze_count > 0 AND total_autoanalyze_time > 0) THEN
      RETURN true; /* in func */
    END IF; /* in func */
    PERFORM pg_sleep(0.1); /* in func */
  END LOOP; /* in func */
  RAISE EXCEPTION 'timed out waiting for auto-analyze'; /* in func */
END; /* in func */
$$;
SELECT wait_for_analyze_times();
SELECT count(*) = 2 AND bool_and(s.analyze_count = 0 AND s.autoanalyze_count > 0
                AND s.total_analyze_time = 0 AND s.total_autoanalyze_time > 0
                AND s.total_autoanalyze_time = q.total_autoanalyze_time) AS auto_times_match
  FROM gp_stat_all_tables_summary s JOIN pg_stat_all_tables q USING (relid)
 WHERE s.relname IN ('analyze_time_hash', 'analyze_time_repl');
DROP FUNCTION wait_for_analyze_times();
DROP TABLE analyze_time_hash, analyze_time_repl;
ALTER SYSTEM RESET autovacuum;
ALTER SYSTEM RESET autovacuum_naptime;
SELECT pg_reload_conf();
RESET gp_autostats_mode;
