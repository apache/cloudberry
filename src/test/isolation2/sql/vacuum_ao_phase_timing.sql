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

-- Time is published per completed phase, even when distributed commit fails.
-- It measures consumed time; completed-vacuum counts still require post-cleanup.
-- start_matchsubs
-- m/^ERROR:.*fault name:'start_prepare'.*/
-- s/^ERROR:.*fault name:'start_prepare'.*/ERROR: injected prepare failure/
-- m/^ERROR:.*canceling statement due to user request.*/
-- s/^ERROR:.*canceling statement due to user request.*/ERROR: injected prepare failure/
-- end_matchsubs
SET gp_autostats_mode = none;
CREATE TABLE ao_phase_time (a int) WITH (appendonly=true)
  DISTRIBUTED REPLICATED;
INSERT INTO ao_phase_time SELECT generate_series(1, 1000);
DELETE FROM ao_phase_time WHERE a % 2 = 0;
1: SELECT gp_inject_fault('vacuum_ao_after_compact', 'sleep', '', '', '', 1, 1, 1,
                          dbid, current_setting('gp_session_id')::int)
     FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1: SELECT gp_inject_fault('start_prepare', 'error', '', '', '', 1, 1, 0,
                          dbid, current_setting('gp_session_id')::int)
     FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1: VACUUM ao_phase_time;
1: SELECT gp_inject_fault('start_prepare', 'reset', dbid)
     FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
1: SELECT gp_inject_fault('vacuum_ao_after_compact', 'reset', dbid)
     FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
-- At least the QE that raises the prepare error has finished its phase;
-- peers can be canceled before they finish and need not publish a time.
SELECT sum(total_vacuum_time) >= 1000 AND bool_and(vacuum_count = 0) AS aborted_phase_timed
  FROM gp_stat_all_tables WHERE relname = 'ao_phase_time' AND gp_segment_id > -1;
CREATE TEMP TABLE ao_time_before AS SELECT gp_segment_id AS content, total_vacuum_time
  FROM gp_stat_all_tables WHERE relname = 'ao_phase_time' AND gp_segment_id > -1
  DISTRIBUTED BY (content);
1: VACUUM ao_phase_time;
SELECT bool_and(s.vacuum_count = 1 AND s.total_vacuum_time >= b.total_vacuum_time)
       AS completed_once
  FROM gp_stat_all_tables s JOIN ao_time_before b ON s.gp_segment_id = b.content
 WHERE s.relname = 'ao_phase_time';
SELECT count(*) = 500 AS rows_preserved FROM ao_phase_time;
DROP TABLE ao_phase_time, ao_time_before;
RESET gp_autostats_mode;
