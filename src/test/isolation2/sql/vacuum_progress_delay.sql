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

-- Both progress summaries sum consumed delay across workers, including
-- replicated copies. Configure utility sessions directly: timing is unsync.
CREATE TABLE progress_delay_heap (a int, padding text)
  WITH (autovacuum_enabled=false, fillfactor=10) DISTRIBUTED REPLICATED;
INSERT INTO progress_delay_heap SELECT g, repeat('x', 100)
  FROM generate_series(1, 1000) g;
CREATE TABLE progress_delay_ao (a int) WITH (appendonly=true)
  DISTRIBUTED REPLICATED;
INSERT INTO progress_delay_ao SELECT generate_series(1, 1000);
DELETE FROM progress_delay_ao WHERE a % 2 = 0;
0U: SET track_cost_delay_timing = on;
0U: SET vacuum_cost_delay = '1ms';
0U: SET vacuum_cost_limit = 1;
1U: SET track_cost_delay_timing = on;
1U: SET vacuum_cost_delay = '1ms';
1U: SET vacuum_cost_limit = 1;
SELECT gp_inject_fault('analyze_block', 'suspend', '', '', '', 5, 5, 0, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
0U&: ANALYZE progress_delay_heap;
1U&: ANALYZE progress_delay_heap;
SELECT gp_wait_until_triggered_fault('analyze_block', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
SELECT s.delay_time > 0 AND s.delay_time = d.delay_time AS analyze_delay_summed
  FROM gp_stat_progress_analyze_summary s
  JOIN (SELECT relid, phase, sum(delay_time) AS delay_time
          FROM gp_stat_progress_analyze WHERE gp_segment_id > -1
         GROUP BY relid, phase) d USING (relid, phase)
 WHERE s.relid = 'progress_delay_heap'::regclass;
SELECT gp_inject_fault('analyze_block', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
0U<:
1U<:
SELECT gp_inject_fault('vacuum_ao_after_compact', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
0U&: VACUUM progress_delay_ao;
1U&: VACUUM progress_delay_ao;
SELECT gp_wait_until_triggered_fault('vacuum_ao_after_compact', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
SELECT s.delay_time > 0 AND s.delay_time = d.delay_time AS vacuum_delay_summed
  FROM gp_stat_progress_vacuum_summary s
  JOIN (SELECT relid, phase, sum(delay_time) AS delay_time
          FROM gp_stat_progress_vacuum WHERE gp_segment_id > -1
         GROUP BY relid, phase) d USING (relid, phase)
 WHERE s.relid = 'progress_delay_ao'::regclass;
SELECT gp_inject_fault('vacuum_ao_after_compact', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content IN (0, 1);
0U<:
1U<:
0Uq:
1Uq:
DROP TABLE progress_delay_heap, progress_delay_ao;
