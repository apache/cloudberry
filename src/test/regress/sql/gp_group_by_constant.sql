-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

BEGIN;
SET LOCAL optimizer = off;
SET LOCAL enable_parallel = off;
SET LOCAL gp_enable_multiphase_agg = on;

CREATE TABLE group_by_constant_empty (n int, c0 boolean)
    WITH (parallel_workers = 2) DISTRIBUTED BY (n);
CREATE TABLE group_by_constant_data (n int, c0 boolean)
    WITH (parallel_workers = 2) DISTRIBUTED BY (n);
INSERT INTO group_by_constant_data
    SELECT n, false FROM generate_series(1, 10000) n;
ANALYZE group_by_constant_data;
SELECT count(DISTINCT gp_segment_id) > 1 AS multiple_segments
    FROM group_by_constant_data;

-- Removing all physical grouping keys must not create a group on empty input.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT count(*) FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT 1 FROM group_by_constant_empty
    GROUP BY (0.25)::money HAVING count(*) = 0;
SELECT count(*) FROM group_by_constant_data WHERE n % 10000 = -1
    GROUP BY 'x'::text;
SELECT count(*) FROM group_by_constant_empty
    WHERE n % 2 = 0 GROUP BY n % 2;

-- All segments contribute; then only one segment contributes.
SELECT count(*), sum(n) FROM group_by_constant_data GROUP BY 'x'::text;
SELECT count(*), sum(n) FROM group_by_constant_data WHERE n % 10000 = 1
    GROUP BY 'x'::text;

-- Global aggregates and empty grouping sets still produce a row.
SELECT count(*) FROM group_by_constant_empty;
SELECT count(*) FROM group_by_constant_empty GROUP BY GROUPING SETS ((), ());

SELECT 'x'::text FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT DISTINCT 'x'::text FROM group_by_constant_empty;
SELECT 'x'::text FROM group_by_constant_data GROUP BY 'x'::text;

-- DISTINCT arguments may require redistribution before aggregation.
EXPLAIN (COSTS OFF)
SELECT count(DISTINCT n) FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT count(DISTINCT n) FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT count(DISTINCT c0) FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT count(DISTINCT c0), sum(n) FROM group_by_constant_empty
    GROUP BY 'x'::text;
SELECT count(DISTINCT n), count(DISTINCT c0) FROM group_by_constant_empty
    GROUP BY 'x'::text;
SELECT count(DISTINCT n) FROM group_by_constant_data GROUP BY 'x'::text;
SELECT count(DISTINCT c0) FROM group_by_constant_data GROUP BY 'x'::text;
SELECT count(DISTINCT c0), sum(n) FROM group_by_constant_data
    GROUP BY 'x'::text;
SELECT count(DISTINCT n), count(DISTINCT c0) FROM group_by_constant_data
    GROUP BY 'x'::text;
SELECT count(DISTINCT n) FROM group_by_constant_empty;

-- Filtering all aggregate arguments must not remove an existing group.
SELECT count(DISTINCT n) FILTER (WHERE n < 0),
       count(DISTINCT c0) FILTER (WHERE n < 0)
    FROM group_by_constant_data GROUP BY 'x'::text;

-- Compare with single-phase aggregation and ORCA.
SET LOCAL gp_enable_multiphase_agg = off;
SELECT count(*) FROM group_by_constant_empty GROUP BY 'x'::text;
SET LOCAL optimizer = on;
SET LOCAL gp_enable_multiphase_agg = on;
SELECT count(*) FROM group_by_constant_empty GROUP BY 'x'::text;

-- Exercise worker partial aggregation as well as the final MPP stage.
SET LOCAL optimizer = off;
SET LOCAL enable_parallel = on;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL max_parallel_workers_per_gather = 2;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM group_by_constant_data WHERE n % 10000 = -1
    GROUP BY 'x'::text;
SELECT count(*) FROM group_by_constant_data WHERE n % 10000 = -1
    GROUP BY 'x'::text;
SELECT count(*), sum(n) FROM group_by_constant_data GROUP BY 'x'::text;
SELECT 'x'::text FROM group_by_constant_empty GROUP BY 'x'::text;
SELECT 'x'::text FROM group_by_constant_data GROUP BY 'x'::text;
COMMIT;
