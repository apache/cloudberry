--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

--
-- Test reject_partition_fullscan extension
--
-- Load extension via LOAD (alternative to shared_preload_libraries)
LOAD 'reject_partition_fullscan';

-- Create test partitioned table with 3 range partitions
CREATE TABLE pfr_test (id int, dt date, val text)
	PARTITION BY RANGE (dt);
CREATE TABLE pfr_test_p1 PARTITION OF pfr_test
	FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE pfr_test_p2 PARTITION OF pfr_test
	FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE pfr_test_p3 PARTITION OF pfr_test
	FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');

-- Single-partition table for exemption test
CREATE TABLE pfr_single (id int, dt date)
	PARTITION BY RANGE (dt);
CREATE TABLE pfr_single_p1 PARTITION OF pfr_single
	FOR VALUES FROM ('2025-01-01') TO ('2025-12-31');

-- ==============================
-- Test 1: Basic rejection - no WHERE clause
-- ==============================
SET reject_partition_fullscan = on;
SET partition_fullscan_threshold = 0;

SELECT * FROM pfr_test;
SELECT count(*) FROM pfr_test;

-- ==============================
-- Test 2: Pruning passes - WHERE on partition key
-- ==============================
SELECT * FROM pfr_test WHERE dt = '2025-02-01';
SELECT * FROM pfr_test
	WHERE dt >= '2025-01-01' AND dt < '2025-04-01';

-- ==============================
-- Test 3: WHERE not on partition key - should reject
-- ==============================
SELECT * FROM pfr_test WHERE val = 'x';
SELECT * FROM pfr_test WHERE id = 1;

-- ==============================
-- Test 4: WHERE 1=1 - should reject (constant folded to NIL)
-- ==============================
SELECT * FROM pfr_test WHERE 1 = 1;
SELECT * FROM pfr_test WHERE true;

-- ==============================
-- Test 5: GUC off - allow full scan
-- ==============================
SET reject_partition_fullscan = off;
SELECT * FROM pfr_test;
SET reject_partition_fullscan = on;

-- ==============================
-- Test 6: enable_partition_pruning=off exemption
-- ==============================
SET enable_partition_pruning = off;
SELECT * FROM pfr_test;
SET enable_partition_pruning = on;

-- ==============================
-- Test 7: Single-partition table exemption
-- ==============================
SELECT * FROM pfr_single;

-- ==============================
-- Test 8: Threshold mode
-- ==============================
SET partition_fullscan_threshold = 2;

-- Pruned to 2 partitions, within threshold, should pass
SELECT * FROM pfr_test
	WHERE dt >= '2025-01-01' AND dt < '2025-07-01';

-- All 3 partitions exceed threshold of 2, should reject
SELECT * FROM pfr_test;

SET partition_fullscan_threshold = 0;

-- ==============================
-- Test 9: Prepared statement with parameter (exemption)
-- ==============================
PREPARE pfr_q AS SELECT * FROM pfr_test WHERE dt = $1;
EXECUTE pfr_q('2025-02-01');
DEALLOCATE pfr_q;

-- ==============================
-- Test 10: UPDATE/DELETE without WHERE - should reject
-- ==============================
UPDATE pfr_test SET val = 'y';
DELETE FROM pfr_test;

-- UPDATE/DELETE with partition key - should pass
UPDATE pfr_test SET val = 'y' WHERE dt = '2025-02-01';
DELETE FROM pfr_test WHERE dt = '2025-02-01';

-- ==============================
-- Test 11: Subquery containing partitioned table
-- ==============================
SELECT * FROM (SELECT * FROM pfr_test) sub;

-- ==============================
-- Cleanup
-- ==============================
DROP TABLE pfr_test;
DROP TABLE pfr_single;
RESET reject_partition_fullscan;
RESET partition_fullscan_threshold;
RESET enable_partition_pruning;
