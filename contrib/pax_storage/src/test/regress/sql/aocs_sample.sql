CREATE TABLE test_aocs_tablesample_append (dist int, id int, name text) WITH (appendonly=true, orientation=column)  DISTRIBUTED BY (dist);
-- use fillfactor so we don't have to load too much data to get multiple pages

-- Changed the column length in order to match the expected results based on relation's blocksz
INSERT INTO test_aocs_tablesample_append SELECT 0, i, repeat(i::text, 875) FROM generate_series(0, 9) s(i) ORDER BY i;
INSERT INTO test_aocs_tablesample_append SELECT 3, i, repeat(i::text, 875) FROM generate_series(10, 19) s(i) ORDER BY i;
INSERT INTO test_aocs_tablesample_append SELECT 5, i, repeat(i::text, 875) FROM generate_series(20, 29) s(i) ORDER BY i;

-- Verify that each segment has the same amount of rows;
SELECT gp_segment_id, count(dist) FROM test_aocs_tablesample_append GROUP BY 1 ORDER BY 1;

SELECT t.id FROM test_aocs_tablesample_append AS t TABLESAMPLE SYSTEM (50) REPEATABLE (0);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (100.0/11) REPEATABLE (0);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (0);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE BERNOULLI (50) REPEATABLE (0);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE BERNOULLI (5.5) REPEATABLE (0);

-- 100% should give repeatable count results (ie, all rows) in any case
SELECT count(*) FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (100);
SELECT count(*) FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (100) REPEATABLE (1+2);
SELECT count(*) FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (100) REPEATABLE (0.4);

CREATE VIEW test_aocs_tablesample_append_v1 AS
  SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (10*2) REPEATABLE (2);
CREATE VIEW test_aocs_tablesample_append_v2 AS
  SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (99);
\d+ test_aocs_tablesample_append_v1
\d+ test_aocs_tablesample_append_v2

-- check a sampled query doesn't affect cursor in progress
BEGIN;
DECLARE tablesample_cur CURSOR FOR
  SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (0) ORDER BY id;

FETCH FIRST FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;

SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (0);

FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;

-- Cloudberry: Going backwards on cursors is not supported. By closing the
-- cursor and starting again we pass the tests and keep the file closer to
-- upstream. We do test the rescan methods of tablesample afterwards.
CLOSE tablesample_cur;
DECLARE tablesample_cur CURSOR FOR SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (0) ORDER BY id;
FETCH FIRST FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;
FETCH NEXT FROM tablesample_cur;

CLOSE tablesample_cur;
END;

EXPLAIN (COSTS OFF)
  SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (2);
EXPLAIN (COSTS OFF)
  SELECT * FROM test_aocs_tablesample_append_v1;

-- Cloudberry: Test rescan paths by forcing a nested loop
CREATE TABLE ttr1_aocs_append (a int, b int)  with(appendonly=true, orientation=column)   DISTRIBUTED BY (a);
CREATE TABLE ttr2_aocs_append (a int, b int)  with(appendonly=true, orientation=column)  DISTRIBUTED BY (a);
INSERT INTO ttr1_aocs_append VALUES (1, 1), (12, 1), (31, 1), (NULL, NULL);
INSERT INTO ttr2_aocs_append VALUES (1, 2), (12, 2), (31, 2), (NULL, 6);
ANALYZE ttr1_aocs_append;
ANALYZE ttr2_aocs_append;
SET enable_hashjoin TO OFF;
SET enable_mergejoin TO OFF;
SET enable_nestloop TO ON;

EXPLAIN (COSTS OFF) SELECT * FROM ttr1_aocs_append TABLESAMPLE BERNOULLI (50) REPEATABLE (1), ttr2_aocs_append TABLESAMPLE BERNOULLI (50) REPEATABLE (1) WHERE ttr1_aocs_append.a = ttr2_aocs_append.a;
SELECT * FROM ttr1_aocs_append TABLESAMPLE BERNOULLI (50) REPEATABLE (1), ttr2_aocs_append TABLESAMPLE BERNOULLI (50) REPEATABLE (1) WHERE ttr1_aocs_append.a = ttr2_aocs_append.a;
EXPLAIN (COSTS OFF) SELECT * FROM ttr1_aocs_append TABLESAMPLE SYSTEM (50) REPEATABLE (1), ttr2_aocs_append TABLESAMPLE SYSTEM (50) REPEATABLE (1) WHERE ttr1_aocs_append.a = ttr2_aocs_append.a;
SELECT * FROM ttr1_aocs_append TABLESAMPLE SYSTEM (50) REPEATABLE (1), ttr2_aocs_append TABLESAMPLE SYSTEM (50) REPEATABLE (1) WHERE ttr1_aocs_append.a = ttr2_aocs_append.a;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_nestloop;

-- check behavior during rescans, as well as correct handling of min/max pct
-- Cloudberry: does not support laterals completely, rescan specific tests above
-- start_ignore
select * from
  (values (0),(100)) v(pct),
  lateral (select count(*) from tenk1 tablesample bernoulli (pct)) ss;
select * from
  (values (0),(100)) v(pct),
  lateral (select count(*) from tenk1 tablesample system (pct)) ss;
explain (costs off)
select pct, count(unique1) from
  (values (0),(100)) v(pct),
  lateral (select * from tenk1 tablesample bernoulli (pct)) ss
  group by pct;
select pct, count(unique1) from
  (values (0),(100)) v(pct),
  lateral (select * from tenk1 tablesample bernoulli (pct)) ss
  group by pct;
select pct, count(unique1) from
  (values (0),(100)) v(pct),
  lateral (select * from tenk1 tablesample system (pct)) ss
  group by pct;
-- end_ignore

-- errors
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE FOOBAR (1);

SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (NULL);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (50) REPEATABLE (NULL);

SELECT id FROM test_aocs_tablesample_append TABLESAMPLE BERNOULLI (-1);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE BERNOULLI (200);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (-1);
SELECT id FROM test_aocs_tablesample_append TABLESAMPLE SYSTEM (200);

SELECT id FROM test_aocs_tablesample_append_v1 TABLESAMPLE BERNOULLI (1);
INSERT INTO test_aocs_tablesample_append_v1 VALUES(1);

WITH query_select AS (SELECT * FROM test_aocs_tablesample_append)
SELECT * FROM query_select TABLESAMPLE BERNOULLI (5.5) REPEATABLE (1);

SELECT q.* FROM (SELECT * FROM test_aocs_tablesample_append) as q TABLESAMPLE BERNOULLI (5);
