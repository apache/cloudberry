-- start_matchsubs
-- m/\(actual time=\d+\.\d+..\d+\.\d+ rows=\d+ loops=\d+\)/
-- s/\(actual time=\d+\.\d+..\d+\.\d+ rows=\d+ loops=\d+\)/(actual time=##.###..##.### rows=# loops=#)/
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes\./
-- s/Executor memory: (\d+)\w bytes\./Executor memory: (#####)K bytes./
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./
-- s/Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./Executor memory: ####K bytes avg x #### workers, ####K bytes max (seg#)./
-- m/Work_mem: \d+\w bytes max\./
-- s/Work_mem: \d+\w bytes max\. */Work_mem: ###K bytes max./
-- m/Execution time: \d+\.\d+ ms/
-- s/Execution time: \d+\.\d+ ms/Execution time: ##.### ms/
-- m/Planning time: \d+\.\d+ ms/
-- s/Planning time: \d+\.\d+ ms/Planning time: ##.### ms/
-- m/cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+/
-- s/\(cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+\)/(cost=##.###..##.### rows=### width=###)/
-- m/Memory used:  \d+\w?B/
-- s/Memory used:  \d+\w?B/Memory used: ###B/
-- end_matchsubs
--
-- DEFAULT syntax
CREATE TABLE apples(id int PRIMARY KEY, type text);
INSERT INTO apples(id) SELECT generate_series(1, 100000);
CREATE TABLE box_locations(id int PRIMARY KEY, address text);
CREATE TABLE boxes(id int PRIMARY KEY, apple_id int REFERENCES apples(id), location_id int REFERENCES box_locations(id));

--- Check Explain Text format output
-- explain_processing_off
EXPLAIN SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;
-- explain_processing_on

--- Check Explain Analyze Text output that include the slices information
-- explain_processing_off
EXPLAIN (ANALYZE) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;
-- explain_processing_on

-- Unaligned output format is better for the YAML / XML / JSON outputs.
-- In aligned format, you have end-of-line markers at the end of each line,
-- and its position depends on the longest line. If the width changes, all
-- lines need to be adjusted for the moved end-of-line-marker.
\a

-- YAML Required replaces for costs and time changes
-- start_matchsubs
-- m/ Loops: \d+/
-- s/ Loops: \d+/ Loops: #/
-- m/ Cost: \d+\.\d+/
-- s/ Cost: \d+\.\d+/ Cost: ###.##/
-- m/ Rows: \d+/
-- s/ Rows: \d+/ Rows: #####/
-- m/ Plan Width: \d+/
-- s/ Plan Width: \d+/ Plan Width: ##/
-- m/ Time: \d+\.\d+/
-- s/ Time: \d+\.\d+/ Time: ##.###/
-- m/Execution Time: \d+\.\d+/
-- s/Execution Time: \d+\.\d+/Execution Time: ##.###/
-- m/Segments: \d+/
-- s/Segments: \d+/Segments: #/
-- m/Pivotal Optimizer \(GPORCA\) version \d+\.\d+\.\d+",?/
-- s/Pivotal Optimizer \(GPORCA\) version \d+\.\d+\.\d+",?/Pivotal Optimizer \(GPORCA\)"/
-- m/ Memory: \d+/
-- s/ Memory: \d+/ Memory: ###/
-- m/Maximum Memory Used: \d+/
-- s/Maximum Memory Used: \d+/Maximum Memory Used: ###/
-- m/Workers: \d+/
-- s/Workers: \d+/Workers: ##/
-- m/Average: \d+/
-- s/Average: \d+/Average: ##/
-- m/Total memory used across slices: \d+/
-- s/Total memory used across slices: \d+\s*/Total memory used across slices: ###/
-- m/Memory used: \d+/
-- s/Memory used: \d+/Memory used: ###/
-- end_matchsubs
-- Check Explain YAML output
EXPLAIN (FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;

--- Check Explain Analyze YAML output that include the slices information
-- explain_processing_off
EXPLAIN (ANALYZE, FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;
-- explain_processing_on

--
-- Test a simple case with JSON and XML output, too.
--
-- This should be enough for those format. The only difference between JSON,
-- XML, and YAML is in the formatting, after all.

-- Check JSON format
--
-- start_matchsubs
-- m/Pivotal Optimizer \(GPORCA\) version \d+\.\d+\.\d+/
-- s/Pivotal Optimizer \(GPORCA\) version \d+\.\d+\.\d+/Pivotal Optimizer \(GPORCA\)/
-- end_matchsubs
-- explain_processing_off
EXPLAIN (FORMAT JSON, COSTS OFF) SELECT * FROM generate_series(1, 10);

EXPLAIN (FORMAT XML, COSTS OFF) SELECT * FROM generate_series(1, 10);

-- Test for an old bug in printing Sequence nodes in JSON/XML format
-- (https://github.com/GreengageDB/greengage/issues/9410)
CREATE TABLE jsonexplaintest (i int4) PARTITION BY RANGE (i) (START(1) END(3) EVERY(1));
EXPLAIN (FORMAT JSON, COSTS OFF) SELECT * FROM jsonexplaintest WHERE i = 2;

-- explain_processing_on


-- Test for github issue #9359
--
-- The plan contains an Agg and a Hash node on top of each other, neither of
-- which have a plan->flow set. Explain should be able to dig the flow from
-- the grandchild node then.

-- JSON Required replaces for costs and time changes
-- start_matchsubs
-- m/ "Startup Cost": \d+.\d+/
-- s/ "Startup Cost": \d+.\d+/ "Startup Cost": ##.###/
-- m/ "Total Cost": \d+.\d+/
-- s/ "Total Cost": \d+.\d+/ "Total Cost": ##.###/
-- m/ "Plan Rows": \d+/
-- s/ "Plan Rows": \d+/ "Plan Rows": #####/
-- m/ "Actual Startup Time": \d+.\d+/
-- s/ "Actual Startup Time": \d+.\d+/ "Actual Startup Time": ##.###/
-- m/ "Actual Total Time": \d+.\d+/
-- s/ "Actual Total Time": \d+.\d+/ "Actual Total Time": ##.###/
-- m/ "Time To First Result": "\d+(.\d+)?"/
-- s/ "Time To First Result": "\d+(.\d+)?"/ "Time To First Result": "##.###"/
-- m/ "Time To Total Result": "\d+(.\d+)?"/
-- s/ "Time To Total Result": "\d+(.\d+)?"/ "Time To Total Result": "##.###"/
-- m/ "Planning Time": \d+.\d+/
-- s/ "Planning Time": \d+.\d+/ "Planning Time": ##.###/
-- m/ "Executor Memory": \d+/
-- s/ "Executor Memory": \d+/ "Executor Memory": #####/
-- m/ "Average": \d+/
-- s/ "Average": \d+/ "Average": #####/
-- m/ "Maximum Memory Used": \d+/
-- s/ "Maximum Memory Used": \d+/ "Maximum Memory Used": #####/
-- m/ "Memory used": \d+/
-- s/ "Memory used": \d+/ "Memory used": #####/
-- m/ "Execution Time": \d+.\d+/
-- s/ "Execution Time": \d+.\d+/ "Execution Time": ##.###/
-- end_matchsubs
CREATE SCHEMA explaintest;
SET search_path=explaintest;
CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
);
explain (format json) SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL));

-- Test for similar bug of missing flow with bitmap index scan.
-- (github issue #9404).
CREATE INDEX ss_f1 on SUBSELECT_TBL(f1);
begin;
set local enable_seqscan=off;
set local enable_indexscan=off;
set local enable_bitmapscan=on;
explain (format json, costs off) select * from subselect_tbl where f1 < 10;
commit;

-- Yet another variant, with missing flow in Append. (github issue #9819)
create table subselect_tbl_child() INHERITS (subselect_tbl);
explain (verbose, format json) select * from (select * from subselect_tbl) p where f1 in (select f1 from subselect_tbl where f2 >= 19);

-- Test Allstats formatting
create table allstat_test(a int);
set gp_enable_explain_allstat=true;
explain (analyze, format json) select * from allstat_test;
reset gp_enable_explain_allstat;

-- Cleanup
RESET search_path;
DROP SCHEMA explaintest cascade;

DROP TABLE boxes;
DROP TABLE apples;
DROP TABLE box_locations;
DROP TABLE jsonexplaintest;
