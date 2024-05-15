-- Test Optimizer Row Hints Feature
--
-- Purpose: Test that row hints may be used to edit cardinality estimates
-- Row hints have not implments in Postgres-based planner
LOAD 'pg_hint_plan';

DROP SCHEMA IF EXISTS rowhints CASCADE;

CREATE SCHEMA rowhints;
SET search_path=rowhints;
SET optimizer_trace_fallback=on;

-- Setup tables
CREATE TABLE my_table(a int, b int);
CREATE INDEX my_awesome_index ON my_table(a);

CREATE TABLE your_table(a int, b int) WITH (appendonly=true);
CREATE INDEX your_awesome_index ON your_table(a);

CREATE TABLE our_table(a int, b int) PARTITION BY RANGE (a) (PARTITION p1 START(0) END(100) INCLUSIVE EVERY(20));
CREATE INDEX our_awesome_index ON our_table(a);

INSERT INTO my_table SELECT i, i FROM generate_series(1, 100)i;
INSERT INTO your_table SELECT i, i FROM generate_series(1, 100)i;
INSERT INTO our_table SELECT i, i FROM generate_series(1, 100)i;
ANALYZE my_table, your_table, our_table;

--------------------------------------------------------------------
-- Test the different row hint types:
--
--     - Absolute
--     - Add
--     - Subtract
--     - Multiply
--------------------------------------------------------------------

-- Baseline no hints
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;

SET client_min_messages TO log;
SET pg_hint_plan.debug_print TO ON;

-- Replace timestamp while logging with static string
-- start_matchsubs
-- m/[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{6} [A-Z]{3}/
-- s/[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{6} [A-Z]{3}/YYYY-MM-DD HH:MM:SS:MSMSMS TMZ/
-- end_matchsubs

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 t3 #123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 t3 +123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 t3 -123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 t3 *123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout


--------------------------------------------------------------------
--
-- Subqueries
--
--------------------------------------------------------------------

\o results/pg_hint_plan.tmpout
/*+
    Rows(my_table your_table #123)
 */
EXPLAIN SELECT * FROM my_table, (SELECT * FROM your_table) AS q;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout


--------------------------------------------------------------------
--
-- CTE
--
--------------------------------------------------------------------

\o results/pg_hint_plan.tmpout
/*+
    Rows(my_table your_table #123)
 */
EXPLAIN WITH cte AS (SELECT * FROM my_table, (SELECT * FROM your_table) as q) SELECT * FROM cte;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout


--------------------------------------------------------------------
-- Test updating lower join row hint
--------------------------------------------------------------------

-- force join order to isolate lower join row hint
set optimizer_join_order=query;

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 #123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

\o results/pg_hint_plan.tmpout
/*+
    Rows(t1 t2 *123)
 */
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout


--------------------------------------------------------------------
-- Test Semi/AntiSemi Joins with RowHints
--------------------------------------------------------------------
\o results/pg_hint_plan.tmpout
/*+
Rows(t1 t2 #123)
*/
EXPLAIN SELECT * FROM my_table AS t1 WHERE t1.a IN (SELECT t2.a FROM our_table t2);
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

\o results/pg_hint_plan.tmpout
/*+
Rows(t1 t2 #123)
*/
EXPLAIN SELECT * FROM my_table AS t1 WHERE t1.a NOT IN (SELECT t2.a FROM our_table t2);
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

---------------------------------------------------------------------------------------------
-- Test case where we disable InnerJoin alternatives so that Stats for the join group are
-- derived from LeftSemi/LeftAntiSemiJoin operators instead of CLogicalJoin operator.
---------------------------------------------------------------------------------------------

SELECT disable_xform('CXformLeftSemiJoin2InnerJoin');

\o results/pg_hint_plan.tmpout
/*+
Rows(t1 t2 #123)
*/
EXPLAIN SELECT * FROM my_table AS t1 WHERE t1.a NOT IN (SELECT t2.a FROM our_table t2);
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

SELECT enable_xform('CXformLeftSemiJoin2InnerJoin');

--------------------------------------------------------------------
-- Test Joins from project sub queries with RowHints
--------------------------------------------------------------------

CREATE TABLE foo(a int, b int) DISTRIBUTED BY (a);
CREATE TABLE bar(a int, b int) DISTRIBUTED BY (a);

INSERT INTO bar SELECT i, i+3 FROM generate_series(1,5) i;
-- Insert single row
INSERT INTO foo values (-2, 34);

ANALYZE foo;
ANALYZE bar;

-- Nested Loop Left Join operator estimates 41 rows(per segment for 3 segment cluster)
-- honoring the specified RowHint. However, Gather Motion estimates total number of
-- rows as 5 because the outer table bar only has 5 rows and ComputeScalar is being smart
-- about it and estimates 5 rows.
\o results/pg_hint_plan.tmpout
/*+
Rows(f b #123)
*/
EXPLAIN SELECT (SELECT a FROM foo AS f) FROM bar AS b;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

-- Missing alias in query to test Un-used Hint logging
\o results/pg_hint_plan.tmpout
/*+
Rows(y z #123)
*/
EXPLAIN SELECT t1.a, t2.a FROM my_table AS t1, your_table AS t2, our_table AS t3;
\o
\! sql/maskout.sh results/pg_hint_plan.tmpout

RESET client_min_messages;
RESET pg_hint_plan.debug_print;
-- Clean Up
DROP TABLE foo;
DROP TABLE bar;
