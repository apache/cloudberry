-- disable ORCA
SET optimizer TO off;

-- Test case group 1: basic functions
CREATE TABLE agg_pushdown_parent (
	i int primary key,
	x int);

CREATE TABLE agg_pushdown_child1 (
	j int,
	parent int,
	v double precision,
	PRIMARY KEY (j, parent));

CREATE INDEX ON agg_pushdown_child1(parent);

CREATE TABLE agg_pushdown_child2 (
	k int,
	parent int,
	v double precision,
	PRIMARY KEY (k, parent));;

INSERT INTO agg_pushdown_parent(i, x)
SELECT n, n
FROM generate_series(0, 7) AS s(n);

INSERT INTO agg_pushdown_child1(j, parent, v)
SELECT 128 * i + n, i, random()
FROM generate_series(0, 127) AS s(n), agg_pushdown_parent;

INSERT INTO agg_pushdown_child2(k, parent, v)
SELECT 128 * i + n, i, random()
FROM generate_series(0, 127) AS s(n), agg_pushdown_parent;

ANALYZE agg_pushdown_parent;
ANALYZE agg_pushdown_child1;
ANALYZE agg_pushdown_child2;

SET enable_nestloop TO on;
SET enable_hashjoin TO off;
SET enable_mergejoin TO off;

-- Perform scan of a table, aggregate the result, join it to the other table
-- and finalize the aggregation.
--
-- In addition, check that functionally dependent column "p.x" can be
-- referenced by SELECT although GROUP BY references "p.i".
SET gp_enable_agg_pushdown TO off;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.x, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

SET gp_enable_agg_pushdown TO on;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.x, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

-- The same for hash join.
SET enable_nestloop TO off;
SET enable_hashjoin TO on;

EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

-- The same for merge join.
SET enable_hashjoin TO off;
SET enable_mergejoin TO on;

EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

-- Restore the default values.
SET enable_nestloop TO on;
SET enable_hashjoin TO on;

-- Scan index on agg_pushdown_child1(parent) column and aggregate the result
-- using AGG_SORTED strategy.
SET gp_enable_agg_pushdown TO off;
SET enable_seqscan TO off;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

SET gp_enable_agg_pushdown TO on;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

SET enable_seqscan TO on;

-- Join "c1" to "p.x" column, i.e. one that is not in the GROUP BY clause. The
-- planner should still use "c1.parent" as grouping expression for partial
-- aggregation, although it's not in the same equivalence class as the GROUP
-- BY expression ("p.i"). The reason to use "c1.parent" for partial
-- aggregation is that this is the only way for "c1" to provide the join
-- expression with input data.
SET gp_enable_agg_pushdown TO off;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.x GROUP BY p.i;

SET gp_enable_agg_pushdown TO on;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.x GROUP BY p.i;

-- Perform nestloop join between agg_pushdown_child1 and agg_pushdown_child2
-- and aggregate the result.
SET enable_nestloop TO on;
SET enable_hashjoin TO off;
SET enable_mergejoin TO off;

SET gp_enable_agg_pushdown TO off;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

SET gp_enable_agg_pushdown TO on;
EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

-- The same for hash join.
SET enable_nestloop TO off;
SET enable_hashjoin TO on;

EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

-- The same for merge join.
SET enable_hashjoin TO off;
SET enable_mergejoin TO on;
SET enable_seqscan TO off;

EXPLAIN (VERBOSE on, COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

SET enable_seqscan TO on;

-- Clear tables
DROP TABLE agg_pushdown_child1;
DROP TABLE agg_pushdown_child2;
DROP TABLE agg_pushdown_parent;

-- Test case group 2: Pushdown with different join keys and group keys.
DROP TABLE IF EXISTS t1, t2;
CREATE TABLE t1 (id int, val int, comment VARCHAR(20));
CREATE TABLE t2 (id int, val int);

SET enable_nestloop TO off;
SET enable_hashjoin TO on;
SET enable_mergejoin TO off;

SET gp_enable_agg_pushdown TO ON;

-- Join key and group key are the same.
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.id, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t1.id;

-- Join key and group key are different.
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.val, SUM(t1.id) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t1.val;

-- Pushdown with equivclass.
EXPLAIN (VERBOSE on, COSTS off)
SELECT t2.id, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t2.id;

-- Group by column from t2 and aggregate column from t1. 
EXPLAIN (VERBOSE on, COSTS off)
SELECT t2.val, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t2.val;

-- Pushdown with multiply group keys.
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.id, t1.comment, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t1.id, t1.comment;

-- Pushdown with multiply join keys.
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.id, t1.comment, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id and t1.val = t2.val GROUP BY t1.id, t1.comment;

-- Test above case with different data distributions
INSERT INTO t1 SELECT i, i, 'asd' FROM generate_series(1, 10000) s(i);
ANALYZE t1;
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.id, t1.comment, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id and t1.val = t2.val GROUP BY t1.id, t1.comment;

DELETE FROM t1;
INSERT INTO t1 SELECT i % 10, 1, 'asd' FROM generate_series(1, 10000) s(i);
ANALYZE t1;
EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.id, t1.comment, SUM(t1.val) FROM t1, t2 WHERE t1.id = t2.id and t1.val = t2.val GROUP BY t1.id, t1.comment;

-- Clear tables
DROP TABLE t1, t2;

-- Test case group 3: Pushdown in subquery and group from subquery.
DROP TABLE IF EXISTS part, lineitem;
CREATE TABLE part (p_partkey int, p_size int, p_price int);
CREATE TABLE lineitem (l_orderkey int, l_partkey int, l_amount int);

SET enable_nestloop TO off;
SET enable_hashjoin TO on;
SET enable_mergejoin TO off;

SET gp_enable_agg_pushdown TO ON;

-- Pushdown within subquery.
EXPLAIN (VERBOSE on, COSTS off)
SELECT SUM(slp) FROM
	(SELECT l_partkey, SUM(p_price) from lineitem, part
		WHERE l_partkey = p_partkey AND p_size < 40
		GROUP BY l_partkey
		ORDER BY l_partkey
		LIMIT 100) temp(lp, slp)
	WHERE slp > 10;

-- Group base on subquery.
EXPLAIN (VERBOSE on, COSTS off)
SELECT p_partkey, SUM(l_amount) FROM
	part, (SELECT l_partkey, l_amount + 10 
			FROM lineitem ORDER BY l_partkey LIMIT 10000) li(l_partkey, l_amount)
	WHERE l_partkey = p_partkey
	GROUP BY p_partkey;

-- Clear tables
DROP TABLE part, lineitem;

-- Test case group 4: construct grouped join rel from 2 plain rels
DROP TABLE IF EXISTS vendor_pd, customer_pd, nation_pd;
CREATE TABLE vendor_pd (v_id int, v_name VARCHAR(20)) WITH (APPENDONLY=true, ORIENTATION=column);
CREATE TABLE customer_pd (c_id int primary key, c_v_id int, c_n_id int, c_type int, c_consumption int);
CREATE TABLE nation_pd (n_id int, n_name VARCHAR(20), n_type int, n_population int) WITH (APPENDONLY=true, ORIENTATION=column);

INSERT INTO nation_pd SELECT i, 'abc', 1, 1 from generate_series(1, 100) s(i);
INSERT INTO customer_pd SELECT i, i % 100, i % 100, 1, 100 from generate_series(1, 10000) s(i);
ANALYZE nation_pd, customer_pd;

-- For each vendor, calculate the total consumption of qualified customers
EXPLAIN (VERBOSE on, COSTS off)
SELECT v_id, v_name, SUM(c_consumption)
	FROM vendor_pd, customer_pd, nation_pd
	WHERE v_id = c_v_id AND c_n_id = n_id AND c_id > n_population
	GROUP BY v_id, v_name;

-- For each vendor/c_type/n_type, calculate the total consumption of qualified customers
EXPLAIN (VERBOSE on, COSTS off)
SELECT v_id, c_type, n_type, SUM(c_consumption)
	FROM vendor_pd, customer_pd, nation_pd
	WHERE v_id = c_v_id AND c_n_id = n_id AND c_id > n_population
	GROUP BY v_id, c_type, n_type;

-- For each vendor/n_type, calculate the total consumption of customers from nation with condition.
EXPLAIN (VERBOSE on, COSTS off)
SELECT v_id, v_name, n_type, SUM(c_consumption)
	FROM vendor_pd, customer_pd, nation_pd
	WHERE v_id = c_v_id AND c_n_id = n_id AND n_population > 100
	GROUP BY v_id, v_name, n_type;

-- Clear tables
DROP TABLE vendor_pd, customer_pd, nation_pd;

-- Test case group 4: OLAP-like cases
DROP TABLE IF EXISTS fact, dim;
CREATE TABLE fact (id int, did int, fact_time int, val int) WITH (APPENDONLY=true, ORIENTATION=column);
CREATE TABLE dim (did int, proj_name varchar(20), brand int, model int);

INSERT INTO dim SELECT i % 100, 1, 1 FROM generate_series(1, 100) s(i);
INSERT INTO fact SELECT i % 10, i % 100, 30, 1 FROM generate_series(1, 10000) s(i);
ANALYZE dim, fact;

-- Test sum fact vals group by dim column
EXPLAIN (VERBOSE on, COSTS off)
SELECT dim.did, sum(val)
	FROM fact JOIN dim ON fact.did = dim.did
	WHERE fact_time > 10 AND fact_time < 2000
	GROUP BY dim.did;

EXPLAIN (VERBOSE on, COSTS off)
SELECT dim.proj_name, sum(val)
	FROM fact JOIN dim ON fact.did = dim.did
	WHERE fact_time > 10 AND fact_time < 2000
	GROUP BY dim.proj_name;

-- Clear tables
DROP TABLE dim, fact;

-- Test case group 5: partition table and inherit table
SET enable_incremental_sort TO off;

DROP TABLE IF EXISTS pagg_pd;
CREATE TABLE pagg_pd (a int, b int, c text, d int) PARTITION BY LIST(c);
CREATE TABLE pagg_pd_p1 PARTITION OF pagg_pd FOR VALUES IN ('0000', '0001', '0002', '0003', '0004');
CREATE TABLE pagg_pd_p2 PARTITION OF pagg_pd FOR VALUES IN ('0005', '0006', '0007', '0008');
CREATE TABLE pagg_pd_p3 PARTITION OF pagg_pd FOR VALUES IN ('0009', '0010', '0011');
INSERT INTO pagg_pd SELECT i % 20, i % 30, to_char(i % 12, 'FM0000'), i % 30 FROM generate_series(0, 2999) i;
ANALYZE pagg_pd;

EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.c, sum(t1.a)
	FROM pagg_pd t1 JOIN pagg_pd t2 ON t1.c < t2.c
	GROUP BY t1.c
	ORDER BY 1, 2;

SELECT t1.c, sum(t1.a)
	FROM pagg_pd t1 JOIN pagg_pd t2 ON t1.c < t2.c
	GROUP BY t1.c
	ORDER BY 1, 2;

DROP TABLE pagg_pd;

CREATE TABLE pagg_pd_p (a int, b int);
CREATE TABLE pagg_pd (c text, d int) inherits (pagg_pd_p) PARTITION BY LIST(c);

DROP TABLE IF EXISTS pagg_pd, pagg_pd_p;
CREATE TABLE pagg_pd_p (a int, b int, c text) PARTITION BY LIST(c);
CREATE TABLE pagg_pd (d int) inherits (pagg_pd_p);

DROP TABLE IF EXISTS pagg_pd, pagg_pd_p;
CREATE TABLE pagg_pd_p (a int, b int);
CREATE TABLE pagg_pd (c text, d int) inherits (pagg_pd_p);
INSERT INTO pagg_pd SELECT i % 20, i % 30, to_char(i % 12, 'FM0000'), i % 30 FROM generate_series(0, 2999) i;
ANALYZE pagg_pd;

EXPLAIN (VERBOSE on, COSTS off)
SELECT t1.c, sum(t1.a)
	FROM pagg_pd t1 JOIN pagg_pd t2 ON t1.c < t2.c
	GROUP BY t1.c
	ORDER BY 1, 2;

SELECT t1.c, sum(t1.a)
	FROM pagg_pd t1 JOIN pagg_pd t2 ON t1.c < t2.c
	GROUP BY t1.c
	ORDER BY 1, 2;

DROP TABLE pagg_pd, pagg_pd_p;
RESET enable_incremental_sort;

-- Clear settings
SET optimizer TO default;

SET gp_enable_agg_pushdown TO off;

SET enable_seqscan TO on;
SET enable_nestloop TO on;
SET enable_hashjoin TO on;
SET enable_mergejoin TO on;