set force_parallel_mode = 0;
set optimizer = off;

create schema window_parallel;
set search_path to window_parallel;
set gp_appendonly_insert_files = 4;
set min_parallel_table_scan_size = 0;

--
-- Test of Parallel process of Window Functions.
--
CREATE TABLE empsalary (
    depname varchar,
    empno bigint,
    salary int,
    enroll_date date
);

INSERT INTO empsalary VALUES
('develop', 10, 5200, '2007-08-01'),
('sales', 1, 5000, '2006-10-01'),
('personnel', 5, 3500, '2007-12-10'),
('sales', 4, 4800, '2007-08-08'),
('personnel', 2, 3900, '2006-12-23'),
('develop', 7, 4200, '2008-01-01'),
('develop', 9, 4500, '2008-01-01'),
('sales', 3, 4800, '2007-08-01'),
('develop', 8, 6000, '2006-10-01'),
('develop', 11, 5200, '2007-08-15');


-- w1
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;
SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;
SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM empsalary ORDER BY depname, salary;

-- w2
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;
SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;
SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM empsalary;

-- w3
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);
SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);
SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);

-- w4
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;
SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;
SELECT depname, empno, salary, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary) ORDER BY rank() OVER w;

-- w5
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT sum(salary),
	row_number() OVER (ORDER BY depname),
	sum(sum(salary)) OVER (ORDER BY depname DESC)
FROM empsalary GROUP BY depname;
SELECT sum(salary),
	row_number() OVER (ORDER BY depname),
	sum(sum(salary)) OVER (ORDER BY depname DESC)
FROM empsalary GROUP BY depname;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT sum(salary),
	row_number() OVER (ORDER BY depname),
	sum(sum(salary)) OVER (ORDER BY depname DESC)
FROM empsalary GROUP BY depname;
SELECT sum(salary),
	row_number() OVER (ORDER BY depname),
	sum(sum(salary)) OVER (ORDER BY depname DESC)
FROM empsalary GROUP BY depname;


-- w6
set enable_parallel = off;
EXPLAIN(COSTS OFF)
-- identical windows with different names
SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);
SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);
set enable_parallel = on;
EXPLAIN(COSTS OFF)
-- identical windows with different names
SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);
SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);

-- w7
-- mixture of agg/wfunc in the same window
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
SELECT sum(salary) OVER w, rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);

-- w8
-- window agg in CASE WHEN clause
set enable_parallel = off;
EXPLAIN(COSTS OFF)
SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
	SELECT *,
		CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,
		CASE WHEN
			AVG(salary) OVER (PARTITION BY depname) < salary
		THEN 200 END AS depadj FROM empsalary
)s;
SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
	SELECT *,
		CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,
		CASE WHEN
			AVG(salary) OVER (PARTITION BY depname) < salary
		THEN 200 END AS depadj FROM empsalary
)s;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
	SELECT *,
		CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,
		CASE WHEN
			AVG(salary) OVER (PARTITION BY depname) < salary
		THEN 200 END AS depadj FROM empsalary
)s;
SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () FROM(
	SELECT *,
		CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,
		CASE WHEN
			AVG(salary) OVER (PARTITION BY depname) < salary
		THEN 200 END AS depadj FROM empsalary
)s;

-- w9
set enable_parallel = off;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;

-- w10 
set enable_parallel = off;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date desc range between '1 year'::interval preceding and '1 year'::interval following),
	salary, enroll_date from empsalary;

-- w11
set enable_parallel = off;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
	salary, enroll_date from empsalary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
	salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date desc range between '1 year'::interval following and '1 year'::interval following),
	salary, enroll_date from empsalary;

-- w12
set enable_parallel = off;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
	exclude current row), salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
	exclude current row), salary, enroll_date from empsalary;
set enable_parallel = on;
EXPLAIN(COSTS OFF)
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
	exclude current row), salary, enroll_date from empsalary;
select sum(salary) over (order by enroll_date range between '1 year'::interval preceding and '1 year'::interval following
	exclude current row), salary, enroll_date from empsalary;

--
-- End of test of Parallel process of Window Functions.
--

--
-- Test Parallel UNION ALL
--
create table t1(a int, b int) with(parallel_workers=2);
create table t2(a int, b int) with(parallel_workers=2);
insert into t1 select i, i from generate_series(1, 10000) i;
insert into t1 select i, i from generate_series(1, 10000) i;
analyze t1;
analyze t2;

begin;
set local enable_parallel = on;
set local enable_parallel_append = on;
set local min_parallel_table_scan_size = 0;

-- If parallel-aware append encounters a motion hazard, fall back to parallel-oblivious append.
explain(costs off, verbose)
select b, count(*) from t1 group by b union all select b, count(*) from t2 group by b;

set local enable_parallel_append = off;
-- Naturally, use parallel-oblivious append directly when parallel-aware mode is disabled.
explain(costs off, verbose)
select b, count(*) from t1 group by b union all select b, count(*) from t2 group by b;

-- Ensure compatibility between different paths when using parallel workers
set local enable_parallel_append = on;
set max_parallel_workers_per_gather = 3;
alter table t2 set(parallel_workers=3);
explain(costs off, verbose)
select b, count(*) from t1 group by b union all select b, count(*) from t2 group by b;

-- Could not drive a parallel plan if no partial paths are avaliable
alter table t2 set(parallel_workers=0);
-- parallel-aware
explain(costs off, verbose)
select b, count(*) from t1 group by b union all select b, count(*) from t2 group by b;
set local enable_parallel_append = off;
-- Also applies to parallel-oblivious
explain(costs off, verbose)
select b, count(*) from t1 group by b union all select b, count(*) from t2 group by b;
abort;

--
-- End of test Parallel UNION ALL
--
-- start_ignore
drop schema window_parallel cascade;
-- end_ignore
reset min_parallel_table_scan_size;
reset enable_parallel;
reset gp_appendonly_insert_files;
reset force_parallel_mode;
reset optimizer;