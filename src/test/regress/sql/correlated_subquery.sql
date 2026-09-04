SET optimizer_enforce_subplans = 1;
SET optimizer_trace_fallback=on;

SELECT a = ALL (SELECT generate_series(1, 2)), a FROM (values (1),(2)) v(a);
SELECT a = ALL (SELECT generate_series(2, 2)), a FROM (values (1),(2)) v(a);
SELECT 1 = ALL (SELECT generate_series(1, 2)) FROM (values (1),(2)) v(a);
SELECT 2 = ALL (SELECT generate_series(2, 2)) FROM (values (1),(2)) v(a);
SELECT 2 = ALL (SELECT generate_series(2, 3)) FROM (values (1),(2)) v(a);
SELECT 2+1 = ALL (SELECT generate_series(2, 3)) FROM (values (1),(2)) v(a);
SELECT 2+1 = ALL (SELECT generate_series(3, 3)) FROM (values (1),(2)) v(a);
SELECT (SELECT a) = ALL (SELECT generate_series(1, 2)), a FROM (values (1),(2)) v(a);
SELECT (SELECT a) = ALL (SELECT generate_series(2, 2)), a FROM (values (1),(2)) v(a);
SELECT (SELECT a+1) = ALL (SELECT generate_series(2, 2)), a FROM (values (1),(2)) v(a);
SELECT (SELECT 1) = ALL (SELECT generate_series(1, 1)) FROM (values (1),(2)) v(a);
SELECT (SELECT 1) = ALL (SELECT generate_series(1, 2)) FROM  (values (1),(2)) v(a);
SELECT (SELECT 3) = ALL (SELECT generate_series(3, 3)) FROM  (values (1),(2)) v(a);

SELECT (SELECT 1) = ALL (SELECT generate_series(1, 1));
SELECT (SELECT 1) = ALL (SELECT generate_series(1, 2));
SELECT (SELECT 3) = ALL (SELECT generate_series(3, 3));

CREATE TABLE correlated_subquery_test(
   a varchar(100),
   b int
);
SELECT (SELECT a FROM correlated_subquery_test LIMIT 1)=ALL(SELECT a FROM correlated_subquery_test);
-- Use a transaction because following CREATE CAST doesn't necessarily play
-- nicely with other tests.
BEGIN;
CREATE CAST (integer AS text) WITH INOUT AS IMPLICIT;
SELECT (SELECT b FROM correlated_subquery_test LIMIT 1)=ALL(SELECT a FROM correlated_subquery_test);
ROLLBACK;

reset optimizer_trace_fallback;

--
-- Pulling a correlated aggregate subquery up into a join (convert_EXPR_to_join)
-- must not change the answer.  Run with the Postgres planner; ORCA has its own
-- decorrelation and does not take this path.
--
set optimizer to off;

create table csq_pullup_o(a int, d int) distributed by (a);
create table csq_pullup_i(a int) distributed by (a);
create table csq_pullup_empty(a int, d int) distributed by (a);
insert into csq_pullup_o values (3, 1), (1, 9);
insert into csq_pullup_i values (1), (2);

-- (1,9) has no match, so the subquery aggregates over empty input and count(*)
-- yields 0.  The pull-up has to keep that row: a LEFT join, plus a CASE that
-- supplies the empty-input value for the null-extended rows.
explain (costs off)
select * from csq_pullup_o o where o.a > (select count(*) from csq_pullup_i i where i.a = o.d);
select * from csq_pullup_o o where o.a > (select count(*) from csq_pullup_i i where i.a = o.d)
order by 1;

-- A window function has to block the pull-up.  Ungrouped, the subquery returns
-- a single row and count(*) over () sees only that row; the pulled-up subquery
-- is grouped by the correlation column, so the same window would run over every
-- group at once.
explain (costs off)
select * from csq_pullup_o o
where o.a > (select count(*) + count(*) over () from csq_pullup_i i where i.a = o.d);
select * from csq_pullup_o o
where o.a > (select count(*) + count(*) over () from csq_pullup_i i where i.a = o.d)
order by 1;

-- Substituting 0 for count(*) must not turn 1/count(*) into a constant that the
-- planner evaluates: the outer table is empty, so this returns no rows and must
-- not raise "division by zero".
explain (costs off)
select * from csq_pullup_empty e
where e.a > (select 1/count(*) from csq_pullup_i i where i.a = e.d);
select * from csq_pullup_empty e
where e.a > (select 1/count(*) from csq_pullup_i i where i.a = e.d);

drop table csq_pullup_o, csq_pullup_i, csq_pullup_empty;
reset optimizer;
