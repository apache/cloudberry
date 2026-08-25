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

-- COUNT correlated scalar subquery should keep no-match rows (COUNT = 0)
--
-- t_in has no match for t_out's row: the subquery computes COUNT = 0 over
-- empty input, "1 > 0" is true and the row must be returned
drop table if exists t_out, t_in;
create table t_out as select 1 as a distributed by (a);
create table t_in  as select 2 as a distributed by (a);

-- Scenario 1: optimizer=off (always PostgreSQL planner path)
set optimizer=off;

select * from t_out
where a > (select count(*) from t_in where t_in.a = t_out.a);

select * from t_out
where 0 = (select count(*) from t_in where t_in.a = t_out.a);

-- Recreate t_in so that t_out's row now HAS matches: count = 2, "1 > 2" is
-- false, and the row must NOT be returned. If the comparison ran as the LEFT
-- join's condition, this row would look unmatched, get the COUNT = 0 default
-- and wrongly pass as "1 > 0".
drop table t_in;
create table t_in as select 1 as a union all select 1 distributed by (a);

select * from t_out
where a > (select count(*) from t_in where t_in.a = t_out.a);

-- a matched group whose expression is NULL (nullif(2, 2)) must not be
-- revived by the no-match default either
select * from t_out
where 0 = (select nullif(count(*), 2) from t_in where t_in.a = t_out.a);

-- restore the no-match fixture
drop table t_in;
create table t_in as select 2 as a distributed by (a);

reset optimizer;

-- Scenario 2: optimizer=on, but query shape forces ORCA fallback to PostgreSQL
-- planner (ordered aggregate disabled in ORCA by default)
--
-- Note on the EXPLAIN below: the filter prints as "CASE WHEN true THEN ...".
-- The plan has no SubqueryScan node for the pulled-up subquery (the planner
-- removes it as a no-op), so EXPLAIN falls back to printing the flag column
-- by its definition -- the constant TRUE. At run time the executor reads the
-- join column, which is NULL (not true) for null-extended no-match rows.
set optimizer=on;
set optimizer_enable_orderedagg=off;

explain (costs off) select string_agg(a::text, ',' order by a)
from t_out
where a > (select count(*) from t_in where t_in.a = t_out.a);

select string_agg(a::text, ',' order by a)
from t_out
where a > (select count(*) from t_in where t_in.a = t_out.a);

reset optimizer;
reset optimizer_enable_orderedagg;

-- Non-plain COUNT expressions should also preserve no-match semantics
-- NB: the sublink must be the RIGHT operand of the comparison, otherwise the
-- pull-up transform is not applied and the query runs as a plain SubPlan.
-- Scenario 1: optimizer=off (always PostgreSQL planner path)
set optimizer=off;

-- for the no-match row count(*) + 1 = 1
select * from t_out
where 1 = (select count(*) + 1 from t_in where t_in.a = t_out.a);

select * from t_out
where 1 = (select (count(*) + 1)::bigint from t_in where t_in.a = t_out.a);

select * from t_out
where 1 = (select case when count(*) > 0 then count(*) + 1 else 1 end
           from t_in where t_in.a = t_out.a);

select * from t_out
where 1 = (select abs(count(*) - 1) from t_in where t_in.a = t_out.a);

select * from t_out
where 0 = (select count(*) + coalesce(sum(t_in.a), 0) from t_in where t_in.a = t_out.a);

select * from t_out
where -1 = (select coalesce(count(*) + sum(t_in.a), -1) from t_in where t_in.a = t_out.a);

-- sum()'s empty-input value is NULL, not 0: the no-match row must NOT satisfy this
select * from t_out
where 0 = (select count(*) + sum(t_in.a) from t_in where t_in.a = t_out.a);

-- ... while the no-match default of nullif(count(*), 1) is 0 and must keep it
select * from t_out
where 0 = (select nullif(count(*), 1) from t_in where t_in.a = t_out.a);

reset optimizer;

-- Scenario 2: optimizer=on, but query shape forces ORCA fallback to PostgreSQL planner
set optimizer=on;
set optimizer_enable_orderedagg=off;

select string_agg(a::text, ',' order by a)
from t_out
where 1 = (select count(*) + 1 from t_in where t_in.a = t_out.a);

reset optimizer;
reset optimizer_enable_orderedagg;

-- The sublink sits in an outer join's ON clause and references the join's
-- non-nullable side. The pulled-up join (and, in the LEFT-join case, the
-- comparison itself) cannot be attached there, so the pull-up must bail out
-- and the sublink runs as a SubPlan.
set optimizer=off;

explain (costs off) select count(*) from t_out t1 left join t_out t3
    on t1.a > (select count(*) from t_in t2 where t2.a = t1.a);

select count(*) from t_out t1 left join t_out t3
    on t1.a > (select count(*) from t_in t2 where t2.a = t1.a);

reset optimizer;

-- No-match rows can pass the predicate without any COUNT involved.
-- coalesce(sum(..), 0) evaluates to 0 over empty input, so the no-match row
-- satisfies "0 = ..." and must survive the pull-up.
set optimizer=off;

explain (costs off) select * from t_out
where 0 = (select coalesce(sum(t_in.a), 0) from t_in where t_in.a = t_out.a);

select * from t_out
where 0 = (select coalesce(sum(t_in.a), 0) from t_in where t_in.a = t_out.a);

-- In contrast, a plain aggregate whose empty-input value is NULL (min, max,
-- sum, avg) under a strict comparison keeps the INNER join: the no-match row
-- evaluates "1 = NULL", which cannot pass, so dropping it early is correct
explain (costs off) select * from t_out
where a = (select min(t_in.a) from t_in where t_in.a = t_out.a);

select * from t_out
where a = (select min(t_in.a) from t_in where t_in.a = t_out.a);

-- A non-strict comparison operator can return TRUE for "outer OP NULL", so
-- the no-match row must survive even when the expression's empty-input value
-- is plain NULL.
create function csq_ns_eq(int8, int8) returns bool as
$$ select coalesce($1, 0) = coalesce($2, 0) $$ language sql immutable;
create operator |=| (procedure = csq_ns_eq, leftarg = int8, rightarg = int8);

select * from t_out
where 0::int8 |=| (select sum(t_in.a) from t_in where t_in.a = t_out.a);

drop operator |=| (int8, int8);
drop function csq_ns_eq(int8, int8);
reset optimizer;

drop table t_out, t_in;

