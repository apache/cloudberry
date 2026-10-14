create schema orca_empty_input;
set search_path = orca_empty_input, public;
-- Empty-input aggregate semantics in ORCA, independently of PG pull-up.
set optimizer to on;
create table pullup_agg_outer (a int) distributed by (a);
create table pullup_agg_inner (a int, b int) distributed by (a);
insert into pullup_agg_outer values (1), (2), (3);
insert into pullup_agg_inner values (1, 10), (1, 20), (2, NULL);
analyze pullup_agg_outer;
analyze pullup_agg_inner;
-- ORCA uses the same empty-input NULL property for numeric and bigint.
set optimizer to on;
explain (costs off) select a from pullup_agg_outer o
  where 40 > (select avg(i.b::numeric) from pullup_agg_inner i where i.a = o.a);
explain (costs off) select a from pullup_agg_outer o
  where 40 > (select avg(i.b::bigint) from pullup_agg_inner i where i.a = o.a);
explain (costs off) select a from pullup_agg_outer o
  where 40 > (select sum(i.b::numeric) from pullup_agg_inner i where i.a = o.a);
explain (costs off) select a from pullup_agg_outer o
  where 40 > (select sum(i.b::bigint) from pullup_agg_inner i where i.a = o.a);
select a from pullup_agg_outer o
  where 100 > (select avg(i.b::numeric) + avg(i.b::bigint)
                    + sum(i.b::numeric) + sum(i.b::bigint)
             from pullup_agg_inner i where i.a = o.a) order by 1;
-- COALESCE still needs the empty-input value for the missing group.
select a from pullup_agg_outer o
  where 0 = (select coalesce(avg(i.b::numeric), 0)
             from pullup_agg_inner i where i.a = o.a) order by 1;
-- Do not fold or evaluate division by zero for an unused group.
select a from pullup_agg_outer o
  where a = 1 and a < (select 100 / coalesce(avg(i.b::numeric), 0)
                       from pullup_agg_inner i where i.a = o.a) order by 1;
-- A missing group must still raise the error when its value is used.
select a from pullup_agg_outer o
  where a = 3 and a < (select 100 / coalesce(avg(i.b::numeric), 0)
                       from pullup_agg_inner i where i.a = o.a) order by 1;
-- ORCA must also keep the expression around the aggregate above the join.
-- The inner group a = 2 has a NULL sum but is never used by an outer row.
begin;
set local optimizer = on;
create table pullup_agg_orca_outer (a int) distributed by (a);
select a from pullup_agg_orca_outer o
  where a < (select 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
select a from pullup_agg_orca_outer o
  where a < (select count(*) + 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
insert into pullup_agg_orca_outer values (1);
explain (costs off) select a from pullup_agg_orca_outer o
  where a < (select 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
select a from pullup_agg_orca_outer o
  where a < (select 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
select a from pullup_agg_orca_outer o
  where a < (select count(*) + 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
select a, (select 100 / coalesce(sum(i.b), 0)
           from pullup_agg_inner i where i.a = o.a) as quotient
  from pullup_agg_orca_outer o;
select a from pullup_agg_orca_outer o
  where a + 2 in (select 100 / coalesce(sum(i.b), 0)
                 from pullup_agg_inner i where i.a = o.a);
select a from pullup_agg_orca_outer o
  where a = 0 or a + 2 in (select 100 / coalesce(sum(i.b), 0)
                          from pullup_agg_inner i where i.a = o.a);
-- Non-equality correlations retain correlated execution, including all
-- aggregate columns used by the expression and the match flag.
select a from pullup_agg_orca_outer o
  where a < (select count(*) + 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a <= o.a);
explain (costs off) select a from pullup_agg_orca_outer o
  where a + 4 in (select count(*) + 100 / coalesce(sum(i.b), 0)
                 from pullup_agg_inner i where i.a <= o.a);
select a from pullup_agg_orca_outer o
  where a + 4 in (select count(*) + 100 / coalesce(sum(i.b), 0)
                 from pullup_agg_inner i where i.a <= o.a);
-- A matched all-NULL group and a missing group must still raise the error.
savepoint before_error;
insert into pullup_agg_orca_outer values (2);
select a from pullup_agg_orca_outer o
  where a < (select 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
rollback to before_error;
insert into pullup_agg_orca_outer values (3);
select a from pullup_agg_orca_outer o
  where a < (select 100 / coalesce(sum(i.b), 0)
             from pullup_agg_inner i where i.a = o.a);
rollback;

drop table pullup_agg_outer, pullup_agg_inner;
set optimizer to on;
--
-- Test the COUNT bug for a correlated count() subquery under IN.
--
-- An outer row whose correlated group is empty must still match count() = 0.
-- Decorrelating IN into a semi-join drops such rows, so ORCA turns the subquery
-- into a LEFT join and compares against the count, 0 for the rows without a
-- match. That is needed both when the subquery is a plain filter and when it
-- sits inside an OR, which puts it in a value context.
--
-- Check ORCA plans as well as results for the missing and matching groups.
--
drop table if exists count_bug_outer, count_bug_inner;
create table count_bug_outer (a int) distributed by (a);
create table count_bug_inner (x int, y int) distributed by (x);
insert into count_bug_outer values (0), (1), (2);
insert into count_bug_inner values (1, 1), (5, 2), (6, 2);
analyze count_bug_outer;
analyze count_bug_inner;
explain (costs off) select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a);
select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a) order by 1;
explain (costs off) select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a) or a = 99;
select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a) or a = 99
  order by 1;
select a from count_bug_outer o
  where a in (select count(i.x) from count_bug_inner i where i.y = o.a) order by 1;
explain (costs off) select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner);
select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner) order by 1;
select a from count_bug_outer o
  where a in (select count(*) + 1 from count_bug_inner i where i.y = o.a) order by 1;
select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a
              having count(*) > 0) order by 1;
select a from count_bug_outer o
  where a in (select count(*) * 0 from count_bug_inner i where i.y = o.a) order by 1;
select a from count_bug_outer o
  where a in (select count(*) from count_bug_inner i where i.y = o.a
              having count(*) < 2) order by 1;
select a, (select max(c) from (select count(*) c from count_bug_inner i
                               where i.y = o.a) q) from count_bug_outer o order by 1;
drop table count_bug_outer, count_bug_inner;
-- Keep pull-up through HAVING, but distinguish an empty aggregate input
-- from a group rejected by HAVING. The latter yields NULL for a scalar
-- subquery and FALSE for IN, even when IN's left operand is NULL.
create table having_agg_outer (k int, a int) distributed by (k);
create table having_agg_inner (k int, b int) distributed by (k);
insert into having_agg_outer values (0,0), (1,2), (2,1), (3,10), (4,NULL);
insert into having_agg_inner values (1,10), (1,20), (2,NULL);
insert into having_agg_inner select 3, g from generate_series(1,10) g;
analyze having_agg_outer;
analyze having_agg_inner;
explain (costs off) select k from having_agg_outer o
  where 1 = (select count(*) from having_agg_inner i where i.k = o.k
             having count(*) < 10);
select k,
  (select count(*) from having_agg_inner i where i.k = o.k
   having count(*) < 10) as lt10,
  (select count(*) from having_agg_inner i where i.k = o.k
   having count(*) > 0) as gt0,
  (select count(*) from having_agg_inner i where i.k = o.k
   having sum(i.b) is null) as null_sum,
  (select 100 / count(*) from having_agg_inner i where i.k = o.k
   having count(*) > 0) as quotient
  from having_agg_outer o order by k;
select k,
  a in (select count(*) from having_agg_inner i where i.k = o.k
        having count(*) < 10) as in_lt10,
  a in (select count(*) from having_agg_inner i where i.k = o.k
        having count(*) > 0) as in_gt0,
  a not in (select count(*) from having_agg_inner i where i.k = o.k
            having count(*) > 0) as notin_gt0,
  a in (select sum(i.b) from having_agg_inner i where i.k = o.k
        having count(*) > 0) as in_sum,
  a in (select 100 / count(*) from having_agg_inner i where i.k = o.k
        having count(*) > 0) as in_quotient,
  a in (select count(*) from having_agg_inner i where i.k = o.k
        having sum(i.b) > 0) as in_sum_having
  from having_agg_outer o order by k;
select k from having_agg_outer o
  where a = 0 or a in (select count(*) from having_agg_inner i
                      where i.k = o.k having count(*) > 0) order by k;
-- HAVING may itself reference the outer row.
select k,
  (select count(*) from having_agg_inner i where i.k = o.k
   having count(*) >= o.a) as scalar_having,
  a in (select count(*) from having_agg_inner i where i.k = o.k
        having count(*) >= o.a) as in_having
  from having_agg_outer o order by k;
-- Correlations that cannot become a join must still execute correctly.
select k,
  (select count(*) from having_agg_inner i where i.k <= o.k
   having count(*) < 10) as inequality
  from having_agg_outer o order by k;
-- Do not evaluate HAVING or the target expression for unused inner groups.
create table having_agg_div_outer (k int) distributed by (k);
select k from having_agg_div_outer o
  where 0 < (select 100 / coalesce(sum(i.b), 0) from having_agg_inner i
             where i.k = o.k having 100 / coalesce(sum(i.b), 0) > 0);
insert into having_agg_div_outer values (1);
select k from having_agg_div_outer o
  where 0 < (select 100 / coalesce(sum(i.b), 0) from having_agg_inner i
             where i.k = o.k having 100 / coalesce(sum(i.b), 0) > 0);
-- A missing group must still evaluate HAVING on count(*) = 0 and fail.
insert into having_agg_div_outer values (0);
select k from having_agg_div_outer o
  where 0 < (select count(*) from having_agg_inner i where i.k = o.k
             having 100 / count(*) > 0);
drop table having_agg_outer, having_agg_inner, having_agg_div_outer;
-- cases from Greengage #1658, which disabled decorrelation for count() instead
create table count_bug_empty (a int, b int) distributed by (a);
create table count_bug_one (a int, b int) distributed by (a);
insert into count_bug_one values (0, 0);
create table count_bug_foo (a int, b int) distributed by (a);
insert into count_bug_foo values (1, 1);
create table count_bug_bar (c int, d int) distributed by (c);
create table count_bug_jazz (e int, f int) distributed by (e);
select * from count_bug_one
  where count_bug_one.a in (select count(*) from count_bug_empty
                            where count_bug_one.b = count_bug_empty.b);
select * from count_bug_one
  where count_bug_one.a in (select count(*) from count_bug_empty
                            where count_bug_one.b = count_bug_empty.b
                            group by count_bug_empty.a);
select (select jazz.count
          from (select count(*) from count_bug_bar group by c limit 1) as bar,
               (select count(*) from count_bug_jazz where e = a) as jazz)
  from count_bug_foo;
drop table count_bug_empty, count_bug_one, count_bug_foo, count_bug_bar,
  count_bug_jazz;
--
-- Test the COUNT bug for aggregates other than count() that return a non-NULL
-- value on empty input: regr_count(), hypothetical-set aggregates and
-- aggregates with a non-NULL initial state, and for expressions over
-- aggregates. A correlated subquery must return its value over empty input
-- for an outer row without matching inner rows, not NULL, and must not lose
-- such a row in WHERE. ORCA decorrelates these subqueries into an outer join
-- and computes that value for the rows without a match.
--
create table empty_agg_outer (a int, b int, d int) distributed by (a);
create table empty_agg_inner (a int, b int) distributed by (a);
insert into empty_agg_outer values (3,1,1), (1,9,5), (0,2,7), (5,5,1), (2,4,9);
insert into empty_agg_inner values (1,10), (2,20), (1,30);
analyze empty_agg_outer;
analyze empty_agg_inner;
create aggregate sum_from_zero(int) (sfunc = int4pl, stype = int4, initcond = '0');
select a, d,
       (select regr_count(i.a, i.b) from empty_agg_inner i where i.a = o.d) as regr,
       (select rank(5) within group (order by i.b)
          from empty_agg_inner i where i.a = o.d) as rnk,
       (select sum_from_zero(i.a) from empty_agg_inner i where i.a = o.d) as sfz,
       (select coalesce(sum(i.b), 0) from empty_agg_inner i where i.a = o.d) as cs,
       (select sum(i.b) is null from empty_agg_inner i where i.a = o.d) as sn,
       (select avg(i.b) from empty_agg_inner i where i.a = o.d) as avg
  from empty_agg_outer o order by 1, 2;
explain (costs off) select a, b, d from empty_agg_outer o
  where o.a > (select regr_count(i.a, i.b) from empty_agg_inner i where i.a = o.d);
select a, b, d from empty_agg_outer o
  where o.a > (select regr_count(i.a, i.b) from empty_agg_inner i where i.a = o.d)
  order by 1, 2, 3;
select a, b, d from empty_agg_outer o
  where o.a > (select count(*) from empty_agg_inner i where i.a = o.d)
  order by 1, 2, 3;
select a, b, d from empty_agg_outer o
  where o.a > (select count(*) + 1 from empty_agg_inner i where i.a = o.d)
  order by 1, 2, 3;
explain (costs off) select a, b, d from empty_agg_outer o
  where o.a > (select rank(5) within group (order by i.b)
                 from empty_agg_inner i where i.a = o.d);
select a, b, d from empty_agg_outer o
  where o.a > (select rank(5) within group (order by i.b)
                 from empty_agg_inner i where i.a = o.d)
  order by 1, 2, 3;
select a, d from empty_agg_outer o
  where o.a in (select sum_from_zero(i.a) from empty_agg_inner i where i.a = o.d)
  order by 1, 2;
-- Constant direct arguments let rank use 1 without an empty-input aggregate.
-- NULL and multiple direct arguments have the same empty-input rank.
create function empty_rank_arg(int) returns int
  language plpgsql stable strict as $$
begin
  return 10 / $1;
end;
$$;
select a, b, d from empty_agg_outer o
  where o.a > (select rank(NULL::int) within group (order by i.b)
                 from empty_agg_inner i where i.a = o.d) order by 1, 2, 3;
select a, b, d from empty_agg_outer o
  where o.a > (select rank(5, 5) within group (order by i.b, i.b)
                 from empty_agg_inner i where i.a = o.d) order by 1, 2, 3;
select a, b, d from empty_agg_outer o
  where o.a > (select rank(empty_rank_arg(2)) within group (order by i.b)
                 from empty_agg_inner i where i.a = o.d) order by 1, 2, 3;
select a from empty_agg_outer o where d = 9
  and a > (select rank(empty_rank_arg(0)) within group (order by i.b)
             from empty_agg_inner i where i.a = o.d);
-- The surrounding expression must not fold its empty-input zero divisor.
select a from empty_agg_outer o where d = 1
  and a < (select 10 / (rank(25) within group (order by i.b) - 1)
             from empty_agg_inner i where i.a = o.d) order by 1;
select a from empty_agg_outer o where d = 9
  and a < (select 10 / (rank(25) within group (order by i.b) - 1)
             from empty_agg_inner i where i.a = o.d);
drop function empty_rank_arg(int);
select a, b, d from empty_agg_outer o
  where o.a > (select 10 / count(*) from empty_agg_inner i where i.a = o.d);
select a, b, d from empty_agg_outer o
  where o.d = 1
    and o.a > (select 10 / count(*) from empty_agg_inner i where i.a = o.d)
  order by 1;
-- A NULL empty-input value is not safe to fold inside an expression either:
-- COALESCE can turn it into a zero divisor. Do not evaluate the expression
-- when the outer table is empty or every outer row has matching inner rows.
create table empty_agg_null_outer (a int) distributed by (a);
select a from empty_agg_null_outer o
  where a < (select 10 / coalesce(sum(i.a), 0)
             from empty_agg_inner i where i.a = o.a) order by 1;
insert into empty_agg_null_outer values (1);
select a from empty_agg_null_outer o
  where a < (select 10 / coalesce(sum(i.a), 0)
             from empty_agg_inner i where i.a = o.a) order by 1;
-- A row without a match must still raise the error at execution time.
insert into empty_agg_null_outer values (9);
select a from empty_agg_null_outer o
  where a < (select 10 / coalesce(sum(i.a), 0)
             from empty_agg_inner i where i.a = o.a) order by 1;
drop table empty_agg_null_outer;
explain (costs off) select a, d,
       (select avg(i.b) from empty_agg_inner i where i.a = o.d) as avg
  from empty_agg_outer o;
-- A final function may fail on empty input even when the actual input is
-- nonempty. Neither metadata lookup nor decorrelation may call it early.
create function empty_agg_divide_final(int) returns int
  language sql immutable strict as 'select 10 / $1';
create aggregate empty_agg_divide(int)
  (sfunc = int4pl, stype = int4, initcond = '0',
   finalfunc = empty_agg_divide_final);
-- Keep all rows in this table matched, independently of qual evaluation order.
create table empty_agg_matched as
  select * from empty_agg_outer where d = 1 distributed by (a);
explain (costs off) select empty_agg_divide(a) from empty_agg_inner;
select empty_agg_divide(a) from empty_agg_inner;
select a, (select empty_agg_divide(i.a) from empty_agg_inner i where i.a = o.d)
  from empty_agg_matched o order by 1;
select a from empty_agg_matched o
  where a < (select empty_agg_divide(i.a) from empty_agg_inner i where i.a = o.d)
  order by 1;
select a from empty_agg_matched o
  where a in (select empty_agg_divide(i.a) from empty_agg_inner i where i.a = o.d)
  order by 1;
select a from empty_agg_outer o where d = 9
  and a < (select empty_agg_divide(i.a) from empty_agg_inner i where i.a = o.d);
-- A STABLE PL/pgSQL final function is not folded while loading metadata.
-- It still must not run on empty input unless the correlated input is empty.
create function empty_agg_divide_stable_final(int) returns int
  language plpgsql stable strict as $$
begin
  return 10 / $1;
end;
$$;
create aggregate empty_agg_divide_stable(int)
  (sfunc = int4pl, stype = int4, initcond = '0',
   finalfunc = empty_agg_divide_stable_final);
select empty_agg_divide_stable(a) from empty_agg_inner;
explain (costs off)
select a, (select empty_agg_divide_stable(i.a) from empty_agg_inner i
            where i.a = o.d)
  from empty_agg_matched o;
select a, (select empty_agg_divide_stable(i.a) from empty_agg_inner i
            where i.a = o.d)
  from empty_agg_matched o order by 1;
select a from empty_agg_matched o
  where a < (select empty_agg_divide_stable(i.a) from empty_agg_inner i
              where i.a = o.d) order by 1;
select a from empty_agg_matched o
  where a in (select empty_agg_divide_stable(i.a) from empty_agg_inner i
               where i.a = o.d) order by 1;
select a from empty_agg_outer o where d = 9
  and a < (select empty_agg_divide_stable(i.a) from empty_agg_inner i
            where i.a = o.d);
drop aggregate empty_agg_divide_stable(int);
drop function empty_agg_divide_stable_final(int);
drop table empty_agg_matched;
drop aggregate empty_agg_divide(int);
drop function empty_agg_divide_final(int);
drop aggregate sum_from_zero(int);
drop table empty_agg_outer, empty_agg_inner;
reset optimizer;
reset search_path;
drop schema orca_empty_input;
