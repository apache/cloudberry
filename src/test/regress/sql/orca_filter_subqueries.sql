create schema orca_filter_subqueries;
set search_path = orca_filter_subqueries, public;
-- NULL-preserving scalar subquery filters.
set optimizer to on;
create table empty_agg_outer (a int, b int, d int) distributed by (a);
create table empty_agg_inner (a int, b int) distributed by (a);
insert into empty_agg_outer values (3,1,1), (1,9,5), (0,2,7), (5,5,1), (2,4,9);
insert into empty_agg_inner values (1,10), (2,20), (1,30);
analyze empty_agg_outer;
analyze empty_agg_inner;
-- a filter that can be true for a NULL subquery keeps the rows the subquery
-- returns no row for
select a, d from empty_agg_outer o
  where coalesce((select sum(i.b) from empty_agg_inner i where i.a = o.d), 0) = 0
  order by 1, 2;
select a, d from empty_agg_outer o
  where (select i.b from empty_agg_inner i where i.a = o.d and i.b = 10) is null
  order by 1, 2;
explain (costs off) select a from empty_agg_outer o
  where coalesce((select sum(i.b) from empty_agg_inner i
                   where i.a = o.a and i.b = 10),
                 (select sum(i.b) from empty_agg_inner i
                   where i.a = o.a and i.b = 20)) > 0;
select a from empty_agg_outer o
  where coalesce((select sum(i.b) from empty_agg_inner i
                   where i.a = o.a and i.b = 10),
                 (select sum(i.b) from empty_agg_inner i
                   where i.a = o.a and i.b = 20)) > 0 order by 1;
explain (costs off) select a from empty_agg_outer o
  where (array[30,20])[(select max(i.a) from empty_agg_inner i where i.a = o.a)]
      = (select max(i.b) from empty_agg_inner i where i.a = o.a);
select a from empty_agg_outer o
  where (array[30,20])[(select max(i.a) from empty_agg_inner i where i.a = o.a)]
      = (select max(i.b) from empty_agg_inner i where i.a = o.a) order by 1;
explain (costs off) select a from empty_agg_outer o
  where case o.a
          when 1 then (select max(i.b) from empty_agg_inner i where i.a = o.a)
          else 20
        end = (select max(i.b) from empty_agg_inner i where i.a = o.a);
select a from empty_agg_outer o
  where case o.a
          when 1 then (select max(i.b) from empty_agg_inner i where i.a = o.a)
          else 20
        end = (select max(i.b) from empty_agg_inner i where i.a = o.a) order by 1;

-- A singleton constant subquery cannot return zero rows. Keep its SubPlan.
create table foo (a int, b int) distributed by (a);
insert into foo values (1, 2), (2, 3), (3, 4);
analyze foo;
explain (costs off) select foo.a from foo where foo.a <= GREATEST(foo.b, (SELECT 1), NULL);
select foo.a from foo where foo.a <= GREATEST(foo.b, (SELECT 1), NULL) order by 1;
explain (costs off) select foo.a from foo where foo.a <= LEAST(foo.b, (SELECT 1), NULL);
select foo.a from foo where foo.a <= LEAST(foo.b, (SELECT 1), NULL) order by 1;
-- A constant subquery that can return zero rows still needs NULL preservation.
select foo.a from foo where foo.a <= GREATEST(foo.b, (SELECT 1 WHERE false), NULL) order by 1;
select foo.a from foo where foo.a <= LEAST(foo.b, (SELECT 1 WHERE false), NULL) order by 1;
-- A present NULL row is different from an empty result, but neither may
-- discard an outer row under GREATEST.
select foo.a from foo where foo.a <= GREATEST(foo.b, (SELECT NULL::int), NULL) order by 1;
drop table foo;
drop table empty_agg_outer, empty_agg_inner;
reset optimizer;
reset search_path;
drop schema orca_filter_subqueries;
