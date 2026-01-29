
SET optimizer_trace_fallback = on;
-- start_ignore
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists pt1;
drop table if exists pt2;
-- end_ignore

create table t1(v1 int, v2 int);
create table t2(v3 int, v4 int);
create table t3(v5 int, v6 int);

insert into t1 values(generate_series(1, 100), generate_series(1, 100));
insert into t2 values(generate_series(1, 100), generate_series(1, 100));
insert into t3 values(generate_series(1, 100), generate_series(1, 100));

CREATE TABLE pt1 (
    v1 INT,
    v2 INT
) PARTITION BY RANGE (v1);

CREATE TABLE pt1_p1 PARTITION OF pt1 FOR VALUES FROM (0) TO (20);
CREATE TABLE pt1_p2 PARTITION OF pt1 FOR VALUES FROM (20) TO (40);
CREATE TABLE pt1_p3 PARTITION OF pt1 FOR VALUES FROM (40) TO (60);
CREATE TABLE pt1_p4 PARTITION OF pt1 FOR VALUES FROM (60) TO (400);

CREATE TABLE pt2 (
    v3 INT,
    v4 INT
) PARTITION BY RANGE (v3);

CREATE TABLE pt2_p1 PARTITION OF pt2 FOR VALUES FROM (0) TO (20);
CREATE TABLE pt2_p2 PARTITION OF pt2 FOR VALUES FROM (20) TO (40);
CREATE TABLE pt2_p3 PARTITION OF pt2 FOR VALUES FROM (40) TO (60);
CREATE TABLE pt2_p4 PARTITION OF pt2 FOR VALUES FROM (60) TO (400);

insert into pt1 values(generate_series(1, 100), generate_series(1, 100));
insert into pt2 values(generate_series(1, 100), generate_series(1, 100));

analyze t1;
analyze t2;
analyze t3;
analyze pt1;
analyze pt2;

-- dedup the subquery with projection
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10);
explain verbose select * from t1 where v1 in (select v3 from t2 where v3 < 10) and v1 in (select v3 from t2); -- change the order 
explain verbose select * from t1 where v1 in (select v3 from t2 where v3 < 10);
select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10);
select * from t1 where v1 in (select v3 from t2 where v3 < 10);

explain verbose select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2 where v3 < 10);
explain verbose select * from pt1 where v1 in (select v3 from pt2 where v3 < 10);
select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2 where v3 < 10);
select * from pt1 where v1 in (select v3 from pt2 where v3 < 10);

explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1 where v3 < 10);
explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1 where v3 < 10);

with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1 where v3 < 10);
with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1 where v3 < 10);

set optimizer_cte_inlining to on;
set optimizer_cte_inlining_bound to 2;
explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1 where v3 < 10);
with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1 where v3 < 10);
reset optimizer_cte_inlining;
reset optimizer_cte_inlining_bound;

explain verbose select sum(v1) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10);
explain verbose select sum(v1) from t1 where v1 in (select v3 from t2 where v3 < 10);
select sum(v1) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10);
select sum(v1) from t1 where v1 in (select v3 from t2 where v3 < 10);

explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10) order by v1;
explain verbose select * from t1 where v1 in (select v3 from t2 where v3 < 10) order by v1;
select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10) order by v1;
select * from t1 where v1 in (select v3 from t2 where v3 < 10) order by v1;

explain verbose select sum(v1) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10) group by v2;
explain verbose select sum(v1) from t1 where v1 in (select v3 from t2 where v3 < 10) group by v2;
select sum(v1) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where v3 < 10) group by v2;
select sum(v1) from t1 where v1 in (select v3 from t2 where v3 < 10) group by v2;

-- dedup the same subqueryany
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2);
explain verbose select * from t1 where v1 in (select v3 from t2);
select count(*) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2);
select count(*) from t1 where v1 in (select v3 from t2);

explain verbose select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2);
explain verbose select * from pt1 where v1 in (select v3 from pt2);

select count(*) from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2);
select count(*) from pt1 where v1 in (select v3 from pt2);

explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1);
explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1);

with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1);
with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1);

set optimizer_cte_inlining to on;
set optimizer_cte_inlining_bound to 2;
explain verbose with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1);
with cte1 as (select v3 from t2) select count(*) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1);
reset optimizer_cte_inlining;
reset optimizer_cte_inlining_bound;

-- dedup the subquery with inner join
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2,t3 where v4=v6);
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t3,t2 where v4=v6); -- change the join order,different index in inner join
explain verbose select * from t1 where v1 in (select v3 from t2,t3 where v4=v6);
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2,t3 where v4=v6 and v4 < 10);

select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2,t3 where v4=v6 and v4 < 10);
select * from t1 where v1 in (select v3 from t2,t3 where v4=v6 and v4 < 10);

explain verbose select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2,t3 where v4=v6);
explain verbose select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from t3,pt2 where v4=v6);
explain verbose select * from pt1 where v1 in (select v3 from pt2,t3 where v4=v6);
explain verbose select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2,t3 where v4=v6 and v4 < 10);

select * from pt1 where v1 in (select v3 from pt2) and v1 in (select v3 from pt2,t3 where v4=v6 and v4 < 10);
select * from pt1 where v1 in (select v3 from pt2,t3 where v4=v6 and v4 < 10);

explain verbose with cte1 as (select v3 from t2) select * from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1,t3 where v4=v6);
explain verbose with cte1 as (select v3 from t2) select * from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from t3,cte1 where v4=v6);
explain verbose with cte1 as (select v3 from t2) select * from t1,t2 where t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);
explain verbose with cte1 as (select v3 from t2) select * from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);

with cte1 as (select v3 from t2) select sum(v1) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);
with cte1 as (select v3 from t2) select sum(v1) from t1,t2 where t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);

set optimizer_cte_inlining to on;
set optimizer_cte_inlining_bound to 2;
explain verbose with cte1 as (select v3 from t2) select * from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);
with cte1 as (select v3 from t2) select sum(v1) from t1,t2 where t1.v1 in (select v3 from cte1) and t1.v1 in (select v3 from cte1,t3 where v4=v6 and v4 < 10);
reset optimizer_cte_inlining;
reset optimizer_cte_inlining_bound;

-- dedup the subquery with semi join(still be a scalar in the pre-process)
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where exists (SELECT 1 FROM t3 WHERE v5 = v3));
explain verbose select * from t1 where v1 in (select v3 from t2 where exists (SELECT 1 FROM t3 WHERE v5 = v3));

select count(*) from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2 where exists (SELECT 1 FROM t3 WHERE v5 = v3));
select count(*) from t1 where v1 in (select v3 from t2 where exists (SELECT 1 FROM t3 WHERE v5 = v3));

-- inner join different key
explain verbose select v1 from t1 where v1 in (select v3 from t2) and v1 in (select v5 from t2,t3 where v5 = v3);
explain verbose select v1 from t1 where v1 in (select v5 from t2,t3 where v5 = v3);
explain verbose select v1 from t1 where v1 in (select v3 from t2) and v1 in (select v6 from t2,t3 where v5 = v3); -- no dedup, because v6 is not the join key

select count(v1) from t1 where v1 in (select v3 from t2) and v1 in (select v5 from t2,t3 where v5 = v3);
select count(v1) from t1 where v1 in (select v5 from t2,t3 where v5 = v3);

-- can't dedup
explain verbose select * from t1 where v1 in (select v3 from t2) and v1 in (select v4 from t2 where v3 < 10); -- different outpt
explain verbose select * from t1 where v1 in (select v3 from t2) and v2 in (select v3 from t2 where v3 < 10); -- different scalar ident
explain verbose select * from t1 where v1 in (select v3 from t2) and v2 in (select v3 from t2 group by v3); -- group by 
explain verbose select * from t1 where v1 in (select v3 from t2) and v2 in (select v3 from t2 order by v3); -- order by/limit, actully this case can be the subset

reset optimizer_trace_fallback;

-- start_ignore
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists pt1;
drop table if exists pt2;
-- end_ignore
