--
-- Right Semi / Right Anti Join (GPORCA)
--
-- GPORCA can build the hash table on the (smaller) left-hand side of a semi or
-- anti join and probe it with the larger right-hand side, which avoids having
-- to de-duplicate the large right side.  The behaviour is controlled by the
-- developer GUC optimizer_enable_right_semi_join (ON by default).
--
-- These cases force GPORCA (set optimizer=on) so the new
-- CPhysicalRightSemiHashJoin / CPhysicalRightAntiSemiHashJoin xforms are
-- exercised; the matching answer file rightsemijoin_optimizer.out captures the
-- right-semi/right-anti plan shapes, while rightsemijoin.out captures the
-- Postgres planner plans.
--
create schema rsj;
set search_path = rsj, public;

-- stabilize the join method across planners
set enable_nestloop = off;
set enable_mergejoin = off;

create table rsj_small(a int, b int) distributed by (a);
create table rsj_big(a int, b int) distributed by (a);

-- small LHS: 1..50, plus two values absent from rsj_big
insert into rsj_small select i, i from generate_series(1,50) i;
insert into rsj_small values (600001, 1), (600002, 2);

-- large, high-NDV RHS: de-duplicating it would be expensive, so the right-semi
-- plan (build on the small LHS) wins.
insert into rsj_big select i, i from generate_series(1,500000) i;

analyze rsj_small;
analyze rsj_big;

--
-- Semi join (IN): GUC on -> Hash Right Semi Join (build on small LHS)
--
set optimizer_enable_right_semi_join = on;
explain (costs off)
select * from rsj_small s where s.a in (select a from rsj_big b);

-- GUC off -> regular Hash Semi Join (build on RHS)
set optimizer_enable_right_semi_join = off;
explain (costs off)
select * from rsj_small s where s.a in (select a from rsj_big b);

--
-- Anti join (NOT EXISTS): GUC on -> Hash Right Anti Join (build on small LHS)
--
set optimizer_enable_right_semi_join = on;
explain (costs off)
select * from rsj_small s where not exists (select 1 from rsj_big b where b.a = s.a);

-- GUC off -> regular Hash Anti Join
set optimizer_enable_right_semi_join = off;
explain (costs off)
select * from rsj_small s where not exists (select 1 from rsj_big b where b.a = s.a);

--
-- Correctness: results are identical regardless of the GUC / plan shape.
--
set optimizer_enable_right_semi_join = on;
select count(*) as semi_on from rsj_small s where s.a in (select a from rsj_big b);
select count(*) as anti_on from rsj_small s where not exists (select 1 from rsj_big b where b.a = s.a);
set optimizer_enable_right_semi_join = off;
select count(*) as semi_off from rsj_small s where s.a in (select a from rsj_big b);
select count(*) as anti_off from rsj_small s where not exists (select 1 from rsj_big b where b.a = s.a);

reset optimizer_enable_right_semi_join;
reset enable_nestloop;
reset enable_mergejoin;
reset search_path;
set client_min_messages = warning;
drop schema rsj cascade;
reset client_min_messages;
