--
-- Multi-consumer CTE column pruning (ORCA).
--
-- A CTE referenced by several consumers has its shared-scan output pruned to the
-- union of all consumers' required columns. Because ShareInputScan does not
-- project, every consumer must expose exactly the producer's surviving columns.
-- A consumer that projects a superset (e.g. SELECT *) and is referenced directly
-- with a join key used to keep an identity column map and read the shared tuple
-- by stale positions, producing wrong results (join key read from the wrong slot
-- -> LEFT JOIN yields NULLs) and "invalid attnum N for relation shareX_refY" in
-- EXPLAIN. See CPhysicalCTEConsumer. These queries assert correct results under
-- both optimizers.
--
create schema cte_mc;
set search_path = cte_mc;

create table policy(pr_id text, pied_id text, p_id text, amnt numeric(18,2),
                    guarantee_period text, main_risk text, premium numeric(18,2));
create table mid(p_id text, tenant_id text, act_premium numeric(18,2));
insert into policy values('pr1','k1','p1',300000,'1','Y',290);
insert into mid values('p1','t1',290);
analyze policy;
analyze mid;

-- The "info" consumer projects SELECT * but only p_id/amnt/main_risk are needed
-- outside; the producer prunes pr_id/pied_id/guarantee_period. i.amnt and i.p_id
-- must not come back NULL. EXPLAIN previously raised "invalid attnum".
explain (costs off)
with agent as (select pr_id,pied_id,p_id,amnt,guarantee_period,main_risk,premium from policy),
     det as (select p_id, sum(premium) premium from agent group by p_id),
     info as (select * from agent where main_risk='Y' and p_id='p1'),
     unused as (select 1 from mid)
select i.amnt, m.tenant_id, m.p_id, i.p_id, d.p_id, d.premium, m.act_premium
from mid m
left join det  d on d.p_id=m.p_id and d.premium=m.act_premium
left join info i on m.p_id=i.p_id
where m.p_id='p1'
order by 1,2,3,4;

with agent as (select pr_id,pied_id,p_id,amnt,guarantee_period,main_risk,premium from policy),
     det as (select p_id, sum(premium) premium from agent group by p_id),
     info as (select * from agent where main_risk='Y' and p_id='p1'),
     unused as (select 1 from mid)
select i.amnt, m.tenant_id, m.p_id, i.p_id, d.p_id, d.premium, m.act_premium
from mid m
left join det  d on d.p_id=m.p_id and d.premium=m.act_premium
left join info i on m.p_id=i.p_id
where m.p_id='p1'
order by 1,2,3,4;

-- Variant: project a different surviving column (main_risk) from the SELECT *
-- consumer to exercise a different pruned layout.
explain (costs off)
with agent as (select pr_id,pied_id,p_id,amnt,guarantee_period,main_risk,premium from policy),
     det as (select p_id, sum(premium) premium from agent group by p_id),
     info as (select * from agent where p_id='p1')
select m.p_id, i.main_risk, i.amnt, d.premium
from mid m
left join det  d on d.p_id=m.p_id
left join info i on m.p_id=i.p_id
where m.p_id='p1'
order by 1,2,3;

with agent as (select pr_id,pied_id,p_id,amnt,guarantee_period,main_risk,premium from policy),
     det as (select p_id, sum(premium) premium from agent group by p_id),
     info as (select * from agent where p_id='p1')
select m.p_id, i.main_risk, i.amnt, d.premium
from mid m
left join det  d on d.p_id=m.p_id
left join info i on m.p_id=i.p_id
where m.p_id='p1'
order by 1,2,3;

-- Three consumers, each needing a different subset (join key on each).
explain (costs off)
with agent as (select p_id, amnt, main_risk, premium from policy)
select x.p_id, y.amnt, z.main_risk
from agent x
join agent y on x.p_id=y.p_id
join agent z on x.p_id=z.p_id
order by 1,2,3;

with agent as (select p_id, amnt, main_risk, premium from policy)
select x.p_id, y.amnt, z.main_risk
from agent x
join agent y on x.p_id=y.p_id
join agent z on x.p_id=z.p_id
order by 1,2,3;

-- start_ignore
drop schema cte_mc cascade;
-- end_ignore
