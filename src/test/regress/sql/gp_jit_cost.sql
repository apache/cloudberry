--
-- JIT cost thresholds.  A plan from GPORCA is compared against
-- optimizer_jit_above_cost and its inline/optimize siblings, a plan from the
-- planner against jit_above_cost and its siblings; "jit" switches both.
--
-- gp_jit_cost_plan() runs a query under EXPLAIN ANALYZE and reports which
-- optimizer planned it and what the coordinator JIT-compiled.  Each check is
-- written so that it reads true under either optimizer, and also in a build
-- without --with-llvm, where nothing is ever compiled.
--
create table gp_jit_cost (a int, b int) distributed by (a);
insert into gp_jit_cost select g, g % 7 from generate_series(1, 1000) g;
analyze gp_jit_cost;

create function gp_jit_cost_plan(q text, out orca bool, out jitted bool,
                                 out inlined bool, out optimized bool)
language plpgsql as $$
declare
  line text;
begin
  orca := false; jitted := false; inlined := false; optimized := false;
  for line in execute 'explain (analyze, timing off, summary off) ' || q loop
    orca := orca or line ~ '^\s*Optimizer: GPORCA';
    jitted := jitted or line ~ '^\s*Functions: [1-9]';
    inlined := inlined or line ~ '^\s*Options: .*Inlining true';
    optimized := optimized or line ~ '^\s*Options: .*Optimization true';
  end loop;
end $$;

-- the aggregate is finalized on the coordinator, so it compiles something
\set q 'select count(*), sum(b) from gp_jit_cost'

set jit = on;

-- GPORCA's thresholds apply to GPORCA's plans only
set jit_above_cost = -1;
set optimizer_jit_above_cost = 0;
set optimizer_jit_optimize_above_cost = 0;
set optimizer_jit_inline_above_cost = -1;
select not pg_jit_available() or
       (jitted, optimized, inlined) = (orca, orca, false) as ok
from gp_jit_cost_plan(:'q');

-- the planner's thresholds apply to the planner's plans only
set jit_above_cost = 0;
set jit_optimize_above_cost = -1;
set jit_inline_above_cost = 0;
set optimizer_jit_above_cost = -1;
select not pg_jit_available() or
       (jitted, optimized, inlined) = (not orca, false, not orca) as ok
from gp_jit_cost_plan(:'q');

-- jit = off turns off both
set jit = off;
set optimizer_jit_above_cost = 0;
select not jitted as ok from gp_jit_cost_plan(:'q');

reset jit;
reset jit_above_cost;
reset jit_optimize_above_cost;
reset jit_inline_above_cost;
reset optimizer_jit_above_cost;
reset optimizer_jit_optimize_above_cost;
reset optimizer_jit_inline_above_cost;
drop function gp_jit_cost_plan(text);
drop table gp_jit_cost;
