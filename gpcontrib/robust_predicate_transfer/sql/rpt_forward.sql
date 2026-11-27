---------------------------------------------------------------------------
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- Forward reducer, end to end: the InitPlan is built, its filters
-- reach the main plan across the dispatch boundary, intermediate hops work
-- both through Anser and in-process, and results never change.  Both
-- optimizers; every check raises with the plan or dump that failed it.
-- Faults live in rpt_forward_faults.
CREATE EXTENSION robust_predicate_transfer;
SET anser.runtime_filter = off;
SET rpt.prefer_inplace = off;
SET rpt.key_survival = off;
SET rpt.extended_safe_quals = off;
SET rpt.backward = off;
SET statement_timeout = '120s';
-- Star: two dimensions on two different fact keys (two hops into the root).
CREATE TABLE rpt_fwd_fact (id int, d1 int, d2 int, v int) DISTRIBUTED RANDOMLY;
-- The AOCO SQL qual uses v, while the Bloom consumer filters on d1.
CREATE TABLE rpt_fwd_aoco (d1 int, v int)
    WITH (appendonly = true, orientation = column) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwd_d1 (k int, a int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwd_d2 (k int, b int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwd_empty (k int, a int) DISTRIBUTED RANDOMLY;
-- Snowflake: fact -> dimension -> subdimension, a chain of two hops whose
-- first hop is consumed inside the reducer.  rpt_fwd_dl and rpt_fwd_sub are
-- distributed on their join columns, so that hop is local (in-process);
-- rpt_fwd_dr is the same dimension distributed randomly, so the hop is global.
CREATE TABLE rpt_fwd_dl (k int, s int) DISTRIBUTED BY (s);
CREATE TABLE rpt_fwd_dr (k int, s int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwd_sub (k int, c int) DISTRIBUTED BY (k);
INSERT INTO rpt_fwd_fact
    SELECT i, CASE WHEN i % 97 = 0 THEN NULL ELSE i % 1000 END, i % 500, i
    FROM generate_series(1, 200000) i;
INSERT INTO rpt_fwd_aoco SELECT d1 - 500, v FROM rpt_fwd_fact;
ANALYZE rpt_fwd_aoco;
INSERT INTO rpt_fwd_d1 SELECT i, i FROM generate_series(0, 999) i;
INSERT INTO rpt_fwd_d2 SELECT i, i FROM generate_series(0, 499) i;
INSERT INTO rpt_fwd_dl SELECT i, i % 100 FROM generate_series(0, 999) i;
INSERT INTO rpt_fwd_dr SELECT i, i % 100 FROM generate_series(0, 999) i;
INSERT INTO rpt_fwd_sub SELECT i, i FROM generate_series(0, 99) i;
ANALYZE rpt_fwd_fact;
ANALYZE rpt_fwd_d1;
ANALYZE rpt_fwd_d2;
ANALYZE rpt_fwd_empty;
ANALYZE rpt_fwd_dl;
ANALYZE rpt_fwd_dr;
ANALYZE rpt_fwd_sub;
-- Legal LEAKPROOF function: the error reveals nothing about its argument.
-- It is still unsafe for an eager reducer when the main scan never executes.
CREATE FUNCTION pg_temp.rpt_fwd_constant_error(integer) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE LEAKPROOF AS $$
BEGIN
    RAISE EXCEPTION 'constant failure';
END
$$;
CREATE FUNCTION pg_temp.rpt_fwd_plan(q text, do_analyze boolean) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE format('EXPLAIN (%s VERBOSE, FORMAT JSON) %s',
                   CASE WHEN do_analyze THEN 'ANALYZE,' ELSE '' END, q) INTO plan;
    RETURN plan;
END
$$;
-- Result of q as one comparable text value, with RPT on or off.
CREATE FUNCTION pg_temp.rpt_fwd_result(q text, rpt boolean) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    r text;
BEGIN
    PERFORM set_config('rpt.enable', CASE WHEN rpt THEN 'on' ELSE 'off' END, true);
    EXECUTE format('SELECT string_agg(t::text, %L ORDER BY t::text) FROM (%s) t', ';', q) INTO r;
    PERFORM set_config('rpt.enable', 'on', true);
    RETURN coalesce(r, '<empty>');
END
$$;
-- Nodes of a provider inside InitPlans (reducer) or outside them (main plan);
-- a NULL provider matches every node.
CREATE FUNCTION pg_temp.rpt_fwd_nodes(plan jsonb, provider text, in_reducer boolean)
RETURNS SETOF jsonb LANGUAGE sql AS $$
    WITH RECURSIVE walk(node, in_init) AS (
        SELECT plan->0->'Plan', false
        UNION ALL
        SELECT child, in_init OR coalesce(child->>'Parent Relationship', '') = 'InitPlan'
        FROM walk CROSS JOIN LATERAL jsonb_array_elements(coalesce(node->'Plans', '[]')) child
    )
    SELECT node FROM walk
    WHERE in_init = in_reducer AND (provider IS NULL OR node->>'Custom Plan Provider' = provider)
$$;
CREATE FUNCTION pg_temp.rpt_fwd_count(plan jsonb, provider text, in_reducer boolean)
RETURNS bigint LANGUAGE sql AS $$
    SELECT count(*) FROM pg_temp.rpt_fwd_nodes(plan, provider, in_reducer)
$$;
-- Rows removed by forward filters in the main plan: by a consumer itself, or
-- by the Seq Scan it pushed the filter into.
CREATE FUNCTION pg_temp.rpt_fwd_removed(plan jsonb) RETURNS float8
LANGUAGE sql AS $$
    SELECT coalesce(sum(coalesce((node->>'Rows Removed by Bloom Filter')::float8, 0) +
                        coalesce((node->>'Rows Removed by Pushdown Runtime Filter')::float8, 0)), 0)
    FROM pg_temp.rpt_fwd_nodes(plan, NULL, false) node
$$;
DO $$
DECLARE
    opt text;
    q text;
    selectivity_limit text;
    dump text;
    plan jsonb;
    prep_n bigint;
    prep_s bigint;
    star constant text :=
        'SELECT count(*) AS n, sum(f.v) AS s FROM rpt_fwd_fact f
           JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k JOIN rpt_fwd_d2 d2 ON f.d2 = d2.k
          WHERE d1.a < 50 AND d2.b < 100';
    flake_local constant text :=
        'SELECT count(*) AS n, sum(f.v) AS s FROM rpt_fwd_fact f
           JOIN rpt_fwd_dl d ON f.d1 = d.k JOIN rpt_fwd_sub u ON d.s = u.k
          WHERE u.c < 5';
    flake_global constant text :=
        'SELECT count(*) AS n, sum(f.v) AS s FROM rpt_fwd_fact f
           JOIN rpt_fwd_dr d ON f.d1 = d.k JOIN rpt_fwd_sub u ON d.s = u.k
          WHERE u.c < 5';
BEGIN
    PERFORM set_config('rpt.enable', 'on', true);
    PERFORM set_config('rpt.forward_min_root_rows', '0', true);
    PERFORM set_config('rpt.forward_max_exchange_ms', '-1', true);
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);

        -- Check the exact result multiset. Also require real rejections,
        -- by the consumer or by the scan it handed the filter to, so an
        -- unfiltered plan cannot pass this reproducer.
        q := 'SELECT f.v FROM rpt_fwd_aoco f JOIN rpt_fwd_d1 d ON f.d1 = d.k
              WHERE f.v > 100000 AND d.a < 20';
        IF pg_temp.rpt_fwd_result(q, true) IS DISTINCT FROM
           pg_temp.rpt_fwd_result(q, false) THEN
            RAISE EXCEPTION 'AOCO Bloom filter changed the result (optimizer=%)', opt;
        END IF;
        plan := pg_temp.rpt_fwd_plan(q, true);
        IF NOT EXISTS (SELECT FROM pg_temp.rpt_fwd_nodes(plan, 'Anser Bloom Consumer', false)) OR
           pg_temp.rpt_fwd_removed(plan) <= 0 THEN
            RAISE EXCEPTION 'AOCO fixture did not exercise Bloom filtering (optimizer=%)', opt;
        END IF;

        -- Shape: a reducer InitPlan with producers, a consumer in the main plan.
        dump := rpt_transfer_plan(star);
        IF dump NOT LIKE '%decision=inject%' THEN
            RAISE EXCEPTION 'star not selected (optimizer=%): %', opt, dump;
        END IF;
        plan := pg_temp.rpt_fwd_plan(star, false);
        IF pg_temp.rpt_fwd_count(plan, 'Anser Bloom Producer', true) = 0 OR
           pg_temp.rpt_fwd_count(plan, 'Anser Bloom Consumer', false) = 0 THEN
            RAISE EXCEPTION 'reducer shape missing (optimizer=%): %', opt, plan;
        END IF;

        -- Two root consumers must really be stacked. VERBOSE deparses their
        -- INDEX_VAR outputs; copying the child's identity tlist here used to
        -- make EXPLAIN loop forever in resolve_special_varno.
        IF NOT EXISTS (SELECT FROM pg_temp.rpt_fwd_nodes(plan, 'Anser Bloom Consumer', false) n
                       WHERE n->'Plans'->0->>'Custom Plan Provider' = 'Anser Bloom Consumer') THEN
            RAISE EXCEPTION 'star did not exercise stacked consumers (optimizer=%): %', opt, plan;
        END IF;

        -- The property the design rests on: filters produced in the InitPlan's
        -- dispatch are applied by the main plan's dispatch.
        plan := pg_temp.rpt_fwd_plan(star, true);
        IF pg_temp.rpt_fwd_removed(plan) <= 0 THEN
            RAISE EXCEPTION 'no rows removed by a forward filter in the main plan (optimizer=%): %', opt, plan;
        END IF;

        -- Multi-hop, global: the intermediate hop is an Anser consumer
        -- inside the reducer, fed by a producer of the same dispatch.
        dump := rpt_transfer_plan(flake_global);
        IF dump NOT LIKE '%decision=inject%' OR dump NOT LIKE '%channels=2 %' OR
           dump LIKE '%in-process%' THEN
            RAISE EXCEPTION 'global chain not selected as two Anser hops (optimizer=%): %', opt, dump;
        END IF;
        plan := pg_temp.rpt_fwd_plan(flake_global, true);
        IF pg_temp.rpt_fwd_count(plan, 'Anser Bloom Consumer', true) = 0 OR
           pg_temp.rpt_fwd_count(plan, 'Anser Bloom Consumer', false) <> 1 OR
           pg_temp.rpt_fwd_count(plan, 'Anser Bloom Producer', true) < 2 OR
           pg_temp.rpt_fwd_removed(plan) <= 0 THEN
            RAISE EXCEPTION 'global chain shape or effect missing (optimizer=%): %', opt, plan;
        END IF;

        -- Multi-hop, local: the intermediate hop never leaves the QE.
        dump := rpt_transfer_plan(flake_local);
        IF dump NOT LIKE '%decision=inject%' OR dump NOT LIKE '%local in-process%' THEN
            RAISE EXCEPTION 'local hop not in-process (optimizer=%): %', opt, dump;
        END IF;
        plan := pg_temp.rpt_fwd_plan(flake_local, true);
        IF pg_temp.rpt_fwd_count(plan, 'RPT Local Bloom Producer', true) <> 1 OR
           pg_temp.rpt_fwd_count(plan, 'Anser Bloom Consumer', false) <> 1 OR
           pg_temp.rpt_fwd_count(plan, 'RPT Local Bloom Consumer', true) <> 1 OR
           pg_temp.rpt_fwd_count(plan, 'Anser Bloom Consumer', true) <> 0 OR
           (SELECT sum((n->>'Rows Removed by Local Bloom Filter')::float8)
              FROM pg_temp.rpt_fwd_nodes(plan, 'RPT Local Bloom Consumer', true) n) <= 0 OR
           pg_temp.rpt_fwd_removed(plan) <= 0 THEN
            RAISE EXCEPTION 'local chain shape or effect missing (optimizer=%): %', opt, plan;
        END IF;

        -- Results never change: star, chains, empty
        -- filters, NULL keys, a volatile qual (dropped whole in the reducer),
        -- an empty dimension, a preserved side.
        FOREACH q IN ARRAY ARRAY[
            star,
            flake_local,
            flake_global,
            'SELECT f.id, f.v FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 3',
            -- Existing scalar InitPlans and correlated expression SubPlans
            -- must be walked with the statement's subplan lookup context.
            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 50 AND f.v <= (SELECT max(v) FROM rpt_fwd_fact)',
            'SELECT f.id, (SELECT max(b) FROM rpt_fwd_d2 d2 WHERE d2.k = f.d2) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 50 AND f.id < 10',

            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 0',
            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a IN (0,1,2,3,4,5,6,7,8,9)',
            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a NOT IN (0,1,2,3,4,5,6,7,8,9)',
            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 50 AND random() >= 0',
            'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_empty e ON f.d1 = e.k WHERE e.a < 50',
            'SELECT count(*) FROM rpt_fwd_fact f LEFT JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k AND d1.a < 50',
            'SELECT d.k, count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_dl d ON f.d1 = d.k
               JOIN rpt_fwd_sub u ON d.s = u.k WHERE u.c IN (1, 7) GROUP BY d.k'
        ] LOOP
            IF pg_temp.rpt_fwd_result(q, true) IS DISTINCT FROM pg_temp.rpt_fwd_result(q, false) THEN
                RAISE EXCEPTION 'results differ with RPT (optimizer=%): %', opt, q;
            END IF;
        END LOOP;

        -- A prepared statement keeps its reducer and its answer.
        EXECUTE 'PREPARE rpt_fwd_prep AS ' || star;
        FOR i IN 1..2 LOOP
            EXECUTE 'EXECUTE rpt_fwd_prep' INTO prep_n, prep_s;
            IF row(prep_n, prep_s)::text IS DISTINCT FROM pg_temp.rpt_fwd_result(star, false) THEN
                RAISE EXCEPTION 'prepared statement result differs (optimizer=%, run %)', opt, i;
            END IF;
        END LOOP;
        EXECUTE 'DEALLOCATE rpt_fwd_prep';

        -- Gates: below the root-row threshold, nothing is injected.
        PERFORM set_config('rpt.forward_min_root_rows', '1e12', true);
        IF rpt_transfer_plan(star) NOT LIKE '%decision=root rows below rpt.forward_min_root_rows%' OR
           pg_temp.rpt_fwd_count(pg_temp.rpt_fwd_plan(star, false), NULL, true) <> 0 THEN
            RAISE EXCEPTION 'root-row gate did not refuse (optimizer=%)', opt;
        END IF;
        PERFORM set_config('rpt.forward_min_root_rows', '0', true);

        -- Gates: an exchange budget below the estimate refuses the chain.
        PERFORM set_config('rpt.forward_max_exchange_ms', '0.001', true);
        IF rpt_transfer_plan(star) NOT LIKE '%decision=estimated exchange cost above%' THEN
            RAISE EXCEPTION 'cost gate did not refuse (optimizer=%)', opt;
        END IF;
        PERFORM set_config('rpt.forward_max_exchange_ms', '-1', true);

        -- Gates: a dimension keeping 90% of its keys is not worth a reducer.
        q := 'SELECT count(*) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 900';
        IF rpt_transfer_plan(q) NOT LIKE '%decision=estimated root selectivity not below%' OR
           pg_temp.rpt_fwd_count(pg_temp.rpt_fwd_plan(q, false), NULL, true) <> 0 THEN
            RAISE EXCEPTION 'selectivity gate did not refuse (optimizer=%)', opt;
        END IF;

        -- Exercise eager-qual safety independently of admission estimates.
        PERFORM set_config('rpt.forward_total_cost', 'off', true);
        q := 'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE 100 / d1.a > 1';
        dump := rpt_transfer_plan(q);
        IF dump NOT LIKE '%dropped in the reducer (not provably error-free)%' OR
           dump NOT LIKE '%decision=estimated root selectivity not below%' THEN
            RAISE EXCEPTION 'unsafe qual not dropped, or chain not refused (optimizer=%): %', opt, dump;
        END IF;
        q := 'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE pg_temp.rpt_fwd_constant_error(d1.a)';
        IF rpt_transfer_plan(q) NOT LIKE '%dropped in the reducer (not provably error-free)%' THEN
            RAISE EXCEPTION 'leakproof user function accepted by reducer (optimizer=%)', opt;
        END IF;
        FOREACH selectivity_limit IN ARRAY ARRAY['0.35', '1'] LOOP
            PERFORM set_config('rpt.forward_max_selectivity', selectivity_limit, true);
            -- Segment slices can prefetch even under a coordinator LIMIT 0.
            -- Guard errors in the main scan, and verify the reducer really
            -- runs without these unsafe quals instead of relying on timing.
            FOREACH q IN ARRAY ARRAY[
                'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE CASE WHEN d1.a <> 0 THEN 100 / d1.a > 1 ELSE false END LIMIT 0',
                'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE CASE WHEN d1.a <> 0 AND d1.a < 50 THEN 100 / d1.a > 1 ELSE false END LIMIT 0',
                'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE (d1.a::text)::int < 50 LIMIT 0',
                'SELECT f.id FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE CASE WHEN d1.a < 0 THEN pg_temp.rpt_fwd_constant_error(d1.a) ELSE false END LIMIT 0'
            ] LOOP
                IF pg_temp.rpt_fwd_result(q, false) <> '<empty>' OR
                   pg_temp.rpt_fwd_result(q, true) <> '<empty>' THEN
                    RAISE EXCEPTION 'LIMIT 0 returned rows (optimizer=%): %', opt, q;
                END IF;
                -- GPORCA can fold LIMIT 0 away. The PostgreSQL planner
                -- retains it, so require actual unqualified reducer reads.
                IF selectivity_limit = '1' AND opt = 'off' THEN
                    plan := pg_temp.rpt_fwd_plan(q, true);
                    IF NOT EXISTS (SELECT FROM pg_temp.rpt_fwd_nodes(plan, NULL, true) n
                        WHERE n->>'Node Type' = 'Seq Scan'
                          AND n->>'Relation Name' = 'rpt_fwd_d1'
                          AND (n->>'Actual Rows')::float8 > 0
                          AND NOT (n ? 'Filter')) THEN
                        RAISE EXCEPTION 'unsafe-qual fixture did not scan an unqualified reducer: %', plan;
                    END IF;
                END IF;
            END LOOP;
        END LOOP;
        PERFORM set_config('rpt.forward_max_selectivity', '0.35', true);
        PERFORM set_config('rpt.forward_total_cost', 'on', true);
        -- ... while provably safe quals are kept.
        IF rpt_transfer_plan(star) LIKE '%dropped in the reducer%' THEN
            RAISE EXCEPTION 'safe quals dropped (optimizer=%)', opt;
        END IF;

        -- Exclusivity: with Anser's own runtime filter on, RPT+ stands down.
        PERFORM set_config('anser.runtime_filter', 'on', true);
        IF rpt_transfer_plan(star) NOT LIKE '%forward: needs anser.enable=on and anser.runtime_filter=off%' OR
           pg_temp.rpt_fwd_count(pg_temp.rpt_fwd_plan(star, false), NULL, true) <> 0 THEN
            RAISE EXCEPTION 'RPT+ did not stand down for anser.runtime_filter (optimizer=%)', opt;
        END IF;
        PERFORM set_config('anser.runtime_filter', 'off', true);
    END LOOP;
END
$$;
-- Extended comparisons may run in an eager reducer. Check admission and
-- compare the same execution with and without zero-copy filter wrappers.
CREATE TABLE rpt_fwd_typed (k int, label varchar(8), day date) DISTRIBUTED RANDOMLY;
INSERT INTO rpt_fwd_typed
    SELECT i, CASE WHEN i < 20 THEN 'keep' ELSE 'skip' END, date '2001-01-01' + i
    FROM generate_series(0, 999) i;
ANALYZE rpt_fwd_typed;
DO $$
DECLARE
    q constant text := $query$SELECT f.id, f.v FROM rpt_fwd_fact f JOIN rpt_fwd_typed d ON f.d1 = d.k
        WHERE d.label = 'keep' AND d.day BETWEEN timestamp '2001-01-01' AND timestamp '2001-01-10'$query$;
    dump text;
    expected text;
BEGIN
    PERFORM set_config('optimizer', 'off', true);
    PERFORM set_config('rpt.enable', 'on', true);
    PERFORM set_config('rpt.forward_min_root_rows', '0', true);
    PERFORM set_config('rpt.extended_safe_quals', 'on', true);
    dump := rpt_transfer_plan(q);
    IF dump NOT LIKE '%forward component%decision=inject%' OR dump LIKE '%quals of % dropped%' THEN
        RAISE EXCEPTION 'extended reducer predicates did not survive: %', dump;
    END IF;
    PERFORM set_config('anser.zero_copy', 'off', true);
    expected := pg_temp.rpt_fwd_result(q, false);
    IF expected = '<empty>' OR pg_temp.rpt_fwd_result(q, true) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'extended reducer predicates changed results';
    END IF;
    PERFORM set_config('anser.zero_copy', 'on', true);
    IF pg_temp.rpt_fwd_count(pg_temp.rpt_fwd_plan(q, true), 'Anser Bloom Consumer', false) = 0 THEN
        RAISE EXCEPTION 'zero-copy fixture did not execute the filter';
    END IF;
    IF pg_temp.rpt_fwd_result(q, true) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'zero-copy filter wrappers changed results';
    END IF;
END
$$;
DROP TABLE rpt_fwd_typed;
-- PL/pgSQL (SPI) runs the reducer like any other caller.  Its static
-- statements keep the plan of their first call, so the function runs once,
-- with RPT on, and the baseline comes from uncached dynamic SQL.
CREATE FUNCTION pg_temp.rpt_fwd_spi() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    total bigint := 0;
    r record;
BEGIN
    FOR r IN SELECT f.v FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k WHERE d1.a < 50 LOOP
        total := total + r.v;
    END LOOP;
    FOR r IN SELECT f.v FROM rpt_fwd_fact f JOIN rpt_fwd_dl d ON f.d1 = d.k
               JOIN rpt_fwd_sub u ON d.s = u.k WHERE u.c < 5 LOOP
        total := total + r.v;
    END LOOP;
    RETURN total;
END
$$;
DO $$
DECLARE
    with_rpt bigint;
    without bigint;
BEGIN
    PERFORM set_config('rpt.forward_min_root_rows', '0', true);
    PERFORM set_config('rpt.forward_max_exchange_ms', '-1', true);
    PERFORM set_config('rpt.enable', 'on', true);
    with_rpt := pg_temp.rpt_fwd_spi();
    PERFORM set_config('rpt.enable', 'off', true);
    EXECUTE 'SELECT (SELECT sum(f.v) FROM rpt_fwd_fact f JOIN rpt_fwd_d1 d1 ON f.d1 = d1.k
                      WHERE d1.a < 50) +
                    (SELECT sum(f.v) FROM rpt_fwd_fact f JOIN rpt_fwd_dl d ON f.d1 = d.k
                      JOIN rpt_fwd_sub u ON d.s = u.k WHERE u.c < 5)' INTO without;
    IF with_rpt IS DISTINCT FROM without THEN
        RAISE EXCEPTION 'SPI result differs: % vs %', with_rpt, without;
    END IF;
END
$$;
DROP TABLE rpt_fwd_fact, rpt_fwd_aoco, rpt_fwd_d1, rpt_fwd_d2, rpt_fwd_empty,
           rpt_fwd_dl, rpt_fwd_dr, rpt_fwd_sub;
DROP EXTENSION robust_predicate_transfer;
RESET rpt.prefer_inplace;
RESET rpt.key_survival;
RESET rpt.extended_safe_quals;
RESET anser.runtime_filter;
RESET rpt.backward;
RESET statement_timeout;
