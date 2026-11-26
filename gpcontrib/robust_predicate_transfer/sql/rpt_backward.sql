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
-- Backward producers run in the main plan above forward consumers. Every
-- result query is dynamic SQL, so GUC changes cannot reuse an earlier plan.
CREATE EXTENSION robust_predicate_transfer;
SET anser.runtime_filter = off;
SET rpt.prefer_inplace = off;
SET rpt.key_survival = off;
SET anser.timeout_ms = 600000;
-- This suite forces reducer shapes; admission policy is tested separately.
SET rpt.forward_total_cost = off;
-- This timeout covers the entire assertion DO block, under both optimizers.
SET statement_timeout = '120s';
SET work_mem = '1MB';
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET gp_enable_hashjoin_size_heuristic = on;
SET join_collapse_limit = 1;
SET optimizer_join_order = query;
CREATE TABLE rpt_bw_root (k int, s int) DISTRIBUTED BY (k);
CREATE TABLE rpt_bw_member (k int, pad text) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_bw_dim (s int, v int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_bw_bridge (k int, s int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_bw_empty (k int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_bw_local (k int) DISTRIBUTED BY (k);
CREATE TABLE rpt_bw_replica (k int) DISTRIBUTED REPLICATED;
CREATE TABLE rpt_bw_part (k int, pad text) DISTRIBUTED RANDOMLY
    PARTITION BY RANGE (k) (START (0) END (2000) EVERY (1000));
-- The root has more rows but a smaller hash footprint than the wide member.
-- Duplicate root keys also make its backward filter selective without a
-- root restriction that would change the largest-root choice.
INSERT INTO rpt_bw_root SELECT CASE WHEN i % 101 = 0 THEN NULL ELSE i % 10 END,
    i % 100 FROM generate_series(1, 10000) i;
-- Incompressible padding keeps the member's hash footprint genuinely wide.
INSERT INTO rpt_bw_member SELECT i,
    (SELECT string_agg(md5(i::text || ':' || j::text), '') FROM generate_series(1, 100) j)
    FROM generate_series(0, 999) i;
INSERT INTO rpt_bw_dim SELECT i, i FROM generate_series(0, 99) i;
INSERT INTO rpt_bw_bridge SELECT i, i % 100 FROM generate_series(0, 999) i;
INSERT INTO rpt_bw_local SELECT i FROM generate_series(0, 999) i;
INSERT INTO rpt_bw_replica SELECT i FROM generate_series(0, 999) i;
INSERT INTO rpt_bw_part SELECT * FROM rpt_bw_member;
ANALYZE rpt_bw_root;
ANALYZE rpt_bw_member;
ANALYZE rpt_bw_dim;
ANALYZE rpt_bw_bridge;
ANALYZE rpt_bw_empty;
ANALYZE rpt_bw_local;
ANALYZE rpt_bw_replica;
ANALYZE rpt_bw_part;
CREATE FUNCTION pg_temp.rpt_bw_result(q text, enabled boolean, backward boolean)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    r text;
BEGIN
    PERFORM set_config('rpt.enable', CASE WHEN enabled THEN 'on' ELSE 'off' END, true);
    PERFORM set_config('rpt.backward', CASE WHEN backward THEN 'on' ELSE 'off' END, true);
    q := format('SELECT coalesce(string_agg(t::text, %L ORDER BY t::text), %L) FROM (%s) t',
                ';', '<empty>', q);
    EXECUTE q INTO r;
    PERFORM set_config('rpt.enable', 'on', true);
    PERFORM set_config('rpt.backward', 'on', true);
    RETURN r;
END
$$;
CREATE FUNCTION pg_temp.rpt_bw_plan(q text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) ' || q INTO plan;
    RETURN plan;
END
$$;
CREATE FUNCTION pg_temp.rpt_bw_nodes(plan jsonb)
RETURNS SETOF jsonb LANGUAGE sql AS $$
    WITH RECURSIVE walk(node, in_init) AS (
        SELECT plan->0->'Plan', false
        UNION ALL
        SELECT child, in_init OR coalesce(child->>'Parent Relationship', '') = 'InitPlan'
        FROM walk CROSS JOIN LATERAL jsonb_array_elements(coalesce(node->'Plans', '[]')) child
    ) SELECT node FROM walk WHERE NOT in_init
$$;
CREATE FUNCTION pg_temp.rpt_bw_expect(q text, relation_name text, reason text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    dump text := rpt_transfer_plan(q);
    member_rtindex text;
BEGIN
    member_rtindex := substring(dump FROM ('vertex ([0-9]+) ' || relation_name || ' rows='));
    IF member_rtindex IS NULL OR NOT EXISTS (
        SELECT FROM regexp_split_to_table(dump, chr(10)) line
        WHERE line LIKE ('backward consumer ' || member_rtindex || ' root=%')
          AND line LIKE ('% decision=' || reason)) THEN
        RAISE EXCEPTION 'missing backward decision % for %, optimizer=%: %',
            reason, relation_name, current_setting('optimizer'), dump;
    END IF;
END
$$;
DO $$
DECLARE
    opt text;
    q text;
    dump text;
    plan jsonb;
    baseline text;
    answer text;
    cursor_name refcursor;
    first_row record;
    plain constant text := 'SELECT r.k, m.pad FROM rpt_bw_member m JOIN rpt_bw_root r ON m.k = r.k';
    chained constant text := 'SELECT r.k, m.pad FROM rpt_bw_member m JOIN
        (rpt_bw_root r JOIN rpt_bw_dim d ON r.s = d.s) ON m.k = r.k WHERE d.v < 5';
BEGIN
    PERFORM set_config('rpt.enable', 'on', true);
    PERFORM set_config('rpt.forward_min_root_rows', '0', true);
    PERFORM set_config('rpt.forward_max_exchange_ms', '-1', true);
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);
        PERFORM set_config('rpt.forward', 'off', true);
        PERFORM pg_temp.rpt_bw_expect(plain, 'm', 'inject');
        -- Forward skipping must not suppress an unfiltered root's broadcast.
        -- Redundant safe predicates must not be required to recover its edges.
        q := plain || ' WHERE m.k >= 0 AND r.s >= 0';
        PERFORM pg_temp.rpt_bw_expect(q, 'm', 'inject');
        IF pg_temp.rpt_bw_result(plain, true, true) IS DISTINCT FROM
           pg_temp.rpt_bw_result(q, true, true) THEN
            RAISE EXCEPTION 'redundant qualifiers changed backward results';
        END IF;
        plan := pg_temp.rpt_bw_plan(plain);
        IF NOT EXISTS (SELECT FROM pg_temp.rpt_bw_nodes(plan) n
                       WHERE n->>'Custom Plan Provider' = 'Anser Bloom Producer') OR
           NOT EXISTS (SELECT FROM pg_temp.rpt_bw_nodes(plan) n
                       WHERE n->>'Custom Plan Provider' = 'Anser Bloom Consumer') OR
           plan::text LIKE '%InitPlan%' THEN
            RAISE EXCEPTION 'backward-only shape wrong (optimizer=%): %', opt, plan;
        END IF;
        IF (SELECT coalesce(sum(coalesce((n->>'Rows Removed by Bloom Filter')::float8, 0) +
                                    coalesce((n->>'Rows Removed by Pushdown Runtime Filter')::float8, 0)), 0)
            FROM pg_temp.rpt_bw_nodes(plan) n) <= 0 THEN
            RAISE EXCEPTION 'backward consumer removed no rows (optimizer=%): %', opt, plan;
        END IF;
        -- The root producer must directly wrap the forward consumer stack.
        PERFORM set_config('rpt.forward', 'on', true);
        PERFORM pg_temp.rpt_bw_expect(chained, 'm', 'inject');
        plan := pg_temp.rpt_bw_plan(chained);
        IF plan::text NOT LIKE '%InitPlan%' OR NOT EXISTS (
            SELECT FROM pg_temp.rpt_bw_nodes(plan) n
            WHERE n->>'Custom Plan Provider' = 'Anser Bloom Producer'
              AND n->'Plans'->0->>'Custom Plan Provider' = 'Anser Bloom Consumer') THEN
            RAISE EXCEPTION 'backward producer is not above forward consumer (optimizer=%): %', opt, plan;
        END IF;
        -- Refusing forward must not suppress a useful backward filter.
        PERFORM set_config('rpt.forward_min_root_rows', '1e12', true);
        PERFORM pg_temp.rpt_bw_expect(chained, 'm', 'inject');
        IF rpt_transfer_plan(chained) NOT LIKE '%decision=root rows below rpt.forward_min_root_rows%' THEN
            RAISE EXCEPTION 'forward gate fixture did not actually refuse';
        END IF;
        PERFORM set_config('rpt.forward_min_root_rows', '0', true);
        -- Runtime gates, with the same fixture known to inject above.
        PERFORM set_config('rpt.forward_max_selectivity', '0', true);
        PERFORM pg_temp.rpt_bw_expect(plain, 'm',
            'estimated member selectivity not below rpt.forward_max_selectivity');
        PERFORM set_config('rpt.forward_max_selectivity', '0.35', true);
        PERFORM set_config('rpt.forward_max_exchange_ms', '0', true);
        PERFORM pg_temp.rpt_bw_expect(plain, 'm',
            'estimated exchange cost above rpt.forward_max_exchange_ms');
        PERFORM set_config('rpt.forward_max_exchange_ms', '-1', true);
        PERFORM pg_temp.rpt_bw_expect(
            'SELECT * FROM rpt_bw_root r JOIN rpt_bw_local l ON r.k = l.k',
            'l', 'local edge uses core runtime filtering');
        -- A selective local build makes the segment-side filter reject
        -- actual probe rows. Isolate it from the forward reducer and core
        -- pushdown so the scan counter can only come from Anser in-place.
        q := 'SELECT r.k FROM rpt_bw_root r JOIN rpt_bw_local l ON r.k = l.k WHERE l.k < 5';
        PERFORM set_config('rpt.prefer_inplace', 'on', true);
        PERFORM set_config('anser.inplace_with_consumer', 'on', true);
        PERFORM set_config('rpt.forward', 'off', true);
        PERFORM set_config('gp_enable_runtime_filter_pushdown', 'off', true);
        PERFORM set_config('anser.runtime_filter_inplace', 'on', true);
        plan := pg_temp.rpt_bw_plan(q);
        IF NOT EXISTS (
               SELECT FROM pg_temp.rpt_bw_nodes(plan) n
               WHERE n->>'Relation Name' = 'rpt_bw_root'
                 AND (n->>'Rows Removed by Pushdown Runtime Filter')::float8 > 0
           ) OR EXISTS (
               SELECT FROM pg_temp.rpt_bw_nodes(plan) n
               WHERE n->>'Custom Plan Provider' IN ('Anser Bloom Producer', 'Anser Bloom Consumer')
           ) THEN
            RAISE EXCEPTION 'expected in-place pruning without an exchange (optimizer=%): %', opt, plan;
        END IF;
        IF pg_temp.rpt_bw_result(q, true, true) IS DISTINCT FROM
           pg_temp.rpt_bw_result(q, false, false) THEN
            RAISE EXCEPTION 'in-place RPT filter changed results (optimizer=%)', opt;
        END IF;
        PERFORM set_config('rpt.prefer_inplace', 'off', true);
        PERFORM set_config('anser.inplace_with_consumer', 'off', true);
        PERFORM set_config('rpt.forward', 'on', true);
        -- Narrow projection makes the smaller member the hash build side.
        PERFORM pg_temp.rpt_bw_expect(
            'SELECT count(*) FROM rpt_bw_root r JOIN rpt_bw_member m ON r.k = m.k',
            'm', 'producer is not on a qualifying hash build side');
        -- LEFT's preserved root cannot be filtered by its nullable member.
        -- Its member has no unprotected equivalence class for broadcasting.
        PERFORM pg_temp.rpt_bw_expect(
            'SELECT r.k, m.pad FROM rpt_bw_root r LEFT JOIN rpt_bw_member m ON r.k = m.k',
            'm', 'edge has no unprotected equivalence class');
        dump := rpt_transfer_plan('SELECT * FROM rpt_bw_root r JOIN rpt_bw_bridge b
            ON r.k = b.k JOIN rpt_bw_dim d ON b.s = d.s WHERE d.v < 5');
        IF dump NOT LIKE '%backward class %decision=bridge-led class is deferred%' THEN
            RAISE EXCEPTION 'bridge-led backward class was not deferred (optimizer=%): %', opt, dump;
        END IF;
        -- Keep padding only in each join's input. Reference the non-key s:
        -- using k lets equivalence-class inference copy the predicate above
        -- both joins, retaining wide intermediate tuples and flipping builds.
        -- Let ORCA choose the smaller build subtree rather than fixing the
        -- syntactic join order for this fan-out fixture.
        PERFORM set_config('optimizer_join_order', 'exhaustive', true);
        q := 'SELECT count(*) FROM
            (rpt_bw_root r JOIN rpt_bw_member m1 ON m1.k = r.k AND length(m1.pad) > r.s)
            JOIN rpt_bw_member m2 ON m2.k = r.k AND length(m2.pad) > r.s';
        PERFORM set_config('rpt.max_backward_consumers', '4', true);
        dump := rpt_transfer_plan(q);
        IF (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE 'backward consumer % root=% decision=inject') <> 2 THEN
            RAISE EXCEPTION 'fan-out fixture needs two runtime consumers (optimizer=%): %', opt, dump;
        END IF;
        PERFORM set_config('rpt.max_backward_consumers', '1', true);
        dump := rpt_transfer_plan(q);
        IF (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE 'backward consumer % root=% decision=inject') <> 1 OR
           (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE 'backward consumer % root=% decision=backward consumer cap exceeded') <> 1 THEN
            RAISE EXCEPTION 'runtime backward fan-out cap not exercised (optimizer=%): %', opt, dump;
        END IF;
        plan := pg_temp.rpt_bw_plan(q);
        IF (SELECT count(*) FROM pg_temp.rpt_bw_nodes(plan) n
            WHERE n->>'Custom Plan Provider' = 'Anser Bloom Consumer') <> 1 OR
           pg_temp.rpt_bw_result(q, true, true) IS DISTINCT FROM
           pg_temp.rpt_bw_result(q, false, false) THEN
            RAISE EXCEPTION 'capped fan-out plan or answer differs (optimizer=%): %', opt, plan;
        END IF;
        PERFORM set_config('rpt.max_backward_consumers', '4', true);
        PERFORM set_config('optimizer_join_order', 'query', true);
        -- Every runtime proof refusal is retained verbatim in the new dump;
        -- real-plan proof cases also run in rpt_backward_eligibility.
        q := 'SELECT r.k, (SELECT count(*) FROM rpt_bw_member m
            JOIN rpt_bw_dim d ON m.k = d.s WHERE m.k < r.k AND random() < 1 OFFSET 0)
            FROM rpt_bw_root r';
        dump := rpt_transfer_plan(q);
        IF dump = ('no transfer regions' || chr(10)) THEN
            -- A region boundary can reject the whole correlated subtree.
            -- Still require the real SubPlan, and no published Bloom filter.
            EXECUTE 'EXPLAIN (VERBOSE, FORMAT JSON) ' || q INTO plan;
            IF NOT EXISTS (SELECT FROM pg_temp.rpt_bw_nodes(plan) n
                           WHERE n->>'Parent Relationship' = 'SubPlan') OR
               EXISTS (SELECT FROM pg_temp.rpt_bw_nodes(plan) n
                       WHERE n->>'Custom Plan Provider' IN
                             ('Anser Bloom Producer', 'Anser Bloom Consumer')) THEN
                RAISE EXCEPTION 'correlated boundary fixture lost its SubPlan or gained a filter: %', plan;
            END IF;
        ELSIF dump NOT LIKE '%decision=scan inside a SubPlan%' THEN
            RAISE EXCEPTION 'correlated producer was not refused (optimizer=%): %', opt, dump;
        END IF;
        FOREACH q IN ARRAY ARRAY[
            plain, chained,
            'SELECT count(*), sum(r.k) FROM rpt_bw_root r LEFT JOIN rpt_bw_member m ON r.k = m.k',
            'SELECT count(*), sum(r.k) FROM rpt_bw_member m RIGHT JOIN rpt_bw_root r ON r.k = m.k',
            'SELECT count(*), sum(r.k) FROM rpt_bw_root r FULL JOIN rpt_bw_member m ON r.k = m.k',
            'SELECT count(*), sum(r.k) FROM rpt_bw_root r WHERE EXISTS (SELECT FROM rpt_bw_member m WHERE m.k = r.k)',
            'SELECT count(*), sum(r.k) FROM rpt_bw_root r WHERE NOT EXISTS (SELECT FROM rpt_bw_member m WHERE m.k = r.k)',
            'SELECT count(*) FROM rpt_bw_root r JOIN rpt_bw_empty e ON r.k = e.k',
            'SELECT count(*) FROM rpt_bw_empty e JOIN rpt_bw_member m ON e.k = m.k',
            'SELECT count(*) FROM rpt_bw_root r JOIN rpt_bw_replica p ON r.k = p.k',
            'SELECT count(*) FROM rpt_bw_replica p JOIN rpt_bw_member m ON p.k = m.k',
            'SELECT count(*) FROM rpt_bw_root r JOIN rpt_bw_part p ON r.k = p.k',
            'SELECT r.k, m.pad FROM rpt_bw_member m JOIN rpt_bw_root r ON m.k = r.k LIMIT 1',
            'SELECT r.k, m.pad FROM rpt_bw_member m JOIN rpt_bw_root r ON m.k = r.k LIMIT 0'
        ] LOOP
            -- LIMIT 1 has no SQL ordering guarantee; compare a count instead.
            IF q LIKE '%LIMIT 1' THEN q := 'SELECT count(*) FROM (' || q || ') limited'; END IF;
            baseline := pg_temp.rpt_bw_result(q, false, false);
            IF pg_temp.rpt_bw_result(q, true, false) IS DISTINCT FROM baseline OR
               pg_temp.rpt_bw_result(q, true, true) IS DISTINCT FROM baseline THEN
                RAISE EXCEPTION 'backward result differs (optimizer=%): %', opt, q;
            END IF;
        END LOOP;
        q := 'SELECT count(*)::text || ''/'' || sum(length(m.pad || r.k::text))::text
            FROM rpt_bw_member m JOIN rpt_bw_root r ON m.k = r.k';
        PERFORM pg_temp.rpt_bw_expect(q, 'm', 'inject');
        baseline := pg_temp.rpt_bw_result(q, false, false);
        EXECUTE 'PREPARE rpt_bw_prepared AS ' || q;
        FOR i IN 1..2 LOOP
            EXECUTE 'EXECUTE rpt_bw_prepared' INTO answer;
            IF '(' || answer || ')' IS DISTINCT FROM baseline THEN
                RAISE EXCEPTION 'backward prepared answer differs';
            END IF;
        END LOOP;
        EXECUTE 'DEALLOCATE rpt_bw_prepared';
        cursor_name := 'rpt_bw_cursor';
        OPEN cursor_name FOR EXECUTE plain;
        FETCH cursor_name INTO first_row;
        IF NOT FOUND THEN RAISE EXCEPTION 'backward cursor returned no row'; END IF;
        CLOSE cursor_name;
        -- A new command must answer after the cursor squelches its producer.
        IF pg_temp.rpt_bw_result(q, true, true) IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'next command after backward cursor';
        END IF;
        PERFORM set_config('anser.runtime_filter', 'on', true);
        dump := rpt_transfer_plan(chained);
        IF dump LIKE '%key=rpt_b_%' OR dump NOT LIKE '%needs anser.enable=on and anser.runtime_filter=off%' THEN
            RAISE EXCEPTION 'backward did not stand down for Anser: %', dump;
        END IF;
        PERFORM set_config('anser.runtime_filter', 'off', true);
    END LOOP;
END
$$;
DROP TABLE rpt_bw_root, rpt_bw_member, rpt_bw_dim, rpt_bw_bridge, rpt_bw_empty,
           rpt_bw_local, rpt_bw_replica, rpt_bw_part;
DROP EXTENSION robust_predicate_transfer;
RESET rpt.prefer_inplace;
RESET rpt.key_survival;
RESET anser.runtime_filter;
RESET anser.timeout_ms;
RESET statement_timeout;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET gp_enable_hashjoin_size_heuristic;
RESET work_mem;
RESET join_collapse_limit;
RESET optimizer_join_order;
RESET rpt.forward_total_cost;
