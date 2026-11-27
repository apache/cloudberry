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
-- Production walker/resolver coverage. Both optimizer settings run these
-- queries; every assertion resolves vertex IDs from relation names.
CREATE EXTENSION robust_predicate_transfer;
SET rpt.enable = off;
SET anser.runtime_filter = off;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET join_collapse_limit = 1;
SET optimizer_join_order = query;
SET statement_timeout = '30s';
CREATE TABLE rpt_elig_root (pad int, k int, v int) DISTRIBUTED BY (k);
CREATE TABLE rpt_elig_member (k int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_elig_local (k int) DISTRIBUTED BY (k);
CREATE TABLE rpt_elig_replica (k int) DISTRIBUTED REPLICATED;
CREATE TABLE rpt_elig_outer (k int) DISTRIBUTED BY (k);
CREATE TABLE rpt_elig_bridge (k1 int, k2 int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_elig_member2 (k int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_elig_member3 (k int) DISTRIBUTED RANDOMLY;
INSERT INTO rpt_elig_root SELECT 0, i, i FROM generate_series(1, 10000) i;
INSERT INTO rpt_elig_member SELECT i FROM generate_series(1, 10000) i;
INSERT INTO rpt_elig_local SELECT i FROM generate_series(1, 10000) i;
INSERT INTO rpt_elig_replica SELECT i FROM generate_series(1, 10000) i;
INSERT INTO rpt_elig_outer SELECT i FROM generate_series(1, 10) i;
INSERT INTO rpt_elig_bridge SELECT i, i FROM generate_series(1, 1000) i;
INSERT INTO rpt_elig_member2 SELECT i FROM generate_series(1, 10000) i;
INSERT INTO rpt_elig_member3 SELECT i FROM generate_series(1, 10000) i;
ANALYZE rpt_elig_root;
ANALYZE rpt_elig_member;
ANALYZE rpt_elig_local;
ANALYZE rpt_elig_replica;
ANALYZE rpt_elig_outer;
ANALYZE rpt_elig_bridge;
ANALYZE rpt_elig_member2;
ANALYZE rpt_elig_member3;
CREATE FUNCTION pg_temp.rpt_expect(q text, producer text, consumer text,
                                  reasons text[], accepted boolean DEFAULT NULL)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    dump text := rpt_transfer_plan(q);
    producer_rtindex text;
    consumer_rtindex text;
    decision text;
BEGIN
    producer_rtindex := substring(dump FROM ('vertex ([0-9]+) ' || producer || ' rows='));
    consumer_rtindex := substring(dump FROM ('vertex ([0-9]+) ' || consumer || ' rows='));
    IF producer_rtindex IS NULL OR consumer_rtindex IS NULL THEN
        RAISE EXCEPTION 'missing fixture scans % / %: %', producer, consumer, dump;
    END IF;
    SELECT line INTO decision FROM regexp_split_to_table(dump, chr(10)) line
        WHERE line LIKE ('backward consumer ' || consumer_rtindex || ' producer ' || producer_rtindex || ' %')
          AND substring(line FROM ' reason=(.*)$') = ANY(reasons)
          AND (accepted IS NULL OR line LIKE ('% eligible=' || CASE WHEN accepted THEN 'yes' ELSE 'no' END || ' %'))
        LIMIT 1;
    IF decision IS NULL THEN
        RAISE EXCEPTION 'expected % for % -> %, optimizer=%: %', reasons, producer, consumer, current_setting('optimizer'), dump;
    END IF;
    RETURN dump;
END
$$;
CREATE FUNCTION pg_temp.rpt_explain(q text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
    RETURN plan;
END
$$;
CREATE FUNCTION pg_temp.rpt_scan_path(q text, relation_name text) RETURNS text[] LANGUAGE SQL AS $$
    WITH RECURSIVE walk(node, path) AS (
        SELECT pg_temp.rpt_explain(q)->0->'Plan', ARRAY[]::text[]
        UNION ALL
        SELECT child, path || ((node->>'Node Type') || ':' ||
               COALESCE(node->>'Join Type', '') || ':' || COALESCE(child->>'Parent Relationship', ''))
        FROM walk CROSS JOIN LATERAL jsonb_array_elements(COALESCE(node->'Plans', '[]'::jsonb)) AS children(child)
    )
    SELECT path FROM walk WHERE node->>'Relation Name' = relation_name LIMIT 1
$$;
DO $$
DECLARE
    opt text;
    q text;
    dump text;
    path text[];
    producer_path text[];
    split_at integer;
    producer_rtindex text;
    correlated text;
BEGIN
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);
        -- Expressions force the real walker through targetlists and quals.
        -- k is physical column 2; the resolver must use its output position.
        PERFORM pg_temp.rpt_expect(
            'SELECT rpt_elig_root.k + rpt_elig_member.k, rpt_elig_root.v FROM rpt_elig_member JOIN rpt_elig_root USING (k) WHERE rpt_elig_root.v < 100 AND rpt_elig_root.v % 2 = 0',
            'rpt_elig_root', 'rpt_elig_member', ARRAY['eligible'], true);
        PERFORM pg_temp.rpt_expect(
            'SELECT * FROM rpt_elig_root JOIN rpt_elig_local USING (k) WHERE rpt_elig_root.v < 100',
            'rpt_elig_root', 'rpt_elig_local', ARRAY['local edge uses core runtime filtering'], false);
        PERFORM pg_temp.rpt_expect(
            'SELECT * FROM rpt_elig_member JOIN rpt_elig_root USING (k) WHERE rpt_elig_root.v < 100 AND rpt_elig_member.k > 0',
            'rpt_elig_member', 'rpt_elig_root', ARRAY['producer is not on a qualifying hash build side'], false);
        PERFORM pg_temp.rpt_expect(
            'SELECT * FROM rpt_elig_root JOIN rpt_elig_replica USING (k) WHERE rpt_elig_root.v < 100',
            'rpt_elig_root', 'rpt_elig_replica', ARRAY['same or unknown slice'], false);
        PERFORM pg_temp.rpt_expect(
            'SELECT * FROM rpt_elig_root LEFT JOIN rpt_elig_member USING (k) WHERE rpt_elig_root.v < 100',
            'rpt_elig_root', 'rpt_elig_member', ARRAY['edge has no unprotected equivalence class'], false);
        PERFORM pg_temp.rpt_expect(
            'SELECT * FROM rpt_elig_root JOIN rpt_elig_bridge ON rpt_elig_root.k = rpt_elig_bridge.k1 JOIN rpt_elig_member ON rpt_elig_bridge.k2 = rpt_elig_member.k WHERE rpt_elig_root.v < 9000',
            'rpt_elig_bridge', 'rpt_elig_member', ARRAY['bridge-led class is deferred'], false);
        -- ORDER BY can be discarded by ORCA. Assert the refusal only when
        -- the Sort remains below the qualifying hash join; otherwise require
        -- the unblocked query to qualify rather than silently skipping it.
        q := 'SELECT * FROM (SELECT * FROM rpt_elig_member ORDER BY k) rpt_elig_member JOIN rpt_elig_root USING (k) WHERE rpt_elig_root.v < 100';
        path := pg_temp.rpt_scan_path(q, 'rpt_elig_member');
        IF path IS NULL THEN
            RAISE EXCEPTION 'sort fixture lost its member scan';
        END IF;
        IF EXISTS (SELECT FROM unnest(path) WITH ORDINALITY h(tag, n), unnest(path) WITH ORDINALITY s(tag, n)
                   WHERE h.tag LIKE 'Hash Join:%:Outer' AND s.tag LIKE 'Sort:%' AND h.n < s.n) THEN
            PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member',
                ARRAY['sort between consumer and join', 'unsupported consumer key path'], false);
        ELSE
            PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member', ARRAY['eligible'], true);
        END IF;
        -- A strict upper join may simplify the LEFT join to INNER. If it
        -- survives, the nullable path must be refused by the production proof.
        q := 'SELECT * FROM (rpt_elig_outer LEFT JOIN rpt_elig_member USING (k)) JOIN rpt_elig_root ON rpt_elig_root.k = rpt_elig_member.k WHERE rpt_elig_root.v < 100';
        path := pg_temp.rpt_scan_path(q, 'rpt_elig_member');
        IF path IS NULL THEN
            RAISE EXCEPTION 'nullable-side fixture lost its member scan';
        END IF;
        IF EXISTS (SELECT FROM unnest(path) tag WHERE tag LIKE '%:Left:Inner' OR tag LIKE '%:Right:Outer') THEN
            PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member', ARRAY['nullable side of outer join'], false);
        ELSE
            -- Simplification does not fix hash build/probe order. Compare
            -- the first differing path step: the shared parent is the join
            -- separating these named scans, regardless of range-table IDs.
            producer_path := pg_temp.rpt_scan_path(q, 'rpt_elig_root');
            IF producer_path IS NULL THEN
                RAISE EXCEPTION 'nullable-side fixture lost its root scan';
            END IF;
            SELECT min(n) INTO split_at FROM generate_subscripts(path, 1) n
                WHERE path[n] IS DISTINCT FROM producer_path[n];
            IF split_at IS NULL THEN
                RAISE EXCEPTION 'nullable-side fixture has no separating join';
            END IF;
            IF producer_path[split_at] LIKE 'Hash Join:%:Outer'
               AND path[split_at] LIKE 'Hash Join:%:Inner' THEN
                PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member',
                    ARRAY['producer is not on a qualifying hash build side'], false);
            ELSE
                PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member', ARRAY['eligible'], true);
            END IF;
        END IF;
        -- An InitPlan is evaluated once, and its presence alone does not
        -- imply a nonempty extParam on the qualifying hash build. Depending
        -- on planner placement it can be internal to that build or external.
        -- The native test checks an explicit extParam bitmap; the correlated
        -- SubPlan below exercises the rescan refusal on a real SQL plan.
        q := 'SELECT * FROM rpt_elig_root JOIN rpt_elig_member USING (k) WHERE rpt_elig_root.v < (SELECT max(k) FROM rpt_elig_outer)';
        PERFORM pg_temp.rpt_expect(q, 'rpt_elig_root', 'rpt_elig_member',
            ARRAY['eligible', 'parameterized producer can be rescanned',
                  'unsupported producer key path', 'producer is not on a qualifying hash build side']);
        -- Volatility and OFFSET retain a correlated SubPlan instead of
        -- allowing a decorrelating join rewrite. Its internal Motion must
        -- not undo the explicit SubPlan refusal.
        correlated := 'SELECT rpt_elig_outer.k, (SELECT count(*) FROM rpt_elig_root JOIN rpt_elig_member USING (k) WHERE rpt_elig_root.v < rpt_elig_outer.k AND random() < 0.9 OFFSET 0) FROM rpt_elig_outer';
        IF pg_temp.rpt_explain(correlated)::text NOT LIKE '%SubPlan%' THEN
            RAISE EXCEPTION 'correlated fixture no longer contains a SubPlan (optimizer=%)', opt;
        END IF;
        path := pg_temp.rpt_scan_path(correlated, 'rpt_elig_root');
        IF path IS NULL OR NOT EXISTS (
            SELECT FROM unnest(path) WITH ORDINALITY sp(tag, n), unnest(path) WITH ORDINALITY motion(tag, n)
            WHERE sp.tag LIKE '%:SubPlan' AND motion.tag LIKE '%Motion:%' AND sp.n < motion.n) THEN
            RAISE EXCEPTION 'correlated fixture lost the Motion inside its SubPlan (optimizer=%)', opt;
        END IF;
        -- Some plans keep the correlated scans behind a region boundary.
        -- The plan checks above still require a SubPlan and an internal Motion.
        dump := rpt_transfer_plan(correlated);
        IF dump <> ('no transfer regions' || chr(10)) THEN
            dump := pg_temp.rpt_expect(correlated, 'rpt_elig_root', 'rpt_elig_member',
                                      ARRAY['scan inside a SubPlan'], false);
        END IF;
        IF EXISTS (SELECT FROM regexp_split_to_table(dump, chr(10)) line
                   WHERE line LIKE 'backward consumer %' AND line LIKE '% eligible=yes %') THEN
            RAISE EXCEPTION 'SubPlan produced an eligible consumer: %', dump;
        END IF;
        -- Right-deep joins keep the filtered producer on each hash build
        -- side with either planner's query-order setting.
        q := 'SELECT * FROM rpt_elig_member3 JOIN
            (rpt_elig_member2 JOIN (rpt_elig_member JOIN rpt_elig_root
             ON rpt_elig_root.k = rpt_elig_member.k)
             ON rpt_elig_root.k = rpt_elig_member2.k)
            ON rpt_elig_root.k = rpt_elig_member3.k WHERE rpt_elig_root.v < 100';
        PERFORM set_config('rpt.max_backward_consumers', '4', true);
        dump := rpt_transfer_plan(q);
        producer_rtindex := substring(dump FROM 'vertex ([0-9]+) rpt_elig_root rows=');
        IF producer_rtindex IS NULL OR
           (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE ('backward consumer % producer ' || producer_rtindex || ' %')
              AND line LIKE '% eligible=yes %') <> 3 THEN
            RAISE EXCEPTION 'fan-out fixture needs three eligible consumers (optimizer=%): %', opt, dump;
        END IF;
        PERFORM set_config('rpt.max_backward_consumers', '1', true);
        dump := rpt_transfer_plan(q);
        IF producer_rtindex IS NULL OR
           (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE ('backward consumer % producer ' || producer_rtindex || ' %')
              AND line LIKE '% reason=backward consumer cap exceeded') <> 2 OR
           (SELECT count(*) FROM regexp_split_to_table(dump, chr(10)) line
            WHERE line LIKE ('backward consumer % producer ' || producer_rtindex || ' %')
              AND line LIKE '% eligible=yes %') <> 1 THEN
            RAISE EXCEPTION 'fan-out cap fixture did not retain exactly one consumer (optimizer=%): %', opt, dump;
        END IF;
        PERFORM set_config('rpt.max_backward_consumers', '4', true);
    END LOOP;
END
$$;
DROP TABLE rpt_elig_root, rpt_elig_member, rpt_elig_local, rpt_elig_replica,
           rpt_elig_outer, rpt_elig_bridge, rpt_elig_member2, rpt_elig_member3;
DROP EXTENSION robust_predicate_transfer;
RESET optimizer;
RESET rpt.enable;
RESET anser.runtime_filter;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET join_collapse_limit;
RESET optimizer_join_order;
RESET statement_timeout;
