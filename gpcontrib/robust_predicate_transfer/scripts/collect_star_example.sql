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
-- Run from an empty output directory; see scripts/README.md.
-- Use a dedicated database. Existing fixture schemas are never overwritten.
\set ON_ERROR_STOP on
\pset pager off
\pset format unaligned
\pset tuples_only on
\timing off
\if :{?fact_rows}
\else
\set fact_rows 1000000
\endif
\if :{?samples}
\else
\set samples 7
\endif
\if :{?warmups}
\else
\set warmups 2
\endif
SELECT :'fact_rows'::bigint >= 100000 AND :'samples'::int >= 3
       AND :'warmups'::int >= 1 AS valid_input \gset
\if :valid_input
\else
\echo 'Require fact_rows >= 100000, samples >= 3, warmups >= 1.'
DO $$ BEGIN RAISE EXCEPTION 'invalid collection settings or preload configuration'; END $$;
\endif
SELECT coalesce(current_setting('anser.enable', true) = 'on'
       AND array_position(libraries, 'anser')
           < array_position(libraries, 'robust_predicate_transfer'), false) AS ready
FROM (SELECT string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ',')
             AS libraries) preload \gset
\if :ready
\else
\echo 'Preload anser,robust_predicate_transfer in that order and enable Anser; restart first.'
DO $$ BEGIN RAISE EXCEPTION 'invalid collection settings or preload configuration'; END $$;
\endif
SET application_name = 'rpt_readme_star_capture';
SET statement_timeout = '120s';
SET anser.runtime_filter = off;
SET anser.debug = off;
SET rpt.enable = off;
SET rpt.debug = off;
\if :{?exchange_budget_ms}
SET rpt.forward_max_exchange_ms = :'exchange_budget_ms';
\endif
CREATE EXTENSION IF NOT EXISTS robust_predicate_transfer WITH SCHEMA public;
\echo RPT_STAR setup
BEGIN;
CREATE SCHEMA rpt_readme_star;
CREATE TABLE rpt_readme_star.fact (
    sale_id int, customer_key int, product_key int, store_key int, amount_cents int
) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_readme_star.customer (k int, region int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_readme_star.product (k int, category int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_readme_star.store (k int, zone int) DISTRIBUTED RANDOMLY;
INSERT INTO rpt_readme_star.customer SELECT k, k % 10 FROM generate_series(0, 1008) k;
INSERT INTO rpt_readme_star.product SELECT k, k % 10 FROM generate_series(0, 502) k;
INSERT INTO rpt_readme_star.store SELECT k, k % 10 FROM generate_series(0, 210) k;
INSERT INTO rpt_readme_star.fact
    SELECT i, (i - 1) % 1009, (i - 1) % 503, (i - 1) % 211, i % 10000
    FROM generate_series(1, :'fact_rows'::int) i;
ANALYZE rpt_readme_star.fact;
ANALYZE rpt_readme_star.customer;
ANALYZE rpt_readme_star.product;
ANALYZE rpt_readme_star.store;
COMMIT;
SELECT $query$
SELECT count(*) AS matched_rows,
       coalesce(sum(f.amount_cents), 0) AS total_amount_cents
FROM rpt_readme_star.fact f
JOIN rpt_readme_star.customer c ON f.customer_key = c.k
JOIN rpt_readme_star.product p ON f.product_key = p.k
JOIN rpt_readme_star.store s ON f.store_key = s.k
WHERE c.region < 2 AND p.category < 2 AND s.zone < 2
$query$ AS star_query \gset
\o query.sql
\qecho :star_query ;
\o
\pset tuples_only off
\o environment.txt
SELECT version();
SELECT pg_backend_pid() AS backend_pid;
SELECT count(*) FILTER (WHERE content >= 0 AND role = 'p') AS primaries,
       count(DISTINCT hostname) FILTER (WHERE content >= 0 AND role = 'p') AS primary_hosts
FROM gp_segment_configuration;
SELECT content, role, status, mode FROM gp_segment_configuration WHERE role = 'p' ORDER BY content;
SELECT extname, extversion FROM pg_extension
WHERE extname IN ('robust_predicate_transfer', 'anser') ORDER BY extname;
SELECT name, setting, unit, source FROM pg_settings
WHERE name LIKE 'rpt.%' OR name LIKE 'anser.%' OR name IN (
    'shared_preload_libraries', 'optimizer', 'optimizer_join_order',
    'gp_enable_hashjoin_size_heuristic', 'gp_enable_runtime_filter',
    'gp_enable_runtime_filter_pushdown', 'work_mem', 'statement_mem',
    'max_parallel_workers_per_gather', 'max_parallel_workers',
    'gp_resource_manager', 'gp_interconnect_type', 'statement_timeout'
) ORDER BY name;
\o
\pset format csv
\pset tuples_only off
\pset footer off
\o distribution.csv
SELECT c.relname, p.policytype, p.numsegments, p.distkey::text, p.distclass::text
FROM gp_distribution_policy p JOIN pg_class c ON c.oid = p.localoid
WHERE c.relnamespace = 'rpt_readme_star'::regnamespace ORDER BY c.relname;
\o
\o statistics.csv
SELECT tablename, attname, null_frac, avg_width, n_distinct,
       most_common_vals::text, most_common_freqs::text
FROM pg_stats WHERE schemaname = 'rpt_readme_star' ORDER BY tablename, attname;
\o
\o cardinalities.csv
SELECT 'customer' AS relation, count(*) AS rows, count(*) FILTER (WHERE region < 2) AS selected,
       count(DISTINCT k) AS key_ndv, count(DISTINCT k) FILTER (WHERE region < 2) AS selected_ndv
FROM rpt_readme_star.customer
UNION ALL
SELECT 'product', count(*), count(*) FILTER (WHERE category < 2),
       count(DISTINCT k), count(DISTINCT k) FILTER (WHERE category < 2) FROM rpt_readme_star.product
UNION ALL
SELECT 'store', count(*), count(*) FILTER (WHERE zone < 2),
       count(DISTINCT k), count(DISTINCT k) FILTER (WHERE zone < 2) FROM rpt_readme_star.store;
\o
\o fact-cardinalities.csv
SELECT count(*) AS rows, count(DISTINCT customer_key) AS customer_ndv,
       count(DISTINCT product_key) AS product_ndv, count(DISTINCT store_key) AS store_ndv
FROM rpt_readme_star.fact;
\o
\pset format unaligned
\pset tuples_only on

\echo RPT_STAR optimizer=off
SET optimizer = off;
\echo RPT_STAR capture=off-baseline
SET rpt.enable = off;
SET rpt.forward = on;
SET rpt.backward = on;
:star_query \gset got_
\set expected_matched_rows :got_matched_rows
\set expected_total_amount_cents :got_total_amount_cents
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o off-baseline.result.csv
SELECT 'off' AS optimizer_requested, 'baseline' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o off-baseline.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o off-baseline.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=off-forward
SET rpt.enable = on;
SET rpt.forward = on;
SET rpt.backward = off;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o off-forward.result.csv
SELECT 'off' AS optimizer_requested, 'forward' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o off-forward.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o off-forward.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o off-forward.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=off-backward
SET rpt.enable = on;
SET rpt.forward = off;
SET rpt.backward = on;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o off-backward.result.csv
SELECT 'off' AS optimizer_requested, 'backward' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o off-backward.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o off-backward.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o off-backward.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=off-full
SET rpt.enable = on;
SET rpt.forward = on;
SET rpt.backward = on;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o off-full.result.csv
SELECT 'off' AS optimizer_requested, 'full' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o off-full.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o off-full.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o off-full.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR timings optimizer=off
\timing on
-- gexec sends each column as a separate statement: settings, marker, query.
-- Only the Time line following the two-column aggregate result is a query sample.
SELECT format('SET rpt.enable = %L', enabled),
       format('SET rpt.forward = %L', forward),
       format('SET rpt.backward = %L', backward),
       format('SELECT %L', format('RPT_SAMPLE optimizer=off arm=%s phase=%s run=%s', arm,
              CASE WHEN iteration < :'warmups'::int THEN 'warmup' ELSE 'sample' END,
              CASE WHEN iteration < :'warmups'::int THEN iteration + 1
                   ELSE iteration - :'warmups'::int + 1 END)),
       :'star_query'
FROM generate_series(0, :'warmups'::int + :'samples'::int - 1) iteration
CROSS JOIN (VALUES
    (0, 'baseline', 'off', 'on', 'on'),
    (1, 'forward', 'on', 'on', 'off'),
    (2, 'backward', 'on', 'off', 'on'),
    (3, 'full', 'on', 'on', 'on')
) arms(arm_index, arm, enabled, forward, backward)
ORDER BY iteration, (arm_index + iteration) % 4
\gexec
\timing off

\echo RPT_STAR optimizer=on
SET optimizer = on;
\echo RPT_STAR capture=on-baseline
SET rpt.enable = off;
SET rpt.forward = on;
SET rpt.backward = on;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o on-baseline.result.csv
SELECT 'on' AS optimizer_requested, 'baseline' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o on-baseline.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o on-baseline.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=on-forward
SET rpt.enable = on;
SET rpt.forward = on;
SET rpt.backward = off;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o on-forward.result.csv
SELECT 'on' AS optimizer_requested, 'forward' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o on-forward.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o on-forward.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o on-forward.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=on-backward
SET rpt.enable = on;
SET rpt.forward = off;
SET rpt.backward = on;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o on-backward.result.csv
SELECT 'on' AS optimizer_requested, 'backward' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o on-backward.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o on-backward.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o on-backward.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR capture=on-full
SET rpt.enable = on;
SET rpt.forward = on;
SET rpt.backward = on;
:star_query \gset got_
SELECT :got_matched_rows::bigint = :expected_matched_rows::bigint
   AND :got_total_amount_cents::bigint = :expected_total_amount_cents::bigint AS answer_matches \gset
\if :answer_matches
\else
\echo 'Result mismatch; retain all artifacts and stop.'
DO $$ BEGIN RAISE EXCEPTION 'star example result mismatch'; END $$;
\endif
\pset format csv
\pset tuples_only off
\o on-full.result.csv
SELECT 'on' AS optimizer_requested, 'full' AS arm,
       :got_matched_rows::bigint AS matched_rows,
       :got_total_amount_cents::bigint AS total_amount_cents;
\o
\pset format unaligned
\pset tuples_only on
\o on-full.transfer.txt
SELECT public.rpt_transfer_plan(:'star_query');
\o
\o on-full.explain.txt
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) :star_query;
\o
\o on-full.explain.json
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY, FORMAT JSON) :star_query;
\o
\echo RPT_STAR timings optimizer=on
\timing on
-- gexec sends each column as a separate statement: settings, marker, query.
-- Only the Time line following the two-column aggregate result is a query sample.
SELECT format('SET rpt.enable = %L', enabled),
       format('SET rpt.forward = %L', forward),
       format('SET rpt.backward = %L', backward),
       format('SELECT %L', format('RPT_SAMPLE optimizer=on arm=%s phase=%s run=%s', arm,
              CASE WHEN iteration < :'warmups'::int THEN 'warmup' ELSE 'sample' END,
              CASE WHEN iteration < :'warmups'::int THEN iteration + 1
                   ELSE iteration - :'warmups'::int + 1 END)),
       :'star_query'
FROM generate_series(0, :'warmups'::int + :'samples'::int - 1) iteration
CROSS JOIN (VALUES
    (0, 'baseline', 'off', 'on', 'on'),
    (1, 'forward', 'on', 'on', 'off'),
    (2, 'backward', 'on', 'off', 'on'),
    (3, 'full', 'on', 'on', 'on')
) arms(arm_index, arm, enabled, forward, backward)
ORDER BY iteration, (arm_index + iteration) % 4
\gexec
\timing off

SET rpt.enable = off;
\echo RPT_STAR complete
-- The fixture remains for inspection. Drop the dedicated database when finished.
