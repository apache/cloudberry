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
-- Capture stdout to retain the plans, transfer decisions, and answers.
-- Use a dedicated database. Existing fixture schemas are never overwritten.
\set ON_ERROR_STOP on
\if :{?fact_rows}
\else
\set fact_rows 1000000
\endif

SELECT coalesce(array_position(libs, 'anser') <
                array_position(libs, 'robust_predicate_transfer'), false)
       AND current_setting('anser.enable', true) = 'on' AS ready
FROM (SELECT string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ',')
             AS libs) preload \gset
\if :ready
\else
DO $$ BEGIN RAISE EXCEPTION 'Enable Anser and preload anser,robust_predicate_transfer in that order'; END $$;
\endif

CREATE EXTENSION IF NOT EXISTS robust_predicate_transfer WITH SCHEMA public;
SET statement_timeout = '180s';
SET anser.runtime_filter = off;
SET rpt.debug = off;
-- Remove the exchange-cost gate for this plan-shape experiment.
SET rpt.forward_max_exchange_ms = -1;
SET rpt.forward_min_root_rows = 0;

-- Match the join-shape settings in sql/rpt_backward.sql.
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET gp_enable_hashjoin_size_heuristic = on;
SET join_collapse_limit = 1;
SET optimizer_join_order = query;
SET work_mem = '64MB';

BEGIN;
CREATE SCHEMA rpt_readme_bridge;
CREATE TABLE rpt_readme_bridge.campaign (id int, cohort int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_readme_bridge.campaign_route (id int, campaign_id int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_readme_bridge.sale (account_id int, route_id int, cents int)
    DISTRIBUTED BY (account_id);
CREATE TABLE rpt_readme_bridge.event (account_id int, payload text) DISTRIBUTED RANDOMLY;

INSERT INTO rpt_readme_bridge.campaign SELECT i, i % 20 FROM generate_series(0, 999) i;
INSERT INTO rpt_readme_bridge.campaign_route SELECT i, i % 1000 FROM generate_series(0, 9999) i;
-- The larger scan stays narrow. The event aggregate keeps the wide payload
-- in the plan so the smaller row count does not imply a smaller hash build.
INSERT INTO rpt_readme_bridge.sale
SELECT i % 10000, i % 10000, i % 100 FROM generate_series(0, 2 * :'fact_rows'::int - 1) i;
INSERT INTO rpt_readme_bridge.event
SELECT i % 100000, repeat(md5(i::text), 8)
FROM generate_series(0, :'fact_rows'::int - 1) i;
ANALYZE rpt_readme_bridge.campaign;
ANALYZE rpt_readme_bridge.campaign_route;
ANALYZE rpt_readme_bridge.sale;
ANALYZE rpt_readme_bridge.event;
COMMIT;

SELECT $query$
SELECT count(*) AS matches, sum(s.cents) AS cents, sum(length(e.payload)) AS payload_chars
FROM rpt_readme_bridge.event e
JOIN (
    rpt_readme_bridge.sale s
    JOIN (
        rpt_readme_bridge.campaign_route b
        JOIN rpt_readme_bridge.campaign c ON c.id = b.campaign_id
    ) ON b.id = s.route_id
) ON e.account_id = s.account_id
WHERE c.cohort = 0
$query$ AS bridge_query \gset

SELECT version(), :'fact_rows'::int AS event_rows, 2 * :'fact_rows'::int AS sale_rows;
SELECT name, setting FROM pg_settings
WHERE name LIKE 'rpt.%' OR name LIKE 'anser.%'
   OR name IN ('shared_preload_libraries', 'work_mem', 'gp_enable_hashjoin_size_heuristic')
ORDER BY name;

-- One report holds the transfer decisions, real plans, timings and answers.
-- The requested optimizer is printed alongside EXPLAIN's actual optimizer.
SELECT format('SET optimizer = %L', opt),
       format('SET rpt.enable = %L', enabled),
       format('SET rpt.forward = %L', forward),
       format('SET rpt.backward = %L', backward),
       format('SELECT %L AS run', 'optimizer=' || opt || ' arm=' || arm),
       CASE WHEN enabled THEN format('SELECT public.rpt_transfer_plan(%L)', :'bridge_query') END,
       format('EXPLAIN (ANALYZE, VERBOSE, BUFFERS, SETTINGS, SUMMARY) %s', :'bridge_query'),
       :'bridge_query'
FROM (VALUES ('off', 0), ('on', 1)) optimizers(opt, opt_order)
CROSS JOIN (VALUES
    (0, 'baseline', false, false, false),
    (1, 'forward',  true,  true,  false),
    (2, 'backward', true,  false, true),
    (3, 'full',     true,  true,  true)
) arms(arm_order, arm, enabled, forward, backward)
ORDER BY opt_order, arm_order
\gexec
