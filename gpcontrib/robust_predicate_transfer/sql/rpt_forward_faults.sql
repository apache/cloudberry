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
-- Forward reducer under forced transport failures.  Kept apart from
-- rpt_forward because gp_inject_fault exists only with the debug extensions,
-- and the regression driver skips any file that calls it.
--
-- The chain is fact <- dimension <- subdimension with a random dimension, so
-- both hops are Anser channels and the first one has its consumer inside the
-- reducer InitPlan.  Every case must return the RPT-off answer, and the next
-- command on the same gangs must answer: a reducer's dispatcher releases its
-- gangs before ExecutorEnd. If one of those connections is destroyed, libpq's
-- PGEVT_CONNDESTROY callback must remove Anser's references before it is freed.
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
-- end_ignore
CREATE EXTENSION robust_predicate_transfer;
SET optimizer = off;
SET anser.runtime_filter = off;
SET rpt.prefer_inplace = off;
SET rpt.key_survival = off;
SET rpt.backward = off;
SET anser.debug = on;
SET rpt.enable = on;
SET rpt.forward_min_root_rows = 0;
SET rpt.forward_max_exchange_ms = -1;
SET rpt.forward_total_cost = off;
SET statement_timeout = '120s';
CREATE TABLE rpt_fwf_fact (id int, d int, v int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwf_dim (k int, s int) DISTRIBUTED RANDOMLY;
CREATE TABLE rpt_fwf_sub (k int, c int) DISTRIBUTED RANDOMLY;
INSERT INTO rpt_fwf_fact SELECT i, i % 1000, i FROM generate_series(1, 200000) i;
INSERT INTO rpt_fwf_dim SELECT i, i % 100 FROM generate_series(0, 999) i;
INSERT INTO rpt_fwf_sub SELECT i, i FROM generate_series(0, 99) i;
ANALYZE rpt_fwf_fact;
ANALYZE rpt_fwf_dim;
ANALYZE rpt_fwf_sub;
-- Dynamic SQL: a static statement would keep the plan of its first call,
-- with or without the reducer, whatever rpt.enable says later.
CREATE FUNCTION pg_temp.rpt_fwf_run(rpt boolean) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    r text;
BEGIN
    PERFORM set_config('rpt.enable', CASE WHEN rpt THEN 'on' ELSE 'off' END, true);
    EXECUTE 'SELECT count(*)::text || ''/'' || sum(f.v)::text
               FROM rpt_fwf_fact f JOIN rpt_fwf_dim d ON f.d = d.k
               JOIN rpt_fwf_sub u ON d.s = u.k WHERE u.c < 5' INTO r;
    PERFORM set_config('rpt.enable', 'on', true);
    RETURN r;
END
$$;
-- How often a fault fired, so a case cannot pass without exercising it.
CREATE FUNCTION pg_temp.rpt_fwf_hits(fault text, coordinator boolean)
RETURNS int LANGUAGE sql AS $$
    SELECT coalesce(sum(substring(gp_inject_fault(fault, 'status', dbid)
                                  FROM 'num times hit:''([0-9]+)''')::int), 0)::int
      FROM gp_segment_configuration
     WHERE role = 'p' AND (content = -1) = coordinator
$$;
-- First occurrence only; on the coordinator a 'sleep' fault sleeps 1 s.
CREATE FUNCTION pg_temp.rpt_fwf_fault(fault text, action text, coordinator boolean)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF action = 'reset' THEN
        PERFORM gp_inject_fault(fault, 'reset', dbid)
           FROM gp_segment_configuration
          WHERE role = 'p' AND (content = -1) = coordinator;
    ELSE
        PERFORM gp_inject_fault(fault, action, '', '', '', 1, 1,
                                CASE WHEN coordinator THEN 1 ELSE 0 END, dbid)
           FROM gp_segment_configuration
          WHERE role = 'p' AND (content = -1) = coordinator;
    END IF;
END
$$;
DO $$
DECLARE
    expected text := pg_temp.rpt_fwf_run(false);
    got text;
BEGIN
    IF rpt_transfer_plan('SELECT count(*) FROM rpt_fwf_fact f JOIN rpt_fwf_dim d ON f.d = d.k
                            JOIN rpt_fwf_sub u ON d.s = u.k WHERE u.c < 5')
       NOT LIKE '%channels=2 %decision=inject%' THEN
        RAISE EXCEPTION 'fixture chain not selected';
    END IF;

    -- 1. The coordinator holds the first delivery for a second; every
    -- consumer waiting on it, the reducer's included, gives up at 100 ms and
    -- runs unfiltered.
    PERFORM set_config('anser.timeout_ms', '100', true);
    PERFORM pg_temp.rpt_fwf_fault('anser_before_deliver', 'sleep', true);
    got := pg_temp.rpt_fwf_run(true);
    IF pg_temp.rpt_fwf_hits('anser_before_deliver', true) = 0 THEN
        RAISE EXCEPTION 'held delivery: fault never fired';
    END IF;
    PERFORM pg_temp.rpt_fwf_fault('anser_before_deliver', 'reset', true);
    IF got IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'held delivery: % <> %', got, expected;
    END IF;
    IF pg_temp.rpt_fwf_run(true) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'next command after held delivery';
    END IF;

    -- 2. The first consumer on each segment -- the reducer's -- walks away
    -- after the first chunk of a 64 MB filter, leaving its delivery
    -- unfinished on a connection of the InitPlan's gang, which is released
    -- before the main plan runs.
    PERFORM set_config('anser.timeout_ms', '500', true);
    PERFORM set_config('anser.debug_delivery_chunk_bytes', '8192', true);
    PERFORM set_config('anser.debug_min_filter_bytes', '67108864', true);
    PERFORM pg_temp.rpt_fwf_fault('anser_after_chunk', 'skip', false);
    got := pg_temp.rpt_fwf_run(true);
    IF pg_temp.rpt_fwf_hits('anser_after_chunk', false) = 0 THEN
        RAISE EXCEPTION 'walk-away: fault never fired';
    END IF;
    PERFORM pg_temp.rpt_fwf_fault('anser_after_chunk', 'reset', false);
    IF got IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'walk-away: % <> %', got, expected;
    END IF;
    FOR i IN 1..2 LOOP
        IF pg_temp.rpt_fwf_run(true) IS DISTINCT FROM expected THEN
            RAISE EXCEPTION 'next command % after walk-away', i;
        END IF;
    END LOOP;
    PERFORM set_config('anser.debug_delivery_chunk_bytes', '0', true);
    PERFORM set_config('anser.debug_min_filter_bytes', '0', true);
END
$$;
-- 3. The statement is cancelled while the reducer waits for a delivery, so
-- the reducer's dispatcher is torn down on the abort path.  Whether the
-- cancel lands before the query finishes is timing, so its output is not
-- compared; the session and its gangs must survive with the right answers.
DO $$
BEGIN
    PERFORM pg_temp.rpt_fwf_fault('anser_before_deliver', 'sleep', true);
END
$$;
SET anser.timeout_ms = 5000;
SET statement_timeout = '300ms';
-- start_ignore
SELECT pg_temp.rpt_fwf_run(true) IS NOT NULL AS finished;
-- end_ignore
SET statement_timeout = '120s';
RESET anser.timeout_ms;
DO $$
BEGIN
    PERFORM pg_temp.rpt_fwf_fault('anser_before_deliver', 'reset', true);
    IF pg_temp.rpt_fwf_run(true) IS DISTINCT FROM pg_temp.rpt_fwf_run(false) THEN
        RAISE EXCEPTION 'next command after cancel';
    END IF;
END
$$;
SELECT count(*) AS dim_rows FROM rpt_fwf_dim;
DROP TABLE rpt_fwf_fact, rpt_fwf_dim, rpt_fwf_sub;
DROP EXTENSION robust_predicate_transfer;
RESET rpt.enable;
RESET rpt.forward_min_root_rows;
RESET rpt.forward_max_exchange_ms;
RESET rpt.forward_total_cost;
RESET anser.debug;
RESET rpt.prefer_inplace;
RESET rpt.key_survival;
RESET anser.runtime_filter;
RESET rpt.backward;
RESET optimizer;
RESET statement_timeout;
