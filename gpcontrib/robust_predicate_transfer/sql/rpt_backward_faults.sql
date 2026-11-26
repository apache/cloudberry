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
-- Backward-only transport failures. Every armed fault must actually fire.
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE EXTENSION IF NOT EXISTS anser_test;
-- end_ignore
CREATE EXTENSION robust_predicate_transfer;
SET anser.runtime_filter = off;
SET rpt.key_survival = off;
SET rpt.enable = on;
SET rpt.forward = off;
SET rpt.backward = on;
SET rpt.forward_max_exchange_ms = -1;
SET statement_timeout = '120s';
SET work_mem = '1MB';
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET gp_enable_hashjoin_size_heuristic = on;
CREATE TABLE rpt_bwf_root (k int) DISTRIBUTED BY (k);
CREATE TABLE rpt_bwf_member (k int, pad text) DISTRIBUTED RANDOMLY;
INSERT INTO rpt_bwf_root SELECT i % 10 FROM generate_series(1, 10000) i;
INSERT INTO rpt_bwf_member SELECT i,
    (SELECT string_agg(md5(i::text || ':' || j::text), '') FROM generate_series(1, 100) j)
    FROM generate_series(0, 999) i;
ANALYZE rpt_bwf_root;
ANALYZE rpt_bwf_member;
CREATE FUNCTION pg_temp.rpt_bwf_query() RETURNS text LANGUAGE sql AS $$
    SELECT 'SELECT count(*)::text || ''/'' || sum(length(m.pad || r.k::text))::text
        FROM rpt_bwf_member m JOIN rpt_bwf_root r ON m.k = r.k'
$$;
CREATE FUNCTION pg_temp.rpt_bwf_run(enabled boolean) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    q text := pg_temp.rpt_bwf_query();
    r text;
BEGIN
    PERFORM set_config('rpt.enable', CASE WHEN enabled THEN 'on' ELSE 'off' END, true);
    EXECUTE q INTO r;
    PERFORM set_config('rpt.enable', 'on', true);
    RETURN r;
END
$$;
-- How often a fault fired, so a case cannot pass without exercising it.
CREATE FUNCTION pg_temp.rpt_bwf_hits(fault text, coordinator boolean)
RETURNS int LANGUAGE sql AS $$
    SELECT coalesce(sum(substring(gp_inject_fault(fault, 'status', dbid)
                                  FROM 'num times hit:''([0-9]+)''')::int), 0)::int
      FROM gp_segment_configuration
     WHERE role = 'p' AND (content = -1) = coordinator
$$;
-- First occurrence only; on the coordinator a 'sleep' fault sleeps 1 s.
CREATE FUNCTION pg_temp.rpt_bwf_fault(fault text, action text, coordinator boolean)
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
    opt text;
    baseline text;
    answer text;
    plan jsonb;
    deliveries_before bigint;
BEGIN
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);
        baseline := pg_temp.rpt_bwf_run(false);
        IF rpt_transfer_plan(pg_temp.rpt_bwf_query()) NOT LIKE '%backward class %decision=inject%' THEN
            RAISE EXCEPTION 'backward fault fixture not selected (optimizer=%)', opt;
        END IF;
        PERFORM set_config('anser.timeout_ms', '100', true);
        PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'sleep', true);
        answer := pg_temp.rpt_bwf_run(true);
        IF pg_temp.rpt_bwf_hits('anser_before_deliver', true) = 0 THEN
            RAISE EXCEPTION 'backward delivery sleep never fired';
        END IF;
        PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'reset', true);
        IF answer IS DISTINCT FROM baseline OR pg_temp.rpt_bwf_run(true) IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'backward timeout or next command changed results';
        END IF;
        PERFORM set_config('anser.timeout_ms', '1000', true);
        PERFORM set_config('anser.debug_delivery_chunk_bytes', '8192', true);
        PERFORM set_config('anser.debug_min_filter_bytes', '8388608', true);
        PERFORM pg_temp.rpt_bwf_fault('anser_after_chunk', 'skip', false);
        answer := pg_temp.rpt_bwf_run(true);
        IF pg_temp.rpt_bwf_hits('anser_after_chunk', false) = 0 THEN
            RAISE EXCEPTION 'backward consumer walk-away never fired';
        END IF;
        PERFORM pg_temp.rpt_bwf_fault('anser_after_chunk', 'reset', false);
        IF answer IS DISTINCT FROM baseline OR pg_temp.rpt_bwf_run(true) IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'backward walk-away or next command changed results';
        END IF;
        -- Plan with an 8 MiB filter, then lower the runtime payload cap.
        -- EXPLAIN EXECUTE forces the generic plan before the cap changes;
        -- the second EXPLAIN proves the cached producer was not resized away.
        EXECUTE 'PREPARE rpt_bwf_oversized AS ' || pg_temp.rpt_bwf_query();
        EXECUTE 'EXPLAIN (FORMAT JSON) EXECUTE rpt_bwf_oversized' INTO plan;
        IF plan::text NOT LIKE '%8388608%' THEN
            RAISE EXCEPTION 'oversized producer fixture was not planned: %', plan;
        END IF;
        PERFORM set_config('anser.max_info_size', '2097152', true);
        EXECUTE 'EXPLAIN (FORMAT JSON) EXECUTE rpt_bwf_oversized' INTO plan;
        IF plan::text NOT LIKE '%8388608%' THEN
            RAISE EXCEPTION 'oversized producer fixture was replanned: %', plan;
        END IF;
        PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'skip', true);
        deliveries_before := anser_test_deliveries_completed();
        EXECUTE 'EXECUTE rpt_bwf_oversized' INTO answer;
        -- The fault proves delivery was attempted; only the counter can
        -- distinguish a cancellation from a successfully delivered filter.
        IF anser_test_deliveries_completed() <> deliveries_before THEN
            RAISE EXCEPTION 'oversized producer delivered a filter instead of cancelling';
        END IF;
        IF pg_temp.rpt_bwf_hits('anser_before_deliver', true) = 0 THEN
            RAISE EXCEPTION 'oversized producer never delivered its cancellation';
        END IF;
        PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'reset', true);
        EXECUTE 'DEALLOCATE rpt_bwf_oversized';
        PERFORM set_config('anser.max_info_size', '68157440', true);
        PERFORM set_config('anser.debug_min_filter_bytes', '0', true);
        PERFORM set_config('anser.debug_delivery_chunk_bytes', '0', true);
        IF answer IS DISTINCT FROM baseline OR pg_temp.rpt_bwf_run(true) IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'backward producer cancellation changed results';
        END IF;
    END LOOP;
END
$$;
CREATE FUNCTION pg_temp.rpt_bwf_cancel() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    cancelled boolean := false;
    answer text;
BEGIN
    BEGIN
        EXECUTE 'EXECUTE rpt_bwf_cancel_plan' INTO answer;
    EXCEPTION WHEN query_canceled THEN
        cancelled := true;
    END;
    IF NOT cancelled THEN
        RAISE EXCEPTION 'backward wait was not cancelled (optimizer=%, delivery hits=%)',
            current_setting('optimizer'), pg_temp.rpt_bwf_hits('anser_before_deliver', true);
    END IF;
END
$$;
-- Prepare and verify each optimizer's plan before starting the short timer.
-- The cancellation check then measures executor/transport cleanup, not planning.
CREATE FUNCTION pg_temp.rpt_bwf_prepare_cancel() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    IF rpt_transfer_plan(pg_temp.rpt_bwf_query()) NOT LIKE '%backward class %decision=inject%' THEN
        RAISE EXCEPTION 'cancel fixture has no backward injection (optimizer=%)', current_setting('optimizer');
    END IF;
    EXECUTE 'PREPARE rpt_bwf_cancel_plan AS ' || pg_temp.rpt_bwf_query();
    EXECUTE 'EXPLAIN (VERBOSE, FORMAT JSON) EXECUTE rpt_bwf_cancel_plan' INTO plan;
    IF plan::text NOT LIKE '%Anser Bloom Producer%' OR plan::text NOT LIKE '%Anser Bloom Consumer%' THEN
        RAISE EXCEPTION 'cancel fixture lost its runtime nodes: %', plan;
    END IF;
END
$$;
-- Suspend checks interrupts until cancellation; sleep can return early on
-- a signal without processing the pending interrupt. No timed race is needed.
-- Separate top-level commands arm statement_timeout for each optimizer.
SET optimizer = off;
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_prepare_cancel(); END $$;
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'suspend', true); END $$;
SET anser.timeout_ms = 600000;
SET statement_timeout = '300ms';
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_cancel(); END $$;
SET statement_timeout = '120s';
DO $$
BEGIN
    IF pg_temp.rpt_bwf_hits('anser_before_deliver', true) = 0 THEN
        RAISE EXCEPTION 'cancel test never reached backward delivery';
    END IF;
    PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'reset', true);
    EXECUTE 'DEALLOCATE rpt_bwf_cancel_plan';
    IF pg_temp.rpt_bwf_run(true) IS DISTINCT FROM pg_temp.rpt_bwf_run(false) THEN
        RAISE EXCEPTION 'session failed after backward cancellation';
    END IF;
END
$$;
SET optimizer = on;
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_prepare_cancel(); END $$;
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'suspend', true); END $$;
SET statement_timeout = '300ms';
DO $$ BEGIN PERFORM pg_temp.rpt_bwf_cancel(); END $$;
SET statement_timeout = '120s';
DO $$
BEGIN
    IF pg_temp.rpt_bwf_hits('anser_before_deliver', true) = 0 THEN
        RAISE EXCEPTION 'ORCA cancel test never reached backward delivery';
    END IF;
    PERFORM pg_temp.rpt_bwf_fault('anser_before_deliver', 'reset', true);
    EXECUTE 'DEALLOCATE rpt_bwf_cancel_plan';
    IF pg_temp.rpt_bwf_run(true) IS DISTINCT FROM pg_temp.rpt_bwf_run(false) THEN
        RAISE EXCEPTION 'ORCA session failed after backward cancellation';
    END IF;
END
$$;
DROP TABLE rpt_bwf_root, rpt_bwf_member;
DROP EXTENSION robust_predicate_transfer;
RESET rpt.enable;
RESET rpt.forward;
RESET rpt.backward;
RESET rpt.forward_max_exchange_ms;
RESET rpt.key_survival;
RESET anser.runtime_filter;
RESET anser.timeout_ms;
RESET optimizer;
RESET statement_timeout;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET gp_enable_hashjoin_size_heuristic;
RESET work_mem;
