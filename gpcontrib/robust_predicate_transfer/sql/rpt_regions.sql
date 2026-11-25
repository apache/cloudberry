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

-- Region discovery: transfer regions are connected components of the join
-- graph over base scans, and every operator the key resolver cannot descend
-- (aggregate, set op, window, limit, SRF/UNNEST, CteScan, partitioned
-- table) is a boundary -- queries whose only join key comes through one have
-- no region at all.
CREATE EXTENSION robust_predicate_transfer;
CREATE TABLE rpt_a (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE rpt_b (a int) DISTRIBUTED BY (a);
CREATE TABLE rpt_c (a int) DISTRIBUTED BY (a);
INSERT INTO rpt_a SELECT i, i FROM generate_series(1, 100) i;
INSERT INTO rpt_b SELECT i FROM generate_series(1, 10) i;
INSERT INTO rpt_c SELECT i FROM generate_series(1, 50) i;
ANALYZE rpt_a;
ANALYZE rpt_b;
ANALYZE rpt_c;
-- A partitioned table joins through an Append, which resolution cannot cross.
CREATE TABLE rpt_p (a int) DISTRIBUTED BY (a)
    PARTITION BY RANGE (a) (START (1) END (101) EVERY (50));
INSERT INTO rpt_p SELECT i FROM generate_series(1, 100) i;
ANALYZE rpt_p;
DO $$
DECLARE
    opt text;
    q text;
    dump text;
    expected_members text;
    member_count int;
BEGIN
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);
        FOREACH q IN ARRAY ARRAY[
            'SELECT 1',
            'SELECT * FROM rpt_a a JOIN (SELECT a, count(*) c FROM rpt_b GROUP BY a) s ON a.a = s.a WHERE s.c > 1',
            'SELECT * FROM rpt_a a JOIN (SELECT a FROM rpt_b UNION SELECT a FROM rpt_c) u ON a.a = u.a',
            'SELECT * FROM rpt_a a JOIN (SELECT a, row_number() OVER () r FROM rpt_b) w ON a.a = w.a WHERE w.r > 1',
            'SELECT * FROM rpt_a a JOIN (SELECT a FROM rpt_b ORDER BY a LIMIT 5) l ON a.a = l.a',
            'SELECT * FROM rpt_a a JOIN (SELECT u FROM rpt_b, unnest(ARRAY[a]) u) s ON a.a = s.u',
            'SELECT * FROM rpt_a a JOIN rpt_p p ON a.a = p.a'
        ] LOOP
            dump := rpt_transfer_plan(q);
            IF dump <> ('no transfer regions' || chr(10)) THEN
                RAISE EXCEPTION 'region boundary crossed (optimizer=%): %: %', opt, q, dump;
            END IF;
        END LOOP;
        -- PostgreSQL retains a CteScan boundary here. ORCA exposes the
        -- underlying base scans, so its unfiltered class is expected.
        q := 'WITH w AS MATERIALIZED (SELECT a FROM rpt_b) SELECT * FROM rpt_a a JOIN w ON a.a = w.a';
        dump := rpt_transfer_plan(q);
        IF opt = 'off' THEN
            IF dump <> ('no transfer regions' || chr(10)) THEN
                RAISE EXCEPTION 'CteScan boundary crossed: %', dump;
            END IF;
        ELSE
            SELECT count(*), string_agg(parts[1], ',' ORDER BY parts[1]::int)
                INTO member_count, expected_members
                FROM regexp_matches(dump, 'vertex ([0-9]+) (a|rpt_b) rows=', 'g') AS vertices(parts);
            IF member_count <> 2 OR
               dump NOT LIKE ('%members=' || expected_members || ' leader=%') OR
               dump NOT LIKE ('%reducer order:' || chr(10) || '%') OR
               dump LIKE '%forward: build for%' THEN
                RAISE EXCEPTION 'unexpected ORCA materialized CTE plan: %', dump;
            END IF;
        END IF;
        dump := rpt_transfer_plan('SELECT * FROM rpt_b JOIN rpt_a ON rpt_b.a = rpt_a.a');
        SELECT count(*), string_agg(parts[1], ',' ORDER BY parts[1]::int)
            INTO member_count, expected_members
            FROM regexp_matches(dump, 'vertex ([0-9]+) (rpt_a|rpt_b) rows=', 'g') AS vertices(parts);
        IF member_count <> 2 OR dump NOT LIKE ('%members=' || expected_members || ' leader=%') OR
           dump NOT LIKE ('%reducer order:' || chr(10) || '%') OR
           dump LIKE '%forward: build for%' THEN
            RAISE EXCEPTION 'unfiltered pair produced transfers: %', dump;
        END IF;
    END LOOP;
END
$$;
DROP TABLE rpt_p;
DROP TABLE rpt_a, rpt_b, rpt_c;
DROP EXTENSION robust_predicate_transfer;
