-- Anser runtime bloom filter: plan-tree integration (PR4).
--
-- Requires the cluster to run with anser.enable=on so the gather/send
-- services are live.  On a multi-segment cluster the build side redistributes,
-- exercising the Motion-directly-under-CustomScan case; on a single segment it
-- degrades to the leaf case.  Either way the feature must (a) inject the
-- producer/consumer nodes and (b) never change query results.


-- Deterministic plan shape: force a hash join.
SET enable_nestloop = off;
SET enable_mergejoin = off;

-- A regress test must not inherit the production deadline.  anser.timeout_ms
-- defaults to 100 s because delivery is serial and a wide slice needs that
-- long to be served; a test that waits it out has turned a delivery failure
-- into a slow pass.  Pinned short here, so a filter that never arrives fails
-- the run instead of hiding in the clock.
SET anser.timeout_ms = 5000;

-- build is the smaller (hashed / preserved) side, distributed so the join key
-- must be redistributed; probe is the larger side distributed by the join key.
CREATE TABLE anser_rf_build (id int, name text) DISTRIBUTED BY (name);
CREATE TABLE anser_rf_probe (id int, payload text) DISTRIBUTED BY (id);
INSERT INTO anser_rf_build SELECT g, 'b' || g FROM generate_series(1, 200) g;
INSERT INTO anser_rf_probe SELECT g, 'p' || g FROM generate_series(1, 2000) g;
ANALYZE anser_rf_build;
ANALYZE anser_rf_probe;

-- Use the Postgres planner for a deterministic plan shape.
SET optimizer = off;

-- With the filter on the plan carries an "Anser Bloom Producer" under the Hash
-- and an "Anser Bloom Consumer" above the probe scan (with the planned bloom
-- size); with it off neither node appears.  (COSTS OFF keeps the output stable.)
SET anser.runtime_filter = on;
EXPLAIN (COSTS OFF)
SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id;

SET anser.runtime_filter = off;
EXPLAIN (COSTS OFF)
SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id;

-- Correctness: identical results with the filter on vs off (Postgres planner).
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_r_on AS
    SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id
    DISTRIBUTED BY (name);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_r_off AS
    SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id
    DISTRIBUTED BY (name);

SELECT count(*) AS rows_on FROM anser_rf_r_on;
SELECT count(*) AS rows_off FROM anser_rf_r_off;
SELECT count(*) AS only_on
    FROM (SELECT * FROM anser_rf_r_on EXCEPT ALL SELECT * FROM anser_rf_r_off) d;
SELECT count(*) AS only_off
    FROM (SELECT * FROM anser_rf_r_off EXCEPT ALL SELECT * FROM anser_rf_r_on) d;

-- Correctness must also hold under ORCA (whether or not it injects the nodes).
SET optimizer = on;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_r_orca AS
    SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id
    DISTRIBUTED BY (name);
SELECT count(*) AS only_orca
    FROM (SELECT * FROM anser_rf_r_orca EXCEPT ALL SELECT * FROM anser_rf_r_off) d;
SELECT count(*) AS only_off_orca
    FROM (SELECT * FROM anser_rf_r_off EXCEPT ALL SELECT * FROM anser_rf_r_orca) d;

-- Pushdown mix: with gp_enable_runtime_filter_pushdown on, the consumer hands
-- the unioned filter to the probe SeqScan as an SK_BLOOM_FILTER scan key and
-- the scan (or the table AM) does the pruning.  Results must still match the
-- filter-off run.
SET optimizer = off;
SET gp_enable_runtime_filter_pushdown = on;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_r_push AS
    SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id
    DISTRIBUTED BY (name);
SELECT count(*) AS only_push
    FROM (SELECT * FROM anser_rf_r_push EXCEPT ALL SELECT * FROM anser_rf_r_off) d;
SELECT count(*) AS only_off_push
    FROM (SELECT * FROM anser_rf_r_off EXCEPT ALL SELECT * FROM anser_rf_r_push) d;
RESET gp_enable_runtime_filter_pushdown;

-- Pushdown where the key's table attribute number and its position in the
-- scan's output targetlist disagree.  The case above cannot see the
-- difference: anser_rf_probe's key is its first column and also the first
-- thing the scan emits, so the two numbers coincide.  Here the key is the
-- probe table's *second* column and its *only* projected one, so a scan key
-- carrying the output position makes the scan test `payload` against a filter
-- built on `id` -- and since no payload is ever a build key, every row is
-- discarded.  Both counts must be 200.
SET optimizer = on;
CREATE TABLE anser_rf_proj_build (id int) DISTRIBUTED BY (id);
CREATE TABLE anser_rf_proj_probe (payload int, id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_proj_build SELECT g FROM generate_series(1, 200) g;
INSERT INTO anser_rf_proj_probe SELECT -g, g FROM generate_series(1, 2000) g;
ANALYZE anser_rf_proj_build;
ANALYZE anser_rf_proj_probe;
SET gp_enable_runtime_filter_pushdown = on;
SET anser.runtime_filter = off;
SELECT count(p.id) AS proj_push_off
    FROM anser_rf_proj_build b LEFT JOIN anser_rf_proj_probe p ON b.id = p.id;
SET anser.runtime_filter = on;
SELECT count(p.id) AS proj_push_on
    FROM anser_rf_proj_build b LEFT JOIN anser_rf_proj_probe p ON b.id = p.id;

-- The alias is not the column.  This subquery renames `payload` to the name
-- `id` and the real `id` to `id2`, and joins on id2 -- so resolving the key by
-- name would land on `payload`, the one column guaranteed never to match a
-- filter built from the build side.  Resolution runs position -> TargetEntry
-- -> Var -> varattno and never reads a name, so the rename is invisible to it.
-- Both counts must be 200.
SET anser.runtime_filter = off;
SELECT count(p.id2) AS rename_push_off
    FROM anser_rf_proj_build b
    LEFT JOIN (SELECT payload AS id, id AS id2 FROM anser_rf_proj_probe) p
           ON b.id = p.id2;
SET anser.runtime_filter = on;
SELECT count(p.id2) AS rename_push_on
    FROM anser_rf_proj_build b
    LEFT JOIN (SELECT payload AS id, id AS id2 FROM anser_rf_proj_probe) p
           ON b.id = p.id2;

-- A computed key has no table attribute number to push down: the scan would
-- have to evaluate `id * 2`, which PassByBloomFilter cannot do.  Note the join
-- clause itself still looks like a plain Var to the planner (it references the
-- scan's output position), so the key resolves and the nodes are injected --
-- it is the scan's own targetlist entry that is an expression.  The consumer
-- must decline the pushdown and probe the projected slot itself.  Only the
-- 100 even build ids match, so both counts must be 100.
SET anser.runtime_filter = off;
SELECT count(p.k) AS expr_push_off
    FROM anser_rf_proj_build b
    LEFT JOIN (SELECT id * 2 AS k FROM anser_rf_proj_probe) p ON b.id = p.k;
SET anser.runtime_filter = on;
SELECT count(p.k) AS expr_push_on
    FROM anser_rf_proj_build b
    LEFT JOIN (SELECT id * 2 AS k FROM anser_rf_proj_probe) p ON b.id = p.k;
RESET gp_enable_runtime_filter_pushdown;
DROP TABLE anser_rf_proj_build, anser_rf_proj_probe;
SET optimizer = off;


-- Everything above checks that the nodes were injected and that the results did
-- not change.  Both of those also hold when the filter never arrives and every
-- consumer fails open -- which is how a broken exchange passes a green test.
-- This is the assertion that the filter was used: the probe side holds 2000
-- rows of which 200 join, so a filter that arrived removes the rest.  Only the
-- Postgres planner is asserted, because its plan shape here is fixed; ORCA
-- chooses and the count would not be a stable thing to require.
CREATE FUNCTION anser_rf_pruned(q text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    line    text;
    removed bigint := 0;
BEGIN
    FOR line IN EXECUTE 'EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) ' || q LOOP
        IF line ~ 'Rows Removed by (Bloom Filter|Pushdown Runtime Filter): ' THEN
            removed := removed + coalesce(substring(line from ': *([0-9]+)')::bigint, 0);
        END IF;
    END LOOP;
    RETURN removed;
END;
$$;
SET optimizer = off;
SET anser.runtime_filter = on;
SELECT 'the filter pruned nothing: it never arrived, or it arrived empty' AS problem
 WHERE anser_rf_pruned('SELECT b.name, p.payload FROM anser_rf_build b LEFT JOIN anser_rf_probe p ON b.id = p.id') = 0;
DROP FUNCTION anser_rf_pruned(text);

DROP TABLE anser_rf_r_push;
DROP TABLE anser_rf_r_on, anser_rf_r_off, anser_rf_r_orca;
DROP TABLE anser_rf_build, anser_rf_probe;

-- Datatype guard: producer and consumer hash the raw Datum bytes, so injection
-- is restricted to keys where SQL equality is bitwise Datum equality.  A
-- cross-type join (float4 vs float8 Datums for equal values differ) must not
-- be injected; results must match the filter-off run either way.
CREATE TABLE anser_rf_build_f (id float8, name text) DISTRIBUTED BY (name);
CREATE TABLE anser_rf_probe_f (id float4, payload text) DISTRIBUTED BY (id);
INSERT INTO anser_rf_build_f SELECT g, 'b' || g FROM generate_series(1, 200) g;
INSERT INTO anser_rf_probe_f SELECT g, 'p' || g FROM generate_series(1, 2000) g;
ANALYZE anser_rf_build_f;
ANALYZE anser_rf_probe_f;

SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_f_on AS
    SELECT b.name, p.payload FROM anser_rf_build_f b JOIN anser_rf_probe_f p ON b.id = p.id
    DISTRIBUTED BY (name);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_f_off AS
    SELECT b.name, p.payload FROM anser_rf_build_f b JOIN anser_rf_probe_f p ON b.id = p.id
    DISTRIBUTED BY (name);

SELECT count(*) AS rows_on FROM anser_rf_f_on;
SELECT count(*) AS only_on
    FROM (SELECT * FROM anser_rf_f_on EXCEPT ALL SELECT * FROM anser_rf_f_off) d;
SELECT count(*) AS only_off
    FROM (SELECT * FROM anser_rf_f_off EXCEPT ALL SELECT * FROM anser_rf_f_on) d;

DROP TABLE anser_rf_f_on, anser_rf_f_off, anser_rf_build_f, anser_rf_probe_f;

-- Same-typed float keys are also excluded: -0.0 and 0.0 compare equal in SQL
-- but are not bitwise equal, so hashing raw Datums could prune a joinable row.
CREATE TABLE anser_rf_build_z (id float8, name text) DISTRIBUTED BY (name);
CREATE TABLE anser_rf_probe_z (id float8, payload text) DISTRIBUTED BY (id);
INSERT INTO anser_rf_build_z VALUES (1, 'b1'), (-0.0, 'bz');
INSERT INTO anser_rf_probe_z VALUES (1, 'p1'), (0.0, 'pz'), (2, 'p2');
ANALYZE anser_rf_build_z;
ANALYZE anser_rf_probe_z;

SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_z_on AS
    SELECT b.name, p.payload FROM anser_rf_build_z b JOIN anser_rf_probe_z p ON b.id = p.id
    DISTRIBUTED BY (name);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_z_off AS
    SELECT b.name, p.payload FROM anser_rf_build_z b JOIN anser_rf_probe_z p ON b.id = p.id
    DISTRIBUTED BY (name);

SELECT count(*) AS rows_on FROM anser_rf_z_on;
SELECT count(*) AS only_on
    FROM (SELECT * FROM anser_rf_z_on EXCEPT ALL SELECT * FROM anser_rf_z_off) d;
SELECT count(*) AS only_off
    FROM (SELECT * FROM anser_rf_z_off EXCEPT ALL SELECT * FROM anser_rf_z_on) d;

DROP TABLE anser_rf_z_on, anser_rf_z_off, anser_rf_build_z, anser_rf_probe_z;

-- Parameterized rescan guard: a build side whose rows depend on a parameter an
-- enclosing nested loop reassigns yields a different key set per outer tuple,
-- and neither node rebuilds on rescan -- the producer stays published, the
-- consumer keeps the filter it already has.  The second iteration would be
-- probed against the first iteration's keys, dropping every row that joins.
-- So the plan pass must refuse the join outright.
--
-- OFFSET 0 keeps the subquery from being pulled up, so it stays a nested loop
-- with a correlated inner side.  Each group's ids are disjoint, which is what
-- makes a stale filter visible: with the first group's filter still in place
-- the second group matches nothing.
CREATE TABLE anser_rf_nl_outer (g int) DISTRIBUTED REPLICATED;
CREATE TABLE anser_rf_nl_build (g int, id int) DISTRIBUTED BY (id);
CREATE TABLE anser_rf_nl_probe (id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_nl_outer VALUES (1), (2);
INSERT INTO anser_rf_nl_build SELECT 1, g FROM generate_series(1, 100) g;
INSERT INTO anser_rf_nl_build SELECT 2, g FROM generate_series(101, 200) g;
INSERT INTO anser_rf_nl_probe SELECT g FROM generate_series(1, 20000) g;
ANALYZE anser_rf_nl_outer;
ANALYZE anser_rf_nl_build;
ANALYZE anser_rf_nl_probe;

CREATE FUNCTION anser_rf_nodes(q text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    line  text;
    nodes bigint := 0;
BEGIN
    FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
        IF line ~ 'Anser Bloom' THEN
            nodes := nodes + 1;
        END IF;
    END LOOP;
    RETURN nodes;
END;
$$;

-- Both groups must report 100, with the filter on and off alike, and the plan
-- must carry no Anser node at all.
SET anser.runtime_filter = off;
SELECT o.g, s.n AS nl_off
FROM anser_rf_nl_outer o
CROSS JOIN LATERAL (SELECT count(*) n FROM anser_rf_nl_probe p
                      JOIN anser_rf_nl_build b ON p.id = b.id
                     WHERE b.g = o.g OFFSET 0) s
ORDER BY o.g;
SET anser.runtime_filter = on;
SELECT o.g, s.n AS nl_on
FROM anser_rf_nl_outer o
CROSS JOIN LATERAL (SELECT count(*) n FROM anser_rf_nl_probe p
                      JOIN anser_rf_nl_build b ON p.id = b.id
                     WHERE b.g = o.g OFFSET 0) s
ORDER BY o.g;
SELECT anser_rf_nodes('SELECT o.g, s.n FROM anser_rf_nl_outer o
CROSS JOIN LATERAL (SELECT count(*) n FROM anser_rf_nl_probe p
                      JOIN anser_rf_nl_build b ON p.id = b.id
                     WHERE b.g = o.g OFFSET 0) s') AS nodes_in_parameterized_plan;

-- The same two tables joined without the correlation are still injected into:
-- the guard must refuse a parameterized build side, not every build side.
SELECT anser_rf_nodes('SELECT count(*) FROM anser_rf_nl_probe p
                         JOIN anser_rf_nl_build b ON p.id = b.id
                        WHERE b.g = 1') AS nodes_when_not_parameterized;

DROP FUNCTION anser_rf_nodes(text);
DROP TABLE anser_rf_nl_outer, anser_rf_nl_build, anser_rf_nl_probe;

-- ---------------------------------------------------------------------------
-- Parallel execution.
--
-- A parallel slice runs numsegments * parallel_workers processes and every one
-- of them publishes its own part, so the coordinator must wait for that many
-- parts rather than for one per segment.  Undercounting completes the channel
-- early and delivers a filter missing build keys; a bloom filter short of keys
-- gives false negatives, which silently drops joinable rows.  So the assertion
-- that matters is not the plan shape but that results are *identical* with the
-- filter on and off -- at more than one worker count, and under both planners,
-- which disagree here: for one query the Postgres planner may give a serial
-- build slice where GPORCA gives a parallel one.
--
-- The join key is the distribution key on both sides, so no Motion is needed
-- to co-locate the join.  That is the only shape either planner parallelises:
-- a redistributed or broadcast build side is fine for GPORCA but the Postgres
-- planner marks Motion paths parallel-unsafe.
-- ---------------------------------------------------------------------------
CREATE TABLE anser_rf_par_build (id int, v text) DISTRIBUTED BY (id);
CREATE TABLE anser_rf_par_probe (id int, pad text) DISTRIBUTED BY (id);
INSERT INTO anser_rf_par_build SELECT g, 'b' || g FROM generate_series(1, 500) g;
INSERT INTO anser_rf_par_probe SELECT g, 'p' || g FROM generate_series(1, 20000) g;
ANALYZE anser_rf_par_build;
ANALYZE anser_rf_par_probe;

-- Force a parallel plan on tables this small, the way PostgreSQL's own tests
-- do; without this the scans are far too cheap to be worth splitting.
SET enable_parallel = on;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- Postgres planner, serial baseline
SET optimizer = off;
SET max_parallel_workers_per_gather = 0;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_par_on AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_par_off AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SELECT (SELECT count(*) FROM anser_rf_par_on) AS rows_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_on
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_off) d) AS only_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_off
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_on) d) AS only_off;
DROP TABLE anser_rf_par_on, anser_rf_par_off;

-- Postgres planner, parallel
SET optimizer = off;
SET max_parallel_workers_per_gather = 4;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_par_on AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_par_off AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SELECT (SELECT count(*) FROM anser_rf_par_on) AS rows_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_on
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_off) d) AS only_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_off
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_on) d) AS only_off;
DROP TABLE anser_rf_par_on, anser_rf_par_off;

-- GPORCA, serial baseline
SET optimizer = on;
SET max_parallel_workers_per_gather = 0;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_par_on AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_par_off AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SELECT (SELECT count(*) FROM anser_rf_par_on) AS rows_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_on
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_off) d) AS only_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_off
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_on) d) AS only_off;
DROP TABLE anser_rf_par_on, anser_rf_par_off;

-- GPORCA, parallel
SET optimizer = on;
SET max_parallel_workers_per_gather = 4;
SET anser.runtime_filter = on;
CREATE TEMP TABLE anser_rf_par_on AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SET anser.runtime_filter = off;
CREATE TEMP TABLE anser_rf_par_off AS
    SELECT b.v, p.pad FROM anser_rf_par_probe p
        JOIN anser_rf_par_build b ON p.id = b.id
    DISTRIBUTED BY (v);
SELECT (SELECT count(*) FROM anser_rf_par_on) AS rows_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_on
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_off) d) AS only_on,
       (SELECT count(*) FROM (SELECT * FROM anser_rf_par_off
                              EXCEPT ALL
                              SELECT * FROM anser_rf_par_on) d) AS only_off;
DROP TABLE anser_rf_par_on, anser_rf_par_off;

DROP TABLE anser_rf_par_build, anser_rf_par_probe;
RESET enable_parallel;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET max_parallel_workers_per_gather;

RESET anser.runtime_filter;
RESET optimizer;
RESET enable_nestloop;
RESET enable_mergejoin;

