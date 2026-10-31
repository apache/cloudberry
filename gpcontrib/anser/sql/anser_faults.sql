-- Anser runtime bloom filter: the cases that need gp_inject_fault.
--
-- Kept apart from anser_runtime_filter so that a build configured with
-- --disable-debug-extensions loses only these.  gp_inject_fault is built only
-- with the debug extensions, and the regression driver excludes any file that
-- calls one -- see faultinjector_prep in src/Makefile.global.in -- so whatever
-- shares a file with a fault is skipped along with it.  Everything that can be
-- asked for with data alone therefore stays in the other file.
--
-- Requires the cluster to run with anser.enable=on, like the rest of the suite.
--
-- The extension may or may not already be there, and saying so is not part of
-- what is being tested; it is never dropped, for the same reason -- it can
-- predate this test, and other tests in the same database rely on it.
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
-- end_ignore

-- Deterministic plan shape: a hash join under the Postgres planner, as in
-- anser_runtime_filter.  A fresh session inherits none of its settings.
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET optimizer = off;

-- Trace every exchange step into the server logs (never reaches the client),
-- so a CI run that goes wrong can be diagnosed from the log artifacts.
SET anser.debug = on;

-- nextval guard: the QD sends two kinds of message down a QE's dispatch
-- connection -- the nextval reply and a sideband message -- and each has its
-- own reader.  A reader that meets the other's message has to skip it whole:
-- taking the type byte and erroring leaves the body in the stream, and the
-- session then dies somewhere else with "protocol synchronization was lost".
--
-- In the field this needs an Anser consumer to give up on a filter the
-- coordinator goes on to deliver anyway, which is a race and cannot be asked
-- for from SQL.  The fault puts a sideband message in front of one nextval
-- reply instead, so the ordering is certain rather than likely; the
-- three-argument form is occurrence 1..1, so it fires once.
--
-- Nothing here is Anser's -- the fault supplies the message and the fix is in
-- cdb_sequence_nextval_qe -- but Anser is why a second message type exists on
-- that connection at all, so the test lives with it.
--
CREATE SEQUENCE anser_rf_seq;
CREATE TABLE anser_rf_seq_t (id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_seq_t SELECT g FROM generate_series(1, 10) g;
SELECT gp_inject_fault('nextval_sideband_before_response', 'skip', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(nextval('anser_rf_seq')) AS seq_rows FROM anser_rf_seq_t;
SELECT gp_inject_fault('nextval_sideband_before_response', 'reset', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DROP SEQUENCE anser_rf_seq;
DROP TABLE anser_rf_seq_t;

-- Delivery to a consumer that stopped reading must not block the coordinator.
--
-- A merged filter is 1-64 MB and the dispatch connections are blocking, so
-- pushing to a QE that is no longer reading used to wait in pqWait() with no
-- deadline and no CHECK_FOR_INTERRUPTS -- inside the dispatcher loop, which
-- then served nobody.  Neither cancel nor statement_timeout could end it.
--
-- The ordering cannot be arranged with data: the channel completes when the
-- last part lands, which is normally while the consumers are still waiting.
-- Holding the delivery back is what lets every consumer reach its deadline
-- and move on to its probe side -- and it is while they are busy there that
-- the coordinator finally writes to all of them.
--
-- Note this one hangs rather than fails if the fix is reverted, for the same
-- reason the bug was unkillable.  It is here because the alternative is a
-- regression nobody notices until a cluster wedges.
SET anser.timeout_ms = 1;
CREATE TABLE anser_rf_push_build (id int, pad text) DISTRIBUTED BY (pad);
CREATE TABLE anser_rf_push_probe (id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_push_build SELECT g, 'b' || g FROM generate_series(1, 1000) g;
INSERT INTO anser_rf_push_probe SELECT g % 1000 + 1 FROM generate_series(1, 1000000) g;
ANALYZE anser_rf_push_build;
ANALYZE anser_rf_push_probe;

-- One segment publishes a second late; every other consumer gives up at 1 ms.
SELECT gp_inject_fault('anser_before_deliver', 'sleep', '', '', '', 1, 1, 1, dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SET anser.runtime_filter = on;
SELECT count(*) AS push_rows
    FROM anser_rf_push_probe p JOIN anser_rf_push_build b ON p.id = b.id;
SELECT gp_inject_fault('anser_before_deliver', 'reset', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DROP TABLE anser_rf_push_build, anser_rf_push_probe;

-- A consumer that runs out of time withdraws, so the coordinator stops
-- counting it as a subscriber.  Without that it would keep delivering to a
-- process that has moved on -- which is what the push above has to survive,
-- and what this keeps from quietly regressing into.
--
-- The withdrawal leaves no trace a query can see: the channel is gone by the
-- time the query returns, and a filter that merely arrived late looks the
-- same.  So the coordinator counts them, and this reads the difference.
CREATE TABLE anser_rf_unsub_build (id int, pad text) DISTRIBUTED BY (pad);
CREATE TABLE anser_rf_unsub_probe (id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_unsub_build SELECT g, 'b' || g FROM generate_series(1, 1000) g;
INSERT INTO anser_rf_unsub_probe SELECT g % 1000 + 1 FROM generate_series(1, 200000) g;
ANALYZE anser_rf_unsub_build;
ANALYZE anser_rf_unsub_probe;
-- start_ignore
CREATE EXTENSION IF NOT EXISTS anser_test;
-- end_ignore
SELECT anser_test_unsubscribes() AS unsub_before \gset
SELECT gp_inject_fault('anser_before_deliver', 'sleep', '', '', '', 1, 1, 1, dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(*) AS unsub_rows
    FROM anser_rf_unsub_probe p JOIN anser_rf_unsub_build b ON p.id = b.id;
SELECT gp_inject_fault('anser_before_deliver', 'reset', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT anser_test_unsubscribes() > :unsub_before AS consumers_withdrew;
DROP EXTENSION anser_test;
DROP TABLE anser_rf_unsub_build, anser_rf_unsub_probe;

-- A partially delivered filter must not wedge the next command.
--
-- The push above is torn when the filter is bigger than the connection
-- buffers: the consumers give up and finish, the coordinator's non-blocking
-- write leaves the tail queued, and the poll loop stops flushing once the
-- peer's command completes.  The next dispatch on the same gang would then
-- overwrite the queued bytes (PQsendGpQuery_shared), leaving the QE waiting
-- forever for a body that never comes -- the session only ended with the
-- cluster.  ExecutorEnd now completes the flush, or poisons the connection.
--
-- The filter is forced to 64 MB (anser.debug_min_filter_bytes) so the push
-- cannot fit the buffers even on loopback.  This test hangs rather than fails
-- if the fix is reverted, the same warning as the push test above.
SET anser.timeout_ms = 100;
SET anser.debug_min_filter_bytes = 67108864;
CREATE TABLE anser_rf_stale_build (id int, pad text) DISTRIBUTED BY (pad);
CREATE TABLE anser_rf_stale_probe (id int) DISTRIBUTED BY (id);
INSERT INTO anser_rf_stale_build SELECT g, 'b' || g FROM generate_series(1, 1000) g;
INSERT INTO anser_rf_stale_probe SELECT g % 1000 + 1 FROM generate_series(1, 1000000) g;
ANALYZE anser_rf_stale_build;
ANALYZE anser_rf_stale_probe;

-- Hold the delivery for 1 s so every consumer gives up at 100 ms and the
-- 64 MB filter is pushed to connections nobody is reading.
SELECT gp_inject_fault('anser_before_deliver', 'sleep', '', '', '', 1, 1, 1, dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SET anser.runtime_filter = on;
SELECT count(*) AS stale_rows
    FROM anser_rf_stale_probe p JOIN anser_rf_stale_build b ON p.id = b.id;
SELECT gp_inject_fault('anser_before_deliver', 'reset', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- The torn delivery is completed (or its connection poisoned) at ExecutorEnd,
-- so the next command on the same gang must answer.
SELECT 'next command' AS marker, count(*) AS build_rows FROM anser_rf_stale_build;
DROP TABLE anser_rf_stale_build, anser_rf_stale_probe;
RESET anser.debug_min_filter_bytes;

-- A filter parked by one statement must not be claimed by the next.
--
-- A consumer that waits while a delivery for another channel arrives parks it
-- in the inbox.  If the statement then errors inside a savepoint, there is no
-- ExecutorEnd for it (a failed portal skips it), so the parked entry used to
-- survive -- and the next statement, whose condition keys restart at the same
-- values, claimed it: a filter built from the old build side pruned the new
-- one.  Deliveries now carry the session id and command count and a consumer
-- matches on them exactly, and the subtransaction abort drops the inbox.
--
-- The LEFT JOIN pins j2 as the build side, so its consumer waits first: it
-- gives up at 700 ms while the coordinator sleeps for 1 s, its late delivery
-- is then read -- and parked -- by j1's consumer, and the division errors
-- before anything claims it.  The second run re-executes the same plan shape
-- with a build side disjoint from the parked filter: claiming it would drop
-- every joinable row, so count(j2.id) must be 1000, not 0.
SET anser.timeout_ms = 700;
CREATE TABLE anser_sf_b1 (id int, pad text) DISTRIBUTED BY (pad);
CREATE TABLE anser_sf_p1 (id int) DISTRIBUTED BY (id);
CREATE TABLE anser_sf_b2 (id int, pad text) DISTRIBUTED BY (pad);
CREATE TABLE anser_sf_p2 (id int) DISTRIBUTED BY (id);
INSERT INTO anser_sf_b1 SELECT g, 'b' || g FROM generate_series(1, 2000) g;
INSERT INTO anser_sf_p1 SELECT g FROM generate_series(1, 2000) g;
INSERT INTO anser_sf_b2 SELECT g, 'b' || g FROM generate_series(1, 1000) g;
INSERT INTO anser_sf_p2 SELECT g FROM generate_series(1, 2000) g;
ANALYZE anser_sf_b1;
ANALYZE anser_sf_p1;
ANALYZE anser_sf_b2;
ANALYZE anser_sf_p2;

SELECT gp_inject_fault('anser_before_deliver', 'sleep', '', '', '', 1, 1, 1, dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SET anser.runtime_filter = on;
BEGIN;
SAVEPOINT anser_sf;
SELECT j1.id / (j1.id - j1.id) AS boom
FROM (SELECT p1.id FROM anser_sf_p1 p1 JOIN anser_sf_b1 b1 ON p1.id = b1.id) j1
LEFT JOIN (SELECT p2.id FROM anser_sf_p2 p2 JOIN anser_sf_b2 b2 ON p2.id = b2.id) j2
    ON j1.id = j2.id;
ROLLBACK TO SAVEPOINT anser_sf;
DELETE FROM anser_sf_b2;
INSERT INTO anser_sf_b2 SELECT 1000 + g, 'b' || g FROM generate_series(1, 1000) g;
SELECT count(j2.id) AS reused_rows
FROM (SELECT p1.id FROM anser_sf_p1 p1 JOIN anser_sf_b1 b1 ON p1.id = b1.id) j1
LEFT JOIN (SELECT p2.id FROM anser_sf_p2 p2 JOIN anser_sf_b2 b2 ON p2.id = b2.id) j2
    ON j1.id = j2.id;
COMMIT;
SELECT gp_inject_fault('anser_before_deliver', 'reset', dbid)
    FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DROP TABLE anser_sf_b1, anser_sf_p1, anser_sf_b2, anser_sf_p2;

RESET anser.runtime_filter;
RESET anser.debug;
RESET anser.timeout_ms;
RESET optimizer;
RESET enable_nestloop;
RESET enable_mergejoin;
