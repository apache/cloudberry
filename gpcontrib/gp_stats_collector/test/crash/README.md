<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# gp_stats_collector crash test

Liveness test: with the runtime query-state feature fully enabled and a poller
tracing every running query, Cloudberry must not crash and queries must still
finish with the same results as without the feature.

Driven by `.github/workflows/gpsc-crash-test.yaml`, which runs on every push and
pull request against `REL_2_STABLE`, and on `workflow_dispatch`. It is not path
filtered on purpose: the feature walks a *live* plan tree and calls runtime
`EXPLAIN` on it, so a change anywhere in the executor — a new plan node, a
different `PlanState` lifecycle, altered `Instrumentation` timing — can break it,
not just a change under `gpcontrib/gp_stats_collector/`.

One build, one demo cluster, two `installcheck-parallel` passes on it:

1. **baseline** — feature OFF (stock Cloudberry, module not preloaded) → record
   failed tests.
2. **traced** — feature ON + poller running → record failed tests, then the
   crash gate.

Between the two passes the job drops the `regression` database and every
`regress*`/`mdb*` role. `installcheck` recreates its database each pass, but
`CREATE ROLE` makes cluster-global roles that outlive it, so without this the
traced pass would fail in `test_setup` with "role already exists".

## The verdict

**Hard verdict: the crash gate** (no PANIC / signal / segment down / dead
coordinator). The failed-test delta `traced \ baseline` is reported for
information only and does **not** fail the job: `installcheck-parallel` is not
diff-deterministic, so a tracing-only failure is not, by itself, a regression —
inspect the uploaded `run2-traced.diffs` by hand.

One test is carved out of that delta as known-flaky: `strings`. Its QD
parse-time warnings (`nonstandard use of \\`, from `scan.l`'s
`escape_string_warning`) re-emit non-deterministically when the poller's
`ProcSignal` lands mid-statement. The query has no runtime stats to report, so
the diff is client-message noise rather than a correctness signal.

Workload is `installcheck-parallel` (upstream `parallel_schedule`): fast and
fault-free. Because it injects no faults, any PANIC in the logs is a genuine
crash, which keeps the crash gate simple and honest.

## Running it locally

Build the tree and create a demo cluster as usual, then from the source root:

```bash
source /usr/local/cloudberry-db/cloudberry-env.sh
source gpAux/gpdemo/gpdemo-env.sh
CRASH=gpcontrib/gp_stats_collector/test/crash

# 1. baseline pass -- a failing installcheck is expected, it is not the verdict
make -C src/test/regress installcheck-parallel > /tmp/run1-baseline.log 2>&1 || true
$CRASH/extract_failures.sh /tmp/run1-baseline.log > /tmp/baseline-failures.txt

# 2. clear the roles the baseline leaked
psql -X -d postgres -c 'DROP DATABASE IF EXISTS regression;'
psql -X -q -A -t -d postgres \
  -c "SELECT format('DROP ROLE IF EXISTS %I;', rolname) FROM pg_roles WHERE rolname ~ '^(regress|mdb)'" \
  | psql -X -d postgres -f -

# 3. turn the feature on -- two restarts: the module has to be loaded before
#    its own GUCs are recognised by gpconfig
gpconfig -c shared_preload_libraries -v 'gp_stats_collector'
gpstop -ar
for guc in pg_query_state.enable pg_query_state.enable_timing \
           pg_query_state.enable_buffers gpsc.enable gpsc.enable_analyze \
           gpsc.enable_cdbstats gpsc.report_nested_queries; do
  gpconfig -c $guc -v on
done
gpconfig -c gpsc.logging_mode -v UDS
gpconfig -c gpsc.uds_path -v /tmp/gpsc_agent.sock
gpconfig -c compute_query_id -v regress
gpstop -ar
psql -X -d postgres -c 'CREATE EXTENSION IF NOT EXISTS gp_stats_collector;'

# 4. sink + tracer
$CRASH/uds_drain.py --path /tmp/gpsc_agent.sock &
rm -f /tmp/gpsc_poller.stop
$CRASH/poller.py --stop-file /tmp/gpsc_poller.stop &

# 5. traced pass
make -C src/test/regress installcheck-parallel > /tmp/run2-traced.log 2>&1 || true
touch /tmp/gpsc_poller.stop

# 6. the verdict
$CRASH/crash_scan.sh gpAux/gpdemo/datadirs
```

Step 3 is what "feature ON" means; skipping any of it makes the traced pass
weaker than CI's. If `gpsc.pg_query_state` is not resolvable after step 3 the
run is vacuous — every poll just errors on a missing function — so CI asserts
`'gpsc.pg_query_state(int,bytea)'::regprocedure` resolves before starting.

## Files

- `poller.py` — single-process tracer: loops over active client backends in
  `pg_stat_activity` and calls `gpsc.pg_query_state(pid, trace_id)` on each, with
  a per-pid cooldown so no pid is polled while a prior poll is in flight (the
  extension does not support overlapping polls of one pid). Uses `psql`, no
  Python DB driver. Runs until `--stop-file` appears.
- `uds_drain.py` — minimal `AF_UNIX` sink for `gpsc.uds_path`; reads and discards
  so the serialize+send path runs without the real collector agent.
- `extract_failures.sh` — pulls the sorted set of `... FAILED` test names from a
  `make installcheck-parallel` log.
- `crash_scan.sh <log-root>` — the crash gate: log crash markers, `gpstate -e`,
  `SELECT 1`. Plain `FATAL` is ignored on purpose, being routine during
  regression runs.
