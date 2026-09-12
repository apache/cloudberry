# gp_stats_collector crash test

Liveness test: with the runtime query-state feature fully enabled and a poller
tracing every running query, Cloudberry must not crash and queries must still
finish with the same results as without the feature.

Driven by `.github/workflows/gpsc-crash-test.yaml` (manual / nightly). One build,
one demo cluster, two `installcheck-parallel` passes on it:

1. **baseline** — feature OFF (stock Cloudberry, module not preloaded) → record
   failed tests.
2. **traced** — feature ON + poller running → record failed tests, then the
   crash gate.

**Hard verdict: the crash gate** (no PANIC / signal / segment down / dead
coordinator). The failed-test delta `traced \ baseline` is reported for
information only and does **not** fail the job: `installcheck-parallel` is not
diff-deterministic, so a tracing-only failure is not, by itself, a regression —
inspect the uploaded `run2-traced.diffs` by hand.

Workload is `installcheck-parallel` (upstream `parallel_schedule`): fast and
fault-free. Because it injects no faults, any PANIC in the logs is a genuine
crash, which keeps the crash gate simple and honest.

## Files

- `poller.py` — single-process tracer: loops over active client backends in
  `pg_stat_activity` and calls `gpsc.pg_query_state(pid, trace_id)` on each, with
  a per-pid cooldown so no pid is polled while a prior poll is in flight (the
  extension does not support overlapping polls of one pid). Uses `psql`, no
  Python DB driver.
- `uds_drain.py` — minimal `AF_UNIX` sink for `gpsc.uds_path`; reads and discards
  so the serialize+send path runs without the real yagpcc.
- `extract_failures.sh` — pulls the set of `... FAILED` test names from a
  `make installcheck-parallel` log.
- `crash_scan.sh` — the crash gate: log crash markers, `gpstate -e`, `SELECT 1`.

Design notes: `../../docs/gpsc-crash-test-design.md` (local, not committed).
