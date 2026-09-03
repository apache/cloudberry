# pg_query_state signal layer

On-demand inspection of a *running* query: on request, every backend executing
the query (the coordinator and all its QEs) walks its live plan tree and reports
per-node runtime stats, without waiting for the query to finish.

The code here is derived from [pg_query_state](https://github.com/postgrespro/pg_query_state)
(PostgreSQL License) but the transport and keying differ substantially, so this
note describes the design as implemented in this tree, not the upstream one.

## Files in this directory

- `pg_query_state.c` — `_PG_init` wiring, GUCs, executor hooks, the SQL entry
  points, the permission gate, and the per-backend `qs_trace_slots` shmem.
- `signal_handler.c` — the two custom ProcSignal handlers, the plan-tree walker,
  the per-node delta computation, and the plan-doc builder.
- `qs_types.h` — `GpscNodeSample` (one per-node sample) and the status enums.

The C++ side that serializes and ships the samples lives one level up in `../`
(`PlanNodeEmitter`, `UDSConnector`, `ProtoUtils`); the wire messages are in
`../../protos/`. The receiver is yagpcc; its side is in `../../../../../yagpcc`.

## The key: trace_id

Every collection is keyed by a **trace_id** — 16 raw bytes minted once, on the
coordinator, per `pg_query_state()` call. It is threaded to every participating
backend and stamped into every message. yagpcc groups the snapshots of one poll
by trace_id, and uses it to tell an in-flight poll apart from an overlapping poll
of the same pid. `(tmid, ssid, ccnt)` are display-only.

## Data flow

The sequence diagram lives in
[`../../docs/pg_query_state_dataflow.puml`](../../docs/pg_query_state_dataflow.puml)
(render with `plantuml docs/pg_query_state_dataflow.puml`). In short:

1. **Trigger.** yagpcc mints the trace_id and calls `gpsc.pg_query_state(pid,
   trace_id)` on the coordinator (`EXECUTE ON COORDINATOR`). This runs on a fresh
   *requestor* backend, not the backend running the observed query. yagpcc
   separately calls `gpsc.pg_query_state_backends(pid)` to learn the exact set of
   backends to expect — the completeness barrier for the pull below.
2. **Fan-out.** The requestor validates the trace_id and checks the permission
   gate, then resolves the observed query's QEs by signalling its coordinator
   backend (`BackendInfoPollReason`; `SendCdbComponents` replies over shm_mq). For
   each *target* backend it stamps `qs_trace_slots[backendId] = trace_id` and then
   signals it with `QueryStatePollReason`: the target QD directly, the target QEs
   via a dispatched `cbdb_mpp_query_state(gp_segment_pid[], trace_id)` that runs on
   a requestor backend on each segment host. Requestor and target always sit on
   the same host and only ever talk by signal.
3. **Collection.** `SendQueryState()` runs in the signalled *target* backend. It
   walks the live plan tree, builds one `GpscNodeSample` per node, and pushes the
   whole snapshot as a single `SetPerNodeBatchReq` over UDS to the *local* yagpcc
   (one connection per backend, not per node). The coordinator additionally builds
   the deparsed plan document and sends it as a `SetQueryPlanReq`, rate-limited so
   repeated polls of a long query do not resend an unchanged plan.
4. **Storage & pull.** yagpcc stores each batch keyed by trace_id. The master
   pulls every segment's batch for that trace, folds in the QD's own nodes, pivots
   the flat samples into a per-slice tree, and returns it to the UI.

## What a node sample carries

`GpscNodeSample` (see `qs_types.h`) per plan node: identity (`plan_node_id`,
`parent`, `slice_id`, `node_type`, scan `relation_oid`), counters (`ntuples`,
`tuplecount`, `nloops`), timing (`startup`, `total`, `firsttuple`), buffers
(`shared_blks_hit/read`), spill (`workmem_used/wanted`, `workfile_created`),
status (`INITIALIZED`/`EXECUTING`/`FINISHED`, `eof`), and C-side derived rates
(`ntuples_delta`, `tuples_per_sec`, `time_since_init_sec`, `stalled`). The rates
come from a per-node rolling state keyed by `plan_node_id`, reset on executor
start and end. The walk root reports `parent = -1`
(`GPSC_NO_PARENT_PLAN_NODE_ID`); `0` would be ambiguous, since `plan_node_id`
counters start there.

## Permissions

The SQL functions are granted to `PUBLIC` so a non-superuser monitoring agent
can run them; access is gated in C: a caller may poll a backend only if it is a
superuser or owns the target query (`GetUserId() == proc->roleId`).
