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

## GP Stats Collector

An extension for collecting query execution metrics and reporting them to an external agent.

### Collected Statistics

#### 1. Query Lifecycle
-   **What:** Captures query text, normalized query text, timestamps (submit, start, end, done), and user/database info.
-   **GUC:** `gpsc.enable`.

#### 2. `EXPLAIN` data
-   **What:** Triggers generation of the `EXPLAIN (TEXT, COSTS, VERBOSE)` and captures it.
-   **GUC:** `gpsc.enable`.

#### 3. `EXPLAIN ANALYZE` data
-   **What:** Triggers generation of the `EXPLAIN (TEXT, ANALYZE, BUFFERS, TIMING, VERBOSE)` and captures it.
-   **GUCs:** `gpsc.enable`, `gpsc.min_analyze_time`, `gpsc.enable_cdbstats`(ANALYZE), `gpsc.enable_analyze`(BUFFERS, TIMING, VERBOSE).

#### 4. Other Metrics
-   **What:** Captures Instrument, System, Network, Interconnect, Spill metrics.
-   **GUC:** `gpsc.enable`.

### General Configuration
-   **Nested Queries:** When `gpsc.report_nested_queries` is `false`, only top-level queries are reported from the coordinator and segments, when `true`, both top-level and nested queries are reported from the coordinator, from segments collected as aggregates.
-   **Data Destination:** All collected data is sent to a Unix Domain Socket. Configure the path with `gpsc.uds_path`.
-   **User Filtering:** To exclude activity from certain roles, add them to the comma-separated list in `gpsc.ignored_users_list`.
-   **Trimming plans:** Query texts and execution plans are trimmed based on `gpsc.max_text_size` and `gpsc.max_plan_size` (default: 1024KB). For now, it is not recommended to set these GUCs higher than 1024KB.
-   **Analyze collection:** Analyze is sent if execution time exceeds `gpsc.min_analyze_time`, which is 10 seconds by default. Analyze is collected if `gpsc.enable_analyze` is true.

### Runtime Query State (`pg_query_state`)

On-demand inspection of the live execution state of another running backend. The target's active plan tree is walked across the coordinator (QD) and every segment (QE), collecting per-node instrumentation, without waiting for the query to finish. Each backend pushes its own snapshot to the UDS sink configured by `gpsc.uds_path`, keyed by the caller-supplied `trace_id`.

Delivery is best-effort, exactly like the rest of the extension: a snapshot that does not fit into the socket is dropped rather than retried, so a slow or absent reader never adds latency to the query being observed.

The functions live in the `gpsc` schema (extension version 1.2).

#### 1. `pg_query_state(pid, trace_id)`
-   **What:** Triggers runtime per-node collection for the query running on backend `pid`. Fans a poll out to every participating QE and to the QD; each backend walks its plan tree and pushes one per-node batch. The coordinator additionally pushes the deparsed plan document, rate-limited so that repeated polls of a long query do not resend an unchanged plan. Fire-and-forget: returns `void`.
-   **Arguments:** `trace_id` is a `bytea` of exactly 16 bytes, minted by the caller and used as the collection key on the receiving side.
-   **Executes on:** the coordinator only.
-   **GUC:** `pg_query_state.enable`.

#### 2. `pg_query_state_backends(pid)`
-   **What:** Lists the QE backends participating in the query running on backend `pid`, as `(segid, pid)` rows, so that a collector knows how many batches to expect. A coordinator-only query (`INSERT ... VALUES`, catalog reads) allocates no gang, and is reported as a single row for the coordinator itself with `segid < 0`. Returns an empty set when the target is not running a query or has the module disabled.
-   **GUC:** `pg_query_state.enable`.

#### 3. `cbdb_mpp_query_state(gp_segment_pid[], trace_id)`
-   **What:** QE-side dispatch target used internally by `pg_query_state()`; not intended for direct use.

### Runtime Query State Configuration
-   **Enable:** `pg_query_state.enable` (default `on`) turns the executor hooks and signal handling on or off. Additional GUCs `pg_query_state.enable_timing` and `pg_query_state.enable_buffers` control the level of instrumentation collected.
-   **Permissions:** The functions are granted to `PUBLIC`, but access is checked in the server: a caller may poll a backend only if it is a superuser or owns the target query. This lets monitoring agents run under a non-superuser role while still preventing one role from observing another's queries.
-   **Preload:** The module registers custom signal handlers at startup, so `gp_stats_collector` must be listed in `shared_preload_libraries`.
