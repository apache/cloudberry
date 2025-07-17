## YAGP Hooks Collector

An extension for collecting greenplum query execution metrics and reporting them to an external agent.

### Collected Statistics

#### 1. Query Lifecycle
-   **What:** Captures query text, normalized query text, timestamps (submit, start, end, done), and user/database info.
-   **GUC:** `yagpcc.enable`.

#### 2. `EXPLAIN` data
-   **What:** Triggers generation of the `EXPLAIN (TEXT, COSTS, VERBOSE)` and captures it.
-   **GUC:** `yagpcc.enable`.

#### 3. `EXPLAIN ANALYZE` data
-   **What:** Triggers generation of the `EXPLAIN (JSON, ANALYZE, BUFFERS, TIMING, VERBOSE)` and captures it.
-   **GUCs:** `yagpcc.enable`, `yagpcc.min_analyze_time`, `yagpcc.enable_cdbstats`(ANALYZE), `yagpcc.enable_analyze`(BUFFERS, TIMING, VERBOSE).

#### 4. Other Metrics
-   **What:** Captures Instrument, Greenplum, System, Network, Interconnect, Spill metrics.
-   **GUC:** `yagpcc.enable`.

### General Configuration
-   **Nested Queries:** When `yagpcc.report_nested_queries` is `false`, only top-level queries are reported from the coordinator and segments, when `true`, both top-level and nested queries are reported from the coordinator, from segments collected as aggregates.
-   **Data Destination:** All collected data is sent to a Unix Domain Socket. Configure the path with `yagpcc.uds_path`.
-   **User Filtering:** To exclude activity from certain roles, add them to the comma-separated list in `yagpcc.ignored_users_list`.
-   **Trimming plans:** Query texts and execution plans are trimmed based on `yagpcc.max_text_size` and `yagpcc.max_plan_size` (default: 1024KB). For now, it is not recommended to set these GUCs higher than 1024KB.

