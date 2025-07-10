## YAGP Hooks Collector

An extension for collecting greenplum query execution metrics and reporting them to an external agent

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

#### 4. Nested queries
-   **What:** 
    -   Disabled: Top-level queries are being reported from coordinator and segments.
    -   Enabled: Top-level and nested queries are being reported from coordinator. Any nested queries from segments are collected as aggregates.
-   **GUC:** `yagpcc.report_nested_queries`.

#### 5. Other Metrics
-   **What:** Captures Instrument, Greenplum, System, Network, Interconnect, Spill metrics.
-   **GUC:** `yagpcc.enable`.

### General Configuration
-   **Data Destination:** All collected data is sent to a Unix Domain Socket. Configure the path with `yagpcc.uds_path`.
-   **User Filtering:** To exclude activity from certain roles, add them to the comma-separated list in `yagpcc.ignored_users_list`.

