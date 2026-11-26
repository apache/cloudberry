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

# RPT scripts

Use a development cluster with Anser and RPT installed and
[preloaded in the required order](../README.md#building-and-enabling).
The examples use a database where your role can create the extension
and fixture tables. Use your normal libpq connection settings.

| Script | Purpose |
| --- | --- |
| [collect_bridge_example.sql](collect_bridge_example.sql) | Capture the bridge example under both optimizers |
| [collect_star_example.sql](collect_star_example.sql) | Capture the four-table star and collect repeated timing samples |
| [diagnose_rpt.py](diagnose_rpt.py) | Run a regression workload with step markers and a watchdog |
| [diagnose_forward.py](diagnose_forward.py) | Compatibility entry point for the diagnostic runner |
| [compare_tpcds_results.py](compare_tpcds_results.py) | Compare exact results with filters off, Anser on, and RPT on |
| [survey.py](survey.py) | Survey RPT and Anser decisions on benchmark queries |
| [tpcds_rf_metrics.sql](tpcds_rf_metrics.sql) | Install a JSON EXPLAIN runtime-filter metrics helper for TPC-DS |

## Collect the bridge example

Run from the repository root against a dedicated database.

```sh
psql -X -d rpt_bridge_test \
  -f gpcontrib/robust_predicate_transfer/scripts/collect_bridge_example.sql \
  > bridge-example.log 2>&1
```

The script creates `rpt_readme_bridge`. Set `-v fact_rows=1000000` to
control the row count of `event`. The `sale` table has twice as many rows.
Use a fresh database for each collection.

The log has eight runs, one for each combination of optimizer and RPT
mode. Compare `baseline`, `forward`, `backward`, and `full` within the
same optimizer. Check the reported `Optimizer` for fallback, the
`decision=` fields for accepted or refused transfers, and the result
row for matching aggregates. The `Execution Time` values are single
`EXPLAIN ANALYZE` runs.

## Collect the four-table star example

Run from an empty output directory against a dedicated database.

```sh
rpt_repo=/path/to/cloudberry-leborchuk
mkdir -p /tmp/rpt-star-example
cd /tmp/rpt-star-example
psql -X -q -d rpt_star_test \
  -v fact_rows=1000000 -v warmups=2 -v samples=7 \
  -f "$rpt_repo/gpcontrib/robust_predicate_transfer/scripts/collect_star_example.sql" \
  > collection.log 2>&1
```

The script creates `rpt_readme_star` and checks that the aggregates
match across all eight runs. Output filenames use
`<optimizer>-<mode>`. The prefixes `off` and `on` mean PostgreSQL and
GPORCA were requested, respectively.

| Output | How to read it |
| --- | --- |
| `query.sql`, `environment.txt`, metadata CSVs | Query text, settings, table sizes, statistics, and distribution |
| `*.transfer.txt` | Accepted and refused transfers, estimated costs, and refusal reasons |
| `*.explain.txt`, `*.explain.json` | Executed plans and counters from separate runs. Check the actual optimizer in each plan |
| `*.result.csv` | Aggregate result for each run |
| `collection.log` | Warmups and measured query timings, labelled with `RPT_SAMPLE` markers |

For timing comparisons, take the `Time:` line immediately after each
aggregate result marked `phase=sample`. Exclude warmups and timings for
`SET` or marker statements. These samples include planning and client
round-trip time.

## Diagnose a regression workload

```sh
python3 gpcontrib/robust_predicate_transfer/scripts/diagnose_rpt.py \
  --suite backward --database rpt_test --output /tmp/rpt-backward
```

Use an isolated test database. The suites are `forward` (default),
`backward`, and `backward_faults`. The fault suite requires
`gp_inject_fault` and `anser_test`. Use `--emit-only` to save the SQL
without executing it. The watchdog limits are `--wall-timeout` (180
seconds) and `--cancel-grace` (15 seconds).
[diagnose_forward.py](diagnose_forward.py) accepts the same arguments.

Start with `result.json` for client status, elapsed time, and timeout.
Use `client.log` to find the last step reached or first error,
`decisions.json` for the captured decisions, and `workload.sql` for the
exact SQL. A timeout also produces activity and cancellation snapshots.

## Survey benchmark plans

```sh
python3 gpcontrib/robust_predicate_transfer/scripts/survey.py \
  --job /path/to/job-sql --job-dbname imdb \
  --tpch /path/to/tpch-sql --tpch-dbname tpch \
  --tpcds /path/to/tpcds-sql --tpcds-dbname tpcds \
  --output /tmp/rpt-survey
```

The survey plans queries without executing them. The connecting role
needs EXECUTE on `rpt_transfer_plan(text)` in each database. Supply
instantiated SELECT or WITH queries. Full inputs contain 113 JOB, 22
TPC-H, and 103 TPC-DS statements. Keep both statements for TPC-DS
14/23/24/39 and use the CTE form of TPC-H Q15. Use `--allow-partial`
for a smaller pilot.

Start with `summary.md` for coverage and failures. Use `results.csv` or
`results.json` for per-query decisions and optimizer fallback. The
`raw/` directory contains transfer dumps and Anser JSON plans. Failed
queries remain in the report denominator.

## Compare TPC-DS query results

Use the same instantiated SQL and database as the benchmark.

```sh
python3 gpcontrib/robust_predicate_transfer/scripts/compare_tpcds_results.py \
  --database tpcds /path/to/query18.sql /path/to/query33.sql /path/to/query60.sql
```

Each statement reports missing and extra rows against the no-filter
baseline, including duplicate counts. A mismatch or query error returns
a nonzero exit status. Use `--optimizer on` to repeat with GPORCA.


## Collect TPC-DS runtime-filter metrics

Install the helper in a benchmark database that already has the TPC-DS
extension and generated queries.

```sh
psql -X -d rpt_tpcds -f gpcontrib/robust_predicate_transfer/scripts/tpcds_rf_metrics.sql
```

Run `SELECT * FROM tpcds.rf_metrics(93);` in the benchmark session. The
helper executes the query and reports filter nodes, scan pruning, and
InitPlan timings by statement. The difference between a wrapper's
reported time and its child's is an approximation. In distributed plans,
the two times can come from different workers.

The tuning switches default to on. For A/B testing, disable one switch
at a time to measure its effect.

| Setting | Enables |
| --- | --- |
| `anser.zero_copy` | Return child slots from identity filter wrappers |
| `rpt.prefer_inplace` | Prefer eligible local Hash filters over exchanges on the same probe key |
| `anser.inplace_with_consumer` | Allow in-place filters alongside existing Anser consumers |
| `rpt.key_survival` | Use retained distinct-key estimates and choose the strongest equality on multi-key forward edges |
| `rpt.extended_safe_quals` | Evaluate additional built-in comparisons in the reducer |

With `rpt.key_survival` enabled, tune `rpt.cost_scan_row`,
`rpt.cost_scan_byte`, `rpt.cost_build_row`, `rpt.cost_probe_row`, and
`rpt.cost_downstream_row` in milliseconds. The defaults are the existing
model coefficients. Save the session settings and exact query results
alongside each benchmark run.
