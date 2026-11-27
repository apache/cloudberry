# RPT+ for Apache Cloudberry

RPT+ (Robust Predicate Transfer with Dynamic Execution) reduces the work of
multi-table joins by transferring runtime filters between related tables.
A selective predicate on one table can reduce another table before their
join executes, and that reduction can propagate through the join graph.
The goal is to make query performance less sensitive to join order and
inaccurate cardinality estimates.

A Bloom filter summarizes the join keys that survive filtering. A receiving
scan can discard keys that are definitely absent, while the join still
checks all remaining rows. Bloom-filter false positives can add work but do
not remove matching rows. RPT+ chooses transfer schedules and controls their
overhead; building and exchanging filters is useful only when the saved
query work exceeds that cost.

## References and attribution

- **RPT+ paper:** Yiming Qiao, Peter Boncz and Huanchen Zhang,
  [*Robust Predicate Transfer with Dynamic Execution*](https://doi.org/10.14778/3797919.3797934),
  PVLDB 19(6):1278–1290, 2026.
- **RPT+ artifact:**
  [embryo-labs/dynamic-predicate-transfer](https://github.com/embryo-labs/dynamic-predicate-transfer).
  The transfer-graph code in this extension is adapted from this artifact.
- **Earlier RPT implementation:**
  [robust-sql/robust](https://github.com/robust-sql/robust), the baseline
  implementation extended by the RPT+ work.
- **Anser paper:** Liang Lin et al.,
  [*Anser: Adaptive Information Sharing Framework of AnalyticDB*](https://doi.org/10.14778/3611540.3611553),
  PVLDB 16(12):3636–3648, 2023.

The adapted artifact code is MIT-licensed, Copyright 2018–2025 Stichting
DuckDB Foundation. See [NOTICE](../../NOTICE), [LICENSE](../../LICENSE) and
[the artifact license](../../licenses/LICENSE-mit-duckdb-rpt.txt).

## Building and enabling

Configure Cloudberry and install both extensions on every host.

```sh
./configure --with-anser --with-rpt
make -C gpcontrib/anser install
make -C gpcontrib/robust_predicate_transfer install
```

On the coordinator and segments, preload Anser before RPT so its helpers
are available when RPT loads. Restart the cluster after changing preload
settings or replacing either library.

```conf
shared_preload_libraries = 'anser,robust_predicate_transfer'
anser.enable = on
```

Enable RPT for the session and disable Anser's own filter placement.

```sql
SET anser.runtime_filter = off;
SET rpt.enable = on;
```

To use the optional `rpt_transfer_plan(query text)` diagnostic, run
`CREATE EXTENSION IF NOT EXISTS robust_predicate_transfer;` in the target
database. It reports the transfer decisions without executing the query.
Preloading and the session settings above are sufficient for runtime filtering.

RPT uses Anser's adaptive Bloom probing for exchanged filters and for
in-process reducer hops. A consumer can skip an ineffective filter and
sample again later. RPT also uses [Anser's in-place filters](#build-filters-in-place-when-the-probe-shares-the-join-slice)
for eligible hash joins.

## How it works on an example

| Optimizer | RPT off | Forward only | Backward only | Full RPT |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 783.442 ms | 694.808 ms | 786.770 ms | 471.149 ms |
| GPORCA | 2,604.943 ms | 876.686 ms | 2,044.376 ms | 816.806 ms |

These are single `EXPLAIN ANALYZE` runs on a three-segment development
cluster. Full RPT took about **40% less time** under PostgreSQL and **69%
less time** under GPORCA than the baseline. All eight runs returned the
same answer. The forward and backward columns isolate each pass. In this
GPORCA run, the full plan lets the forward pass shrink the inputs to the
first join and the backward pass prune wide event rows before redistribution.

### The bridge query

The [collector](scripts/collect_bridge_example.sql) creates four tables:

| Table | Rows | Distribution | Role |
| --- | ---: | --- | --- |
| `campaign c` | 1,000 | Random | `cohort = 0` keeps 50 campaigns |
| `campaign_route b` | 10,000 | Random | Bridges campaign IDs to route IDs |
| `sale s` | 2,000,000 | By `account_id` | Narrow fact table, joined to the bridge by `route_id` |
| `event e` | 1,000,000 | Random | Second fact table, with a 256-character payload |

```sql
SELECT count(*) AS matches, sum(s.cents) AS cents, sum(length(e.payload)) AS payload_chars
FROM rpt_readme_bridge.event e
JOIN (
    rpt_readme_bridge.sale s
    JOIN (
        rpt_readme_bridge.campaign_route b
        JOIN rpt_readme_bridge.campaign c ON c.id = b.campaign_id
    ) ON b.id = s.route_id
) ON e.account_id = s.account_id
WHERE c.cohort = 0;
```

The payload-length aggregate keeps the wide event column in the plan.
This helps explain why the hash join can build on the narrow sale side
even though it has more rows.

In the captured GPORCA baseline, the first hash join joins the two large
facts. It redistributes events by account ID to match sales, then joins
the result to the broadcast bridge and finally to the filtered campaigns.
The excerpts retain node costs, estimated rows, actual times, and actual
rows. The first `rows` field is the estimate. The one under `actual time`
is the measured count reported for that node. Distributed node counts can
describe individual workers and need not equal cluster-wide totals.

```text
Finalize Aggregate  (cost=0.00..19716.56 rows=1 width=24) (actual time=2604.730..2604.733 rows=1 loops=1)
  ->  Gather Motion 3:1  (slice1; segments: 3)  (cost=0.00..19716.56 rows=1 width=24) (actual time=2594.990..2604.723 rows=3 loops=1)
        ->  Partial Aggregate  (cost=0.00..19716.56 rows=1 width=24) (actual time=2594.249..2594.253 rows=1 loops=1)
              ->  Hash Join  (cost=0.00..19663.51 rows=298988 width=264) (actual time=151.874..2454.226 rows=356000 loops=1)
                    Hash Cond: (b.campaign_id = c.id)
                    ->  Hash Join  (cost=0.00..16739.42 rows=6577801 width=268) (actual time=157.514..2037.946 rows=6772000 loops=1)
                          Hash Cond: (s.route_id = b.id)
                          ->  Hash Join  (cost=0.00..7917.27 rows=6577801 width=268) (actual time=156.153..1251.186 rows=6772000 loops=1)
                                Hash Cond: (e.account_id = s.account_id)
                                ->  Redistribute Motion 3:3  (slice2; segments: 3)  (cost=0.00..924.75 rows=333334 width=264) (actual time=0.010..283.305 rows=334620 loops=1)
                                      ->  Seq Scan on rpt_readme_bridge.event e  (cost=0.00..485.63 rows=333334 width=264) (actual time=0.010..32.359 rows=333584 loops=1)
                                ->  Hash  (cost=447.87..447.87 rows=666667 width=12) (actual time=156.037..156.038 rows=677200 loops=1)
                                      ->  Seq Scan on rpt_readme_bridge.sale s  (cost=0.00..447.87 rows=666667 width=12) (actual time=0.016..116.656 rows=677200 loops=1)
                          ->  Hash  (cost=432.56..432.56 rows=10000 width=8) (actual time=1.197..1.197 rows=10000 loops=1)
                                ->  Broadcast Motion 3:3  (slice3; segments: 3)  (cost=0.00..432.56 rows=10000 width=8) (actual time=0.010..0.507 rows=10000 loops=1)
                                      ->  Seq Scan on rpt_readme_bridge.campaign_route b  (cost=0.00..431.08 rows=3334 width=8) (actual time=0.021..0.190 rows=3370 loops=1)
                    ->  Hash  (cost=431.02..431.02 rows=50 width=4) (actual time=1.185..1.186 rows=50 loops=1)
                          ->  Broadcast Motion 3:3  (slice4; segments: 3)  (cost=0.00..431.02 rows=50 width=4) (actual time=1.160..1.166 rows=50 loops=1)
                                ->  Seq Scan on rpt_readme_bridge.campaign c  (cost=0.00..431.02 rows=17 width=4) (actual time=0.013..0.027 rows=22 loops=1)
                                      Filter: (c.cohort = 0)
```

### The GPORCA plan with full RPT

RPT adds a reducer InitPlan and wraps the two fact scans. The main plan
keeps the three hash joins and their Motions.

```text
Finalize Aggregate  (cost=0.00..19716.56 rows=1 width=24) (actual time=815.278..815.286 rows=1 loops=1)
  InitPlan 1  (slice5)
    ->  Gather Motion 3:1  (slice6; segments: 3)  (cost=0.00..0.00 rows=0 width=0) (actual time=0.780..0.780 rows=0 loops=1)
          ->  Sequence  (cost=0.00..0.00 rows=0 width=0) (actual time=0.389..0.391 rows=0 loops=1)
                ->  Custom Scan (Anser Bloom Producer)  (cost=0.00..431.02 rows=17 width=4) (actual time=0.007..0.047 rows=22 loops=1)
                      Bloom Filter Size: 8192 bytes
                      ->  Seq Scan on rpt_readme_bridge.campaign c  (cost=0.00..431.02 rows=17 width=4) (actual time=0.007..0.020 rows=22 loops=1)
                            Filter: (c.cohort = 0)
                ->  Custom Scan (Anser Bloom Producer)  (cost=0.00..431.08 rows=3334 width=8) (actual time=0.186..0.342 rows=172 loops=1)
                      Bloom Filter Size: 8192 bytes
                      ->  Custom Scan (Anser Bloom Consumer)  (cost=0.00..431.08 rows=3334 width=8) (actual time=0.186..0.306 rows=172 loops=1)
                            Bloom Filter Size: 8192 bytes
                            ->  Seq Scan on rpt_readme_bridge.campaign_route b  (cost=0.00..431.08 rows=3334 width=8) (actual time=0.009..0.122 rows=172 loops=1)
                                  Rows Removed by Pushdown Runtime Filter: 3198
                ->  Result  (cost=0.00..0.00 rows=0 width=0) (actual time=0.000..0.000 rows=0 loops=1)
                      One-Time Filter: false
  ->  Gather Motion 3:1  (slice1; segments: 3)  (cost=0.00..19716.56 rows=1 width=24) (actual time=785.860..815.273 rows=3 loops=1)
        ->  Partial Aggregate  (cost=0.00..19716.56 rows=1 width=24) (actual time=814.766..814.770 rows=1 loops=1)
              ->  Hash Join  (cost=0.00..19663.51 rows=298988 width=264) (actual time=95.136..686.836 rows=356000 loops=1)
                    Hash Cond: (b.campaign_id = c.id)
                    ->  Hash Join  (cost=0.00..16739.42 rows=6577801 width=268) (actual time=93.902..484.949 rows=356000 loops=1)
                          Hash Cond: (s.route_id = b.id)
                          ->  Hash Join  (cost=0.00..7917.27 rows=6577801 width=268) (actual time=92.321..188.438 rows=356000 loops=1)
                                Hash Cond: (e.account_id = s.account_id)
                                ->  Redistribute Motion 3:3  (slice2; segments: 3)  (cost=0.00..924.75 rows=333334 width=264) (actual time=3.834..4.290 rows=1780 loops=1)
                                      ->  Custom Scan (Anser Bloom Consumer)  (cost=0.00..485.63 rows=333334 width=264) (actual time=93.664..112.890 rows=1728 loops=1)
                                            Bloom Filter Size: 65536 bytes
                                            ->  Seq Scan on rpt_readme_bridge.event e  (cost=0.00..485.63 rows=333334 width=264) (actual time=0.028..18.925 rows=1728 loops=1)
                                                  Rows Removed by Pushdown Runtime Filter: 331856
                                ->  Hash  (cost=447.87..447.87 rows=666667 width=12) (actual time=88.306..88.308 rows=35600 loops=1)
                                      ->  Custom Scan (Anser Bloom Producer)  (cost=0.00..447.87 rows=666667 width=12) (actual time=0.063..85.899 rows=35600 loops=1)
                                            Bloom Filter Size: 65536 bytes
                                            ->  Custom Scan (Anser Bloom Consumer)  (cost=0.00..447.87 rows=666667 width=12) (actual time=0.062..83.479 rows=35600 loops=1)
                                                  Bloom Filter Size: 8192 bytes
                                                  ->  Seq Scan on rpt_readme_bridge.sale s  (cost=0.00..447.87 rows=666667 width=12) (actual time=0.013..81.757 rows=35600 loops=1)
                                                        Rows Removed by Pushdown Runtime Filter: 638000
                          ->  Hash  (cost=432.56..432.56 rows=10000 width=8) (actual time=1.475..1.475 rows=10000 loops=1)
                                ->  Broadcast Motion 3:3  (slice3; segments: 3)  (cost=0.00..432.56 rows=10000 width=8) (actual time=0.016..0.694 rows=10000 loops=1)
                                      ->  Seq Scan on rpt_readme_bridge.campaign_route b  (cost=0.00..431.08 rows=3334 width=8) (actual time=0.013..0.189 rows=3370 loops=1)
                    ->  Hash  (cost=431.02..431.02 rows=50 width=4) (actual time=1.166..1.166 rows=50 loops=1)
                          ->  Broadcast Motion 3:3  (slice4; segments: 3)  (cost=0.00..431.02 rows=50 width=4) (actual time=0.229..1.158 rows=50 loops=1)
                                ->  Seq Scan on rpt_readme_bridge.campaign c  (cost=0.00..431.02 rows=17 width=4) (actual time=0.007..0.016 rows=22 loops=1)
                                      Filter: (c.cohort = 0)
```

### Build key classes and classify scans

RPT walks the finished GPORCA plan and records the scan occurrences and
supported equijoins. The diagnostic uses the plan's range-table indexes
(`rtindex`), which identify scan occurrences. The row estimates below are
the scan's estimated output after its own restrictions. [Graph collection](src/rptregion.cpp)

| Vertex | Scan | Estimated rows per segment | Classification |
| --- | --- | ---: | --- |
| `1` | `event e` | 333,334 | Unfiltered |
| `2` | `sale s` | 666,667 | Intermediate |
| `3` | `campaign_route b` | 3,334 | Intermediate |
| `4` | `campaign c` | 17 | Filtered by `cohort = 0` |

#### How union-find builds the key classes

Each join key starts in its own set. RPT identifies a key as
`(rtindex, attno)`, so `3.1` is `b.id` and `3.2` is `b.campaign_id`.
For each supported equality on an unprotected join edge, RPT finds the
representative of each endpoint by following parent pointers. If the
representatives differ, it makes the second representative a child of
the first and combines their member sets. If they already match, the edge
adds nothing. Each lookup also rewrites the visited pointers to point
directly to the representative, which makes later lookups shorter.
[Union and path compression](src/rptgraph.cpp)

The captured plan yields these three unions. The representative in the
last column is the class ID printed by the diagnostic.

| Equality | Keys united | Representative |
| --- | --- | --- |
| `e.account_id = s.account_id` | `1.1`, `2.1` | `1.1` |
| `s.route_id = b.id` | `2.2`, `3.1` | `2.2` |
| `b.campaign_id = c.id` | `3.2`, `4.1` | `3.2` |

Every equality here uses a different pair of columns, so all three sets
stay separate. If another join equated `x.account_id` with
`s.account_id`, finding the representative of `s.account_id` would bring
the new key into the existing account class, which already contains
`e.account_id`. RPT makes such transitive edges explicit for scheduling.
Protected edges, such as an outer join edge that preserves unmatched
rows, are excluded from the union step. [Class construction](src/rptgraph.cpp)

#### How RPT assigns scan categories

RPT classifies each scan occurrence in this order:

1. **Filtered.** The finished scan has a restriction qual. For index and
   bitmap scans, RPT also checks the corresponding scan conditions.
   `campaign` qualifies because its scan has `cohort = 0`, and the
   restriction takes precedence over the class count.
2. **Intermediate.** With no scan restriction, RPT finds the class
   representative of each join key and counts distinct representatives.
   A key outside any union counts as its own class. `sale` touches account
   class `1.1` and route class `2.2`. The bridge touches route class
   `2.2` and campaign class `3.2`. Each therefore has two classes.
3. **Unfiltered.** With no restriction and at most one distinct class,
   the scan falls into this category. `event` has only the account class
   `1.1`.

These categories come from the plan's scan conditions and key classes.
Estimated row counts are used later to choose a root and assess filter
cost and benefit. The bridge connects its two classes through **rows**:
a row whose `campaign_id` passes the campaign filter contributes its
own `id` to the route filter. [Restriction detection](src/rptregion.cpp) · [Scan classification](src/rptgraph.cpp)

### Choose the root and reducer order

The runtime schedule chooses `sale` as root `2` because it has the largest
estimated scan output. RPT orients the forward path from the filtered
`campaign` scan toward that root. The unfiltered `event` leaf supplies no
forward producer. It can still receive a filter in the backward pass.
[Forward schedule](src/rptplan.cpp)

```text
campaign (4) → campaign_route (3) → sale (2)
reducer order: [4 3]
```

The reducer scans `campaign` first so its filter is ready when
`campaign_route` starts. `sale` receives the bridge's filter on its original
main-plan scan. This path has a dependency between its two reducer scans.

### Select the transfers

RPT estimates the fraction each scan keeps from its own safe restrictions
and incoming filters. It propagates that estimate along the chain, using
distinct join-key counts where available. Here `cohort = 0` starts the
reduction at roughly 5%, and the GPORCA diagnostic estimates that the
route filter will retain **5.1%** of sale rows. [Forward selection](src/rptforwardplan.cpp)

Both hops are global and use 8 KiB Bloom filters. With three producer
parts and three deliveries per hop, the estimated exchange cost is
`(3 × 3.5 + 3 × 0.8) × 8 / 1024 ≈ 0.101 ms` per hop, or **0.202 ms**
combined. This is the exchange term in the
[forward cost model](#estimate-whether-forward-reduction-pays-off).
The collector leaves
the `0.35` selectivity threshold in place and disables the exchange-cost
and minimum-root-size gates for the experiment. [Collector settings](scripts/collect_bridge_example.sql)

For backward transfer, RPT checks whether the physical plan allows the
consumer to wait safely. The eligible pair is `sale → event` on
`account_id`. GPORCA places `sale` on the build side in slice 1 and the
event scan on the probe path in slice 2. The hash join builds first, so
the diagnostic reports `defer_first=no`. `sale → campaign_route` fails
the qualifying build-side check, and the bridge-led class for
`campaign_route → campaign` is deferred. Backward sizing uses the estimate
after forward reduction and selects a **64 KiB** filter. [Placement proof](src/rpteligible.cpp) · [Backward selection](src/rptbackwardplan.cpp)

### Inject the plan nodes

RPT works on a copy of the finished plan. It copies the two reducer scans
with key-only target lists and retains restrictions that are safe to
evaluate eagerly. The campaign producer reads the copied scan with
`cohort = 0`. The bridge producer wraps the campaign consumer, so it
builds a fresh filter on route IDs from the bridge rows that passed.
[Forward injection](src/rptforward.cpp)

RPT puts the two producer subtrees into a segment-side `Sequence`, adds a
final `Result` with a false one-time qual, and wraps the sequence in a
Gather Motion. `Sequence` drains its preceding children before returning
the final child's output. The empty final result means the `EXISTS`
InitPlan runs to completion without emitting a row. Registration adds
a coordinator slice, a segment slice, and a boolean `PARAM_EXEC` slot for
the InitPlan result. [InitPlan construction](src/rptinject.cpp) · [Sequence executor](../../src/backend/executor/nodeSequence.c)

In the main plan, RPT adds the route consumer above `sale` and puts the
backward account producer above that consumer. The producer therefore
sees the surviving sales. The corresponding event consumer goes below
the `Redistribute Motion` so it can prune rows on their source segments.
[Backward injection](src/rptbackward.cpp)

### Execute the transfers

During executor startup, the coordinator runs the reducer InitPlan before
dispatching the main plan. Every segment scans `campaign` and publishes
its part at end of scan. Anser ORs the parts for that channel and delivers
the result to the bridge consumers. A fast segment can reach the bridge
before another segment finishes `campaign`. That wait has no cycle
because every segment runs the producer first. The bridge then publishes
the route filter, which Anser keeps available for the main-plan dispatch.
[InitPlan dispatch](../../src/backend/cdb/cdbsubplan.c) · [Anser exchange](../anser/src/anserdispatch.c)

In the main plan, the sale scan probes the route filter before the account
producer sees a row. The producer hashes the surviving sale account IDs
and publishes at end of scan. The event consumer receives the merged
account filter and prunes events before redistribution. Each exchange
builds a fresh filter for its own key. If a filter cannot be delivered,
its consumer passes rows through and the joins still enforce the SQL
conditions. [Producer and consumer callbacks](../anser/src/anserplanexec.c)

The GPORCA plan reports **3,198** bridge rows, **638,000** sale rows, and
**331,856** event rows removed by pushdown runtime filters. These are
distributed node counters. In the backward-only run, the event filter
used **1 MiB** and the event scan reported **299,941** rows removed.
The full run used **64 KiB** and pruned more rows because the producer
built the filter after forward reduction. All eight runs returned
**1,000,000 matches**, **40,000,000 cents**, and **256,000,000 payload
characters**.

## How Cloudberry differs from the original RPT+

Cloudberry keeps the paper's use of join-key equivalence classes and
forward and backward Bloom transfers. The differences below come from
executing those transfers across segments and reusing an existing SQL plan.

### Exchange filters through Anser

When matching keys can reside on different segments, a filter must include
the keys from all producers before it can safely reject a row. Producers
send Bloom filter parts over the existing dispatch connections. The
coordinator merges the parts and delivers the completed filter to the
consumers. Intermediate forward transfers can stay in process when the
join keys are co-distributed and the reducer evaluates both sides in that
process. [Anser](../anser/README.md)

### Run the forward reducer as an InitPlan

The paper materializes reduced tuples and reuses them for later filtering
and joins (Figure 7 and Algorithm 2). Cloudberry runs the forward reducer
once as a separate InitPlan of the same statement, under the same
snapshot and before the main plan starts. The reducer publishes filters.
The main plan reads its own inputs, so the producer tables scanned by the
reducer can be read again. The reducer stops before scanning the root
table itself. This arrangement lets the forward chain run in reducer order
without introducing waits into the existing join order.

The backward phase runs in the main plan. The root scan applies its
forward filters before building the backward filter from the surviving
keys. Eligible class members can probe that filter as they scan. Backward
placement depends on the hash join and slice order proving that the
producer can make progress while the consumer waits. Bridge-led backward
chains are deferred. When full RPT installs no exchanges, it can fall back
to ordinary Anser placement. [Paper](https://doi.org/10.14778/3797919.3797934)

### Build filters in place when the probe shares the join slice

Cloudberry divides a distributed plan into slices at Motion boundaries.
An exchanged filter carries a part from each producer to the coordinator,
which merges the parts and sends the completed filter back to each
consumer. This adds network traffic and can make consumers wait.

When the probe scan runs in the hash join's own slice, each segment's Hash
input already contains the keys that its probe rows can match. Anser builds
the Bloom filter from those rows on the segment. The build input can be a
joined subtree, and it can arrive through a Broadcast or Redistribute
Motion. What matters is that the probe scan can use the filter in the same
slice as the join. The filter needs neither a separate reducer scan nor a
network trip through the coordinator. When a transfer is unsafe and an
in-place filter is unavailable, RPT leaves the scan unfiltered.

With `rpt.prefer_inplace` enabled by default, RPT skips planned
forward and backward deliveries on probe keys covered by an eligible
Anser in-place filter. A forward reducer that no longer feeds a consumer
is removed. In-place filters are built from the actual Hash input after
its joins and predicates, which can make them more selective than a filter
from an extra base-table scan. Enable or disable Anser's in-place policy
with `anser.runtime_filter_inplace`. For A/B tests, set
`rpt.prefer_inplace = off` to retain the exchange candidates for those keys.
If an exchanged consumer remains on a scan, `anser.inplace_with_consumer`
allows the same scan to use an eligible local filter as well.
[Anser in-place filters](../anser/README.md#in-place-filters)

### Estimate whether forward reduction pays off

Network exchange adds work on every global hop. For channel $e$, the
estimated cost in milliseconds is

$$
X_e =
\begin{cases}
0 & \text{for an in-process channel}, \\
M_e (aP + bD_e) & \text{for an exchanged channel}.
\end{cases}
$$

Here $M_e$ is the planned filter size in MiB, $P$ is the number of
producer parts, and $D_e$ is the number of receiving processes. The
exchange estimate sums the costs of the hops. For the bridge example's
two 8 KiB hops, three parts and three deliveries per hop give
$2(8/1024)(3\times3.5 + 3\times0.8) \approx 0.202$ ms.

With `rpt.forward_total_cost` enabled, the model also estimates the
extra scan and CPU work and compares it with the downstream work saved.
For one reducer component, let $V$ be the retained reducer scans,
$E$ the retained channels, and $r$ the root. Then

$$
\begin{aligned}
W &= \sum_{v\in V} R_v(c_s+c_w w_v)
     + \sum_{e=(u\to v)\in E}(c_b R_u k_u + c_p R_v + X_e), \\
B &= c_d R_r\left(1-\prod_{e\to r}p_e\right).
\end{aligned}
$$

- $R_v$ is the estimated row count before reducer filtering, and
  $w_v$ is the plan row width in bytes. The scan term uses row count
  and width as proxies for scan, decoding, and predicate work.
- $k_u$ is the fraction expected to survive the producer's retained SQL
  predicates and incoming filters. $p_e$ is the estimated fraction of
  target rows that pass channel $e$. With positive distinct-key
  estimates $d_u<d_v$, the model uses $p_e=k_u d_u/d_v$. Otherwise
  it uses $p_e=k_u$. Multiplication approximates the combined effect
  of multiple filters.
- $R_r$ in the benefit term is the root's estimated cardinality. The
  benefit credits downstream processing avoided and assigns zero savings
  to root scan I/O.

The planner drops channels expected to reject less than 5% of their target
rows, skips channels whose filters cannot be sized, and removes producer
scans that no longer feed the root. A component must pass the root size,
estimated selectivity, and exchange budget gates that are enabled and
have $B>W$. Since accepted components share one InitPlan, their
combined savings must satisfy

$$
\sum_{i\in\text{accepted components}}(B_i-W_i)>C_{\mathrm{start}}.
$$

The paper's dynamic execution can abandon filter creation and
materialization mid-run. Cloudberry decides whether to schedule the
reducer before execution, using the predicates safe to evaluate during
the extra scan. Once a filter is available, Anser's consumer can stop
probing it if it rejects too few rows. That saves probe work for the
rest of the scan. [Forward cost model](src/rptforwardplan.cpp) · [Anser consumer](../anser/src/anserplanexec.c)

### Coefficients and experiments

The exchange defaults come from measurements on November 10, 2026 on
one host with three primaries. The scan, CPU, and startup coefficients
are provisional. The query experiments below measured end-to-end
execution time and did not isolate these costs. [Exchange defaults](src/rptinit.cpp) · [Cost coefficients](src/rptforwardplan.cpp)

| Term | Default | Units | Meaning |
| --- | ---: | --- | --- |
| $a$ | 3.5 | ms per MiB per part | Producer-side exchange |
| $b$ | 0.8 | ms per MiB per delivery | Delivery to a receiving process |
| $C_{\mathrm{start}}$ | 1.0 | ms | One forward InitPlan startup |
| $c_s$ | 0.00015 | ms per row | Reducer scan and predicate work |
| $c_w$ | 0.000002 | ms per byte | Reducer row-width charge |
| $c_b$ | 0.00010 | ms per row | Bloom construction |
| $c_p$ | 0.00008 | ms per row | Bloom probing |
| $c_d$ | 0.00060 | ms per rejected row | Downstream work avoided |

The [bridge experiment](#how-it-works-on-an-example) measured forward,
backward, and combined transfer against RPT off. In the GPORCA run,
forward alone took 876.686 ms and full RPT took 816.806 ms, compared with
2,604.943 ms with RPT off. Each configuration had one timed run.

A separate TPC-DS SF=1 experiment compared ordinary Anser with the earlier
RPT build (`a1aec3e6c37`). It used the PostgreSQL planner, one host with
three primaries, one warmup, and three measured runs per mode. Anser
matched the baseline on all 99 queries. RPT differed on Q18, Q33, and Q60.
For the 96 queries that matched in both modes, the sum of per-query medians
was **64.434 s for Anser and 70.690 s for RPT**, a 9.7% increase.

| Query | Anser median (ms) | RPT median (ms) | Observation from the saved plans |
| --- | ---: | ---: | --- |
| Q95 | 667 | 1,360 | RPT missed three ordinary Anser exchanges. |
| Q85 | 333 | 741 | RPT added five reducer scans, including a large demographics scan. |
| Q72 | 1,173 | 1,721 | RPT also paid for the demographics scan here. |
| Q25 | 235 | 568 | RPT added an InitPlan and had one fewer in-place filter. |
| Q32 | 1,261 | 1,848 | The reducer omitted the date predicate, and the plan had one fewer in-place filter. |
| Q71 | 790 | 223 | RPT reduced sales branches below an Append that the earlier Anser walker skipped. |
| Q46 | 776 | 245 | Small, selective reducer inputs paid off. |

The wrong-answer plans motivated moving narrow-key AOCO Bloom checks
after projection, so the key is available for normalization. The timings
above predate that fix, the total-cost model, ordinary Anser fallback,
and the Anser Append/MergeAppend traversal fix. The effect of those
changes needs a fresh comparison with the updated Anser baseline.

## Planning workflow and call stack

RPT transforms a finished distributed `PlannedStmt` on the coordinator.
PostgreSQL or ORCA chooses the joins, scans and Motions first. RPT then
adds filter nodes and, when forward transfer is selected, a reducer InitPlan.
It does not replace the optimizer or execute queries during planning.

With `anser,robust_predicate_transfer` preloaded in that order, the planning
call sequence is:

```text
pg_plan_query()
  planner()
    rpt_planner()                              [RPT planner hook]
      anser_planner()                          [previous hook]
        standard_planner()                     [PostgreSQL planner or ORCA]
        AnserApplyRuntimeFilters()             [inactive with runtime_filter=off]
      rpt_log_analysis()                      [only with rpt.enable + rpt.debug]
      rpt_try_injection()                     [eligible coordinator SELECT]
        rpt_build_injection()                 [C++ exception boundary]
          RptInject(stmt, materialize=true)
    return final PlannedStmt
```

The hook injects only when `rpt.enable` is on and at least one of
`rpt.forward` or `rpt.backward` is on. It excludes SELECT INTO, COPY INTO and
refresh plans. `RptInject()` also requires `anser.enable=on`,
`anser.runtime_filter=off`, and a plan without existing Anser or RPT filter
nodes. See [rptinit.cpp](src/rptinit.cpp) and
[Anser's hooks](../anser/src/anserinit.c).

`rpt_try_injection()` creates a memory context for construction.
`RptInject()` copies the statement and edits that copy. If construction
fails, the hook discards the context, reports a warning and returns the
original plan. Query cancellation propagates. `RptPgCall()` translates
PostgreSQL errors into C++ exceptions while STL owners unwind; the entry
boundary then returns the error to PostgreSQL's handler.

### Inside the shared injector

[`RptInject()`](src/rptforward.cpp) runs the following sequence once for
runtime injection. Forward and backward use the same largest-root forest
and the same scan pointers into the copied statement.

```text
RptInject()
  check Anser settings and existing filter nodes
  copy PlannedStmt
  RptCollectGraph()                            → analysis: RptTransferMgr
  RptBuildClasses(analysis)
  RptBuildForwardPlan(analysis)                 → runtime schedule: RptTransferMgr
  RptCheckBackwardConsumers(stmt, schedule)     → backward_consumers proofs
  rpt_find_parents()                           → original parents and scan slices
  initialize RptInjectionState                 → IDs and current scan-stack tops
  collect process counts and row/selectivity estimates

  RptInjectForward()                           [when rpt.forward=on]
    RptSelectForward()                         → RptForwardComponent[]
    preflight scans and key output positions
    rpt_assign_forward_channels()              → IDs from shared state
    build reducer scans and producer/consumer wrappers
    rpt_apply_in_main()                        → forward consumers in main plan
    return reducer children and selected components

  RptInjectBackward()                          [when rpt.backward=on]
    AnserSliceProducers()                      → actual slice process counts
    RptSelectBackward()                        → RptBackwardClass[]
    preflight each accepted class against original parents/current stack tops
    assign channel ID from shared state
    AnserBuildBloomProducerScan()              → wrap root's current top
    AnserBuildBloomConsumerScan()              → wrap each accepted member's top
    RptRelinkStack()                           → update original parent and top map

  RptAttachReducerInitPlan()                   [only if reducer children exist]
  return copied statement if anything was injected, otherwise original
```

`RptInjectForward()` is an internal function in
[rptforward.cpp](src/rptforward.cpp). Backward placement lives in
[rptbackward.cpp](src/rptbackward.cpp); its selector is in
[rptbackwardplan.cpp](src/rptbackwardplan.cpp). A dry run follows selection
and preflight without constructing executable nodes.

### Graphs, schedules and placement state

The analysis structures are declared in [rpt.h](include/rpt.h):

| Structure | Produced by | Used for |
| --- | --- | --- |
| `RptVertexInfo` | `RptCollectGraph()` | One base scan, relation OID, row estimate, restriction/distribution properties and join keys; `scan` points into the working statement |
| `RptKey` | Graph collection | Physical column identity `{rtindex, attno}` |
| `RptEdgeInfo` | Graph collection and class building | Equality clauses between two vertices and transfer directions prohibited by join semantics |
| `RptKeyGroup` | `RptBuildClasses()` | An equivalence class, its members and selected leader/key |
| `RptGraphNode` / `RptGraphEdge` | `RptBuildForwardPlan()` | Per-vertex incoming/outgoing transfers for both passes and their local/global classification |
| `RptBackwardConsumer` | `RptCheckBackwardConsumers()` | Per-edge proof or refusal, original parents, output key positions, slice indexes, `defer_first` and estimated benefit |
| `RptForwardComponent` / `RptForwardChannel` | `RptSelectForward()` | One root's ordered reducer chain, channels, sizing, estimates and acceptance decision |
| `RptBackwardClass` / `RptBackwardTarget` | `RptSelectBackward()` | One root producer and its selected/refused members, sizing and exchange-cost decision |
| `RptTransferMgr` | Collection, class building and scheduling | Owns the temporary graph, classes, forest and eligibility decisions |

`RptBuildClasses()` forms unprotected key equivalence classes and makes
implied equality edges explicit. It retains those logical edges in
`logical_neighbor_matrix`, then applies `SkipUnfilteredTable` to the
forward view, `neighbor_matrix`. Forward-only skipping therefore cannot
erase a useful backward edge from an unfiltered root.

Despite its name, `RptBuildForwardPlan()` constructs the shared runtime
forest for **both** passes. It selects the largest scan by estimated
cardinality as each component's root. Frontier selection considers both
graph views; forward parents use the pruned forward edges, and backward
parents use the logical edges. Original join-direction protections remain
in force. `graph`, `root_rtindexes`, `bridge_rtindexes`,
`transfer_order_rtindexes` and `reducer_order_rtindexes` hold the result.
See [rptgraph.cpp](src/rptgraph.cpp) and [rptplan.cpp](src/rptplan.cpp).

Executable endpoints must be plain SeqScans exposing the selected key in
their output. Join keys must pass the supported built-in, canonical by-value
type/equality checks used for raw-Datum Bloom filtering.

The backward proof runs before wrapping any scan. It permits a blocking
consumer only on a supported hash probe path whose producer is in the
build subtree and in a different slice. It checks intervening blocking
nodes, outer-join paths, scans inside SubPlans and external producer
parameters, then
applies the per-class consumer cap. Local edges use core runtime filtering;
bridge-led classes are deferred. See [rpteligible.cpp](src/rpteligible.cpp).

[`RptInjectionState`](include/rptinject.h) holds the condition/node counters
and two maps keyed by the original scan pointer: `parents` records the
original relink point, and `tops` records the outermost wrapper so far.
Forward consumers are added first. Backward producers and consumers wrap
these current tops, preserving forward filters below the backward producer.
Preflight checks that each original parent still holds that top exactly
once and that the key remains available in the scan's output.

These identities have distinct meanings:

| Name | Meaning |
| --- | --- |
| `rtindex` | Relation occurrence in `PlannedStmt.rtable`; self-join aliases have different indexes |
| `relid` | Underlying relation OID; aliases of one table share it |
| `RptKey.attno` | Physical column number in that relation |
| `producer_attno` / `consumer_attno` | Key position in the scan's projected output tuple |
| `class_key` | Representative `RptKey` identifying an equivalence class; distinct from its leader's key |
| `plan_node_id` | Executable plan-node identity |
| `slice_index` | Distributed execution slice |
| `condition_id` | Filter-channel identity, allocated across both runtime passes |

`vertex_index_by_rtindex` maps a relation occurrence to its position in
`vertices`. Relation aliases are obtained from the statement's range table
when dumping, rather than stored in each vertex. The executor receives the
modified `PlannedStmt` and its CustomScan parameters; it does not receive
these C++ analysis containers.
