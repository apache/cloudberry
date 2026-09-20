# vacuum_stats

`vacuum_stats` describes the work done by VACUUM for tables, indexes and
whole databases: how many tuples it removes, what remains to be cleaned,
how it changes page visibility, and how much time it spends. These counters
help evaluate the results and cost of vacuuming over time, alongside the
existing vacuum counts and timestamps.

Collection of extended work counters is enabled with `track_vacuum_statistics = on` in the server
configuration and requires a restart. Set it consistently on the coordinator
and segments. When enabled, the extended vacuum counters and database totals
are stored with ordinary relation and database statistics. When disabled, their storage
is omitted and those extended fields return zero. Restarting with tracking
disabled discards the extended counters while preserving ordinary statistics,
including vacuum times, VM clearings, failsafe and interruption counts.

The `visible_page_marks_cleared` and `frozen_page_marks_cleared` counters describe
visibility-map changes caused by data modifications. They follow `track_counts`
and are collected and retained independently of `track_vacuum_statistics`.

Install with `CREATE EXTENSION vacuum_stats`. All new SQL functions and
views belong to the extension's schema. Existing system views and built-in
function OIDs remain unchanged. VM clearings, maintenance times, failsafe and
interruption counts follow `track_counts` independently of
`track_vacuum_statistics`; the extension reads their ordinary collector entries.

Cost-based delay timing additionally requires `track_cost_delay_timing = on`.
It is disabled by default and can be changed for a session without restarting.
ANALYZE elapsed times are recorded separately from VACUUM times. ANALYZE
sampling delays do not contribute to the VACUUM delay counters.

The counters accumulate until reset, except `total_file_segs`, which records
the state observed by the last completed AO VACUUM. To examine a particular period, compare
two readings without an intervening reset or change in tracking configuration.

## What the counters measure

| Counter | Meaning |
|---------|---------|
| `tuples_deleted` | Table tuples or index entries removed by VACUUM. |
| `dead_tuples` | Heap tuples found dead but not yet removable; for AO tables, hidden tuples remaining after vacuum. |
| `pages_deleted` | Pages truncated from a heap, newly deleted index pages, or space freed from AO segment files expressed in blocks. |
| `bytes_removed` | Bytes physically truncated from heap or AO table files, including AO tails left by aborted inserts. Index page reuse does not increase this counter. |
| `dead_pages` | Heap pages containing unremovable dead tuples; for indexes, deleted pages not yet available for reuse. |
| `pages_frozen` | Heap pages on which VACUUM froze at least one tuple. |
| `pages_all_visible` | Heap pages VACUUM marked all-visible. |
| `visible_page_marks_cleared` | Clearings of the all-visible flag, usually caused by data changes. |
| `frozen_page_marks_cleared` | Clearings of the all-frozen flag, usually caused by data changes. |
| `freeze_age_vacuum_count` | Heap vacuum runs made aggressive by transaction or multixact freeze age. This includes `VACUUM FREEZE`; forcing page scanning alone does not increment it. |
| `vacuum_failsafe_count` | Completed heap vacuum runs that entered failsafe mode to avoid transaction or multixact wraparound. Aggressive scanning alone does not increment it. |
| `total_vacuum_time`, `total_autovacuum_time` | Elapsed time in milliseconds, separately for manual VACUUM and autovacuum. |
| `total_vacuum_delay_time`, `total_autovacuum_delay_time` | Cost-based delays in milliseconds, included in the corresponding elapsed time. |
| `total_analyze_time`, `total_autoanalyze_time` | Table ANALYZE time in milliseconds, separately for manual and automatic runs. |
| `vacuum_interrupt_count` | Database count of heap vacuums interrupted by an ERROR, including cancellation, while the vacuum error callback is installed. |

`total_file_segs` in the table views records the number of AO segment metadata
entries observed after the last VACUUM, including empty and awaiting-drop
segments. For AO column tables it counts logical segments, not each column
file. A subsequent VACUUM replaces this value instead of adding to it. It is
zero before the first report, after reset, and for heap tables. Use it with
`bytes_removed` and remaining hidden tuples to understand compaction results.

The other fields are cumulative counters, not a snapshot of the table's current
contents. In particular, successive runs can count the same unremovable
tuple or page again. Visibility flags can also be set and cleared repeatedly.
A counter difference measures work or observations during the interval,
not necessarily a number of distinct tuples or pages.

## How to use them

- **Find expensive relations.** Compare increases in `total_vacuum_time` and
  `total_autovacuum_time` between tables and indexes over the same interval. Relate that time to tuples
  removed and pages reclaimed to see where maintenance time is spent.
  Low tuple removal alone does not imply wasted work: vacuum also freezes
  tuples and maintains visibility information.
- **Find work that cannot finish.** Repeated increases in heap `dead_tuples`
  and `dead_pages` show that vacuum keeps encountering data it cannot remove.
  Check for old snapshots or long-running transactions before increasing
  vacuum frequency.
- **Separate throttling from other costs.** Compare `total_vacuum_delay_time` with
  `total_vacuum_time`, and the corresponding autovacuum counters. A large share
  spent in cost-based delays helps explain a long run. The remaining time includes execution and other waits; it is
  not a measurement of CPU time.
- **Understand visibility and freezing work.** Compare `pages_all_visible`
  with `visible_page_marks_cleared` to see how often data changes undo visibility
  work. Use `pages_frozen`, `frozen_page_marks_cleared` and
  `freeze_age_vacuum_count` to understand freezing activity and aggressive
  scans. Zero frozen pages can be normal when no tuples need freezing.
- **Compare segments.** Differences in work and time for the same relation
  can help identify uneven data distribution or different execution costs.
  Compare both quantities: a slower segment is not necessarily processing
  more data. Summed segment time represents accumulated work, not the
  wall-clock duration of a distributed VACUUM.

## Tables, indexes and AO

A table's vacuum time includes its index maintenance. Database totals
include table work without adding the index counters again. Use the index
figures to understand that part of the cost, rather than adding them to
table totals. Deleted index pages can become reusable within the index;
they do not necessarily represent space returned to the filesystem.

For AO row and column tables, `tuples_deleted` measures rows discarded by
compaction, and `dead_tuples` counts hidden rows left afterwards. Hidden
rows can remain when compaction is disabled or a segment file is not
eligible for compaction. AO index cleanup can remove entries for relocated
live rows as well as deleted rows, so its tuple count can exceed the table's.
AO `pages_deleted` expresses truncated bytes in blocks, rounded up;
`bytes_removed` preserves the exact byte count. Freed space can include live
rows moved to new segment files, so this is not the net reduction in table size.
AO elapsed time covers the interval from the first phase seen by the
reporting worker to final cleanup, including gaps between those phases.

Heap page visibility, freezing and failsafe counters do not apply to the AO table
itself. Its auxiliary heap relations have their own statistics. Indexes
have tuple-removal, page-deletion and timing counters, but no heap visibility
or freezing work.

The `pg_stat_vacuum_tables`, `pg_stat_vacuum_indexes` and
`pg_stat_vacuum_database` views expose local statistics. Their
`gp_stat_vacuum_*` counterparts include the coordinator and segments,
identified by `gp_segment_id`. In utility mode there is no dispatch: these
views return the connected node's local statistics once, with its segment ID.

The database views include `datid = 0`, with a null `datname`, for shared
relations. `vacuum_interrupt_count` does not include VACUUM FULL, failures
before the heap callback is installed, or AO parent compaction. Auxiliary
heap vacuums are counted independently. An interrupted run does not report
its usual completion counters, so check errors when successful-run totals
alone do not explain maintenance activity.

The on-disk statistics format changes, so older saved statistics are discarded
on first start. The system catalog version is unchanged; these statistics do
not require a new cluster or replacement of system views.
