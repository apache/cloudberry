# ext_vacuum_statistics

Extended vacuum statistics extension for PostgreSQL. It collects and exposes detailed per-table, per-index, and per-database vacuum statistics (buffer I/O, WAL, general, timing) via convenient views in the `ext_vacuum_statistics` schema.

## Installation

```
./configure tmp_install="$(pwd)/my/inst"
make clean && make && make install
cd contrib/ext_vacuum_statistics
make && make install
```

It is essential that the extension is listed in `shared_preload_libraries` because it registers a vacuum hook at server startup.

In your `postgresql.conf`:

```
shared_preload_libraries = 'ext_vacuum_statistics'
```

Restart PostgreSQL.

In your database:

```sql
CREATE EXTENSION ext_vacuum_statistics;
```

## Usage

Query vacuum statistics via the provided views:

```sql
-- Per-table heap and append-optimized vacuum statistics
SELECT * FROM ext_vacuum_statistics.pg_stats_vacuum_tables;

-- Per-index vacuum statistics
SELECT * FROM ext_vacuum_statistics.pg_stats_vacuum_indexes;

-- Per-database aggregate vacuum statistics
SELECT * FROM ext_vacuum_statistics.pg_stats_vacuum_database;
```

Example output:

```
 relname   | total_blks_read | total_blks_hit | wal_records | tuples_deleted | pages_removed
-----------+-----------------+----------------+-------------+----------------+---------------
 mytable   |             120 |            340 |          15 |            500 |            10
```

Reset statistics only on the current node:

```sql
SELECT ext_vacuum_statistics.vacuum_statistics_reset();
```

The reset functions act only on the current node. Calling one on the
coordinator (QD) leaves segment counters unchanged. This also applies to
`extvac_reset_entry()` and `extvac_reset_db_entry()`.

## Configuration (GUCs)

| GUC | Default | Description |
|-----|---------|-------------|
| `vacuum_statistics.enabled` | on | Enable extended vacuum statistics collection |

## Memory usage

Each tracked object (table or index) uses a fixed-size shared memory entry; the exact size depends on the platform.

Per-database aggregates add one entry per tracked database. Entry size includes
the AO phase counters and pgstat bookkeeping; memory estimates must use the
structures of the actual build rather than a fixed byte count from an earlier
version.

The entry of a table or an index is dropped when the relation is dropped (at
commit, so a rolled back `DROP` keeps it), and a new relation that gets the OID
of an old one starts from zero.  The module does that with an
`object_access_hook`.

## Recipes

**Disable statistics collection temporarily:**

```sql
SET vacuum_statistics.enabled = off;
```

## Views

| View | Description |
|------|-------------|
| `ext_vacuum_statistics.pg_stats_vacuum_tables` | Per-table heap vacuum stats (pages scanned, tuples deleted, dead tuples, etc.) |
| `ext_vacuum_statistics.pg_stats_vacuum_indexes` | Per-index vacuum stats |
| `ext_vacuum_statistics.pg_stats_vacuum_database` | Per-database aggregate vacuum stats |

## Limitations

- Must be loaded via `shared_preload_libraries`; it cannot be loaded on demand.
- Starting a server without the module, even once, makes it treat the whole
  statistics file as corrupted and reset all cumulative statistics, the
  built-in ones included.  Use `vacuum_statistics.enabled = off` rather than
  removing the module.

## Native work counters

Heap, index and AO work is also collected by the built-in statistics system
without this module. Use `pg_stat_vacuum_tables`, `pg_stat_vacuum_indexes`
and `pg_stat_vacuum` (and their `gp_stat_*` counterparts) for native counters.
They obey `track_counts` and ordinary `pg_stat_reset*` functions. The extension
retains its existing counters for compatibility and adds resource measurements
such as buffers and WAL. Its counters obey `vacuum_statistics.enabled` and
its own reset functions. Extension-specific resets do not clear native counters;
`pg_stat_reset()` also clears the extension's entries for the current database.

## Heap page counters

`pages_frozen` accumulates pages on which vacuum froze at least one tuple.
`pages_all_visible` accumulates pages whose visibility-map all-visible bit
vacuum changed from unset to set, including empty pages. These count work
across vacuums, not the current number of frozen or all-visible pages. A
page can be counted again after later changes require new work; rescanning
an unchanged page or adding only its all-frozen bit does not add to
`pages_all_visible`. Both counters survive clean restarts and are cleared
by statistics resets.

`dead_pages` accumulates heap pages containing tuples that are dead but
not yet removable (for example, because an old snapshot still needs them).
It differs from `missed_dead_pages`, which counts pages with removable tuples
that vacuum could not remove. For indexes, `dead_pages` accumulates deleted
pages not yet reusable, sampled once after cleanup. Repeated observations
are counted again; this is not a snapshot of the current relation.

`freeze_age_vacuum_count` counts heap vacuums made aggressive by the XID or
MultiXact freeze table age, including `VACUUM FREEZE`. Forcing a scan with
`DISABLE_PAGE_SKIPPING` alone does not increment it, and entering failsafe
mode is counted separately. Both new counters survive clean restarts and
are cleared by statistics resets.

## Append-optimized tables

AO row and AOCS tables and their indexes are reported too.  For the table,
`tuples_deleted` is the number of dead tuples the compaction discarded and
`pages_removed` the space released by truncating and dropping segment files,
in heap-equivalent pages. `pages_scanned` counts the data scanned during
compaction in those units, rounded up per compacted segment (source EOF for
AO row, scan bytes read for AOCS). `compacted_segments` and `tuples_moved`
accumulate actual compactions and live rows moved; skipped candidates add
nothing. `total_file_segs` is the segment metadata entry count after the
latest vacuum, including empty and awaiting-drop entries. It is replaced on
each vacuum, not accumulated; AOCS counts segment numbers, not individual
column files. A statistics reset clears these values, and the next vacuum
refreshes the segment count even without compaction. All three AO-specific
fields stay zero for heap tables.

`awaiting_drop_segments` is the number of segment metadata entries still in
`AWAITING_DROP` at the end of the last completed vacuum. An older snapshot
can delay their recycling even after compaction has finished. This value is
replaced, not accumulated; AOCS counts segment numbers, not column files.
It is zero for heap tables and after reset.

The `ao_pre_cleanup_*`, `ao_compaction_*` and `ao_post_cleanup_*` columns
break down cumulative resource usage by phase. Each prefix has `blks_read`,
`blks_hit`, `blks_dirtied`, `blks_written`, `wal_records`, `wal_fpi`,
`wal_bytes`, `blk_read_time` and `blk_write_time`. Buffer counts include
shared and local buffers, as in `total_blks_*`; I/O times are milliseconds
and require `track_io_timing`. Index resource usage is excluded from its
phase and remains in the index statistics. The three phase counters sum to
the corresponding table total (apart from floating-point rounding of times).
Heap tables have zero phase counters.

All new fields obey collection control and resets and survive clean
restarts. Failed vacuums do not publish a table report; replacement workers
report only phases they executed. This development-series payload change
bumps the statistics-file format: installing the new build discards saved
statistics from the previous format. Rebuild the server and extension
together and recreate the extension's SQL objects when updating a test
installation from the earlier 1.0 definition.

`recently_dead_tuples` accumulates the hidden rows remaining in the AO
visibility map after post-cleanup. This includes rows left because
compaction is disabled or below its threshold. Each completed vacuum adds
its remaining count, so two vacuums that both leave 100 hidden rows add
200; it is not a snapshot or a count of distinct rows. Once compaction
removes those rows, later vacuums add zero. Statistics resets clear the
counter, and clean restarts preserve it.

The heap-only counters (`pages_frozen`, `pages_all_visible`,
`tuples_frozen`, `missed_dead_*`, `dead_pages`, `freeze_age_vacuum_count`) stay zero for AO. The
resource usage covers all phases of the vacuum, which is reported at the end
of the last one.  The compaction moves live tuples to another segment file,
so an index's `tuples_deleted` counts the entries of the moved live tuples
too.

AO vacuum time and cost delay are summed over the active phases executed
by the reporting worker. Gaps between phases are excluded. If a worker is
replaced between phases, only the replacement worker's phases are reported.
Removed tuple and byte counters use 64 bits; conversion of released bytes
to heap-equivalent pages preserves the 64-bit range.

AO auxiliary relations (`aoseg`, `visimap`, `blkdir`) are shown as separate
heap-statistics rows under their own OIDs and names. Their indexes are shown
in the index views. The parent AO row describes its phases; the later heap
vacuum of auxiliary relations is not added to it. To associate an auxiliary
row with its parent, join its `relid` to `pg_appendonly.segrelid`,
`visimaprelid` or `blkdirrelid`.
