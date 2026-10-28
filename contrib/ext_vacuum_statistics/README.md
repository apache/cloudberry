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

From a Cloudberry coordinator, query statistics across the cluster:

```sql
-- Per-table heap vacuum statistics, with gp_segment_id
SELECT * FROM ext_vacuum_statistics.gp_stats_vacuum_tables;

-- Per-index vacuum statistics
SELECT * FROM ext_vacuum_statistics.gp_stats_vacuum_indexes;

-- Per-database aggregate vacuum statistics
SELECT * FROM ext_vacuum_statistics.gp_stats_vacuum_database;
```

The corresponding `pg_stats_vacuum_*` views read only the connected instance.
Use them for a standalone server or a utility connection to a single segment.
Use the `gp_stats_vacuum_*_summary` views for cluster totals.

Example projection of table statistics:

```
 relname   | total_blks_read | total_blks_hit | wal_records | tuples_deleted | pages_removed
-----------+-----------------+----------------+-------------+----------------+---------------
 mytable   |             120 |            340 |          15 |            500 |            10
```

Reset statistics across the cluster from the coordinator:

```sql
SELECT ext_vacuum_statistics.gp_vacuum_statistics_reset();
```

These functions reset only extension-owned metrics; native work is unchanged.
Use the `gp_` wrappers from a normal coordinator connection to reset them
on the coordinator and all primary segments.

**Local functions reset only the current node.** Calling
`ext_vacuum_statistics.vacuum_statistics_reset()` on the coordinator (QD)
resets only the coordinator's counters; segment counters remain unchanged.
The same applies to `extvac_reset_entry()` and `extvac_reset_db_entry()`.
Use the local functions for a utility connection to an individual segment.

All functions below belong to the `ext_vacuum_statistics` schema:

| Reset scope | Cluster wrapper (call on the coordinator) | Local function (current node only) |
|-------------|------------------------------------------|-----------------------------------|
| One table or index | `gp_extvac_reset_entry(dboid, relid)` | `extvac_reset_entry(dboid, relid)` |
| One database and all its relations | `gp_extvac_reset_db_entry(dboid)` | `extvac_reset_db_entry(dboid)` |
| All databases | `gp_vacuum_statistics_reset()` | `vacuum_statistics_reset()` |

For example, reset only the current database across the cluster:

```sql
SELECT ext_vacuum_statistics.gp_extvac_reset_db_entry(oid)
FROM pg_database WHERE datname = current_database();
```

## Configuration (GUCs)

| GUC | Default | Description |
|-----|---------|-------------|
| `vacuum_statistics.enabled` | on | Collect extension resource metrics; native work obeys `track_counts` |

## Memory usage

Each tracked object (table or index) uses a fixed-size shared memory entry; the exact size depends on the platform.

Per-database aggregates add one entry per tracked database. Entry size includes
pgstat bookkeeping; memory estimates must use the structures of the actual
build rather than a fixed byte count from an earlier version.

The entry of a table or an index is dropped when the relation is dropped (at
commit, so a rolled back `DROP` keeps it), and a new relation that gets the OID
of an old one starts from zero.  The module does that with an
`object_access_hook`.

## Recipes

**Disable extension resource collection temporarily (native work continues):**

```sql
SET vacuum_statistics.enabled = off;
```

## Views

| View | Description |
|------|-------------|
| `ext_vacuum_statistics.pg_stats_vacuum_tables` | Per-table heap vacuum stats (pages scanned, tuples deleted and resource usage) |
| `ext_vacuum_statistics.pg_stats_vacuum_indexes` | Per-index vacuum stats |
| `ext_vacuum_statistics.pg_stats_vacuum_database` | Per-database aggregate vacuum stats |

## Limitations

- Must be loaded via `shared_preload_libraries`; it cannot be loaded on demand.
- Starting a server without the module, even once, makes it treat the whole
  statistics file as corrupted and reset all cumulative statistics, the
  built-in ones included.  Use `vacuum_statistics.enabled = off` rather than
  removing the module.

## Native work counters

Heap, index and AO work counters are accumulated once, by native pgstat.
The extension's table and index views read those same counters through
`pg_stat_get_vacuum_stats()`, alongside separately stored buffer, WAL and I/O
measurements. They have the same work-counter history as `pg_stat_vacuum_tables`,
`pg_stat_vacuum_indexes` and their `gp_stat_*` counterparts; enabling the
extension does not start a second history.

Work counters obey `track_counts`. `vacuum_statistics.enabled` controls only
extension resource metrics.
With extension collection disabled, native work remains visible in these views.
Relations without a resource entry show zero resource counters.

Extension reset functions clear only the extension's own metrics, without
touching native work or any other core statistics. To reset native work, use
`pg_stat_reset_vacuum_stats(relid)` for one table or index, or
`pg_stat_reset_vacuum_stats()` for the current database and its non-shared
relations. These functions preserve native timing, vacuum/analyze invocation
counts, and other statistics. On Cloudberry, call the corresponding
`gp_stat_reset_vacuum_stats(relid)` or `gp_stat_reset_vacuum_stats()` on the
coordinator to reach all primary segments. The broader native reset
`pg_stat_reset_single_table_counters()`
clears native work but leaves extension resources intact; `pg_stat_reset()`
clears both for the current database. The two sets of metrics can therefore
cover different periods after a reset or a change in collection settings.

The resource getter signatures and shared payload changed in this development
series. Rebuild the server and extension together and recreate the extension's
SQL objects when updating an earlier 1.0 installation. The new statistics-file
format discards previously saved counters; the new core function also requires
the matching catalog version.

## Cloudberry

Each instance (the coordinator and every segment) keeps the statistics of the
vacuums it runs itself. The local `pg_stats_vacuum_*` views show only that
instance; use `gp_stats_vacuum_*` to read the coordinator and primary segments.

Install the matching extension library, control and SQL files in the Cloudberry
installation on every host, including mirror and standby coordinator hosts.
`gpconfig` changes configuration files; it does not distribute extension files.
Load the module on every instance so it is also available after promotion.

Inspect both the active setting and the configuration files first:

```sh
gpconfig -s shared_preload_libraries
gpconfig -s shared_preload_libraries --file
```

When the existing list is empty, configure it with:

```sh
gpconfig -c shared_preload_libraries -v 'ext_vacuum_statistics'
```

Otherwise pass the complete existing list with `ext_vacuum_statistics` appended.
Preserve any intentional differences between coordinator, primary and mirror
settings; do not replace their other preloaded libraries. With `-v` and no
role restriction, `gpconfig` updates the coordinator, standby, primaries and
mirrors. Resolve any reported unreachable hosts before relying on their setup.
Check the resulting files, then restart the cluster to load the library:

```sh
gpconfig -s shared_preload_libraries --file
gpstop -ar
gpconfig -s shared_preload_libraries --file-compare
```

A configuration reload (`gpstop -u`) alone cannot load a new preload library.
Check that mirror and standby instances also restarted successfully. After the
restart, run the following on the coordinator in each database where the views
are needed; `CREATE EXTENSION` is dispatched to the segments:

```sql
CREATE EXTENSION IF NOT EXISTS ext_vacuum_statistics;
```

Cluster-wide views, like the `gp_stat_*` views of the core:

| View | Description |
|------|-------------|
| `ext_vacuum_statistics.gp_stats_vacuum_tables` | `pg_stats_vacuum_tables` of every instance, with `gp_segment_id` (-1 for the coordinator) |
| `ext_vacuum_statistics.gp_stats_vacuum_indexes` | the same for indexes |
| `ext_vacuum_statistics.gp_stats_vacuum_database` | the same for databases |
| `ext_vacuum_statistics.gp_stats_vacuum_tables_summary` | one row per table: summed over the segments (divided by their number for replicated tables); catalogs as on the coordinator |
| `ext_vacuum_statistics.gp_stats_vacuum_indexes_summary` | the same for indexes |
| `ext_vacuum_statistics.gp_stats_vacuum_database_summary` | one row per database, summed over all instances |

The reset functions without the `gp_` prefix act only on the connected instance;
`gp_vacuum_statistics_reset()`, `gp_extvac_reset_entry(dboid, relid)` and
`gp_extvac_reset_db_entry(dboid)` dispatch the resets from the coordinator to
all primary segments and also reset the coordinator. A `SET` of
`vacuum_statistics.enabled` on the coordinator is passed on to the segments.

The counters are local pgstat state and are not replicated through WAL.
Preloading on a mirror enables collection after promotion; it does not copy
its primary's accumulated counters. A promoted instance reports its own local
statistics, so it cannot continue the former primary's counter history. This
also applies when the standby coordinator is promoted. A compatible clean
restart can preserve an instance's own counters; crash recovery resets them.

Treat promotion as a new measurement interval for the affected instance, even
if its `gp_segment_id` is unchanged. Other instances keep their own counters,
but cluster summaries can decrease when a primary is replaced. Monitoring
should retain previous samples externally and begin a new baseline after
failover, without interpreting the change as negative vacuum work.

The test of the cluster-wide views runs against such a cluster:

```
make -C contrib/ext_vacuum_statistics installcheck-cluster
```

Reset functions return `void` and require superuser privileges by default.
An administrator can delegate access with `GRANT EXECUTE`; cluster wrappers
also require permission to execute the corresponding local reset function.
An extension relation reset clears only that relation's resource counters, leaving its indexes and
the database aggregate unchanged. A database reset clears its aggregate and
all its relation entries. The global reset affects all databases on the current
instance; the `gp_` wrappers apply these operations on the coordinator and all
primary segments.
Existing rows remain visible with zero extension counters after a reset.
Native work counters remain unchanged. `pg_stat_reset()` also resets the
extension's entries for the current database. Resetting does not remove the type of a table, index, or database entry.
