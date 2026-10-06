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

# gp_relaccess_stats: Table access monitoring tool for Greenplum

## Features
gp_relaccess_stats is an extension that records access statistics for Greenplum tables and views. Allowing users to see what objects were used, when and by whom. For example, this allows DBAs to find objects that are not used anymore or objects that are being misused.

Features include:
* support of both tables (regular, external or partitioned) and views
* separate tracking of select, insert, update and delete queries
* separate tracking of last read and write timestamps
* tracking of the last user who accessed the object
* per-database configuration
* in-memory stats survive server restarts (but not crashes)

### Supported versions and platforms
For now it is being tested only for GP6 and Linux. Though, there are no apparent reasons why it should not be working on newer GP versions (or even PG with slight code modification) or other OSes.

### Configuration
As this extension does extensive usage of hooks and shared memory, you need to load gp_relaccess_stats.so on start-up:
```
gpconfig -c shared_preload_libraries -v '<old_shared_preload_libraries_contents,>gp_relaccess_stats' && gpstop -ra
```
gp_relaccess_stats configuration parameters:
| **Parameter** | **Type**     | **Default**  | **Default**  |
| ---------------- | --------------- | ------------ | ------------ |
| `gp_relaccess_stats.enabled` | bool | false | Using `gp_relaccess_stats.enabled` you can enable/disable stats collection either globally or for each database separately. The second option is preferred.|
| `gp_relaccess_stats.max_tables` | integer | 65536 | `gp_relaccess_stats.max_tables` is a hard limit on how many tables can be cached in shared memory. Feel free to make this number higher if necessary, as the overhead is only about 160 bytes per table. Note, that stats cache for a specific table is evicted from memory any time you execute `relaccess_stats_update()` or `relaccess_stats_dump()` and new tables can be recorded. If you call these functions often enough, there is no need for high gp_relaccess_stats.max_tables|
| `gp_relaccess_stats.dump_on_overflow` | bool | false | This parameter configures what happens in case `gp_relaccess_stats.max_tables` was not enough. If set to `true`, `relaccess_stats_dump()` will be called implicitly and stats cache will be freed. Otherwice, you will get a WARNING saying that there is no room for new stats. Is this case, stats for some tables will be lost.|

### Usage
The first thing you need to do after `CREATE EXTENSION` and configuring - execute `SELECT relaccess_stats_init();` in a specific database. This function will fill `relaccess_stats` table with empty stats for each table and partition in this database. This is optional, but will come handy when you try to find tables that haven't been used recently, for example.

Then, either manually or with a cron job start executing `select relaccess_stats_update()`. This function takes all stats cached in shared memory and all stats stored in pg_stat dir (e.g, dumps after restarts, or when `max_tables` was exceeded) and upserts them into `relaccess_stats` table.

The `relaccess_stats` table itself looks like this:
| **Column** | **Description**     |
| ---------------- | --------------- |
| relid | OID of the relation |
| relname | Name of the relation at last access |
| last_reader_id | OID of user who read the table last  |
| last_writer_id | OID of user who wrote the table last  |
| last_read | Timestamp of the most recent select |
| last_write | Timestamp of the most recent insert/delete/update/truncate |
| n_select_queries |  |
| n_insert_queries |  |
| n_update_queries |  |
| n_delete_queries |  |
| n_truncate_queries |  |

**NOTE**: n_*_queries columns count the number of queries executed, not the number of rows read, inserted, deleted or updated.

This table has a view associated with it: `relaccess_stats_root_tables_aggregated`. This view has exactly same columns, however it only shows partitioned tables. To be more specific, it shows aggregated stats for each partitioned table.
For example, if we have 1 insert into `tbl1_prt_1` and 3 inserts into `tbl1_prt_2`, then `select * from relaccess_stats_root_tables_aggregated where relname = 'tbl1'` will show us only root table with n_insert_queries = 4. This view, however, has some limitations. See the next section for more detail.

Another useful function is `relaccess_stats_dump()`, which simply moves cached stats from shared memory to temporary files in pg_stat directory. This function is cheaper than `relaccess_stats_update` but will evict stats cache if needed. Though, stats in temporary files can also get lost. Hence, it is recommended to stick with frequent `select relaccess_stats_update()` calls.

To better understand when it's time to dump or update the stats one might check `select relaccess.relaccess_stats_fillfactor();`. It will show current usage of stats hash table in percents. For example if shared memory for our relaccess hash table is 70% full we will get relaccess_stats_fillfactor=70. It would be a good idea to dump or update when fillfactor is around 70%.

### Limitations and gotchas
There is a number of interesting edge-cases in this simple extension:
* `relaccess_stats_root_tables_aggregated` shows info only about tables that exist **now**. We simply can`t get information about inheritance relationship for deleted tables.
* Stats don't rollback on savepoint rollback. We will see n_select_queries incremented by 2 in the following case:
```sql
BEGIN;
SELECT * FROM tbl;
SAVEPOINT sp;
SELECT * FROM tbl;
ROLLBACK TO SAVEPOINT sp;
COMMIT;
```
There is no technical reason for this limitation. It cat be fixed when there will be need for that.
* Update stats often! Otherwise, data can be lost if any of it happens: 1) there was a crash, 2) `max_tables` exceeded w/o `dump_on_overflow`, 3) temporary pg_stat dir got cleaned.
* no `truncate only` support. There is a TODO in code in case it is ever needed.
* Updates and Deletes also increment n_select_queries. Every update and delete also read the table. That is, n_select_queries get incremented as well. If you need **only** selects, query like this `SELECT n_select_queries - (n_update_queries + n_delete_queries) ... FROM relaccess_stats ...;`. For this same reason last_read and last_reader_id change on update and delete queries.
* view = view + tables. It looks like whenever you select from view, n_select_queries get incremented for both the view and tables it references.
* obviously, we don't know any timestamps before we started tracking. So, the first timestamps are initialized with 0 (something around year 2000), which means those tables haven't been accessed since gp_relaccess_stats was enabled.
