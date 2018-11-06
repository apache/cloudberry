# Overview
Diskquota is an extension that provides disk usage enforcement for database objects in Postgresql. Currently it supports to set quota limit on schema and role in a given database and limit the amount of disk space that a schema or a role can use. 

This project is inspired by Heikki's pg_quota project (link: https://github.com/hlinnaka/pg_quota) and enhance it to support different kinds of DDL and DML which may change the disk usage of database objects.

Diskquota is a soft limit of disk uages. It has some delay to detect the schemas or roles whose quota limit is exceeded. Here 'soft limit' supports two kinds of encforcement:  Query loading data into out-of-quota schema/role will be forbidden before query is running. Query loading data into schema/role with rooms will be cancelled when the quota limit is reached dynamically during the query is running.

# Design
Diskquota extension is based on background worker framework in Postgresql.
There are two kinds of background workers: diskquota launcher and diskquota worker.

There is only one laucher process per database cluster(i.e. one laucher per postmaster).
Launcher process is reponsible for manage worker processes: Calling RegisterDynamicBackgroundWorker()
to create new workers and keep their handle. Calling TerminateBackgroundWorker() to
terminate workers which are disabled when DBA modify diskquota.monitor_databases

There are many worker processes, one for each database which is listed in diskquota.monitor_databases.
Currently, we support to monitor at most 10 databases at the same time.
Worker processes are responsible for monitoring the disk usage of schemas and roles for the target database,
and do quota enfocement. It will periodically (can be set via diskquota.naptime) recalcualte the table size of active tables, and update their corresponding schema or owner's disk usage. Then compare with quota limit for those schemas or roles. If exceeds the limit, put the corresponding schemas or roles into the blacklist in shared memory. Schemas or roles in blacklist are used to do query enforcement to cancel queries which plan to load data into these schemas or roles.

## Active table
Active tables are the tables whose table size may change in the last quota check interval. We use hooks in smgecreate(), smgrextend() and smgrtruncate() to detect active tables and store them(currently relfilenode) in the shared memory. Diskquota worker process will periodically consuming active table in shared memories, convert relfilenode to relaton oid, and calcualte table size by calling pg_total_relation_size(), which will sum the size of table(including: base, vm, fsm, toast and index).

## Enforcement
Enforcement is implemented as hooks. There are two kinds of enforcement hooks: enforcement before query is running and
enforcement during query is running.
The 'before query' one is implemented at ExecutorCheckPerms_hook in function ExecCheckRTPerms()
The 'during query' one is implemented at BufferExtendCheckPerms_hook in function ReadBufferExtended(). Note that the implementation of BufferExtendCheckPerms_hook will firstly check whether function request a new block, if not skip directyly.

## Quota setting store
Quota limit of a schema or a role is stored in table 'quota_config' in 'diskquota' schema in monitored database. So each database stores and manages its own disk quota configuration. Note that although role is a db object in cluster level, we limit the diskquota of a role to be database specific. That is to say, a role may has different quota limit on different databases and their disk usage is isolated between databases.

# Install
1. Add hook functions to Postgres by applying patch. It's required
since disk quota need to add some new hook functions in postgres core.
This step would be skipped after patch is merged into postgres in future.
```
# install patch into postgres_src and rebuild postgres.
cd postgres_src;
git apply $diskquota_src/patch/pg_hooks.patch;
make;
make install;
```
2. Compile and install disk quota.
```
cd $diskquota_src; 
make; 
make install;
```
3. Config postgresql.conf
```
# enable diskquota in preload library.
shared_preload_libraries = 'diskquota'
# set monitored databases
diskquota.monitor_databases = 'postgres'
# set naptime (second) to refresh the disk quota stats periodically
diskquota.naptime = 2
```
4. Create diskquota extension in monitored database.
```
create extension diskquota;
```

5. Reload database configuraion
```
# reset monitored database list in postgresql.conf
diskquota.monitor_databases = 'postgres, postgres2'
# reload configuration
pg_ctl reload
```

# Usage
1. Set/update/delete schema quota limit using diskquota.set_schema_quota
```
create schema s1;
select diskquota.set_schema_quota('s1', '1 MB');
set search_path to s1;

create table a(i int);
# insert small data succeeded
insert into a select generate_series(1,100);
# insert large data failed
insert into a select generate_series(1,10000000);
# insert small data failed
insert into a select generate_series(1,100);

# delete quota configuration
select diskquota.set_schema_quota('s1', '-1');
# insert small data succeed
select pg_sleep(5);
insert into a select generate_series(1,100);
reset search_path;
```

2. Set/update/delete role quota limit using diskquota.set_role_quota
```
create role u1 nologin;
create table b (i int);
alter table b owner to u1;
select diskquota.set_role_quota('u1', '1 MB');

# insert small data succeeded
insert into b select generate_series(1,100);
# insert large data failed
insert into b select generate_series(1,10000000);
# insert small data failed
insert into b select generate_series(1,100);

# delete quota configuration
select diskquota.set_role_quota('u1', '-1');
# insert small data succeed
select pg_sleep(5);
insert into a select generate_series(1,100);
reset search_path;
```

3. Show schema quota limit and current usage
```
select * from diskquota.show_schema_quota_view;
```


# Test
Run regression tests.
```
cd contrib/diskquota;
make installcheck
```

# Benchmark & Performence Test
## Cost of diskquota worker.
During each refresh interval, the disk quota worker need to refresh the disk quota model.

It take less than 100ms under 100K user tables with no avtive tables.

It take less than 200ms under 100K user tables with 1K active tables.

## Impact on OLTP queries
We test OLTP queries to measure the impact of enabling diskquota feature. The range is from 2k tables to 10k tables.
Each connection will insert 100 rows into each table. And the parallel connections range is from 5 to 25. Number of active tables will be around 1k.

Without diskquota enabled (seconds)

|   	|   2k	|   4k	|  6k 	|  8k 	|  10k 	|
|:-:	|:-:	|:-:	|:-:	|:-:	|---	|
|   5	| 4.002 | 11.356	|   18.460	|   28.591	|   41.123	|
|   10	|   4.832	|   11.988	|  21.113 	|  32.829 	|  45.832 	|
|   15	|   6.238	|  16.896 	|  28.722 	|   45.375	|   64.642	|
|   20	|   8.036	|  21.711	|  38.499 	|  61.763 	|   87.875	|
|   25	|   9.909	|   27.175	|   47.996	|  75.688 	|  106.648 	|

With diskquota enabled (seconds)

|   	|   2k	|   4k	|  6k 	|  8k 	|  10k 	|
|:-:	|:-:	|:-:	|:-:	|:-:	|---	|
|   5	|   4.135	|  10.641 	| 18.776  	|  28.804 	|   41.740	|
|   10	|   4.773	|   12.407	| 22.351	|  34.243 	|   47.568	|
|   15	|   6.355	|   17.305	| 30.941  	|  46.967 	|   66.216	|
|   20	|   9.451	|   22.231	| 40.645  	|  61.758 	|   88.309	|
|   25	|   10.096	|   26.844	| 48.910  	|   76.537	|   108.025	|

The performance difference between with/without diskquota enabled are less then 2-3% in most case. Therefore, there is no significant performance downgrade when diskquota is enabled.

# Notes
1. Drop database with diskquota enabled.

If DBA enable monitoring diskquota on a database, there will be a connection
to this database from diskquota worker process. DBA need to first remove this
database from diskquota.monitor_databases in postgres.conf, and reload 
configuration by call `pg_ctl reload`. Then database could be dropped successfully.

2. Temp table.

Diskquota supports to limit the disk usage of temp table as well. But schema and role are different.
For role, i.e. the owner of the temp table, diakquota will treat it the same as normal tables and sum its
table size to its owner's quota. While for schema, temp table is located under namespace 'pg_temp_backend_id',
so temp table size will not sum to the current schema's qouta.

# Known Issue.

1. Since Postgresql doesn't support READ UNCOMMITTED isolation level,
our implementation cannot detect the new created table inside an
uncommitted transaction(See below example). Hence enforcement on 
that newly created table will not work. After transaction commit,
diskquota worker process could detect the newly create table
and do enfocement accordingly in later queries.
```
# suppose quota of schema s1 is 1MB.
set search_path to s1;
create table b;
BEGIN;
create table a;
# Issue: quota enforcement doesn't work on table a
insert into a select generate_series(1,200000);
# quota enforcement works on table b
insert into b select generate_series(1,200000);
# quota enforcement works on table a,
# since quota limit of schema s1 has already exceeds.
insert into a select generate_series(1,200000);
END;
```

One solution direction is that we calculate the additional 'uncommited data size'
for schema and role in worker process. Since pg_total_relation_size need to hold
AccessShareLock to relation(And worker process don't even know this reloid exists),
we need to skip it, and call stat() directly with tolerant to file unlink.
Skip lock is dangerous and we plan to leave it as known issue at current stage.

2. Out of shared memory

Diskquota extension uses two kinds of shared memories. One is used to save black list and another one is
to save active table list. The black list shared memory can support up to 1 MiB database objects which exceed quota limit.
The active table list shared memory can support up to 1 MiB active tables in default, and user could reset it in GUC diskquota_max_active_tables.

As shared memory is pre-allocated, user needs to restart DB if they updated this GUC value.

If black list shared memory is full, it's possible to load data into some schemas or roles which quota limit are reached.
If active table shared memory is full, disk quota worker may failed to detect the corresponding disk usage change in time.

