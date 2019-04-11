# Overview
Diskquota is an extension that provides disk usage enforcement for database 
objects in Greenplum DB. Currently it supports to set quota limit on schema 
and role in a given database and limit the amount of disk space that a schema 
or a role can use. 

This project is inspired by Heikki's 
[pg_quota project](https://github.com/hlinnaka/pg_quota) and enhance it in 
two aspects:

1. To support different kinds of DDL and DML which may change the disk usage 
of database objects.

2. To support diskquota extension on MPP architecture.

Diskquota is a soft limit of disk uages. On one hand it has some delay to 
detect the schemas or roles whose quota limit is exceeded. On the other hand,
'soft limit' supports two kinds of encforcement:  Query loading data into
out-of-quota schema/role will be forbidden before query is running. Query 
loading data into schema/role with rooms will be cancelled when the quota
limit is reached dynamically during the query is running.

# Design
Diskquota extension is based on background worker framework in Greenplum (bg 
worker needs pg_verion >= 9.4, which is supported in Greenplum 6 and later).
There are two kinds of background workers: diskquota launcher and diskquota worker.

There is only one launcher process per database master. There is no launcher
process for segments. 
Launcher process is reponsible for manage worker processes: Calling 
RegisterDynamicBackgroundWorker() to create new workers and keep their handle.
Calling TerminateBackgroundWorker() to terminate workers which are disabled 
when DBA modify GUC diskquota.monitor_databases.

There are many worker processes, one for each database which is listed 
in diskquota.monitor_databases. Same as launcher process, worker processes 
only run at master node. Since each worker process needs to call SPI to fetch 
active table size, to limit the total cost of worker processes, we support to 
monitor at most 10 databases at the same time currently. Worker processes are 
responsible for monitoring the disk usage of schemas and roles for the target 
database, and do quota enforcement. It will periodically (can be set via 
diskquota.naptime) recalculate the table size of active tables, and update 
their corresponding schema or owner's disk usage. Then compare with quota 
limit for those schemas or roles. If exceeds the limit, put the corresponding 
schemas or roles into the blacklist in shared memory. Schemas or roles in 
blacklist are used to do query enforcement to cancel queries which plan to 
load data into these schemas or roles.

From MPP perspective, diskquota launcher and worker processes are all run at
Master side. Master only design allows us to save the memory resource on
Segments, and simplifies the communication from Master to Segment by call SPI
queries periodically. Segments are used to detected the active table and
calculated the active table size. Master aggregate the table size from each
segments and maintain the disk quota model.

## Active table
Active tables are the tables whose table size may change in the last quota 
check interval. Active tables are detected at Segment QE side: hooks in 
smgecreate(), smgrextend() and smgrtruncate() are used to detect active tables
and store them (currently relfilenode) in the shared memory. Diskquota worker
process will periodically call dispatch queries to all the segments and 
consume active tables in shared memories, convert relfilenode to relaton oid, 
and calcualte table size by calling pg_total_relation_size(), which will sum 
the size of table (including: base, vm, fsm, toast and index) in each segment.

## Enforcement
Enforcement is implemented as hooks. There are two kinds of enforcement hooks:
enforcement before query is running and enforcement during query is running.
The 'before query' one is implemented at ExecutorCheckPerms_hook in function 
ExecCheckRTPerms() 
The 'during query' one is implemented at DispatcherCheckPerms_hook in function 
checkDispatchResult(). For queries loading a huge number of data, dispatcher 
will poll the connnection with a poll timeout. Hook will be called at every 
poll timeout with waitMode == DISPATCH_WAIT_NONE. Currently only async 
diskpatcher supports 'during query' quota enforcement.

## Quota setting store
Quota limit of a schema or a role is stored in table 'quota_config' in 
'diskquota' schema in monitored database. So each database stores and manages 
its own disk quota configuration. Note that although role is a db object in 
cluster level, we limit the diskquota of a role to be database specific. 
That is to say, a role may have different quota limit on different databases 
and their disk usage is isolated between databases.

# Install
1. Compile disk quota with pgxs.
```
cd $diskquota_src; 
make; 
make install;
```

2. Create database to store global information.
```
create database diskquota;
```

3. Enable diskquota as preload library 
```
# enable diskquota in preload library.
gpconfig -c shared_preload_libraries -v 'diskquota'
# restart database.
gpstop -ar
```

4. Config GUC of diskquota.
```
# set naptime ( second ) to refresh the disk quota stats periodically
gpconfig -c diskquota.naptime -v 2
```

5. Create diskquota extension in monitored database.
```
create extension diskquota;
```

6. Initialize existing table size information is needed if `create extension` is not executed in a new created database.
```
select diskquota.init_table_size_table();
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
select * from diskquota.show_fast_schema_quota_view;
```


# Test
Run regression tests.
```
cd diskquota_src;
make installcheck
```

# HA
Not implemented yet. One solution would be: start launcher process on standby 
and enable it to fork worker processes when switch from standby Master to Master.

# Benchmark & Performence Test
## Cost of diskquota worker.
To be added.

## Impact on OLTP queries
To be added.

# Notes
1. Drop database with diskquota enabled.

If DBA created diskquota extension in a database, there will be a connection
to this database from diskquota worker process. DBA need to firstly the drop diskquota
extension in this database, and then database could be dropped successfully.

2. Temp table.

Diskquota supports to limit the disk usage of temp table as well. 
But schema and role are different. For role, i.e. the owner of the temp table,
diskquota will treat it the same as normal tables and sum its table size to 
its owner's quota. While for schema, temp table is located under namespace 
'pg_temp_backend_id', so temp table size will not sum to the current schema's qouta.

# Known Issue.

1. Since Greenplum doesn't support READ UNCOMMITTED isolation level,
our implementation cannot detect the new created table inside an
uncommitted transaction (See below example). Hence enforcement on 
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

'Create Table As' command has the similar problem.

One solution direction is that we calculate the additional 'uncommited data size'
for schema and role in worker process. Since pg_total_relation_size need to hold
AccessShareLock to relation (And worker process don't even know this reloid exists),
we need to skip it, and call stat() directly with tolerant to file unlink.
Skip lock is dangerous and we plan to leave it as known issue at current stage.

2. Missing empty schema or role in show_fast_schema_quota_view and show_fast_role_quota_view
Currently, if there is no table in a specific schema or no table's owner is a
specific role, these schemas or roles will not be listed in 
show_fast_schema_quota_view and show_fast_role_quota_view.

3. Out of shared memory

Diskquota extension uses two kinds of shared memories. One is used to save 
black list and another one is to save active table list. The black list shared
memory can support up to 1 MiB database objects which exceed quota limit.
The active table list shared memory can support up to 1 MiB active tables in 
default, and user could reset it in GUC diskquota_max_active_tables.

As shared memory is pre-allocated, user needs to restart DB if they updated 
this GUC value.

If black list shared memory is full, it's possible to load data into some 
schemas or roles which quota limit are reached.
If active table shared memory is full, disk quota worker may failed to detect
the corresponding disk usage change in time.

