# Diskquota for Apache Cloudberry

> **Note**: This project is forked from [greenplum-db/diskquota](https://github.com/greenplum-db/diskquota-archive) 
> and has been adapted specifically for [Apache Cloudberry](https://cloudberry.apache.org/).
> It requires Apache Cloudberry 2.0+ (based on PostgreSQL 14).

## Overview

Diskquota is an extension that provides disk usage enforcement for database 
objects in Apache Cloudberry. Currently it supports setting quota limits on schema 
and role in a given database and limiting the amount of disk space that a schema 
or a role can use. 

This project is inspired by Heikki's 
[pg_quota project](https://github.com/hlinnaka/pg_quota) and enhances it in 
two aspects:

1. To support different kinds of DDL and DML which may change the disk usage 
of database objects.

2. To support diskquota extension on MPP architecture.

Diskquota is a soft limit of disk usage. On one hand it has some delay to 
detect the schemas or roles whose quota limit is exceeded. On the other hand,
'soft limit' supports two kinds of enforcement: Query loading data into
out-of-quota schema/role will be forbidden before query is running. Query 
loading data into schema/role with rooms will be cancelled when the quota
limit is reached dynamically during the query is running.

## Design

Diskquota extension is based on background worker framework in Apache Cloudberry.
There are two kinds of background workers: diskquota launcher and diskquota worker.

There is only one launcher process per database coordinator. There is no launcher
process for segments. 
Launcher process is responsible for managing worker processes: Calling 
RegisterDynamicBackgroundWorker() to create new workers and keep their handle.
Calling TerminateBackgroundWorker() to terminate workers which are disabled 
when DBA modifies GUC diskquota.monitor_databases.

There are many worker processes, one for each database which is listed 
in diskquota.monitor_databases. Same as launcher process, worker processes 
only run at coordinator node. Since each worker process needs to call SPI to fetch 
active table size, to limit the total cost of worker processes, we support to 
monitor at most 10 databases at the same time currently. Worker processes are 
responsible for monitoring the disk usage of schemas and roles for the target 
database, and do quota enforcement. It will periodically (can be set via 
diskquota.naptime) recalculate the table size of active tables, and update 
their corresponding schema or owner's disk usage. Then compare with quota 
limit for those schemas or roles. If exceeds the limit, put the corresponding 
schemas or roles into the rejectmap in shared memory. Schemas or roles in 
rejectmap are used to do query enforcement to cancel queries which plan to 
load data into these schemas or roles.

From MPP perspective, diskquota launcher and worker processes all run at
the Coordinator side. Coordinator-only design allows us to save the memory resource on
Segments, and simplifies the communication from Coordinator to Segment by calling SPI
queries periodically. Segments are used to detect the active tables and
calculate the active table size. Coordinator aggregates the table size from each
segment and maintains the disk quota model.

### Active table

Active tables are the tables whose table size may change in the last quota 
check interval. Active tables are detected at Segment QE side: hooks in 
smgrcreate(), smgrextend() and smgrtruncate() are used to detect active tables
and store them (currently relfilenode) in the shared memory. Diskquota worker
process will periodically call dispatch queries to all the segments and 
consume active tables in shared memories, convert relfilenode to relation oid,
and calculate table size by calling pg_table_size(), which will sum
the size of table (including: base, vm, fsm, toast) in each segment.

### Enforcement

Enforcement is implemented as hooks. There are two kinds of enforcement hooks:
enforcement before query is running and enforcement during query is running.
The 'before query' one is implemented at ExecutorCheckPerms_hook in function 
ExecCheckRTPerms(). 
The 'during query' one is implemented at DispatcherCheckPerms_hook in function 
checkDispatchResult(). For queries loading a huge number of data, dispatcher 
will poll the connection with a poll timeout. Hook will be called at every 
poll timeout with waitMode == DISPATCH_WAIT_NONE. Currently only async 
dispatcher supports 'during query' quota enforcement.

### Quota setting store

Quota limit of a schema or a role is stored in table 'quota_config' in 
'diskquota' schema in monitored database. So each database stores and manages 
its own disk quota configuration. Note that although role is a db object in 
cluster level, we limit the diskquota of a role to be database specific. 
That is to say, a role may have different quota limit on different databases 
and their disk usage is isolated between databases.

## Development

### Prerequisites

The following packages need to be installed:

- openssl-devel
- krb5-devel
- [cmake](https://cmake.org) (>= 3.20)

On RHEL/CentOS/Rocky Linux:
```bash
sudo yum install openssl-devel krb5-devel cmake
```

On Ubuntu/Debian:
```bash
sudo apt-get install libssl-dev libkrb5-dev cmake
```

### Build & Install

Diskquota uses CMake as its build system, wrapped by a Makefile for integration with the Cloudberry build process.

#### Option 1: Build with Apache Cloudberry Source Tree

Diskquota is included in the Apache Cloudberry source tree:

```bash
cd <cloudberry_src>
./configure [options...]

# Build everything
make -j$(nproc)
make install

# Or build diskquota only
make -C gpcontrib/diskquota
make -C gpcontrib/diskquota install
```

#### Option 2: Standalone Build (without source tree)

If you only have an installed Apache Cloudberry (no source tree):

```bash
# Source the environment first
source /path/to/cloudberry-db/cloudberry-env.sh

cd gpcontrib/diskquota
make
make install
```

### Setup

1. Create database to store global information:
```sql
CREATE DATABASE diskquota;
```

2. Enable diskquota as preload library:
```bash
# Set USER environment variable if not set (required by gpconfig)
export USER=$(whoami)

# enable diskquota in preload library
gpconfig -c shared_preload_libraries -v 'diskquota-<major.minor>'
# restart database
gpstop -ar
```

3. Config GUC of diskquota:
```bash
# set naptime (seconds) to refresh the disk quota stats periodically
gpconfig -c diskquota.naptime -v 2
```

4. Create diskquota extension in monitored database:
```sql
CREATE EXTENSION diskquota;
```

5. Initialize existing table size information (needed if `CREATE EXTENSION` is not executed in a newly created database):
```sql
SELECT diskquota.init_table_size_table();
```

## Usage

### Set/update/delete schema quota limit

```sql
CREATE SCHEMA s1;
SELECT diskquota.set_schema_quota('s1', '1 MB');
SET search_path TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);
-- insert small data succeeded
INSERT INTO a SELECT generate_series(1,100);
-- insert large data failed
INSERT INTO a SELECT generate_series(1,10000000);
-- insert small data failed
INSERT INTO a SELECT generate_series(1,100);

-- delete quota configuration
SELECT diskquota.set_schema_quota('s1', '-1');
-- insert small data succeed
SELECT pg_sleep(5);
INSERT INTO a SELECT generate_series(1,100);
RESET search_path;
```

### Set/update/delete role quota limit

```sql
CREATE ROLE u1 NOLOGIN;
CREATE TABLE b (i int) DISTRIBUTED BY (i);
ALTER TABLE b OWNER TO u1;
SELECT diskquota.set_role_quota('u1', '1 MB');

-- insert small data succeeded
INSERT INTO b SELECT generate_series(1,100);
-- insert large data failed
INSERT INTO b SELECT generate_series(1,10000000);
-- insert small data failed
INSERT INTO b SELECT generate_series(1,100);

-- delete quota configuration
SELECT diskquota.set_role_quota('u1', '-1');
-- insert small data succeed
SELECT pg_sleep(5);
INSERT INTO b SELECT generate_series(1,100);
RESET search_path;
```

### Show schema quota limit and current usage

```sql
SELECT * FROM diskquota.show_fast_schema_quota_view;
```

## Test

Before running regression tests, make sure:

1. The diskquota extension is installed (`make install`) on all nodes
2. The `shared_preload_libraries` is configured and the cluster is restarted
3. The `diskquota` database exists

```bash
# Set USER environment variable if not set (required by gpconfig)
export USER=$(whoami)

# Configure shared_preload_libraries (use current version)
gpconfig -c shared_preload_libraries -v 'diskquota-2.3'

# Restart the cluster
gpstop -ar

# Create diskquota database if not exists
createdb diskquota
```

Run regression tests:
```bash
# From source tree build:
make -C gpcontrib/diskquota installcheck

# Or from build directory:
cd gpcontrib/diskquota/build
make installcheck
```

Show quick diff of regress results:
```bash
cd gpcontrib/diskquota/build
make diff_<test_target>_<case_name>
```

## HA

Not implemented yet. One solution would be: start launcher process on standby 
and enable it to fork worker processes when switching from standby Coordinator to Coordinator.

## Benchmark & Performance Test

### Cost of diskquota worker
To be added.

### Impact on OLTP queries
To be added.

## Notes

### Drop database with diskquota enabled

If DBA created diskquota extension in a database, there will be a connection
to this database from diskquota worker process. DBA needs to first drop the diskquota
extension in this database, and then the database can be dropped successfully.

### Temp table

Diskquota supports limiting the disk usage of temp tables as well. 
But schema and role are different. For role, i.e. the owner of the temp table,
diskquota will treat it the same as normal tables and sum its table size to 
its owner's quota. While for schema, temp table is located under namespace 
'pg_temp_backend_id', so temp table size will not be summed to the current schema's quota.

## Known Issues

### Uncommitted transactions

Since Apache Cloudberry doesn't support READ UNCOMMITTED isolation level,
our implementation cannot detect the newly created table inside an
uncommitted transaction (see below example). Hence enforcement on 
that newly created table will not work. After transaction commit,
diskquota worker process could detect the newly created table
and do enforcement accordingly in later queries.

```sql
-- suppose quota of schema s1 is 1MB
SET search_path TO s1;
CREATE TABLE b (i int) DISTRIBUTED BY (i);
BEGIN;
CREATE TABLE a (i int) DISTRIBUTED BY (i);
-- Issue: quota enforcement doesn't work on table a
INSERT INTO a SELECT generate_series(1,200000);
-- quota enforcement works on table b
INSERT INTO b SELECT generate_series(1,200000);
-- quota enforcement works on table a,
-- since quota limit of schema s1 has already been exceeded
INSERT INTO a SELECT generate_series(1,200000);
END;
```

'CREATE TABLE AS' command has the similar problem.

One solution direction is that we calculate the additional 'uncommitted data size'
for schema and role in worker process. Since pg_table_size needs to hold
AccessShareLock to relation (and worker process doesn't even know this reloid exists),
we need to skip it, and call stat() directly with tolerance to file unlink.
Skipping lock is dangerous and we plan to leave it as a known issue at the current stage.

### Missing empty schema or role in views

Currently, if there is no table in a specific schema or no table's owner is a
specific role, these schemas or roles will not be listed in 
show_fast_schema_quota_view and show_fast_role_quota_view.

### Out of shared memory

Diskquota extension uses two kinds of shared memories. One is used to save 
rejectmap and another one is to save active table list. The rejectmap shared
memory can support up to 1 MiB database objects which exceed quota limit.
The active table list shared memory can support up to 1 MiB active tables in 
default, and user could reset it in GUC diskquota_max_active_tables.

As shared memory is pre-allocated, user needs to restart DB if they updated 
this GUC value.

If rejectmap shared memory is full, it's possible to load data into some 
schemas or roles whose quota limits are reached.
If active table shared memory is full, disk quota worker may fail to detect
the corresponding disk usage change in time.
