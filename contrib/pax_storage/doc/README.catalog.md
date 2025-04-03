# PAX Catalog 

## Overview

PAX has two implementations for the catalog:

- Auxiliary table: Use HEAP to implement the auxiliary tables. HEAP will provide Multi-Version Concurrency Control(MVCC).
- Manifest: Use JSON to store metadata, independent MVCC implementation. Better readability, but DDL often leads to poor performance.

In this article, only the auxiliary table is introduced.

In PAX, all auxiliary tables have the namespace `pg_ext_aux`. This namespace will be created when the PAX plugin is initialized. During the compilation phase, `gen_sql.c` will dynamically generate the `pax-cdbinit--1.0.sql`. When the PAX plugin is initialized, the relevant `access method/namespace/auxiliary table` will be inserted into Cloudberry.

Includes:
- pg_ext_aux.pg_pax_fastsequence
- pg_ext_aux.pg_pax_tables

## pg_pax_tables

The auxiliary table `pg_pax_tables` is used to associate the PAX table and blocks table.

```
\d+ pg_ext_aux.pg_pax_tables
                                   Table "pg_ext_aux.pg_pax_tables"
  Column  | Type | Collation | Nullable | Default | Storage | Compression | Stats target | Description
----------+------+-----------+----------+---------+---------+-------------+--------------+-------------
 relid    | oid  |           | not null |         | plain   |             |              |
 auxrelid | oid  |           | not null |         | plain   |             |              |
Indexes:
    "pg_pax_tables_relid_index" UNIQUE, btree (relid)
Access method: heap
```

When a PAX table is created, it will be recorded in `pg_pax_tables`. When we need to find the data file associated with the PAX table, we need to get its corresponding blocks(file). So when PAX deals with DML, PAX will open `pg_pax_tables` to find which blocks need to be processed.

Example:

```
postgres=# create table t1(v1 int) using pax;
CREATE TABLE
postgres=# select * from pg_ext_aux.pg_pax_tables;
 relid | auxrelid
-------+----------
 17025 |    17026   -- the 17025 is the OID of the PAX table, the 17026 is the OID of the blocks relation
(1 row)
```

## pg_pax_blocks_<relid>

The `pg_pax_blocks_<relid>` table records the meta information of the current PAX table. The `<relid>` is the OID of the PAX table.

```
postgres=# \d+ pg_ext_aux.pg_pax_blocks_17025
                                             Table "pg_ext_aux.pg_pax_blocks_17025"
     Column      |          Type          | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
-----------------+------------------------+-----------+----------+---------+----------+-------------+--------------+-------------
 ptblockname     | integer                |           | not null |         | plain    |             |              |
 pttupcount      | integer                |           |          |         | plain    |             |              |
 ptblocksize     | integer                |           |          |         | plain    |             |              |
 ptstatistics    | pg_ext_aux.paxauxstats |           |          |         | extended |             |              |
 ptvisimapname   | name                   |           |          |         | plain    |             |              |
 ptexistexttoast | boolean                |           |          |         | plain    |             |              |
 ptisclustered   | boolean                |           |          |         | plain    |             |              |
Indexes:
    "pg_pax_blocks_17025_idx" PRIMARY KEY, btree (ptblockname)
Access method: heap
```

- ptblockname: File name
- pttupcount: The tuple counts in the current file
- ptblocksize: The estimated size of the current file
- ptstatistics: The statistics(serialized) of the current file
- ptvisimapname: The name of the visibility map
- ptexistexttoast: Exist toast file or not
- ptisclustered: Current file has been clustered or not

Each segment will have a `pg_pax_blocks_<relid>` table, which only records the PAX files generated on the current segment. Each file in the PAX table corresponds to a row of records.

Example:

```
postgres=# create table t1(v1 int) using pax;
CREATE TABLE
postgres=# select * from pg_ext_aux.pg_pax_tables;
 relid | auxrelid
-------+----------
 17025 |    17026   -- the 17025 is the OID of the PAX table, the 17026 is the OID of the blocks relation
(1 row)
postgres=# insert into t1 values(generate_series(0, 100)); -- insert rows
INSERT 0 101
postgres=# select * from gp_dist_random('pg_ext_aux.pg_pax_blocks_17025'); -- use the gp_dist_random to query the blocks table from segments
 ptblockname | pttupcount | ptblocksize |          ptstatistics          | ptvisimapname | ptexistexttoast | ptisclustered
-------------+------------+-------------+--------------------------------+---------------+-----------------+---------------
           0 |         38 |         256 | [(false,false),(38),None,None] |               | f               | f
           0 |         25 |         199 | [(false,false),(25),None,None] |               | f               | f
           0 |         38 |         256 | [(false,false),(38),None,None] |               | f               | f
(3 rows)
```

## pg_pax_fastsequence

You may have noticed that `ptblockname` in the `pg_pax_blocks_<relid>` table is an integer type. This is because all file names in PAX are named with `INT`. The naming numbers are generated by the `pg_pax_fastsequence` table.

```
postgres=# \d+ pg_ext_aux.pg_pax_fastsequence
                                 Table "pg_ext_aux.pg_pax_fastsequence"
 Column |  Type   | Collation | Nullable | Default | Storage | Compression | Stats target | Description
--------+---------+-----------+----------+---------+---------+-------------+--------------+-------------
 objid  | oid     |           | not null |         | plain   |             |              |
 seq    | integer |           | not null |         | plain   |             |              |
Indexes:
    "pg_pax_fastsequence_objid_idx" btree (objid)
Access method: heap
```

The `objid` is the OID of PAX table, and the `seq` is the next number used to name the PAX file.

ex.

```
postgres=# create table t1(v1 int) using pax;
CREATE TABLE
postgres=# select * from pg_ext_aux.pg_pax_tables;
 relid | auxrelid
-------+----------
 17025 |    17026   -- the 17025 is the OID of PAX table, the 17026 is the OID of the blocks relation
(1 row)
postgres=# select * from gp_dist_random('pg_ext_aux.pg_pax_fastsequence'); -- no any data in current table. So the seq is 0.
 objid | seq
-------+-----
 17025 |   0
 17025 |   0
 17025 |   0
(3 rows)
postgres=# insert into t1 values(generate_series(0, 100)); -- insert rows
INSERT 0 101
postgres=# select * from gp_dist_random('pg_ext_aux.pg_pax_fastsequence'); -- Each segment generated a file. So the  seq is 1.
 objid | seq
-------+-----
 17025 |   1
 17025 |   1
 17025 |   1
(3 rows)
```

## MVCC

The auxiliary tables in PAX are all composed of HEAP tables. In fact, the HEAP table has already provided visibility judgment. For PAX, the row visibility in the auxiliary table represents which files the current transaction can see. This may be a simplification. For details, please refer to `pg_catalog/*`

### Visibility map

When some rows in a file are not visible, PAX generate a visibility map to record invisable rows.

The visibility map is implemented by bitmaps, and each bit corresponds to a line. However, the length of its bit array may not equal to the number of rows in the file, but is less than or equal to the number of rows in the file. 

The naming convention for visibility maps is: <blocknum>_<generation>_<tag>.visimap
- `blocknum` is the current data file name
- `generation` is the current visimap generation number. Each deletion of this data file will increase the generation number by 1
- `tag` is the current transaction id. This field is used to ensure the uniqueness of the visimap file name.

