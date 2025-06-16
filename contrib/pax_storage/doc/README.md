# PAX

## Overview

PAX(Partition Attributes Across) is an access method in database that combines the advantages of row storage (NSM, N-ary Storage Model) and column storage (DSM, Decomposition Storage Model), aiming to improve database query performance, especially in terms of cache efficiency. 

In OLAP scenarios, PAX has batch write performance comparable to row storage and read performance comparable to column storage. PAX can adapt to both OSS storage models in cloud environments and traditional offline physical file-based storage methods.

PAX has the following features:

- **CRUD Operations**: Supports basic Create, Read, Update, and Delete capabilities
- **Indexes Support**: Supports multiple types of indexes to accelerate query operations, particularly enhancing data retrieval speed when handling large datasets.
- **Concurrency Control & Isolation**: Utilizes Multiversion concurrency control (MVCC) to achieve efficient concurrency management and read-write isolation, operating at the granularity of individual data files for improved security and performance.
- **Data Encoding & Compression**: Offers multiple encoding schemes (e.g., run-length encoding) and compression methods (e.g., zstd, zlib) with configurable levels, effectively reducing storage requirements while optimizing read performance.
- **Statistics**: Data files contain comprehensive statistical information used for rapid filtering and query optimization, minimizing unnecessary data scanning and accelerating query processing.
- **Sparsing Filter & Row Filter**: Enables efficient sparse filtering and row filtering through statistical metadata to reduce redundant data transfers.
- **Data Clustering**: Supports data sorting via specified algorithms on single or multiple columns to enhance data orderliness and query efficiency.
- **Vectorized Engine**: Includes an experimental vectorized execution engine designed to boost data processing capabilities and query performance, particularly for analytical workloads and report generation scenarios.

## Build

### Build PAX

PAX will be built with `--enable-pax` when you build the Cloudberry. Dependency requirements are as follows:

- **C/C++ Compiler**: GCC/GCC-C++ 8 or later
- **CMake**: 3.11 or later
- **Protobuf**: 3.5.0 or later
- **ZSTD (libzstd)**: 1.4.0 or later

Also, you need to run the following command at the top level of the Cloudberry source code directory to download the submodules:

```
git submodule update --init --recursive
```

The following submodules will be downloaded for building and tesing PAX:
- yyjson (`dependency/yyjson`)
- cpp-stub (`contrib/pax_storage/src/cpp/cotnrib`)
- googlebench (`contrib/pax_storage/src/cpp/cotnrib`)
- googletest (`contrib/pax_storage/src/cpp/cotnrib`)
- tabulate (`contrib/pax_storage/src/cpp/cotnrib`)

### Build debug version

When using the configure script in the Cloudberry, add `--enable-cassert`.

`--enable-cassert` will cause PAX to be compiled in **DEBUG** mode. And the `GTEST` in PAX will be built.

Run GTEST:

```
cd contrib/pax_storage/build
./src/cpp/test_main
```

## Usage

### Create PAX table

To create a table in PAX format, you need to set the table access method to PAX. You can do this by following one of the following methods:

- Use the `USING PAX` clause when creating the table
```
CREATE TABLE t1(a int, b int, c text) USING PAX;
```

- Set the default access mode for the table before create the table
```
-- set default table access method to PAX, All newly created tables will use the PAX.
SET default_table_access_method = pax;

-- Will create the PAX table
CREATE TABLE t1(a int, b int, c text);
```

### Create PAX table with reloptions

Users can specify reloptions (such as `compresstype`, `storage_format`...) when creating a PAX table.

```
--- use the 'WITH' to specify reloptions
CREATE TABLE p2(a INT, b INT, c INT) USING pax WITH(compresstype='zstd');

--- use ',' to split multi reloptions
CREATE TABLE p2(a INT, b INT, c INT) USING pax WITH(compresstype='zstd', compresslevel=5);
```

### Create PAX table with column encoding options 

PAX allows the user to use the `ENCODING` to specify `compresstype` and `compresslevel`. This allows different columns to use different compression/encoding algorithms.

```
CREATE TABLE t1 (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd, compresslevel=5),
                  c3 text, COLUMN c3 ENCODING (compresstype=RLE_TYPE))
                  USING PAX;
```

### Show PAX table

To check whether a table is in PAX format, use any of the following methods:

- Use `\d+ <tablename>`

```
gpadmin=# \d+ t1
                                            Table "public.t1"
 Column |  Type   | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
--------+---------+-----------+----------+---------+----------+-------------+--------------+-------------
 a      | integer |           |          |         | plain    |             |              |
 b      | integer |           |          |         | plain    |             |              |
 c      | text    |           |          |         | extended |             |              |
Distributed by: (a)
Access method: pax
```

- Query the system catalog tables `pg_class` and `pg_am`

```
SELECT relname, amname FROM pg_class, pg_am WHERE relam = pg_am.oid AND relname = 't1';

 relname | amname
---------+--------
 t1      | pax
(1 row)
```

## Options

For AM(access methods) in Cloudberry, each AM has customized relation options. Users can use these options in a `WITH()` clause, for example `WITH(minmax_columns='b,c', storage_format=porc)`.

| Name               | Type   | Optional                                       | Default| Description                                                                                                                          | 
| :-----:            | :----: | :----                                          | :----  | :----                                                                                                                                |
| storage_format     | string | `porc`<br>`porc_vec`<br>                       | `porc` | Controls the internal storage format.                                                                                                                              |
| compresstype       | string | `none`<br>`rle`<br>`delta`<br>`zstd`<br>`zlib` | `none` | The way to compress or encode column values. You can only select one of them.                                                                                                                                |
| compresslevel      | int    | `[0, 19]`                                      | `0`    | Specifies the compression level. Lower values prioritize faster compression speed, while higher values prioritize better compression ratios. This option takes effect only when used with the `compresstype` parameter. |
| minmax_columns     | string | `<column1>,<column2>,...,<columnN>`            | `none` | Specifies columns in which min/max statistics need to be generated . Example: `WITH(minmax_columns='t1,t2,t3')`                                                                                                         | 
| bloomfilter_columns| string | `<column1>,<column2>,...,<columnN>`            | `none` | Specifies columns in which bloom filter statistics need to be generated.                                                                                                                           |
| cluster_type       | string |`zorder`<br>`lexical`                           | `none` | Specify the clustering algorithm.                                                                                                                           |
| cluster_columns    | string | `<column1>,<column2>,...,<columnN>`            | `none` | Specify which columns need to be clustered.                                                                                                                           |


## GUC

| Name                               | Type   | Optional               | Default  | Description                                                                                                                               |
| :-----:                            | :----: | :----                  | :----    | :----                                                                                                                                     |
| pax_enable_sparse_filter           | bool   | `on`/`off`             | `on`     | Specifies whether to enable sparse filtering based on statistics.                                                                                                                               |
| pax_enable_row_filter              | bool   | `on`/`off`             | `off`    | Specifies whether to enable row filtering.                                                                                                                                |
| pax_scan_reuse_buffer_size         | int    | [1048576, 33554432]    | 8388608  | The buffer block size used during scanning.                                                                                                                                 |
| pax_max_tuples_per_group           | int    | [5, 524288]            | 131072   | Specifies the maximum number of tuples allowed in each group.                                                                                                                                    |
| pax_max_tuples_per_file            | int    | [131072, 8388608]      | 1310720  | Specifies the maximum number of tuples allowed in each data file.                                                                                                                                     |
| pax_max_size_per_file              | int    | [8388608, 335544320]   | 67108864 | The maximum physical size allowed for each data file. The default value is 67108864 (64MiB). The actual file size might be slightly larger than the set size. Very large or small values might negatively impact performance. |
| pax_enable_toast                   | bool   | `on`/`off`             | `on`     | Specifies whether to enable TOAST support.                                                                                                                                  |
| pax_min_size_of_compress_toast     | int    | [524288, 1073741824]   | 524288   | Specifies the threshold for creating compressed TOAST tables. If the character length exceeds this threshold, Cloudberry creates compressed TOAST tables for storage.                                                   |
| pax_min_size_of_external_toast     | int    | [10485760, 2147483647] | 10485760 | Specifies the threshold for creating external TOAST tables. If the character length exceeds this threshold, Cloudberry creates external TOAST tables for storage.                                                     |
| pax_default_storage_format         | string | `porc`/`porc_vec`      | `porc`   | Controls the default storage format.                                                                                                                                   | 
| pax_bloom_filter_work_memory_bytes | int    | [1024, 2147483647]     | 10240    | Controls the maximum memory allowed for bloom filter usage.                                                                                                                                    |
