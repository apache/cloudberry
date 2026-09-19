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

# datalake_fdw automation tests

Lake tables are only half local.  Once the metadata engine is connected, the
behaviour worth testing is what happens against a real Hive metastore, real
object storage and a real HDFS -- which comparison against a recorded transcript
cannot express, because the interesting cases are the ones where an external
service is slow, absent, or disagrees.  This directory is where those tests go,
and it exists now so that they are added here rather than somewhere new.

Everything currently here needs no external service.

## Running

```sh
make test              # run the smoke categories
make check-services    # what answers right now
make list-categories   # categories, and what each one needs
```

Service addresses come from `config/test_config.env`, which takes them from the
environment first, so a run against an existing deployment needs no edit:

```sh
DL_HMS_HOST=metastore.example DL_S3_ENDPOINT=http://minio:9000 make test
```

A category whose services are absent is **skipped and reported as skipped**, so
a developer without a metastore still gets a useful run and nobody reads a skip
as a pass.

## Layout

```
config/                 service addresses and switches
scripts/setup/          service probes
scripts/test/           category runners
scripts/utils/          shared shell helpers
sqlrepo/smoke/          one directory per category
  iceberg_am/           DDL, refusals and privileges -- no external service
  format_parquet/       a table through a local Parquet file and back
```

Each of these holds cases pg_regress runs, and pg_regress takes one
`--inputdir`, so each category is a run of its own: the module's `Makefile` has
`installcheck` for `iceberg_am` and `installcheck-format-parquet` for the other,
and hangs the second off the first so that asking for `installcheck` gets both.
`make test` from this directory runs the same cases.  They live here rather than
in a `sql/` directory of their own so that there is one place to look for test
material.

## What arrives with the metadata engine

Named here so the shape is known before the code lands, rather than reserved as
empty directories:

- `docker/` -- compose definitions bringing up a metastore, MinIO and a
  single-node HDFS, so a category can run anywhere
- `prepare/` -- fixture loading per service: create the warehouse, the bucket,
  the namespaces
- `lib/` -- SQL and shell fragments shared by categories
- `tools/` -- data generators for the volume and scale categories
- `sqlrepo/smoke/iceberg_hive`, `iceberg_s3`, `iceberg_hdfs` -- the read and
  write paths against each service
- `sqlrepo/feature`, `sqlrepo/negative` -- everything past the smoke level
