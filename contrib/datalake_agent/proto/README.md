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

# The datalake gRPC contract

`datalake_fdw` keeps the metadata of an Iceberg table -- which files make up
a snapshot, which snapshot is current -- in a metadata engine behind the
`IcebergMetaEngine` vtable (`contrib/datalake_fdw/src/meta/iceberg_meta_engine.h`).
The first engine is `datalake_agent`, a Java service the extension reaches
over gRPC. These files are what the two sides agree on.

| File | Holds |
|---|---|
| `common.proto` | identifiers, Iceberg schemas, specs and sort orders, storage and credentials, the version header, errors |
| `iceberg_catalog.proto` | `IcebergCatalogService`: one RPC per vtable entry |
| `catalog_mgmt.proto` | `CatalogManagementService`: creating and listing catalogs and namespaces |
| `contrib/datalake_fdw/src/meta/fragment.proto` | fragments, written-file reports and pushed-down predicates |

`fragment.proto` lives with the C side because its messages are not the
agent's: they are what a scan is planned into and what a write reports, and an
engine that is not the agent would use the same ones. It is in the same
package, `cloudberry.datalake.v1`, and `iceberg_catalog.proto` imports it.

There is one copy of each file. Nothing generated from them is committed; each
side runs `protoc` when it builds, with both directories as include roots:

```
protoc -I contrib/datalake_agent/proto -I contrib/datalake_fdw/src/meta ...
```

The files use proto3 `optional` fields, so protoc has to be 3.15 or later, or
3.12 to 3.14 given `--experimental_allow_proto3_optional`; Rocky 9 ships 3.14,
and later versions accept the flag and ignore it.

The `datalake proto` workflow runs the C++ and the Java gRPC generators on
every change to these files, with warnings fatal.

## Compatibility

Fields are only ever appended. A field number is never reused, and a removed
field's number is `reserved`. A message's meaning does not change under the
same name; a new meaning is a new message or a new RPC.

## Conventions

Three rules hold for every message; `common.proto` states them at the top.

- **Presence.** A scalar whose zero is a legal value -- a snapshot id, a
  sequence number (every snapshot of a format v1 table has 0), a segment -- is
  `optional`. Absent is what "none" or "current" means; zero is never borrowed
  for it.
- **Exact numbers.** A `google.protobuf.Struct` holds numbers as doubles,
  exact only up to 2^53, so a 64-bit integer goes into one as its decimal
  string. Iceberg's own objects -- schemas, partition specs, sort orders --
  travel as Iceberg's JSON text, the form a table metadata file holds them in.
- **Credentials** go only in `StorageConfig.credentials`, which every request
  that reaches a table's files carries, and never in a `Header` or a `Struct`.
  Neither side logs a value of that map, and the server scrubs each from any
  text it logs during the call.

## Version handshake

The contract described by these files is version **1.0.0**.  Versions are
`MAJOR.MINOR.PATCH`, and compatibility is decided on `MAJOR.MINOR`.

- The client sends its own contract version in the
  `x-cloudberry-client-version` request metadata.
- The server answers every call with `x-cloudberry-server-version` and
  `x-cloudberry-min-client-version` in its metadata, and fills the same two
  values into the `response_header` every response message carries.
- The server refuses a client older than `min_client_version`.
- The client refuses a server whose `min_client_version` is newer than its own
  version, and a server older than the oldest one it was written against. The
  second check is the one a rolling upgrade needs: a field an older server
  does not know arrives absent, and for several fields here absent means
  "none" -- no snapshot, no filter -- rather than "not sent".
- A different `MAJOR` on either side is refused outright.
- Every refusal names both versions.

## Errors

A failed call ends with a `google.rpc.Status` whose details hold one
`ErrorDetail`. A client branches on `business_code` (`BusinessErrorCode`);
`code` repeats the gRPC status name, for logs. Nothing in it is a stack trace:
that stays in the server's log, where `correlation_id` finds it.

Errors come in two kinds, and `ErrorDetail.retryable` says which. A retryable
one -- the server unavailable or overloaded, the call timed out -- may go away
if the same request is sent again with the same `operation_id`; a server
applies an operation at most once, so a retry of one whose first attempt did
land gets that attempt's result rather than a second change. Any other error
is the request's, and is reported. A commit whose outcome the server could not
learn from its catalog is answered `CommitOutcome.UNKNOWN`, and is resolved
the same way: retried with the same `operation_id`, or reported as unknown,
never taken as applied or as rejected.

## Health

The agent serves the standard `grpc.health.v1.Health` service, from gRPC's
own `health.proto`, which is why it is not repeated here. It answers for the
empty service name and for each service above.

## The vtable and the RPCs

From the vtable to the wire:

| `IcebergMetaEngine` entry | RPC |
|---|---|
| `load_table` | `LoadTable` |
| `create_table` | `CreateTable` |
| `drop_table` | `DropTable` |
| `table_exists` | `TableExists` |
| `get_statistics` | `GetStatistics` |
| `append` | `Append` |
| `commit_append` | `CommitAppend` |
| `update` | `Update` |
| `commit_update` | `CommitUpdate` |
| `get_fragment` | `GetFragment` -- opens the stream |
| `fragment_iter_next_batch` | the next `FragmentBatch` on that stream; its end is the iterator's end |
| `fragment_iter_close` | cancels the stream if it is still open |
| `plan_file_groups` | `PlanFileGroups` -- opens the stream |
| `file_group_iter_next` | the next `FileGroup` on that stream |
| `file_group_iter_close` | cancels the stream if it is still open |
| `commit_file_groups` | `CommitFileGroups`, both ways streamed: a start message, then one group per message; the per-group results are folded into one `MetaCommitResult` |
| `alter_table` | none yet |
| `truncate_table` | none yet |

`alter_table` and `truncate_table` have no RPC yet. An engine that cannot
carry them leaves both entries NULL and `DL_CAP_ALTER` and `DL_CAP_TRUNCATE`
out of its capabilities -- registration refuses an engine where the two
disagree -- and the central dispatch answers `DL_ERR_NOT_SUPPORTED` without
calling it. Each gets its RPC, and the messages it needs, in the change that
implements it.

From the wire back to the vtable:

| RPC | `IcebergMetaEngine` entry |
|---|---|
| `IcebergCatalogService.LoadTable` | `load_table` |
| `IcebergCatalogService.CreateTable` | `create_table` |
| `IcebergCatalogService.DropTable` | `drop_table` |
| `IcebergCatalogService.TableExists` | `table_exists` |
| `IcebergCatalogService.GetStatistics` | `get_statistics` |
| `IcebergCatalogService.Append` | `append` |
| `IcebergCatalogService.CommitAppend` | `commit_append` |
| `IcebergCatalogService.Update` | `update` |
| `IcebergCatalogService.CommitUpdate` | `commit_update` |
| `IcebergCatalogService.GetFragment` | `get_fragment`, `fragment_iter_next_batch`, `fragment_iter_close` |
| `IcebergCatalogService.PlanFileGroups` | `plan_file_groups`, `file_group_iter_next`, `file_group_iter_close` |
| `IcebergCatalogService.CommitFileGroups` | `commit_file_groups` |
| `CatalogManagementService.*` | none: administration, not something a table operation calls; the agent serves it only when configured to |
| `grpc.health.v1.Health.*` | none: the connection check before any of the above |

## Known limits

- `AppendResponse` and `UpdateResponse` carry `outcome` and
  `committed_metadata_location`, which only a commit can truthfully report.
  `Append` and `Update` stage and do not commit: the table changes at
  `CommitAppend` and `CommitUpdate`, and a client reads commit state from
  those responses only.
