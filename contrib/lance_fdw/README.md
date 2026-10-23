# lance_fdw

A foreign-data wrapper that lets [Apache Cloudberry](https://cloudberry.apache.org/)
read [Lance](https://lancedb.github.io/lance/) datasets in place — no COPY, no
Python in the middle — with the scan running in parallel on every segment.

The wrapper is read-only. It talks to Lance through
[lance-c](https://github.com/lance-format/lance-c), the official C binding, and
decodes the Arrow batches it returns with a vendored copy of
[nanoarrow](https://github.com/apache/arrow-nanoarrow).

**State of this build: the MPP scan, over every A-tier type.** Servers, user
mappings, foreign tables, `IMPORT FOREIGN SCHEMA` and `SELECT` all work, the
scan runs on every segment, and the value converters cover the whole A-tier
list — booleans, the integer family, the three float widths, the string and
binary families, `date32`, `timestamp` in all four units with and without a
zone, `decimal128`, `fixed_size_list<float>` embeddings, one-dimensional
`list`s, `struct`s as composite types that the import creates, and
`map<utf8, ...>` as `jsonb`. Everything else is refused by name rather than
guessed at; see [Types](#types).

## Building

lance_fdw links `liblance_c`, the C binding of Lance. lance-c is written in
Rust and is not part of the Apache Cloudberry source tree: build or install it
first, then build lance_fdw against it. lance_fdw is tested against lance-c
commit `c43774c` (`LANCE_C_COMMIT` in `ci/build-lance-c.sh`); lance-c's C ABI is
not stable yet, so use that revision.

Building lance-c needs Rust >= 1.91.0, protoc (24.3 is known to work) and
network access to crates.io and GitHub; the first build takes about a quarter
of an hour. `ci/build-lance-c.sh PREFIX` does all of it and installs
`PREFIX/include/lance/lance.h` and `PREFIX/lib/liblance_c.so`. Expect about
230 MiB for the stripped library: lance-c statically links the whole Lance
Rust stack.

In the source tree, configure with `--with-lance` and point the include and
library search paths at lance-c:

```sh
./configure --with-lance --with-includes=/opt/lance-c/include \
    --with-libraries=/opt/lance-c/lib ...
make && make install
make -C contrib/lance_fdw install
```

Against an installed server, with PGXS:

```sh
make USE_PGXS=1 LANCE_C_PREFIX=/opt/lance-c
make USE_PGXS=1 LANCE_C_PREFIX=/opt/lance-c install
```

With `LANCE_C_PREFIX`, `lance_fdw.so` carries an rpath to `PREFIX/lib`;
otherwise `liblance_c.so` has to be where the dynamic loader looks, on every
host of the cluster. Then:

```sh
psql -c 'CREATE EXTENSION lance_fdw'
```

## Using it

```sql
CREATE SERVER lance_local FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri '/data/lance');

IMPORT FOREIGN SCHEMA lance LIMIT TO ("events.lance")
  FROM SERVER lance_local INTO public;

SELECT count(*) FROM public."events.lance";
```

Over S3 (or MinIO, or anything S3-compatible):

```sql
CREATE SERVER lance_s3 FOREIGN DATA WRAPPER lance_fdw
  OPTIONS (base_uri 's3://my-bucket/datasets',
           aws_endpoint 'http://minio.internal:9000',
           aws_region 'us-east-1',
           allow_http 'true');

CREATE USER MAPPING FOR CURRENT_USER SERVER lance_s3
  OPTIONS (aws_access_key_id '...', aws_secret_access_key '...');
```

MinIO needs both a region and an endpoint, and `allow_http` unless the endpoint
is HTTPS.

### How a scan runs

A Lance dataset is a set of *fragments*, and a fragment is the unit of
parallelism. Under the default `mpp_execute 'all segments'`:

- the coordinator opens the dataset once, pins the version it finds, lists the
  fragment ids and puts uri, version, ids and the planned segment count into
  the plan it dispatches. It reads no data itself;
- every segment takes its own share of that list — fragment `i` belongs to
  segment `(i + session_id % N + command_count) % N` — and opens the dataset at
  the version the coordinator pinned. The shares are disjoint and together
  complete, and no segment has to talk to any other to know that;
- `N` is that planned segment count, not the size of the cluster: a foreign
  table or its server may set `num_segments`, and Cloudberry then runs the scan
  on that many segments only, so the split has to use the same number. A
  `num_segments` larger than the cluster is refused, because such a gang
  repeats a segment and the two copies cannot tell themselves apart;
- a segment whose share is empty opens nothing at all, which is what a dataset
  with fewer fragments than segments costs.

Pinning the version is what makes one statement see one snapshot even while the
dataset is being appended to. Two statements are two snapshots: this is
statement-level, not transaction-level, unless the table names a `version`.

Only the columns a query refers to are read, including the ones a `WHERE`
clause reads: every qualifier is evaluated here.
A `count(*)` refers to no column and reads none; it counts the rows Lance
reports per batch.

`EXPLAIN` adds the resolved uri and the version to the plan, and `EXPLAIN
ANALYZE` adds the fragment count, since by then the coordinator has looked:

```
 Gather Motion 3:1  (slice1; segments: 3)
   ->  Foreign Scan on events
         Filter: (lower(name) = 'a'::text)
         Lance URI: s3://my-bucket/datasets/events.lance
         Lance Version: latest
         Lance Fragments: 12
```

A plain `EXPLAIN` opens nothing, so it costs no round trip to the object store
and works even against a server whose credentials are wrong.

### Options

**Foreign data wrapper**

| Option | Meaning |
|---|---|
| `mpp_execute` | `all segments` (the default the extension installs), `coordinator` or `any`; `master` is accepted as an older spelling of `coordinator`. Under `all segments` the coordinator enumerates fragments and each segment reads its own share; the other two make one process read everything. |

**Server**

| Option | Meaning |
|---|---|
| `base_uri` | Prefix that relative dataset names resolve against. Optional: a table may give an absolute uri instead. |
| `aws_endpoint` | Object store endpoint, for S3-compatible services. |
| `aws_region` | Region to sign with. |
| `allow_http` | Boolean; allow a plain-HTTP endpoint. |
| `virtual_hosted_style_request` | Boolean; use virtual-hosted-style addressing instead of path style. |
| `mpp_execute` | Overrides the wrapper's setting. |

**User mapping**

| Option | Meaning |
|---|---|
| `aws_access_key_id` | The access key identifier to sign requests with. |
| `aws_secret_access_key` | The secret half of that key pair. |
| `aws_session_token` | For temporary credentials. |

Credentials are read from the user mapping in whichever process needs them.
`lance_fdw` never puts them into a plan, an `EXPLAIN`, its own log lines or its
error messages. The one place they do appear is the `CREATE USER MAPPING`
statement itself, which PostgreSQL echoes into the server log like any other
DDL when `log_statement` is `ddl` or `all`. No wrapper can prevent that.

**Foreign table**

| Option | Meaning |
|---|---|
| `uri` | Required. Either absolute (`s3://...`, `file:///...`, `/abs/path`) or relative to the server's `base_uri`. |
| `version` | Dataset version to read. `0`, the default, means the latest at the time the statement runs. |
| `batch_size` | Rows per Arrow batch. Unset means lance-c decides, which is 8192 rows unless its own `LANCE_DEFAULT_BATCH_SIZE` says otherwise. Lower it for datasets with large binary columns. |
| `batch_size_bytes` | Bytes per Arrow batch, the bound a row count cannot give. Cannot be combined with `batch_size`: lance lets the byte limit win, so taking both would ignore one without saying so. See [Bounding memory](#bounding-memory). |
| `rows_hint` | Row estimate for the planner. Default 100000. Planning does no I/O, so this is the only way the planner can know better. |
| `mpp_execute` | Overrides the server's setting. |
| `num_segments` | Read by Cloudberry rather than by this wrapper, and it matters only under `mpp_execute 'all segments'`: there it narrows how many segments run the scan, the fragment split follows it, and a value above the number of segments in the cluster is refused, because such a gang repeats a segment and the two copies cannot be told apart. Under `coordinator` or `any` it has no effect and nothing is refused — one process reads every fragment, so there is no split to narrow. |

**Column**

| Option | Meaning |
|---|---|
| `column_name` | The Lance column this PostgreSQL column reads, when the two names differ. |

### GUCs

| Name | Default | Meaning |
|---|---|---|
| `lance_fdw.cpu_threads` | 0 | `LANCE_CPU_THREADS` for the Lance runtime; 0 leaves it at its own default, which scales with the core count. |
| `lance_fdw.io_threads` | 0 | `LANCE_IO_THREADS`; same. |
| `lance_fdw.index_cache_size_mb` | 64 | Per-backend Lance index cache. lance's own default is 6 GiB, which is far too much for one cache per backend. |
| `lance_fdw.metadata_cache_size_mb` | 32 | Per-backend Lance metadata cache. lance's own default is 1 GiB. |
| `lance_fdw.io_buffer_size_mb` | 0 | Bound on the I/O one scan buffers from storage; 0 leaves lance-c at its own default. Bounds buffered reads, not everything a scan allocates. |
| `lance_fdw.batch_readahead` | 0 | Batches decoded concurrently by one scan; 0 leaves lance-c at its own default. Multiplies whatever one batch costs. |
| `lance_fdw.track_memory` | `on` | Charge Arrow batches to Cloudberry's memory accounting, so the resource group and `gp_vmem_protect_limit` can see them. See [Bounding memory](#bounding-memory). |

Every one of these is `SUSET`, and they differ only
in when they are read. The thread counts and cache sizes are read once per
backend, before its first call into lance-c, so changing them mid-session has no
effect on that session. The memory bounds and `track_memory` are read later,
once per scan as it opens, so a `SET` reaches the next scan in the same backend.

### Bounding memory

A batch is the unit this wrapper decodes and holds, so bounding a scan means
bounding a batch. `batch_size` counts **rows**, which is a poor bound whenever
rows vary in width — and Lance rows vary a lot. The same 1024-row batch is a
few kilobytes of `int4`, about four megabytes of 1024-dimension `float32`
vectors, and unbounded once a blob column is in it. `batch_size_bytes` is the
bound that does not depend on the schema, which is why lance lets it take
precedence over the row count, and why setting both is refused here rather than
letting one of them be ignored in silence.

Two more bounds are per-backend GUCs rather than table options, because they
limit what one process may hold, not what one dataset looks like:
`lance_fdw.io_buffer_size_mb` caps the I/O a scan buffers from storage, and
`lance_fdw.batch_readahead` caps how many batches it decodes at once.

What one backend can hold during a scan is therefore roughly

    batch bytes x (1 + batch_readahead)  +  io_buffer_size

and under `mpp_execute 'all segments'` **every segment on the host holds that at
the same time**, so multiply by the segments per host to get what the machine
sees. None of the three is a hard allocator limit — `io_buffer_size` bounds
buffered reads, not everything a scan allocates — so treat the formula as the
shape of the growth, not a ceiling the kernel will enforce.

**The database cannot see this memory on its own.** A batch is allocated by
lance on the Rust side, not by `palloc`, so `gp_vmem_protect_limit`, the
resource group and the runaway detector all read a backend that looks nearly
idle while it holds hundreds of megabytes — and the OOM killer is what notices
instead. With `lance_fdw.track_memory` on, every batch is measured once its
Arrow buffers are bound and charged to the same ledger as everything else, so
running out becomes an ordinary `out of memory` error naming the knob to turn,
rather than a killed process.

Two honest limits on that number. It is charged **after** lance allocated: the
Arrow C Data Interface offers no way to ask first, so this detects rather than
prevents, and what bounds the overshoot is `batch_size_bytes`. And it counts
the buffers of the batch actually held — lance's readahead batches and the
pages behind them are not visible to it, so the figure is a floor rather than
the whole footprint.

One consequence surprises people: a query whose target list names no column of
the foreign table — `SELECT count(*)`, or a scan feeding only a function that
does not read one — projects nothing, so it reads no Arrow buffer and is
charged nothing. A ledger reading zero there is the projection pushdown
working, not the accounting failing.

`lance_fdw_memory()` reports two figures. `current_bytes` is what this backend
has on the ledger right now; it is zero between scans, and staying zero is the
property worth watching, because a reservation released twice or not at all
only shows up later, as an unrelated allocation being refused. `peak_bytes` is
its high-water mark since the backend started, and it is the one to size the
bounds against — a batch is held only between one row-producing call and the
next, so anything asking for the current value almost always finds zero. `lance_fdw_cache_stats()` reports hits, misses,
entries and bytes for the two caches above, which is the only way to size them
on evidence. Both are backend-local: on a segment they describe that segment's
process.

All three bounds default to leaving lance-c at its own defaults, so nothing
changes until one is set. The two GUCs are read once per scan as it opens, so a `SET`
reaches the next scan in the same backend; a segment reads its own value, which
means a cluster-wide bound belongs in `postgresql.conf` rather than in a session
`SET`.

### Deploying

**Threads.** A backend that opens a dataset grows to roughly 18 threads, and to
about 34 once it scans — measured on 16 cores. The count follows the core count
rather than the number of datasets, and it then stays put. One query on a
three-segment cluster pays it four times over: the coordinator opens the dataset
to list its fragments, and each of the three segments scans. With many
concurrent sessions this is the first thing to run out of, so set
`lance_fdw.cpu_threads` and `lance_fdw.io_threads` to something small (2 and 4,
say) if concurrency matters more to you than the throughput of one query.

**Caches.** `index_cache_size_mb` and `metadata_cache_size_mb` are per-backend
ceilings, not reservations: a backend grows into them as it reads manifests, and
100 sessions can hold 100 of them. That is why the defaults are 64 and 32 MiB
against lance's own 6 GiB and 1 GiB — one cache per backend is a different
proposition from one cache per process. This block never asks Lance for an
index, so the index cache has almost nothing to hold yet; the GUC is there for
when it does.

**Batches.** `batch_size` is in rows, and lance-c exposes no byte budget, so a
dataset with MiB-sized `binary` or `large_binary` values needs the row count
lowered by hand: the default of 8192 rows times a 1 MiB value is 8 GiB in
flight. A few dozen rows is a reasonable starting point for blob-like columns.

**Row estimates.** Planning does no I/O, so the planner starts from 100000 rows
for every Lance table. Where that is wrong enough to pick the wrong join order,
set `rows_hint` on the table.

### Types

`IMPORT FOREIGN SCHEMA` writes these; a hand-written foreign table may also
declare a wider type of the same family (an Arrow `int32` may be read into
`bigint`, for instance). "Readable" is whether this build can produce values
for the column — the rest import and plan, but refuse at scan time with a
message naming the column, the Arrow type and the declared type.

| Arrow | PostgreSQL | Readable |
|---|---|---|
| `bool` | `boolean` | yes |
| `int8`, `int16`, `uint8` | `smallint` | yes |
| `int32`, `uint16` | `integer` | yes |
| `int64`, `uint32` | `bigint` | yes |
| `float16`, `float32` | `real` | yes |
| `float64` | `double precision` | yes |
| `utf8`, `large_utf8` | `text` | yes |
| `binary`, `large_binary` | `bytea` | yes |
| `date32` | `date` | yes |
| `timestamp(s\|ms\|us\|ns)` without a zone | `timestamp(6)` | yes |
| `timestamp(s\|ms\|us\|ns)` with a zone | `timestamptz(6)` — the value is a UTC epoch and the zone name is not kept | yes |
| `decimal128(p,s)` | `numeric(p,s)` | yes |
| `fixed_size_list<float32, N>` | `real[]` | yes |
| `fixed_size_list<float64, N>` | `double precision[]` | yes |
| `list<T>` where T is anything below a list | that type's one-dimensional array | yes |
| `struct<...>` where every subfield is readable | a composite type `IMPORT FOREIGN SCHEMA` creates, named `lance_<table>_<field path>` | yes |
| `map<utf8\|large_utf8, T>` where T is a scalar above | `jsonb` | yes |

Widening is allowed only where it cannot lose anything, and a declaration that
would cost information is refused at the start of the scan rather than turned
into a wrong value. Four rules follow from that:

- **No narrowing.** `int32` into `bigint` yes, `int64` into `integer` no;
  `uint32` needs `bigint` for the same reason, `float64` does not fit `real`,
  and a length-limited `varchar(n)` is refused because enforcing the limit
  would mean truncating. A list gets these rules on its element, so
  `list<int64>` is `bigint[]` and not `integer[]`.
- **No loss of precision.** A `timestamp` column has to be able to hold
  microseconds, so `timestamp(6)` and a bare `timestamp` are accepted and
  `timestamp(3)` is not. A `decimal128(p,s)` needs a `numeric(P,S)` with
  `S >= s` and `P - S >= p - s`, or a plain `numeric`, which constrains
  nothing.
- **No crossing between the two timestamp types.** A zone on the Arrow type
  means `timestamptz` and no zone means `timestamp`; reading either as the
  other would reinterpret the instant, so both crossings are errors.
- **No rounding of nanoseconds.** PostgreSQL keeps microseconds, so a
  nanosecond value that is not a whole microsecond is an error on that value
  rather than a rounded result. A value outside PostgreSQL's date or timestamp
  range is an error for the same reason.

The two nested shapes have a rule each on top of those:

- **A struct is read as a whole or not at all.** Every subfield has to be
  readable, recursively; one B-tier subfield makes the whole column B-tier, and
  the message names it. There is no partial projection, because a composite
  value with fields missing for reasons the user cannot see is worse than a
  refusal. `IMPORT FOREIGN SCHEMA` creates one composite type per struct and
  one more per level of nesting, named `lance_<table>_<field path>` in lower
  case with everything outside `[a-z0-9_]` replaced by `_`; a nested struct's
  path is `<parent path>_<subfield>`, and a list of structs uses the column's
  own path. A name that is already taken is reused when the type is field for
  field the same and is an error when it is not.
- **A map is read as `jsonb`, and only with string keys.** A JSON object has no
  other kind of key, so `map<int32, ...>` is refused rather than printed into
  one, and a map whose values are themselves containers is refused rather than
  flattened. An absent key, a key whose value is JSON `null` and an empty map
  are three different things and stay that way.

Everything else is B-tier: `uint64`, `dictionary`, `duration` and the interval
types, `time32`/`time64`, `date64`, `decimal256`, `large_list`, lists of lists,
`fixed_size_binary`, the view types, unions — and any field carrying Lance's
`lance-encoding: blob` metadata, whatever its Arrow type says, because the
scanner returns a `struct{position, size}` descriptor for those rather than the
bytes. A Lance Blob v2 column is refused by a rule of its own for the same
reason: what a scan returns for it is a descriptor of offsets, not the payload,
and it is a struct of ordinary scalars that would otherwise be read as one.

B-tier is loud, never silent: `IMPORT FOREIGN SCHEMA` skips the column and says
so with a `NOTICE`, and referring to one from a hand-written foreign table is
an error. A dataset with no A-tier column at all cannot be imported.

`IMPORT FOREIGN SCHEMA` requires `LIMIT TO`. Lance has no catalog that lists
the datasets under a uri, and listing an S3 prefix by hand would be a second
dependency; naming the datasets is the honest version of that.

## Testing

`test/fixtures/` generates the Lance datasets the suites read, with pylance;
`test/feature/` holds the pg_regress suites. The suites read the datasets both
from a local directory and from an S3 bucket, and find both through
`test/regress.mk`, which is not part of the tree:

```make
LANCE_FIXTURE_DIR = /path/to/contrib/lance_fdw/test/fixtures/data
LANCE_S3_ENDPOINT = http://127.0.0.1:8333
LANCE_S3_BUCKET = lance-regress
LANCE_S3_REGION = us-east-1
LANCE_S3_KEY = ...
LANCE_S3_SECRET = ...
```

`ci/prepare-tests.sh` writes that file after generating the datasets, starting
a SeaweedFS S3 gateway on the local host and uploading the datasets to it,
which is what CI does. Then, against a running cluster:

```sh
make installcheck
```

The suites create their own database with the `C.UTF-8` locale, whatever the
cluster's: they compare text the way such a database does. They expect
`optimizer=off`.

The suites, in the order they run:

| Suite | What it covers |
|---|---|
| `install` | the extension's objects, the wrapper's `mpp_execute` default, the GUCs, and the helper functions the other suites use |
| `ddl` | every option accepted and every bad value rejected, `pg_dump` round trip, `DROP EXTENSION CASCADE` |
| `import` | `IMPORT FOREIGN SCHEMA` over `file://` and `s3://`, including the B-tier skips |
| `errors_ddl` | bad path, bucket, credentials and endpoint, seen from `IMPORT` |
| `scan_core` | values against the pylance reference output, deletions, empty and gapped fragment lists, schema evolution, `batch_size`, `count(*)`, joins both ways, `file://` and `s3://` |
| `parallel` | the split is complete and disjoint for 1, 2, 3, 7 and 100 fragments; `coordinator` and `any` agree with `all segments` |
| `snapshot` | version pinning, in both the rows and the fragment count, including a version whose successor deleted rows from existing fragments — the case an append-only history cannot tell apart on the segments |
| `explain` | what `EXPLAIN` and `EXPLAIN ANALYZE` say, and that a plain `EXPLAIN` needs no working credentials |
| `creds` | none of the three user mapping credentials is in a plan |
| `errors_scan` | storage failures during a scan, and every shape of type mismatch |
| `types` | all 31 columns of `types_all` against the pylance reference output, twice over at two batch sizes, plus the MiB-sized text and binary values |
| `types_errors` | the four strictness rules above, one case each, and the declarations they must not refuse |
| `types_nested` | the four struct shapes and the map column against the pylance reference output, under both execution modes and at a batch size that straddles a fragment, plus the composite types the import creates and their reuse on a second import |
| `types_nested_errors` | the refusals: a map with non-string keys, a struct with one B-tier subfield, a Blob v2 descriptor under both execution modes, and composite declarations that do not match |
| `sigmask` | read back from `/proc`: after a scan in this backend every lance-c thread blocks the signals a backend is driven by, and the main thread does not |

## Known limits

- The A-tier list in the type table above is complete; everything outside it is
  refused rather than guessed at.
- Lance's two blob encodings are both B-tier whatever the Arrow type says, but
  for two different reasons. A field carrying `lance-encoding: blob` metadata
  (Lance's v1 encoding) is a `large_binary` whose bytes **do** arrive with the
  scan — measured on values of 1, 2 and 4 MiB — so refusing it is a policy
  choice this block has not revisited, not a technical limit. A field carrying
  `ARROW:extension:name = lance.blob.v2` reaches the scanner as a five-field
  descriptor struct (`kind`, `position`, `size`, `blob_id`, `blob_uri`) with
  the extension name stripped, and lance-c v0.1.9 exposes no call that turns a
  descriptor back into a payload; it is refused by a rule of its own, which
  since struct columns became readable is what keeps it refused.
- **How a Blob v2 column is recognised on a scan is fragile.** The two schemas
  differ: `lance_dataset_schema()`, which `IMPORT FOREIGN SCHEMA` and the
  coordinator's projection check read, keeps the Arrow extension name, and that
  half of the rule is exact. The scanner's schema does not keep it, so a scan
  can only match the descriptor's five child names — and if Lance renames one
  or adds a sixth, the rule stops matching, in the direction that reads the
  descriptor as an ordinary struct and hands out file offsets as if they were
  data. Whether the stream schema can keep the extension name is open with
  upstream (issue #76). Today the descriptor's `position` child is `uint64`,
  which is B-tier on its own, so the column is refused even if the name match
  fails; that is a property of this version of the descriptor and not something
  to depend on.
- **A composite type created by `IMPORT FOREIGN SCHEMA` outlives the foreign
  table.** Dropping the table does not drop the types made for its struct
  columns — they are ordinary database objects, and PostgreSQL records no
  dependency that would take them with it. Re-importing reuses a type whose
  definition still matches, so the usual cycle leaves no debris; a dataset whose
  struct has changed shape leaves the old type behind, to be dropped by hand.
- The refusal of a nanosecond timestamp that is not a whole microsecond is
  implemented but untested: every timestamp in `test/fixtures` is
  microsecond-aligned, and the fixtures are not this package's to change.
- `IMPORT FOREIGN SCHEMA` requires `LIMIT TO`. Nothing in lance-c lists the
  datasets under a uri, so the names have to come from you.
- Snapshots are statement-level, not transaction-level: two statements in one
  transaction can read two versions of a dataset that is being appended to,
  unless the table names a `version`.
- Nothing is pushed down to Lance but the projection: every qualifier, limit
  and sort is evaluated by PostgreSQL.
- Rows are decoded one at a time into `Datum`s. Arrow batches are not handed to
  the executor as they are, so a scan is correct rather than fast.
- Fragments are handed to segments round-robin. lance-c exposes no per-fragment
  row count, so a dataset whose fragments differ wildly in size will skew.
- Cancellation takes effect at a batch boundary, so a single slow read from an
  object store delays it by however long that read takes.
- Storage errors can take seconds to arrive: the object store retries a refused
  connection with backoff, so an endpoint nothing listens on takes about ten
  seconds to fail rather than failing at once.
- The suites run with `optimizer=off`, so the plans they check are the
  PostgreSQL planner's. ORCA is built into the CI cluster, but no suite in this
  change scans a Lance table under it.
- lance-c is built with `panic = "unwind"`, which the revision CI builds
  requires. A panic it catches at its API boundary comes back as
  `LANCE_ERR_PANIC` and becomes a `lance:` error like any other; a double panic,
  a stack overflow or an allocation failure inside lance-c still aborts the
  process and takes the whole backend down with it. No suite raises a panic,
  because we could not construct one.

## Licence

Apache-2.0. `third_party/nanoarrow` is a vendored copy of Apache Arrow
nanoarrow 0.7.0 (Apache-2.0), with its symbols in the namespace `LanceFdw`.
