# Anser — adaptive information sharing

Anser is a runtime pub/sub facility for MPP query execution. Producers on the
segments publish a small piece of information about a running query (today: a
bloom filter over a join-build key), the coordinator combines the per-segment
parts into one, and consumers on the segments receive it and use it to prune
work (today: skip probe rows that cannot join).

Everything travels over the **dispatch connection the coordinator already holds
open to every segment**. There is no shared memory, no background worker, no
second connection and nothing extra to authenticate: a channel exists only in
the coordinator backend running the query, for exactly as long as that query
runs.

This document covers installation, the architecture, the wire protocol, the
configuration surface, and what a *channel* is. For the plan-tree integration
see `anserplan.c`; for the payload/bloom protocol see `anserfilter.c` and
`lib/bloomfilter.c`.

## Installation

Anser is configured entirely by GUCs — it creates no catalog objects, so there
is nothing to `CREATE EXTENSION` in each database.

```
gpconfig -c shared_preload_libraries -v "'anser'" --skipvalidation
gpconfig -c anser.enable -v on
gpstop -ra
```

The library **must** be preloaded: a segment backend deserializing a dispatched
plan has no opportunity to load it on demand, and its CustomScan providers have
to be registered before that happens.

Then turn the filter on where you want it — `anser.runtime_filter` is
`PGC_USERSET`, so per session, per role, or cluster-wide.

`anser_test` is a separate control file over the same library, exposing the
internal C API to the regression tests. It is test-only; do not create it in
production databases.

## Architecture

A **channel** is one rendezvous point between the producers and consumers of a
single piece of runtime information, for a single query:

```
AnserChannelKey = { gp_session_id, gp_command_count, condition_id, condition_key[64] }
```

- `gp_session_id` + `gp_command_count` scope the channel to one query execution,
  so keys never collide across sessions or across statements in a session.
- `condition_id` distinguishes multiple filters within the same query.
- `condition_key` is an opaque string describing the filtered condition (today a
  synthetic `rf:<build>.<attno>=<probe>.<attno>` string). Both sides derive it
  independently and must agree — it is what makes a producer and a consumer meet
  on the same channel.

Channels live in a hash in the coordinator backend, created on first use and
dropped at `ExecutorEnd` (or on transaction abort). Since the merge and the
delivery both happen in that one process, the accumulator is an ordinary
`palloc`'d buffer.

### How it attaches to the server

Everything is reached through an existing extensibility point, so the server
carries no Anser-specific code (`anserinit.c`):

| Hook | Used for |
| --- | --- |
| `planner_hook` | the injection pass, on the finished plan; wrapping the hook covers ORCA too, since it is dispatched from inside `standard_planner()` |
| `RegisterCustomScanMethods` | the producer/consumer nodes, so their methods resolve by name in every backend that deserializes a dispatched plan |
| `cdbdisp_notify_hook` | parts and subscriptions arriving from segments |
| `ExecutorEnd_hook` | dropping a query's channels |
| `DefineCustom*Variable` | the `anser.*` GUCs below |

The first two already existed; `cdbdisp_notify_hook` and the
`GP_SIDEBAND_MESSAGE` tolerance in the QE command loop are the only additions
Anser needed in the server, and neither mentions Anser.

## Wire protocol

The two directions are deliberately asymmetric, because the constraints differ.

**Segment → coordinator** is a `NOTIFY` on channel `anser_rf`, the model
`nextval()` uses (`cdb_sequence_nextval_qe` in `commands/sequence.c`). The QD is
a libpq *client*, and libpq rejects message types it does not know, so this
direction has to be a message type libpq already understands. `NotifyMyFrontEnd`
imposes no length limit of its own — the ~8 KB `NOTIFY_PAYLOAD_MAX_LENGTH`
applies to the SQL-level `NOTIFY`, which must fit a queue page — but it delivers
through `pq_sendstring`, so the payload must be a NUL-free string:

```
anser1 <kind> <ssid> <ccnt> <condid> <part> <total> <flags> <keylen> <bodylen>\n
<condition_key><base64 body>
```

`kind` is `P` (a producer's part) or `S` (a consumer subscribing). The header
holds only numbers and one character, so it cannot contain the newline that ends
it; key and body are taken by length, so neither needs escaping.

**Coordinator → segment** is a `GP_SIDEBAND_MESSAGE`, written with `pqPutnchar`,
which performs no conversion — so the merged filter travels as **raw binary**,
with no base64 tax. That is the direction that matters most, since the merged
payload is sent once *per consumer* while each part is sent once.

The coordinator services these while it is blocked receiving tuples: the
interconnect adds every dispatch socket to its wait set
(`ic_udpifc.c`, `ic_tcp.c`), and a readable one leads to
`checkForCancelFromQD` → `cdbdisp_checkForCancel` → `processResults`, which is
where the notify handler runs. Under `gp_interconnect_type=proxy` that check is
driven by a 2 s timer instead (`ic_proxy_backend.c`), so filter delivery can lag
by up to that long.

## GUCs

| GUC | Default | Context | Meaning |
| --- | --- | --- | --- |
| `anser.enable` | `off` | SIGHUP | Master switch. With it off the plan pass never injects anything and no filters are exchanged. |
| `anser.runtime_filter` | `off` | USERSET | Enables the post-planning pass that injects bloom-filter producer/consumer nodes into a matching plan. Requires `anser.enable`. |
| `anser.max_info_size` | `65 MB` | POSTMASTER | Maximum serialized payload (merged bloom filter + part header) a channel may hold; caps the effective bloom-filter size. The default is `64 MB + 1 MB` so a full 64 MB power-of-two bitset fits with its header; `bloom_create` also floors every bitset at 1 MB. |
| `anser.timeout_ms` | `1000` | USERSET | How long a consumer waits for its filter before running unfiltered. The deadline matters because a producer that gets squelched never publishes at all: `ExecSquelchNode` only marks a `CustomScanState`, it does not call the node back. |

## Data flow: producer → merge (bitwise union) → consumer

The parts from all segments are combined into **one** payload by a **bitwise OR
on the coordinator**, and that single combined payload is delivered to every
consumer. This is the core of Anser and worth stating precisely, because it is
*not* a concatenation:

```
segment 0 producer:  bitset 0000 0001  ┐
segment 1 producer:  bitset 0000 0010  ├─ NOTIFY ─► QD backend (notify hook)
segment N producer:        ...         ┘             │
                                                     │  fold each part into the
                                                     │  running merged bitset:
                                                     │     0000 0001
                                                     │  OR 0000 0010
                                                     ▼  = 0000 0011   (one part)
                                            channel payload = single merged bitset
                                                     │
                            sideband push ───────────┼───────────────┐
                                       ▼             ▼               ▼
                              consumer seg 0   consumer seg 1 ... consumer seg N
                              each receives the SAME combined 0000 0011
```

Step by step:

1. **Produce (per segment, in parallel).** Each segment's producer builds a bloom
   filter over its local build keys (`bloom_create` from `total_elems` /
   `max_payload` / a `condition_key`-derived seed, all carried in the plan node —
   *not* on the wire) and serializes it as one *part*. Because every producer and
   the consumer pass the identical parameters, every part has a byte-for-byte
   identical size and shape. Publishing is fire-and-forget: the producer sends
   its part and carries on without waiting for an acknowledgement.

2. **Merge (coordinator, once per part).** The coordinator never reconstructs a
   filter — it works on raw bytes. The **first** part is stored verbatim; every
   later part is folded into the channel's payload with an in-place **bitwise OR**
   of the bitset (`AnserBloomFoldPartInPlace` in `anserfilter.c`, called from
   `anserdispatch.c`). The payload is therefore always a **single merged bitset**,
   the size of one filter — it does **not** grow with the segment count. Folding
   happens as parts arrive, so only the last fold is on the critical path. The OR
   requires the incoming part to be the same size as the accumulator (guaranteed
   by the shared parameters); a mismatch cancels the channel and consumers fail
   open.

3. **Deliver (coordinator → every consumer).** Once every expected part is
   folded, the merged bitset is pushed to each subscriber. Delivery is per
   consumer: a failed write costs that one segment its filter and leaves the
   others alone. A consumer that subscribes *after* the channel completed — which
   happens routinely, since producers on other segments may finish first — is
   served immediately.

4. **Consume (per segment).** Each consumer rebuilds an empty filter from its own
   plan parameters (the same `total_elems` / `max_payload` / seed the producers
   used) and loads the received bitset into it (`AnserBloomDeserializePart`),
   requiring the received length to match exactly (else it fails open). It does
   **not** re-union anything.

Correctness note: the combined filter is the OR (super-set) of every segment's
build keys, so it can only ever have *false positives*, never false negatives —
a probe row it rejects genuinely cannot join. Anser therefore only changes
performance, never results; any failure along this path degrades to "no filter"
(fail open).

## Failure handling

Every failure mode ends in unfiltered execution, never in a wrong answer and
never in an error raised into the query:

- a producer that is squelched, errors, or produces an oversized part → the
  channel never completes (or is cancelled) → consumers hit `anser.timeout_ms`
  and run unfiltered;
- a malformed or undecodable part → the channel is cancelled and every consumer
  is told so;
- a broken connection → the query is failing anyway, and the interconnect
  reports it far more usefully than the filter path could;
- query cancellation → the consumer's wait is a `CHECK_FOR_INTERRUPTS` loop, and
  a delivery that arrives after nobody is waiting is discarded by the QE command
  loop (`GP_SIDEBAND_MESSAGE` is accepted and ignored there).
