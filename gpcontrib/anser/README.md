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
- `condition_key` is an opaque string describing the filtered condition — today
  just `anser_rf_<condition_id>`, since the planner stamps the same key into
  both plan nodes at injection time. It exists for the case where the key has to
  be derived from the build's semantic identity instead (so that producer and
  consumer can find each other without having been planned together), which is
  why the wire format treats it as arbitrary bytes rather than as an identifier.

Channels live in a hash in the coordinator backend, created on first use and
dropped at `ExecutorEnd` (or on transaction abort). Since the merge and the
delivery both happen in that one process, the accumulator is an ordinary
`palloc`'d buffer.

### Giving up early

A bloom filter that is too small for its key count matches almost everything: it
costs a hash and `k` probes per probe row and eliminates nothing. Anser checks
for that at the three points where new information becomes available, and always
fails open — giving up means the query runs unfiltered, never that it runs wrong.

| Where | Knows | Check | Cost of giving up |
| --- | --- | --- | --- |
| Planner (`anserplan.c`) | the row estimate | bitset bits / estimated keys ≥ 4 | nothing — the nodes are never injected |
| Producer init (`anserfilter.c`) | the plan parameters | the filter fits the payload cap, and still ≥ 4 bits/key | one `palloc0`, freed immediately; cancel published on the first tuple, before the build side is scanned |
| Producer publish (`anserbloomproduce.c`) | the filter, fully built | estimated FPR (`fill^k`) ≤ 50% | the build scan, already spent; saves the network and every consumer's probing |
| Coordinator merge (`anserdispatch.c`) | the merged payload | fill ≤ 95% | the merge, already spent; saves delivery and probing |

The planner gate is the one that matters, because it is the only one that costs
nothing. The rest exist because a row estimate can be wrong, and the last one
exists because **folding changes the answer**: OR-ing three parts that are each
60% full gives a merged filter that is 94% full, so no producer can tell whether
the result will be useful.

One subtlety worth knowing if you touch the sizing: do **not** clamp the element
estimate to make a filter fit. `total_elems` is also what `optimal_k()` derives
the hash count from, so understating it misconfigures the filter — a 128M-row
build side declared as 33.5M gets `k=10` where the optimum is `k=3`, turning a
13% false positive rate into 38%. The keys all go in regardless of what was
written down. Clamp the *size*; keep the count honest.

### Payload types

A channel carries one **payload type**, declared on the wire and registered in
`anserpayload.c`. The transport itself moves opaque bytes and never branches on
what they mean; everything type-specific lives in one descriptor
(`AnserPayloadOps`):

| | `fold()` | `checksum_body` |
| --- | --- | --- |
| `B` — bloom filter | bitwise OR of equal-sized bitsets | yes |
| `-` — none (a subscription) | n/a | n/a |

`fold()` is how the coordinator reduces one part per producer to a single
payload; there is no generic answer, which is why each type supplies it. A row
count, for instance, would fold by summing.

`checksum_body` is about consequence, not size. A bloom filter fails
asymmetrically — a bit flipped 1 → 0 removes a key, so a joinable row is
rejected and the query silently returns too few rows — whereas a row count only
feeds a planning decision, where corruption costs a worse plan and never a wrong
answer. Types of the second kind opt out and pay nothing. (The header and
condition key are checksummed either way; that is ~100 ns and it is what
protects the routing fields.)

Adding a type is three steps, none of which touch the framing: define a code,
add a row to `AnserPayloadTable`, and pass the code from the producer and
consumer nodes. `anserpayload.h` has the details.

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
anser3 K T SSSSSSSSSS CCCCCCCCCC DDDDDDDDDD PPPPPPPPPP TTTTTTTTTT FFFF KKKK BBBBBBBBBB XXXXXXXX
^tag   ^ ^  session    command    condition  part       total      flags  keylen bodylen  crc
       |  \ payload type
        \ kind
<condition_key bytes><base64 body>
```

`kind` is `P` (a producer's part) or `S` (a consumer subscribing) — what the
message *does*, as opposed to the payload type, which is what it *carries*. The
header is **95 bytes of fixed-width ASCII**, so the key and the body begin at
offsets that nothing in their own contents can shift, and the two lengths must
account for the message exactly — a short, long or misaligned message is rejected
before any of it is used. There is deliberately no delimiter anywhere in the
format: the earlier version ended its header with a newline and found it with
`strchr()`, which was correct only while the key could not itself contain a
newline, an invariant nothing enforced.

**Coordinator → segment** is a `GP_SIDEBAND_MESSAGE`, written with `pqPutnchar`,
which performs no conversion — so the merged filter travels as **raw binary**,
with no base64 tax. That is the direction that matters most, since the merged
payload is sent once *per consumer* while each part is sent once.

Both directions carry a **CRC32C**, and both discard a message that fails it.
The transport is TCP, not the UDP interconnect, so this is not about a lossy
link — TCP's 16-bit checksum is simply thin cover for a megabyte, and hardware
CRC32C costs about 0.05 ms/MB. The header and key are always covered; whether
the body is covered too is the payload type's decision, for the reasons in
[Payload types](#payload-types) above. Where it applies, the segment →
coordinator CRC covers the pre-base64 bytes, so it validates the decode as well.

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
| `anser.debug` | `off` | USERSET | Traces the exchange — publish, merge, delivery, receive — in the log of the process each step happens in. See below. |

### Tracing an exchange

Until a filter is either used or timed out, none of the handoff is visible in
`EXPLAIN`: a consumer that waited and got nothing looks exactly like one whose
producers never published. `anser.debug` makes each step log where it happened,
which is normally the fastest way to find where an exchange broke:

```
seg0  producer init cond=0 part=0/3 elems=3334 payload=67108928 state=ok
seg0  producer child exhausted, publishing (state=ok)
seg0  published cond=0 part=0/3 bytes=1048592 cancelled=0 sent=1
QD    part cond=0 from seg0 (says part 0 of 3) 1/3 bytes=1048592 -> collecting
QD    part cond=0 from seg1 (says part 1 of 3) 2/3 bytes=1048592 -> collecting
QD    part cond=0 from seg2 (says part 2 of 3) 3/3 bytes=1048592 -> complete
QD    delivering cond=0 to 3 subscriber(s)
QD    pushed cond=0 bytes=1048592 cancelled=0
seg0  received cond=0 bytes=1048592 cancelled=0
```

Set it in `postgresql.conf` (`gpconfig -c anser.debug -v on`) rather than with
`SET` if you need to see the producer gang: a session-level `SET` does not
reliably reach every gang, and the producers are the half you usually want.

A consumer that gives up reports what it saw — `read 0 message(s), 0
unclaimed` means nothing arrived at all, whereas unclaimed messages mean
something arrived for a channel it was not waiting on.

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
   folded, the merged payload is judged once (see [Giving up
   early](#giving-up-early)) and then pushed to each subscriber. Delivery is per
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

## Tests

`make installcheck` runs two suites:

- `anser_test` — the payload protocol (serialize, fold, reject a mismatched
  part) and the four give-up decisions, each walked along its boundary: the
  estimate either side of 4 bits/key, the payload cap either side of "1 MB plus
  a header", fill either side of the 95% limit, and a producer whose filter
  saturates. The case tables live in the `.sql` file, so the expected output
  records real sizes rather than a bare `t`.
- `anser_runtime_filter` — plan-tree integration end to end: the nodes are
  injected, and query results are identical with the feature on and off.
