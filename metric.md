# YAGP Hooks Collector Metrics

## Introduction

### States  
A Postgres process goes through 4 executor functions to execute a query:  
1) `ExecutorStart()` - resource allocation for the query.  
2) `ExecutorRun()` - query execution.  
3) `ExecutorFinish()` - cleanup.  
4) `ExecutorEnd()` - cleanup.  

yagp-hooks-collector sends messages with 4 states, from _Dispatcher_ and/or _Execute_ processes: `submit`, `start`, `end`, `done`, in this order:  
```
submit -> ExecutorStart() -> start -> ExecutorRun() -> ExecutorFinish() -> end -> ExecutorEnd() -> done
```

## Key Points  
- Some queries may skip the _end_ state, then the _end_ statistics is sent during _done_.  
- If a query finishes with an error (`METRICS_QUERY_ERROR`), or is cancelled (`METRICS_QUERY_CANCELLED`), statistics is sent at _done_.  
- Some statistics is calculated as the difference between the current global metric and the previous. The initial snapshot is taken at submit, and at _end_/_done_ the diff is calculated.  
- Nested queries on _Dispatcher_ become top-level on _Execute_.  
- Each process (_Dispatcher_/_Execute_) sends its own statistics.  

## Notations  
- **S** = Submit event.  
- **T** = Start event.  
- **E** = End event.  
- **D** = Done event.  
- **DIFF** = current_value - submit_value (submit event).  
- **ABS** = Absolute value, or where diff is not applicable, the value taken.  
- **Local*** - Statistics that starts counting from zero for each new query. A nested query is also considered new.  

## Statistics Table

| Proto Field                  | Type   | When    | DIFF/ABS | Local* | Scope   | Dispatcher | Execute | Units   | Notes                                               |
| :--------------------------- | :----- | :------ | :------- | ------ | :------ | :--------: | :-----: | :------ | :-------------------------------------------------- |
| **SystemStat**               |        |         |          |        |         |            |         |         |                                                     |
| `runningTimeSeconds`         | double | E, D    | DIFF     | -      | Node    |     +      |    +    | seconds | Wall clock time                                     |
| `userTimeSeconds`            | double | E, D    | DIFF     | -      | Node    |     +      |    +    | seconds | /proc/pid/stat utime                                |
| `kernelTimeSeconds`          | double | E, D    | DIFF     | -      | Node    |     +      |    +    | seconds | /proc/pid/stat stime                                |
| `vsize`                      | uint64 | E, D    | ABS      | -      | Node    |     +      |    +    | pages   | /proc/pid/stat vsize                                |
| `rss`                        | uint64 | E, D    | ABS      | -      | Node    |     +      |    +    | pages   | /proc/pid/stat rss                                  |
| `VmSizeKb`                   | uint64 | E, D    | ABS      | -      | Node    |     +      |    +    | KB      | /proc/pid/status VmSize                             |
| `VmPeakKb`                   | uint64 | E, D    | ABS      | -      | Node    |     +      |    +    | KB      | /proc/pid/status VmPeak                             |
| `rchar`                      | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | /proc/pid/io rchar                                  |
| `wchar`                      | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | /proc/pid/io wchar                                  |
| `syscr`                      | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | count   | /proc/pid/io syscr                                  |
| `syscw`                      | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | count   | /proc/pid/io syscw                                  |
| `read_bytes`                 | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | /proc/pid/io read_bytes                             |
| `write_bytes`                | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | /proc/pid/io write_bytes                            |
| `cancelled_write_bytes`      | uint64 | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | /proc/pid/io cancelled_write_bytes                  |
| **MetricInstrumentation**    |        |         |          |        |         |            |         |         |                                                     |
| `ntuples`                    | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | tuples  | Accumulated total tuples                            |
| `nloops`                     | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | count   | Number of cycles                                    |
| `tuplecount`                 | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | tuples  | Accumulated tuples per cycle                        |
| `firsttuple`                 | double | E, D    | ABS      | +      | Node    |     +      |    +    | seconds | Time for first tuple of this cycle                  |
| `startup`                    | double | E, D    | ABS      | +      | Node    |     +      |    +    | seconds | Start time of current iteration                     |
| `total`                      | double | E, D    | ABS      | +      | Node    |     +      |    +    | seconds | Total time taken                                    |
| `shared_blks_hit`            | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Shared buffer blocks found in cache                 |
| `shared_blks_read`           | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Shared buffer blocks read from disk                 |
| `shared_blks_dirtied`        | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Shared blocks dirtied                               |
| `shared_blks_written`        | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Dirty shared buffer blocks written to disk          |
| `local_blks_hit`             | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Local buffer hits                                   |
| `local_blks_read`            | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Disk blocks read                                    |
| `local_blks_dirtied`         | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Local blocks dirtied                                |
| `local_blks_written`         | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Local blocks written to disk                        |
| `temp_blks_read`             | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Temp file blocks read                               |
| `temp_blks_written`          | uint64 | E, D    | ABS      | +      | Node    |     +      |    +    | blocks  | Temp file blocks written                            |
| `blk_read_time`              | double | E, D    | ABS      | +      | Node    |     +      |    +    | seconds | Time reading data blocks                            |
| `blk_write_time`             | double | E, D    | ABS      | +      | Node    |     +      |    +    | seconds | Time writing data blocks                            |
| `inherited_calls`            | uint64 | E, D    | ABS      | -      | Node    |     +      |    +    | count   | Nested query count (YAGPCC-specific)                |
| `inherited_time`             | double | E, D    | ABS      | -      | Node    |     +      |    +    | seconds | Nested query time (YAGPCC-specific)                 |
| **NetworkStat (sent)**       |        |         |          |        |         |            |         |         |                                                     |
| `sent.total_bytes`           | uint32 | D       | ABS      | -      | Node    |     +      |    +    | bytes   | Bytes sent, including headers                       |
| `sent.tuple_bytes`           | uint32 | D       | ABS      | -      | Node    |     +      |    +    | bytes   | Bytes of pure tuple-data sent                       |
| `sent.chunks`                | uint32 | D       | ABS      | -      | Node    |     +      |    +    | count   | Tuple-chunks sent                                   |
| **NetworkStat (received)**   |        |         |          |        |         |            |         |         |                                                     |
| `received.total_bytes`       | uint32 | D       | ABS      | -      | Node    |     +      |    +    | bytes   | Bytes of pure tuple-data received                   |
| `received.tuple_bytes`       | uint32 | D       | ABS      | -      | Node    |     +      |    +    | bytes   | Bytes of pure tuple-data received                   |
| `received.chunks`            | uint32 | D       | ABS      | -      | Node    |     +      |    +    | count   | Tuple-chunks received                               |
| **InterconnectStat**         |        |         |          |        |         |            |         |         |                                                     |
| `total_recv_queue_size`      | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | bytes   | Receive queue size sum                              |
| `recv_queue_size_counting_t` | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | count   | Counting times when computing total_recv_queue_size |
| `total_capacity`             | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | bytes   | the capacity sum for sent packets                   |
| `capacity_counting_time`     | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | count   | counting times used to compute total_capacity       |
| `total_buffers`              | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | count   | Available buffers                                   |
| `buffer_counting_time`       | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | count   | counting times when compute total_buffers           |
| `active_connections_num`     | uint64 | D       | DIFF     | -      | Node    |     +      |    +    | count   | Active connections                                  |
| `retransmits`                | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Packet retransmits                                  |
| `startup_cached_pkt_num`     | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Startup cached packets                              |
| `mismatch_num`               | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Mismatched packets received                         |
| `crc_errors`                 | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | CRC errors                                          |
| `snd_pkt_num`                | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Packets sent                                        |
| `recv_pkt_num`               | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Packets received                                    |
| `disordered_pkt_num`         | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Out-of-order packets                                |
| `duplicated_pkt_num`         | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Duplicate packets                                   |
| `recv_ack_num`               | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | ACKs received                                       |
| `status_query_msg_num`       | int64  | D       | DIFF     | -      | Node    |     +      |    +    | count   | Status query messages sent                          |
| **SpillInfo**                |        |         |          |        |         |            |         |         |                                                     |
| `fileCount`                  | int32  | E, D    | DIFF     | -      | Node    |     +      |    +    | count   | Spill (temp) files created                          |
| `totalBytes`                 | int64  | E, D    | DIFF     | -      | Node    |     +      |    +    | bytes   | Spill bytes written                                 |
| **QueryInfo**                |        |         |          |        |         |            |         |         |                                                     |
| `generator`                  | enum   | T, E, D | ABS      | -      | Cluster |     +      |    -    | enum    | Planner/Optimizer                                   |
| `query_id`                   | uint64 | T, E, D | ABS      | -      | Cluster |     +      |    -    | id      | Query ID                                            |
| `plan_id`                    | uint64 | T, E, D | ABS      | -      | Cluster |     +      |    -    | id      | Hash of normalized plan                             |
| `query_text`                 | string | S       | ABS      | -      | Cluster |     +      |    -    | text    | Query text                                          |
| `plan_text`                  | string | T       | ABS      | -      | Cluster |     +      |    -    | text    | EXPLAIN text                                        |
| `template_query_text`        | string | S       | ABS      | -      | Cluster |     +      |    -    | text    | Normalized query text                               |
| `template_plan_text`         | string | T       | ABS      | -      | Cluster |     +      |    -    | text    | Normalized plan text                                |
| `userName`                   | string | All     | ABS      | -      | Cluster |     +      |    -    | text    | Session user                                        |
| `databaseName`               | string | All     | ABS      | -      | Cluster |     +      |    -    | text    | Database name                                       |
| `rsgname`                    | string | All     | ABS      | -      | Cluster |     +      |    -    | text    | Resource group name                                 |
| `analyze_text`               | string | D       | ABS      | -      | Cluster |     +      |    -    | text    | EXPLAIN ANALYZE JSON                                |
| **AdditionalQueryInfo**      |        |         |          |        |         |            |         |         |                                                     |
| `nested_level`               | int64  | All     | ABS      | -      | Node    |     +      |    +    | count   | Current nesting level                               |
| `error_message`              | string | D       | ABS      | -      | Node    |     +      |    +    | text    | Error message                                       |
| `slice_id`                   | int64  | All     | ABS      | -      | Node    |     +      |    +    | id      | Slice ID                                            |
| **QueryKey**                 |        |         |          |        |         |            |         |         |                                                     |
| `tmid`                       | int32  | All     | ABS      | -      | Node    |     +      |    +    | id      | Time ID                                             |
| `ssid`                       | int32  | All     | ABS      | -      | Node    |     +      |    +    | id      | Session ID                                          |
| `ccnt`                       | int32  | All     | ABS      | -      | Node    |     +      |    +    | count   | Command counter                                     |
| **SegmentKey**               |        |         |          |        |         |            |         |         |                                                     |
| `dbid`                       | int32  | All     | ABS      | -      | Node    |     +      |    +    | id      | Database ID                                         |
| `segment_index`              | int32  | All     | ABS      | -      | Node    |     +      |    +    | id      | Segment index (-1=coordinator)                      |

---

