# Parallel sequential scan of AOCS tables

How the parallel workers of one segment share the scan of an AOCS table.
Segment file and block formats: `src/backend/access/appendonly/README.md`.


## 1. Before and after

```
BEFORE: one worker per segment file

   segfile 1  |==================================================|  <- worker A reads it all
   segfile 2  |==========|                                          <- worker B
                                                                       workers C, D: idle

AFTER: segment files cut into chunks, workers take chunks from one counter

   segfile 1  |-c0-|-c1-|-c2-|-c3-|-c4-|-c5-|-c6-|-c7-|-c8-|-c9-|
   segfile 2  |-c10-|-c11-|
                A    B    C    D    A    C    B    D    A    ...
```


## 2. Why not split by block, like heap?

```
HEAP: one file, fixed 8 KB pages, each page holds whole rows

   |pg 0|pg 1|pg 2|pg 3|pg 4|      page N is at N * 8192  -> hand out page ranges


AOCS: one file PER COLUMN, variable-length blocks, boundaries differ per column

   column a (int)   |    blk 0: rows 1-8181     |   blk 1: rows 8182-16362   | ...
   column c (text)  | blk 0: 1-5210 | blk 1: 5211-9885 | blk 2: 9886-14560 | ...
                                    ^
                    "block 1" is different rows in each column  -> no common block unit

   offset of block N = ?            (depends on compression and values)
   row number of row N = ?          (gp_fastsequence leaves gaps)
```

The only coordinate every column file agrees on is the row's position in the
segment file:

```
   ordinal:          0     1     2     3     4   ...
   column a file:   a0    a1    a2    a3    a4   ...
   column b file:   b0    b1    b2    b3    b4   ...
   column c file:   c0    c1    c2    c3    c4   ...
                    \_____ row 3 = 3rd value of every column file
```


## 3. Chunk layout

```
pg_aoseg.pg_aocsseg_<oid>  (one row per segment file, read with the scan's snapshot)

   segno | tupcount | state
   ------+----------+------
     1   |   35116  |  1
     2   |   66653  |  1
     3   |   99877  |  1          AWAITING_DROP or tupcount = 0 -> skipped

rows_per_chunk = max(total_rows / 2048, 16384) = max(201646 / 2048, 16384) = 16384

             ordinal 0      16384      32768  35116
   segfile 1        |-- c0 --|-- c1 --|-c2-|                       3 chunks

             ordinal 0      16384      32768      49152      65536 66653
   segfile 2        |-- c3 --|-- c4 --|-- c5 --|-- c6 --|c7|       5 chunks

             ordinal 0      16384             81920      98304 99877
   segfile 3        |-- c8 --|-- c9 --|  ...  |-- c13 --|c14|      7 chunks
```

- A chunk never spans two segment files.
- The last chunk of a segment file is short.
- `tupcount` includes deleted rows, so chunks cover every row physically stored.


## 4. Shared and local state

```
SHARED: ParallelBlockTableScanDescData  (relscan.h, the same struct heap uses)
+--------------------------------------------------------------+
| base             relid, snapshot offset, ...                 |
| phs_nblocks      15     <- for AOCS: total number of chunks   |
| phs_nallocated   atomic counter: chunks handed out so far     |
| phs_startblock, phs_mutex   not used                          |
+--------------------------------------------------------------+

LOCAL, in every process: AOCSScanDescData  (cdbaocsam.h)
+--------------------------------------------------------------+
| seginfo[]        this process's own pg_aocsseg rows           |
| rows_per_chunk   16384                                        |
| chunk_first[]    0, 3, 8   (first chunk of seginfo[0..2])     |
| chunk_seg, chunk_end_row, segrowsprocessed                    |
| chunks_claimed, rows_claimed   (for the DEBUG1 summary)       |
+--------------------------------------------------------------+
```

Every process reads `pg_aocsseg` with the same snapshot, and
`GetAllAOCSFileSegInfo()` sorts the rows by `segno`, so every process computes
the same layout.  The shared state holds only the chunk count, which each
process checks against its own.


## 5. Who does what

```
            FIRST PROCESS (once per segment)              EVERY PROCESS
            --------------------------------              -------------
ExecSeqScanInitializeDSM()
  table_parallelscan_initialize()
    aoco_parallelscan_initialize()   reset descriptor
    GetAllAOCSFileSegInfo(snapshot)  read pg_aocsseg
    phs_nblocks = aocs_parallelscan_nchunks()
                  |
                  |  shared memory                 aocs_beginscan()
                  v                                  GetAllAOCSFileSegInfo(snapshot)
      +------------------+                           aocs_parallelscan_init_layout()
      | phs_nblocks      |  <---- check ----------     compute chunk_first[] locally
      | phs_nallocated   |  <-------------+
      +------------------+                |      aocs_getnext*()
                                          |        open_next_scan_seg()
                                          +-------   claim_next_chunk()
                                                       fetch_add(phs_nallocated)
                                                       open segfile if needed
                                                       aocs_skip_rows()
                                                     scan rows until aocs_chunk_done()
```


## 6. Claiming a chunk

```
chunk = fetch_add(phs_nallocated, 1) = 5     (>= phs_nblocks -> scan finished)

chunk_first[]:  0      3      8          (local, one entry per seginfo[])
                |      |      |
                s1     s2     s3        binary search: 3 <= 5 < 8 -> seginfo[1], segfile 2

start = (5 - 3) * 16384 = 32768
end   = min(32768 + 16384, 66653) = 49152

-> scan ordinals [32768, 49152) of segfile 2
```

Empty and AWAITING_DROP segment files get no chunks, so their entry in
`chunk_first[]` equals the next one, and the search (last entry not after the
chunk number) never picks them.


## 7. Finding the start of a chunk in each column file

Each block header is never compressed and stores the block's row count and
length:

```
 one block
 +----------------------------------+---------------------------------------+
 | header: rowCount, length,        | content: values, maybe compressed     |
 |         compressedLength         | (zstd / zlib / lz4 / rle_type)        |
 +----------------------------------+---------------------------------------+
   read only this to skip a block     decompressed only for the target block
```

Positioning every projected column at ordinal 32768 (`aocs_skip_rows()`):

```
column a (int)
 rows:  0-8180   8181-16361  16362-24542  24543-32723  32724-40904
       | blk 0  |  blk 1    |  blk 2     |  blk 3     |  blk 4     | ...
         header    header      header       header       header
         skip      skip        skip         skip         LOAD, skip 44 values inside
                                                              ^ 32768

column c (text)
 rows:  0-2399 2400-4799  ...  28800-31199  31200-33599
       | b0   | b1      | ... | b12       | b13        | ...
         skip   skip      ...   skip        LOAD, skip 1568 values inside
                                                 ^ 32768

=> next datumstreamread_advance() on every column returns row 32768
```

```
for each projected column:
    left = start - rows already consumed
    +-> is the target in the current block? -- yes --> datumstreamread_find()
    |        | no
    |   read next header (datumstreamread_block_info)
    |   target in this block? -- yes --> decompress it (datumstreamread_block_content)
    |        | no                        then datumstreamread_find()
    |   left -= rowCount
    +-- jump over the content (AppendOnlyStorageRead_SkipCurrentBlock)
```


## 8. A worker only moves forward

```
worker A claims c0, then c4, then c9 (other workers took the ones in between)

segfile 2   |-- c3 --|-- c4 --|-- c5 --|-- c6 --|c7|
                     [ scan  ]  skip     skip
                              ^ position kept in segrowsprocessed

segfile 3   |-- c8 --|-- c9 --| ...
              skip    [ scan ]

- chunk numbers only increase -> the next chunk is always ahead
- each column file's headers are walked at most once per worker
- new segfile -> open it, start from its first block
```


## 9. Ending a chunk

```
top of every scan loop (aocs_getnext_noqual / _withqual / _sample):

   +------------------------------------------------+
   | cur_seg < 0  OR  err < 0  OR  aocs_chunk_done() | --yes--> open_next_scan_seg()
   +------------------------------------------------+              -> claim_next_chunk()
                    | no
                    v
           advance every column by one row
           segrowsprocessed++
           visible in visimap? -- no --> next row
                    | yes
                    v
                return row

aocs_chunk_done():  parallel scan  AND  segrowsprocessed >= chunk_end_row
```


## 10. Deleted rows, row numbers, compression

```
DELETE: data and tupcount unchanged, row marked in the visibility map

   chunk:     |------------------ c5 ------------------|
   rows:       r  r  x  r  x  x  x  r  r  r  x  r  r  r      x = deleted
               scanned like any row, dropped by the visimap check

ROW NUMBERS: used after positioning, never for positioning

   blk 0: firstRowNum 1,    rowCount 100   -> ordinals 0-99,    rownums 1-100
   blk 1: firstRowNum 201,  rowCount 100   -> ordinals 100-199, rownums 201-300
                                              (gap 101-200: aborted insert)
   ctid / visimap lookup use rownum = blk.firstRowNum + position in block

COMPRESSION: headers are plain, content is compressed

   skipped block  -> header only, never decompressed
   target block   -> decompressed once
   rle_type       -> more rows per block, longer skip inside the target block
   large value    -> spans several blocks, counts as one row, skipped as a unit
```


## 11. Limitations

```
Skipping still reads the bytes (no seek):

   worker A  |>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>|   reads ~ whole column file
   worker B  |>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>|
   worker C  |>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>|   => ~ W x file bytes in total
   worker D  |>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>|      (page cache if it fits in RAM)

With a block directory (table has an index), a worker could seek instead:

   pg_aoblkdir_<oid>: (segno, column, first_row_no) -> minipage
                       minipage entry {firstRowNum, fileOffset, rowCount}

   GetEntry(segno, rownum, col) -> fileOffset -> seek -> load one block
   (keyed by row NUMBER, so chunk boundaries would come from directory entries)
```

- One chunk per claim; no smaller chunks near the end of the scan (heap does both).
- AO row tables still split by whole segment file.
- Building a block directory during a scan (`scan->blockDirectory`) is not supported in parallel.


## 12. Code map

```
src/include/access/relscan.h      ParallelBlockTableScanDescData: AOCS meaning of
                                  phs_nblocks and phs_nallocated (comment only)
src/include/cdb/cdbaocsam.h       AOCSScanDescData: rows_per_chunk, chunk_first,
                                  chunk_seg, chunk_end_row, chunks_claimed, rows_claimed
                                  aocs_parallelscan_nchunks() declaration

src/backend/access/table/tableam.c
    table_parallelscan_initialize(): phs_nblocks = aocs_parallelscan_nchunks()

src/backend/access/aocs/aocsam.c
    AOCS_PARALLEL_SCAN_NCHUNKS (2048), AOCS_PARALLEL_SCAN_MIN_CHUNK_ROWS (16384)
    aocs_parallelscan_layout()       chunk layout from seginfo[] (used by all)
    aocs_parallelscan_nchunks()      first process: number of chunks
    aocs_parallelscan_init_layout()  every process: local layout + check
    claim_next_chunk()               take a chunk, open segfile
    aocs_skip_rows()                 position every column
    aocs_block_rows_consumed()
    aocs_chunk_done()                checked in the three aocs_getnext* loops
```
