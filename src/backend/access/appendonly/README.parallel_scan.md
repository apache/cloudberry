This describes how the parallel workers of one segment share a sequential
scan of an AO row or AOCS table.  A worker here is any process taking part
in the scan, including the leader. Both use the same chunk layout and the same shared
state; they differ only in how a worker reads a chunk.

Segment file and block formats: `src/backend/access/appendonly/README.md`.


# Chunks

Each segment file is split into chunks of rows, and the workers claim chunks
one at a time from a shared counter:

AO row: one file per segment file.

```
segfile 1   <relfilenode>.1     | c0 | c1 | c2 |
segfile 2   <relfilenode>.2     | c3 | c4 | c5 | c6 |
```

AOCS: one file per column and segment file.  A chunk covers the same rows in
every column file of its segment file.

```
segfile 1   column a  <relfilenode>.1     | c0 | c1 | c2 |
            column b  <relfilenode>.129   | c0 | c1 | c2 |
segfile 2   column a  <relfilenode>.2     | c3 | c4 | c5 | c6 |
            column b  <relfilenode>.130   | c3 | c4 | c5 | c6 |
```

In both cases, with three workers A, B and C, the chunks may be claimed as:

```
chunk    c0  c1  c2  c3  c4  c5  c6
worker   A   B   C   A   C   B   A
```

A chunk is a range of row ordinals: a row's position in its segment file,
counting from 0.  Ordinals are used because they can be computed without
reading the files: `tupcount` in `pg_aoseg` gives the number of rows of each
segment file.  For AOCS, the ordinal is also the only position all column
files agree on, since each column file has its own block boundaries.

Ordinals are not row numbers, which can have gaps.


# Chunk layout

Every worker reads the segment files from `pg_aoseg` (AO row) or
`pg_aocsseg` (AOCS) with the scan's snapshot, and computes the same layout:

```
rows_per_chunk = 1000

segno  tupcount   chunks        seg_first_chunk
  1      2500     c0 c1 c2      0
  2         0     -             3      (empty: no chunks)
  3      1200     c3 c4         3
```

- `rows_per_chunk` aims at about 2048 chunks per scan, each between 512kB and
  32MB of uncompressed row data (`AppendOnlyParallelScan_RowsPerChunk()`).
  Small chunks keep the blocks other workers skip over in the OS cache.
- The row size is that of the data the scan reads: whole rows for AO row, and
  only the scanned columns for AOCS.  A narrow projection of a wide AOCS table
  gets larger chunks, so that a block of a scanned column is not loaded by
  several workers.
- A chunk never spans two segment files.  The last one of a file is read to
  the end of the file, as a serial scan does, so no row is skipped even if
  `tupcount` were too small.
- Empty and AWAITING_DROP segment files get no chunks.
- `tupcount` includes deleted rows, so chunks cover every stored row.


# Shared and local state

Shared, in `ParallelBlockTableScanDescData`:

- `phs_nblocks`: the total number of chunks.
- `phs_nallocated`: an atomic counter of the chunks handed out.

Local, in `AppendOnlyScanDescData` and `AOCSScanDescData`:

- `rows_per_chunk`, `seg_first_chunk[]`: the chunk layout.
- `cur_seg`: the segment file being read.
- `chunk_start_row`, `chunk_end_row`: the current chunk.
- `segrowsprocessed`: how far the worker has read in the segment file.

Every worker computes the layout in `*_parallelscan_init()`, and checks it
has the same number of chunks as the shared state.  For AO row,
`table_parallelscan_initialize()` sets the number of chunks
(`appendonly_parallelscan_compute_chunks()`).  For AOCS it depends on the
scanned columns, which are only known in `aocs_beginscan()`: the worker that
sets up the shared state begins its scan right after, before the other
workers start, and stores the number of chunks.


# Claiming a chunk

`*_parallelscan_next_chunk()`:

```
chunk = fetch_add(phs_nallocated, 1)         = 4
segfile = last with seg_first_chunk <= 4     = segfile 3 (first chunk 3)
rows    = [(4 - 3) * 1000, min(2000, 1200))  = [1000, 1200)
```

When `chunk >= phs_nblocks`, the scan is done.


# Reading a chunk

Block headers are not compressed and hold the block's row count, so a worker
can skip a block without decompressing it.  The skipped block is still read
from the file (there is no seek), but only its header is looked at.

## AO row

A block holds whole rows and is never split.  It belongs to the chunk that
holds its first row:

```
rows     0         1000      2000
         |---c0----|---c1----|
blocks   [b0][b1][b2][b3][b4]           400 rows per block

c0 reads b0 b1 b2      (b2 is rows 800-1199, it starts in c0)
c1 reads b3 b4
```

So the rows a worker returns can differ from its chunk's range by less than
one block at each end.  See `appendonly_parallelscan_next_block()`.

## AOCS

Each column file has its own block boundaries, so a chunk is an exact range
of rows.  `aocs_parallelscan_skip_rows()` moves every scanned column to the
chunk's first row, and the scan stops at its last row:

```
rows       0         1000      2000
           |---c0----|---c1----|
column a   [b0][b1][b2][b3][b4]         400 rows per block
column b   [b0   ][b1   ][b2  ]         700 rows per block

c1, column a: skip b0 b1, load b2, skip to row 1000
c1, column b: skip b0, load b1, skip to row 1000
```

A block that crosses a chunk boundary is loaded by both workers, and each
returns only its own rows.


# A worker only moves forward

A worker's chunks have increasing numbers, so its next chunk is always after
the current one.  It never reads a file backwards, and reads each block at
most once.  In each segment file, it reads from the start of the file to the
end of its last chunk there:

```
worker A claims c0, c2, c4
segfile 1   | c0 | c1 | c2 |      read c0, skip c1, read c2
segfile 3   | c3 | c4 |           open, skip c3, read c4
```


# Deleted rows and compression

- Deleted rows are counted in chunks like any other row; the visibility map
  check drops them.
- Row numbers are computed from each block's first row number, so a skipped
  block still advances it (`AppendOnlyExecutionReadBlock_FinishedScanBlock()`
  for AO row).
- A large value stored over several blocks counts as one row.


# Limitations

- Skipping a block does not seek past it, so a worker reads every block
  before its last chunk in a file, not only the blocks of its own chunks:
  the segment file (AO row), or each scanned column file (AOCS).  The
  workers move through a file together, a few chunks apart, so with small
  chunks most of those reads come from the OS cache.
- A block directory could let a worker seek to its chunk instead, but this
  is not implemented.
- Building a block directory during a scan is not supported in parallel.
