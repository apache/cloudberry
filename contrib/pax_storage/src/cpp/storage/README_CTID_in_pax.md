# CTID in  PAX

Pax table is different from heap/ao table, for the last two bytes, it does't has continuous logical address. So it can't be mapped to physical file through ctid.

in heap table ，ctid -> (32 bit page id + 16 bit tuple offset number)

```
  │            page id               │    tuple offset num  │
  ├──────────────────────────────────┼──────────────────────┤
  │             32bit                │         16bit        │
  └──────────────────────────────────┴──────────────────────┘
```

in ao table，ctid -> (7bit segment id  + 40 bit tuple offset number), it take the least significant 15 bits from the row numbers, add one to it.

```
  │ seg id  │                   tuple offset                │
  ├─────────┼────────────────────────────┬┬─────────────────┤
  │   7bit  │           25bit            ││      15bit      │
  └─────────┴────────────────────────────┴┴─────────────────┘
```


 The Pax table data storage will be broken up into multiple blocks, and the size of each block is 64M~512M, which is stored in the object storage service. There is no logical relationship between blocks. The block id is a 36-byte uuid, not an integer like page id/segment id.

```
  ┌─────────┐           ┌─────────┐            ┌─────────┐
  │ segment1│           │ segment2│            │ segment3│
  └─────────┘           └─────────┘            └─────────┘
  ┌──────────────────────────────────────────────────────┐
  │                                                      │
  │                  Object Storage  Service             │
  └──────────────────────────────────────────────────────┘
  ┌───────┐       ┌───────┐      ┌───────┐       ┌───────┐
  │block 1│       │block 2│      │block 3│       │block 4│
  └───────┘       └───────┘      └───────┘       └───────┘
  ┌───────┐       ┌───────┐      ┌───────┐       ┌───────┐
  │block 5│       │block 6│      │block 7│       │block 8│
  └───────┘       └───────┘      └───────┘       └───────┘
```

The location address of the block is http url, for example `https://$(object service url)/$(tablespace)/$(table path)/${block id}`

It is not possible to locate tuples in the physical file using the ctid, as is done with heap/ao tables. However, for the delete and update interfaces of PAX tables, the ItemPointer pointer is used to locate tuples. How can a 48-bit ctid be used to locate tuples in the physical file?

Similar to heap/ao tables, We have split the ctid into two parts: one part(block_no) saves the block information, and the other part(tuple_offset) saves the offset of the tuple within the block.

During the scan phase, for each block file of the table  that is read, a mapping information of block_no to block_id is constructed and stored in shared memory. The block_no is saved in the block_no part of the ctid. When a delete/update operation retrieves the ctid, it can parse out the block_no and query the shared memory to retrieve the corresponding block file information associated with that block_no.

In scenarios where multiple scan processes are simultaneously scanning the same table, it becomes necessary to lock the shared memory where the record mapping information is stored. To avoid locking, parallel scan processes for the same table can be allocated separate slots within an array.

By allocating separate slots for each parallel scan process, they can independently store their respective record mapping information without interfering with each other. This approach eliminates the need for locking the shared memory and allows concurrent scan processes to operate efficiently and independently on the table.

it would be necessary to add slot information to the ctid. Without slot information, the delete phase would not be able to locate the specific slot in the array based solely on the ctid.

In the final implementation, the ctid has been divided into three parts:

Part1(5bit): table_no.   it represents the position of the element in the array for the table_no-th occurrence of the same element.

Part2(22bt): block_no, it represents the index of the block array within the slot.

Part3(21bit): tuple offset, It represents the index of the tuple within the block file.


```
 
  │     bi_ho        │     bi_lo         │      ip_posid    │
  ├──────┬───────────┴────────────────┬──┴─────────┬┬───────┤
  │ 5bit │     22bit                  │   13bit    ││  7bit │
  ├──────┼────────────────────────────┼────────────┴┴───────┤
  │      │                            │                     │
  table_no          block_no                tuple_offset
```

The delete phase is divided into two steps:

The first step: MarkerDelete(ctid), the delete bitmap is constructed when the  `tuple_delete` function is called.  retrieve the block id using block_no and table_no. Each block file has its own bitmap, and the index in bitmap corresponds to the tuple offset number. As a result, there will be N delete bitmaps for N block files.

The second step:  ExecuteDelete(), the deletion is performed when FinishDmlState is called. For the same block file, it performs a sequential scan in order, filters out the tuples that need to be deleted based on the delete bitmap, and inserts the remaining tuples into a new block file.

Update phase: SplitUpdate operator (DELETE + INSERT) is used to implement tuple update, following the AO table. The Update operation is splited into three steps:

1. MarkerDelete(ctid)
2. InsertTuple(tuple)
3. ExecuteDelete()


## implement 

### Share Memory Struct 

because the block map is saved using share memory,  before starting working with pax extension , adding the following line to `postgresql.conf` is required. This change requires a restart of the postgresql database server.

```
shared_preload_libraries = 'pax.so'
```

We use gp_session_id + gp_command_count as the key and utilize a hash table in shared memory to achieve fast lookup of shared structures among different process groups.

Within the same process group, we use the same shared struct structure. Each process is allocated an independent slot when scanning the table. For every block scanned, it is added to the dynamic array pointed to by the dsm_segment within the slot

The graphical structure of the relationships between data structures is as follows：

```
                                        shared state struct for ctid

                                                          dsm segment
                                           ┌───┐      ┌───┬───┬───┬───┬───┐
                                           │tbl├─────►│   │   │   │   │   │
                                           │   │      └───┴───┴───┴───┴─┬─┘
                                           ├───┤                        │       ┌────────┐
                            ┌───┐ ┌───────►│   │                        └──────►│block id│
                            │   │ │        │   │          dsm segment           └────────┘
                            │   │ │        ├───┤      ┌───┬───┬───┬───┬───┐
                            │   ├─┘        │   │      │   │   │   │   │   │
                            │   │          │   │      └───┴───┴───┴───┴───┘
                            │   │          └───┘
                            │   │
                            │   │                         dsm segment
                            │   │          ┌───┐      ┌───┬───┬───┬───┬───┐
                            ├───┤          │   ├─────►│   │   │   │   │   │
                            │   │          │tbl│      └───┴───┴───┴───┴─┬─┘
                            │   │          ├───┤                        │       ┌────────┐
                            │   │          │   │                        └──────►│block id│
                            │   ├─────────►│tbl│          dsm segment           └────────┘
                            │   │          ├───┤      ┌───┬───┬───┬───┬───┐
 hash table ──────────>     │   │          │   │      │   │   │   │   │   │
                            │   │          │   │      └───┴───┴───┴───┴───┘
 key: session_id+command_id │   │          │   │
                            │   │          └───┘
 value: shared state        ├───┤
                            │   │
                            │   │
                            │   │                         dsm segment
                            │   │          ┌───┐      ┌───┬───┬───┬───┬───┐
                            │   ├────┐     │   ├─────►│   │   │   │   │   │
                            │   │    │     │tbl│      └───┴───┴───┴───┴─┬─┘
                            │   │    │     ├───┤                        │       ┌────────┐
                            │   │    │     │   │                        └──────►│block id│
                            │   │    └────►│tbl│          dsm segment           └────────┘
                            └───┘          ├───┤      ┌───┬───┬───┬───┬───┐
                                           │   │      │   │   │   │   │   │
                                           │tbl│      └───┴───┴───┴───┴───┘
                                           └───┘
```

The initialization and release steps for shared memory are as follows:

1. in pg_init step, the necessary locks and hash table in shared memory are initialized. 
2. when execute scan/delete/update sql,  the initialization of shared memory structures required for SQL execution occurs during the beginscan or InitDmlState phase by calling `init_command_resource()`. 
3. When ExecutorEnd_hook call, the gp_writer will release the shared memory by calling. `release_command_resource()`



### Lock

* pax_hash_lock

  ​	we will need a lock to protect operations related to the hash table. This lock ensures that concurrent access to the hash table is properly synchronized and avoids potential data corruption or race conditions.  the numer is 1.

* pax_xact_lock

  We will need a lock to protect the table's slot assigned in same process group.  the numer is MaxConnections

