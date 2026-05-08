# [RFC 001] Hard Limit for Diskquota

This document describes the design of the hard limit feature for Diskquota 2.0.

## Motivation

Diskquota 1.0 only supports so-call "soft limit", meaning that Diskquota will not interrupt any running query even though the amount of data the query writes exceeds some quota. 

Common types of queries that can write a large amount of data include
- `CREATE TABLE AS`
- `CREATE INDEX`
- `VACUUM FULL`

Running one single query of such types can take up all the space of a disk, which can cause issues, such as a [Disk Full Failure](https://www.postgresql.org/docs/current/disk-full.html) that crashes the whole database system at worst.

Therefore, to mitigate the risk of having disk full issues, we plan to introduce "hard limit" in Diskquota 2.0, which enables Diskquota to terminate an in-progress query if the amount of data it writes exceeds some quota.

Due to the difficulty of observing the intermediate states of an in-progress query in Greenplum, implementing hard limit is not easy. Specifically, there are two major challenges in the way:
1. Observing intermediate states of a query under Greenplum's MVCC mechanism.
2. Ensuring data consistency after seeing uncommitted changes.

The rest of this doc will analyze the challenges, propose possible approaches to tackle them, and introduce the design decisions with the rationales behind.

## Challenge 1: Observing Intermediate States

Diskquota cares about what relations, including tables, indexes, and more, that receives new data. Those relations are called "**active**" relations. Diskquota uses background workers (bgworkers) to collect active relations periodically and then calculates their sizes using an OS system call like `stat()`.

Active relations can be produced in two ways:
- Case 1: By writing new data to existing relations, e.g., using `INSERT` or `COPY FROM`. In this case, Diskquota do not need to observe any intermediate state during execution because the information of the active relations is committed and is visible to the background worker.
- Case 2: By creating new relations with data, e.g., using `CREATE TABLE AS` or `CREATE INDEX`. This is the hard part. In this case, the information of the active relations and has not been committed yet during execution. Therefore, the information is not visible to the bgworkers when it scans the catalog tables under MVCC.

For Case 2, to enable the bgworkers to observe the active relations created by an in-progress query, there are two options:
1. **The `SNAPSHOT_DIRTY` approach:** Disregarding MVCC and scanning the catalog tables using `SNAPSHOT_DIRTY`. In this way, the bgworkers can see uncommitted information of the active relations by doing a table scan.
2. **The pub-sub approach:** Publishing the information of newly created active relations to a shared memory area using hooks when executing a query. For example, we can use the `object_access_hook` to write the information in the relation cache of a relation that is being created to the shared memory area. The bgworkers can then retrieve the information from the shared memory area periodically.

## Challenge 2: Ensuring Data Consistency

Since bgworkers are allowed to observe uncommitted states, extra work is required to ensure the bgworkers will never see inconsistent snapshots for both options.
- For the `SNAPSHOT_DIRTY` approach, it is required to determine which version should take effect given that there may be multiple versions for one tuple, including the versions created by aborted transactions.
- For the pub-sub approach, it is required to sync the information in the shared memory area against the latest committed version of the catalogs.

The `SNAPSHOT_DIRTY` approach is more complicated and more error-prone than the pub-sub approach since it requires Diskquota to do visibility checks on its own. Therefore, we choose the pub-sub approach to implement hard limit.

Even though taking the pub-sub approach frees us from the complicated visibility check process, keeping the shared memory area and the catalogs in sync is still non-trivial. Note that the information of a relation in the catalogs can either be updated by altering the relation, or be deleted by dropping the relation. A natural idea is to monitor each of these operations, e.g., using the `object_access_hook`, and replay it to the shared memory area. However, this does not solve the consistency issue because these operations can be aborted. Due to the MVCC mechanism, nothing needs to be done to the catalogs when aborting such operations and no hook can be used to rollback the changes to the shared memory area at that time.

### Aligning with the Catalogs

Given that it is useless to replay each modification operation to the shared memory area, we choose not to replay any operation at all but to align the entries in the shared memory area against tuples in the catalogs.

Specifically, for each entry in the shared memory area, search the catalogs for the tuple with the same key under MVCC, then
- if a tuple is found in the catalogs, that tuple must be written by the latest committed transaction and therefore must be no later than the transaction that writes the entry to the shared memory area. Therefore, the tuple in the catalogs prevails and the shared memory entry is deleted.
- otherwise, there are still two cases:
  1. **Tuple Uncommitted:** the transaction that writes the entry to the shared memory area is the latest one and has not yet been committed. In this case, Diskquota should use the information in the shared memory entry since it is the only source.
  2. **Tuple Deleted:** the tuple in the catalogs has been deleted by a committed transaction and the shared memory area has not been cleaned. We must prevent this case from happening because it is hard to distinguish it from the uncommitted case. Fortunately, Greenplum provides an `unlink` hook that gets called at the end of a transaction to delete files of relations. Diskquota can use the `unlink` hook to delete entries that corresponding to relations to be deleted from the shared memory area.

The alignment process is summarized as the following two pieces of pseudo code:
- Each time the Diskquota bgworker retrieves information of active relations, do
    ```python
    for entry in shared memory area:
        tuple = SearchSysCache(entry.key)
        if HeapTupleIsValid(tuple):
            del entry from shared memory area
    ``` 
- Each time the `unlink` hook gets called for a `relfilenode`, do
  ```python
  entry = Search shared memory area by relfilenode
  del entry from shared memory area
  ```

With alignment, entries in the shared memory area only represents uncommitted relations and tuples in the catalogs are used for committed relations. There is no intersection between the two sets, which guarantees that the Diskquota bgworker will always see a consistent snapshot.

### Limitations and Workarounds

While the pub-sub approach with alignment enables Diskquota to observe uncommitted active relations and guarantees data consistency, it does have some inherent limitations.

One of the most notable limitation is that it does not support hard limit for any operation that modifies existing tuples in the catalogs, such as
- `ALTER TABLE`
- `DROP TABLE`
- `TRUNCATE`

Such operations will not be visible to Diskquota until the transaction is committed. For example, if a user changes the tablespace of a table `t` using 
```sql
ALTER TABLE t SET TABLESPACE new_tablespace;
```

From the Diskquota's perspective, table `t` still belongs to the old tablespace when it is being copied to the new tablespace. As a result, the size of table `t` will be limited by the quota on the *old* tablespace instead of the *new* tablespace until the `ALTER TABLE` command is completed.

The root cause of this limitation that such modification operations will not take effect until the transaction is committed. Specifically,
- Due to MVCC, they will not take effect **in the catalogs** until committed.
- Due to the alignment mechanism, they will not take effect **in the shared memory area** neither given that table `t` is already visible from the catalogs to Diskquota and the corresponding shared memory entry will be deleted when the bgworker retrives active relations.

One way to overcome this limitation is to enhance the **soft limit** mechanism to calculate the resulting quota usage of such catalog modification operations and reject those that will cause quota excess before execution. This is also not trivial to implement but is in our plan.

For now, as a workaround, in order to make the catalog modification operations hard-limited based on the new information of relations instead of the old information, the user can use the `CREATE AS` command to create a new relation with the new information and then drop the old one. Because Diskquota can see relations that have not yet been committed, the `CREATE AS` command can be hard-limited and will be hard-limited based on the new infomation. 

In the above example of changing the tablespace, in order to count the size of table `t` in the quota usage of the new tablespace, the user can replace the `ALTER TABLE` command with the following `CRATE`-`DROP`-`RENAME` transaction:
```sql
BEGIN;
CREATE TABLE t_1 TABLESPACE new_tablespace AS SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_1 RENAME TO t;
COMMIT;
```
