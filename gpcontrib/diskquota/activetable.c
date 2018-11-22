/* -------------------------------------------------------------------------
 *
 * activetable.c
 *
 * This code is responsible for detecting active table for databases
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/relfilenodemap.h"

#include "activetable.h"

HTAB *active_tables_map = NULL;
static SmgrStat_hook_type prev_SmgrStat_hook = NULL;

static void report_active_table_SmgrStat(SMgrRelation reln);
static HTAB* get_active_tables_stats(void);
static HTAB* get_all_tables_stats(void);

void init_active_table_hook(void);
void init_shm_worker_active_tables(void);
void init_lock_active_tables(void);
HTAB* pg_fetch_active_tables(bool);

/*
 * Register smgr hook to detect active table.
 */
void
init_active_table_hook(void)
{
	prev_SmgrStat_hook = SmgrStat_hook;
	SmgrStat_hook = report_active_table_SmgrStat;
}

/*
 * Init active_tables_map shared memory
 */
void
init_shm_worker_active_tables(void)
{
	HASHCTL ctl;
	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hash = tag_hash;

	active_tables_map = ShmemInitHash ("active_tables",
										diskquota_max_active_tables,
										diskquota_max_active_tables,
										&ctl,
										HASH_ELEM | HASH_FUNCTION);
}

/*
 * Init lock of active table map 
 */
void init_lock_active_tables(void)
{
	bool found = false;
	active_table_shm_lock = ShmemInitStruct("disk_quota_active_table_shm_lock",
											sizeof(disk_quota_shared_state),
											&found);

	if (!found)
	{
		active_table_shm_lock->lock = &(GetNamedLWLockTranche("disk_quota_active_table_shm_lock"))->lock;
	}
}

/*
 * Fetch active table file size statistics.
 * If force is true, then fetch all the tables.
 */
HTAB* pg_fetch_active_tables(bool force)
{
	if (force)
	{
		return get_all_tables_stats();
	}
	else
	{
		return get_active_tables_stats();
	}
}

/*
 * Get the table size statistics for all the tables
 */
static HTAB* 
get_all_tables_stats()
{
	HTAB *local_table_stats_map = NULL;
	HASHCTL ctl;
	HeapTuple tuple;
	Relation classRel;
	HeapScanDesc relScan;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_table_stats_map = hash_create("local table map with table size info",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	
	classRel = heap_open(RelationRelationId, AccessShareLock);
    relScan = heap_beginscan_catalog(classRel, 0, NULL);

	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Oid relOid;
		DiskQuotaActiveTableEntry *entry;

		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		if (classForm->relkind != RELKIND_RELATION &&
				classForm->relkind != RELKIND_MATVIEW)
			continue;
		relOid = HeapTupleGetOid(tuple);

		/* ignore system table*/
		if (relOid < FirstNormalObjectId)
			continue;

		entry = (DiskQuotaActiveTableEntry *) hash_search(local_table_stats_map, &relOid, HASH_ENTER, NULL);

		entry->tableoid = relOid;
		entry->tablesize = (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
					ObjectIdGetDatum(relOid)));

	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

    return local_table_stats_map;	
}
/*
 * Get local active table with table oid and table size info.
 * This function first copies active table map from shared memory 
 * to local active table map with refilenode info. Then traverses
 * the local map and find corresponding table oid and table file 
 * size. Finnaly stores them into local active table map and return.
 */
static HTAB* 
get_active_tables_stats()
{
	HASHCTL ctl;
	HTAB *local_active_table_file_map = NULL;
	HTAB *local_active_table_stats_map = NULL;
	HASH_SEQ_STATUS iter;
	DiskQuotaActiveTableFileEntry *active_table_file_entry;
	DiskQuotaActiveTableEntry *active_table_entry;

	Oid relOid;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = tag_hash;

	local_active_table_file_map = hash_create("local active table map with relfilenode info",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/* Move active table from shared memory to local active table map */
	LWLockAcquire(active_table_shm_lock->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, active_tables_map);

	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool  found;
		DiskQuotaActiveTableFileEntry *entry;

		if (active_table_file_entry->dbid != MyDatabaseId)
		{
			continue;
		}

		/* Add the active table entry into local hash table*/
		entry = hash_search(local_active_table_file_map, active_table_file_entry, HASH_ENTER, &found);
		if (entry)
			*entry = *active_table_file_entry;
		hash_search(active_tables_map, active_table_file_entry, HASH_REMOVE, NULL);
	}

	LWLockRelease(active_table_shm_lock->lock);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_active_table_stats_map = hash_create("local active table map with relfilenode info",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/* traverse local active table map and calculate their file size. */
	hash_seq_init(&iter, local_active_table_file_map);
	/* scan whole local map, get the oid of each table and calculate the size of them */
	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		Size tablesize;
		bool found;
		
		relOid = RelidByRelfilenode(active_table_file_entry->tablespaceoid, active_table_file_entry->relfilenode);

		//TODO replace DirectFunctionCall1 by a new total relation size function, which could handle Invalid relOid
		/* avoid to generate ERROR if relOid is not existed (i.e. table has been droped) */
		PG_TRY();
		{
			tablesize = (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
						ObjectIdGetDatum(relOid)));
		}
		PG_CATCH();
		{
			FlushErrorState();
			tablesize = 0;
		}
		PG_END_TRY();
		active_table_entry = hash_search(local_active_table_stats_map, &relOid, HASH_ENTER, &found);
		active_table_entry->tableoid = relOid;
		active_table_entry->tablesize = tablesize;
	}
	elog(DEBUG1, "active table number is:%ld", hash_get_num_entries(local_active_table_file_map));
	hash_destroy(local_active_table_file_map);
	return local_active_table_stats_map;
}

/*
 *  Hook function in smgr to report the active table
 *  information and stroe them in active table shared memory
 *  diskquota worker will consuming these active tables and
 *  recalculate their file size to update diskquota model.
 */
static void
report_active_table_SmgrStat(SMgrRelation reln)
{
	DiskQuotaActiveTableFileEntry *entry;
	DiskQuotaActiveTableFileEntry item;
	bool found = false;

	if (prev_SmgrStat_hook)
		(*prev_SmgrStat_hook)(reln);

	MemSet(&item, 0, sizeof(DiskQuotaActiveTableFileEntry));
	item.dbid = reln->smgr_rnode.node.dbNode;
	item.relfilenode = reln->smgr_rnode.node.relNode;
	item.tablespaceoid = reln->smgr_rnode.node.spcNode;

	LWLockAcquire(active_table_shm_lock->lock, LW_EXCLUSIVE);
	entry = hash_search(active_tables_map, &item, HASH_ENTER_NULL, &found);
	if (entry && !found)
		*entry = item;
	LWLockRelease(active_table_shm_lock->lock);

	if (!found && entry == NULL) {
		/* We may miss the file size change of this relation at current refresh interval.*/
		ereport(WARNING, (errmsg("Share memory is not enough for active tables.")));
	}
}
