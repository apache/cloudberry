/* -------------------------------------------------------------------------
 *
 * quotamodel.c
 *
 * This code is responsible for init disk quota model and refresh disk quota 
 * model.
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "activetable.h"
#include "diskquota.h"

/* cluster level max size of black list */
#define MAX_DISK_QUOTA_BLACK_ENTRIES (1024 * 1024)
/* cluster level init size of black list */
#define INIT_DISK_QUOTA_BLACK_ENTRIES 8192
/* per database level max size of black list */
#define MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES 8192

typedef struct TableSizeEntry TableSizeEntry;
typedef struct NamespaceSizeEntry NamespaceSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct QuotaLimitEntry QuotaLimitEntry;
typedef struct BlackMapEntry BlackMapEntry;
typedef struct LocalBlackMapEntry LocalBlackMapEntry;

/* local cache of table disk size and corresponding schema and owner */
struct TableSizeEntry
{
	Oid			reloid;
	Oid			namespaceoid;
	Oid			owneroid;
	int64		totalsize;
	bool		is_exist; /* flag used to check whether table is already dropped */
};

/* local cache of namespace disk size */
struct NamespaceSizeEntry
{
	Oid			namespaceoid;
	int64		totalsize;
};

/* local cache of role disk size */
struct RoleSizeEntry
{
	Oid			owneroid;
	int64		totalsize;
};

/* local cache of disk quota limit */
struct QuotaLimitEntry
{
	Oid			targetoid;
	int64		limitsize;
};

/* global blacklist for which exceed their quota limit */
struct BlackMapEntry
{
	Oid			targetoid;
	Oid			databaseoid;
	uint32		targettype;
};

/* local blacklist for which exceed their quota limit */
struct LocalBlackMapEntry
{
	BlackMapEntry	keyitem;
	bool			isexceeded;
};

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;
static HTAB *namespace_size_map = NULL;
static HTAB *role_size_map = NULL;
static HTAB *namespace_quota_limit_map = NULL;
static HTAB *role_quota_limit_map = NULL;

/* black list for database objects which exceed their quota limit */
static HTAB *disk_quota_black_map = NULL;
static HTAB *local_disk_quota_black_map = NULL;

static disk_quota_shared_state *black_map_shm_lock;
disk_quota_shared_state *active_table_shm_lock = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* functions to refresh disk quota model*/
static void refresh_disk_quota_usage(bool force);
static void calculate_table_disk_usage(bool force);
static void calculate_schema_disk_usage(void);
static void calculate_role_disk_usage(void);
static void flush_local_black_map(void);
static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage, QuotaType type);
static void update_namespace_map(Oid namespaceoid, int64 updatesize);
static void update_role_map(Oid owneroid, int64 updatesize);
static void remove_namespace_map(Oid namespaceoid);
static void remove_role_map(Oid owneroid);
static bool load_quotas(void);

static Size DiskQuotaShmemSize(void);
static void disk_quota_shmem_startup(void);

/*
 * DiskQuotaShmemSize
 * Compute space needed for diskquota-related shared memory
 */
Size
DiskQuotaShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(disk_quota_shared_state));
	size = add_size(size, size); // two locks
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_BLACK_ENTRIES, sizeof(BlackMapEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaActiveTableEntry)));
	return size;
}

/*
 * DiskQuotaShmemInit
 *		Allocate and initialize diskquota-related shared memory
 */
void
disk_quota_shmem_startup(void)
{
	bool		found;
	HASHCTL		hash_ctl;

	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();

	black_map_shm_lock = NULL;
	disk_quota_black_map = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	black_map_shm_lock = ShmemInitStruct("disk_quota_black_map_shm_lock",
								 sizeof(disk_quota_shared_state),
								 &found);

	if (!found)
	{
		black_map_shm_lock->lock = &(GetNamedLWLockTranche("disk_quota_black_map_shm_lock"))->lock;
	}

	init_lock_active_tables();

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(BlackMapEntry);
	hash_ctl.entrysize = sizeof(BlackMapEntry);
	hash_ctl.hash = tag_hash;

	disk_quota_black_map = ShmemInitHash("blackmap whose quota limitation is reached",
									INIT_DISK_QUOTA_BLACK_ENTRIES,
									MAX_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_FUNCTION);

	init_shm_worker_active_tables();

	LWLockRelease(AddinShmemInitLock);
}

void
init_disk_quota_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(DiskQuotaShmemSize());
	RequestNamedLWLockTranche("disk_quota_black_map_shm_lock", 1);
	RequestNamedLWLockTranche("disk_quota_active_table_shm_lock", 1);

	/*
	 * Install startup hook to initialize our shared memory.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = disk_quota_shmem_startup;
}

/*
 * Init disk quota model when the worker process firstly started.
 */
void
init_disk_quota_model(void)
{
	HASHCTL		hash_ctl;

	/* init hash table for table/schema/role etc.*/
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = oid_hash;

	table_size_map = hash_create("TableSizeEntry map",
								1024 * 8,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(NamespaceSizeEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = oid_hash;

	namespace_size_map = hash_create("NamespaceSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(RoleSizeEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = oid_hash;

	role_size_map = hash_create("RoleSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(QuotaLimitEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = oid_hash;

	namespace_quota_limit_map = hash_create("Namespace QuotaLimitEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	role_quota_limit_map = hash_create("Role QuotaLimitEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(BlackMapEntry);
	hash_ctl.entrysize = sizeof(LocalBlackMapEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = tag_hash;

	local_disk_quota_black_map = hash_create("local blackmap whose quota limitation is reached",
									MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

/*
 * diskquota worker will refresh disk quota model
 * periodically. It will reload quota setting and 
 * recalculate the changed disk usage.
 */
void
refresh_disk_quota_model(bool force)
{
	elog(DEBUG1,"check disk quota begin");
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	/* skip refresh model when load_quotas failed */
	if (load_quotas())
	{
		refresh_disk_quota_usage(force);
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	elog(DEBUG1,"check disk quota end");
}

/*
 * Update the disk usage of nameapsce and role.
 * Put the exceeded namespace and role into shared black map.
 */
static void
refresh_disk_quota_usage(bool force)
{
	/* recalculate the disk usage of table, schema and role */
	calculate_table_disk_usage(force);
	calculate_schema_disk_usage();
	calculate_role_disk_usage();
	/* copy local black map back to shared black map */
	flush_local_black_map();
}

/*
 * Generate the new shared blacklist from the local_black_list which
 * exceed the quota limit.
 * local_balck_list is used to reduce the lock race.
 */
static void
flush_local_black_map(void)
{
	HASH_SEQ_STATUS iter;
	LocalBlackMapEntry* localblackentry;
	BlackMapEntry* blackentry;
	bool found;

	LWLockAcquire(black_map_shm_lock->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, local_disk_quota_black_map);
	while ((localblackentry = hash_seq_search(&iter)) != NULL)
	{
		if (localblackentry->isexceeded)
		{
			blackentry = (BlackMapEntry*) hash_search(disk_quota_black_map,
							   (void *) &localblackentry->keyitem,
							   HASH_ENTER_NULL, &found);
			if (blackentry == NULL)
			{
				elog(WARNING, "shared disk quota black map size limit reached.");
			}
			else
			{
				/* new db objects which exceed quota limit */
				if (!found)
				{
					blackentry->targetoid = localblackentry->keyitem.targetoid;
					blackentry->databaseoid = MyDatabaseId;
					blackentry->targettype = localblackentry->keyitem.targettype;
				}
			}
			localblackentry->isexceeded = false;
		}
		else
		{
			/* db objects are removed or under quota limit in the new loop */
			(void) hash_search(disk_quota_black_map,
							   (void *) &localblackentry->keyitem,
							   HASH_REMOVE, NULL);
			(void) hash_search(local_disk_quota_black_map,
							   (void *) &localblackentry->keyitem,
							   HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(black_map_shm_lock->lock);
}

/*
 * Compare the disk quota limit and current usage of a database object.
 * Put them into local blacklist if quota limit is exceeded.
 */
static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage, QuotaType type)
{
	bool					found;
	int32 					quota_limit_mb;
	int32 					current_usage_mb;
	LocalBlackMapEntry*		localblackentry;
	BlackMapEntry 			keyitem;

	QuotaLimitEntry* quota_entry;
	if (type == NAMESPACE_QUOTA)
	{
		quota_entry = (QuotaLimitEntry *)hash_search(namespace_quota_limit_map,
											&targetOid,
											HASH_FIND, &found);
	}
	else if (type == ROLE_QUOTA)
	{
		quota_entry = (QuotaLimitEntry *)hash_search(role_quota_limit_map,
											&targetOid,
											HASH_FIND, &found);
	}
	else
	{
		/* skip check if not namespace or role quota*/
		return;
	}

	if (!found)
	{
		/* default no limit */
		return;
	}

	quota_limit_mb = quota_entry->limitsize;
	current_usage_mb = current_usage / (1024 *1024);
	if(current_usage_mb >= quota_limit_mb)
	{
		memset(&keyitem, 0, sizeof(BlackMapEntry));
		keyitem.targetoid = targetOid;
		keyitem.databaseoid = MyDatabaseId;
		keyitem.targettype = (uint32)type;
		elog(DEBUG1,"Put object %u to blacklist with quota limit:%d, current usage:%d",
				targetOid, quota_limit_mb, current_usage_mb);
		localblackentry = (LocalBlackMapEntry*) hash_search(local_disk_quota_black_map,
					&keyitem,
					HASH_ENTER, &found);
		localblackentry->isexceeded = true;
	}

}

/*
 *  Remove a namespace from local namespace_size_map
 */
static void
remove_namespace_map(Oid namespaceoid)
{
	hash_search(namespace_size_map,
			&namespaceoid,
			HASH_REMOVE, NULL);
}

/*
 * Update the current disk usage of a namespace in namespace_size_map.
 */
static void
update_namespace_map(Oid namespaceoid, int64 updatesize)
{
	bool found;
	NamespaceSizeEntry* nsentry;
	nsentry = (NamespaceSizeEntry *)hash_search(namespace_size_map,
			&namespaceoid,
			HASH_ENTER, &found);
	if (!found)
	{
		nsentry->namespaceoid = namespaceoid;
		nsentry->totalsize = updatesize;
	}
	else {
		nsentry->totalsize += updatesize;
	}

}

/*
 *  Remove a namespace from local role_size_map
 */
static void
remove_role_map(Oid owneroid)
{
	hash_search(role_size_map,
			&owneroid,
			HASH_REMOVE, NULL);
}

/*
 * Update the current disk usage of a namespace in role_size_map.
 */
static void
update_role_map(Oid owneroid, int64 updatesize)
{
	bool found;
	RoleSizeEntry* rolentry;
	rolentry = (RoleSizeEntry *)hash_search(role_size_map,
			&owneroid,
			HASH_ENTER, &found);
	if (!found)
	{
		rolentry->owneroid = owneroid;
		rolentry->totalsize = updatesize;
	}
	else {
		rolentry->totalsize += updatesize;
	}

}

/*
 *  Incremental way to update the disk quota of every database objects
 *  Recalculate the table's disk usage when it's a new table or active table.
 *  Detect the removed table if it's nolonger in pg_class.
 *  If change happens, no matter size change or owner change,
 *  update namespace_size_map and role_size_map correspondingly.
 *  Parameter 'force' set to true at initialization stage to recalculate 
 *  the file size of all the tables.
 *
 */
static void
calculate_table_disk_usage(bool force)
{
	bool found;
	bool active_tbl_found = false;
	Relation	classRel;
	HeapTuple	tuple;
	HeapScanDesc relScan;
	TableSizeEntry *tsentry = NULL;
	Oid			relOid;
	HASH_SEQ_STATUS iter;
	HTAB *local_active_table_stat_map;
	DiskQuotaActiveTableEntry *active_table_entry;

	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	local_active_table_stat_map = pg_fetch_active_tables(force);

	/* unset is_exist flag for tsentry in table_size_map*/
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		tsentry->is_exist = false;
	}

	/*
	 * scan pg_class to detect table event: drop, reset schema, reset owenr.
	 * calculate the file size for active table and update namespace_size_map
	 * and role_size_map
	 */
	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		found = false;
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;
		relOid = HeapTupleGetOid(tuple);

		/* ignore system table*/
		if(relOid < FirstNormalObjectId)
			continue;

		tsentry = (TableSizeEntry *)hash_search(table_size_map,
							 &relOid,
							 HASH_ENTER, &found);
		/* mark tsentry is_exist */
		if (tsentry)
			tsentry->is_exist = true;

		active_table_entry = (DiskQuotaActiveTableEntry *) hash_search(local_active_table_stat_map, &relOid, HASH_FIND, &active_tbl_found);

		/* skip to recalculate the tables which are not in active list and not at initializatio stage*/
		if(active_tbl_found)
		{

			/* namespace and owner may be changed since last check*/
			if (!found)
			{
				/* if it's a new table*/
				tsentry->reloid = relOid;
				tsentry->namespaceoid = classForm->relnamespace;
				tsentry->owneroid = classForm->relowner;
				tsentry->totalsize = (int64) active_table_entry->tablesize;
				update_namespace_map(tsentry->namespaceoid, tsentry->totalsize);
				update_role_map(tsentry->owneroid, tsentry->totalsize);
			}
			else
			{
				/* if not new table in table_size_map, it must be in active table list */
				int64 oldtotalsize = tsentry->totalsize;
				tsentry->totalsize = (int64) active_table_entry->tablesize;
				update_namespace_map(tsentry->namespaceoid, tsentry->totalsize - oldtotalsize);
				update_role_map(tsentry->owneroid, tsentry->totalsize - oldtotalsize);
			}
		}

		/* if schema change, transfer the file size */
		if (tsentry->namespaceoid != classForm->relnamespace)
		{
			update_namespace_map(tsentry->namespaceoid, -1 * tsentry->totalsize);
			tsentry->namespaceoid = classForm->relnamespace;
			update_namespace_map(tsentry->namespaceoid, tsentry->totalsize);
		}
		/* if owner change, transfer the file size */
		if(tsentry->owneroid != classForm->relowner)
		{
			update_role_map(tsentry->owneroid, -1 * tsentry->totalsize);
			tsentry->owneroid = classForm->relowner;
			update_role_map(tsentry->owneroid, tsentry->totalsize);
		}
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);
	hash_destroy(local_active_table_stat_map);

	/* process removed tables */
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		if (tsentry->is_exist == false)
		{
			update_role_map(tsentry->owneroid, -1 * tsentry->totalsize);
			update_namespace_map(tsentry->namespaceoid, -1 * tsentry->totalsize);

			hash_search(table_size_map,
					&tsentry->reloid,
					HASH_REMOVE, NULL);
			continue;
		}
	}
}

/*
 * Check the namespace quota limit and current usage
 * Remove dropped namespace from namespace_size_map
 */
static void calculate_schema_disk_usage(void)
{
	HeapTuple	tuple;
	HASH_SEQ_STATUS iter;
	NamespaceSizeEntry* nsentry;
	hash_seq_init(&iter, namespace_size_map);

	while ((nsentry = hash_seq_search(&iter)) != NULL)
	{
		/* check if namespace is already be deleted */
		tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsentry->namespaceoid));
		if (!HeapTupleIsValid(tuple))
		{
			remove_namespace_map(nsentry->namespaceoid);
			continue;
		}
		ReleaseSysCache(tuple);
		check_disk_quota_by_oid(nsentry->namespaceoid, nsentry->totalsize, NAMESPACE_QUOTA);
	}
}

/*
 * Check the role quota limit and current usage
 * Remove dropped role from roel_size_map
 */
static void calculate_role_disk_usage(void)
{
	HeapTuple	tuple;
	HASH_SEQ_STATUS iter;
	RoleSizeEntry* rolentry;
	hash_seq_init(&iter, role_size_map);

	while ((rolentry = hash_seq_search(&iter)) != NULL)
	{
		/* check if role is already be deleted */
		tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(rolentry->owneroid));
		if (!HeapTupleIsValid(tuple))
		{
			remove_role_map(rolentry->owneroid);
			continue;
		}
		ReleaseSysCache(tuple);
		check_disk_quota_by_oid(rolentry->owneroid, rolentry->totalsize, ROLE_QUOTA);
	}
}

/*
 * Load quotas from diskquota configuration table(quota_config).
*/
static bool
load_quotas(void)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	bool		found;
	QuotaLimitEntry* quota_entry;
	HASH_SEQ_STATUS iter;

	RangeVar   *rv;
	Relation	rel;

	rv = makeRangeVar("diskquota", "quota_config", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		elog(LOG, "configuration table \"quota_config\" is missing in database \"%s\"," 
				" please recreate diskquota extension",
			 get_database_name(MyDatabaseId));
		return false;
	}
	heap_close(rel, NoLock);

	/* clear entries in quota limit map*/
	hash_seq_init(&iter, namespace_quota_limit_map);
	while ((quota_entry = hash_seq_search(&iter)) != NULL)
	{
		(void) hash_search(namespace_quota_limit_map,
				(void *) &quota_entry->targetoid,
				HASH_REMOVE, NULL);
	}

	hash_seq_init(&iter, role_quota_limit_map);
	while ((quota_entry = hash_seq_search(&iter)) != NULL)
	{
		(void) hash_search(role_quota_limit_map,
				(void *) &quota_entry->targetoid,
				HASH_REMOVE, NULL);
	}

	ret = SPI_execute("select targetoid, quotatype, quotalimitMB from diskquota.quota_config", true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 3 ||
		TupleDescAttr(tupdesc, 0)->atttypid != OIDOID ||
		TupleDescAttr(tupdesc, 1)->atttypid != INT4OID ||
		TupleDescAttr(tupdesc, 2)->atttypid != INT8OID)
	{
		elog(LOG, "configuration table \"quota_config\" is corruptted in database \"%s\"," 
				" please recreate diskquota extension",
			 get_database_name(MyDatabaseId));
		return false;
	}

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		dat;
		Oid			targetOid;
		int64		quota_limit_mb;
		QuotaType	quotatype;
		bool		isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			continue;
		targetOid = DatumGetObjectId(dat);
		
		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		if (isnull)
			continue;
		quotatype = (QuotaType)DatumGetInt32(dat);

		dat = SPI_getbinval(tup, tupdesc, 3, &isnull);
		if (isnull)
			continue;
		quota_limit_mb = DatumGetInt64(dat);

		if (quotatype == NAMESPACE_QUOTA)
		{
			quota_entry = (QuotaLimitEntry *)hash_search(namespace_quota_limit_map,
												&targetOid,
												HASH_ENTER, &found);
			quota_entry->limitsize = quota_limit_mb;
		}
		else if (quotatype == ROLE_QUOTA)
		{
			quota_entry = (QuotaLimitEntry *)hash_search(role_quota_limit_map,
												&targetOid,
												HASH_ENTER, &found);
			quota_entry->limitsize = quota_limit_mb;
		}
	}
	return true;
}

/*
 * Given table oid, search for namespace and owner.
 */
static void
get_rel_owner_schema(Oid relid, Oid *ownerOid, Oid *nsOid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		*ownerOid = reltup->relowner;
		*nsOid = reltup->relnamespace;
		ReleaseSysCache(tp);
	}
	return;
}

/*
 * Given table oid, check whether quota limit
 * of table's schema or table's owner are reached.
 * Do enforcemet if quota exceeds.
 */
bool
quota_check_common(Oid reloid)
{
	Oid ownerOid = InvalidOid;
	Oid nsOid = InvalidOid;
	bool found;
	BlackMapEntry keyitem;
	memset(&keyitem, 0, sizeof(BlackMapEntry));
	get_rel_owner_schema(reloid, &ownerOid, &nsOid);
	LWLockAcquire(black_map_shm_lock->lock, LW_SHARED);

	if ( nsOid != InvalidOid)
	{
		keyitem.targetoid = nsOid;
		keyitem.databaseoid = MyDatabaseId;
		keyitem.targettype = NAMESPACE_QUOTA;
		hash_search(disk_quota_black_map,
				&keyitem,
				HASH_FIND, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("schema's disk space quota exceeded with name:%s", get_namespace_name(nsOid))));
			return false;
		}

	}

	if ( ownerOid != InvalidOid)
	{
		keyitem.targetoid = ownerOid;
		keyitem.databaseoid = MyDatabaseId;
		keyitem.targettype = ROLE_QUOTA;
		hash_search(disk_quota_black_map,
				&keyitem,
				HASH_FIND, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("role's disk space quota exceeded with name:%s", GetUserNameFromId(ownerOid, false))));
			return false;
		}
	}
	LWLockRelease(black_map_shm_lock->lock);
	return true;
}
