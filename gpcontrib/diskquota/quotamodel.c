/* -------------------------------------------------------------------------
 *
 * quotamodel.c
 *
 * This code is responsible for init disk quota model and refresh disk quota
 * model. Disk quota related Shared memory initialization is also implemented
 * in this file.
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		diskquota/quotamodel.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
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

#include <stdlib.h>
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"

#include "gp_activetable.h"
#include "diskquota.h"

/* cluster level max size of black list */
#define MAX_DISK_QUOTA_BLACK_ENTRIES (1024 * 1024)
/* cluster level init size of black list */
#define INIT_DISK_QUOTA_BLACK_ENTRIES 8192
/* per database level max size of black list */
#define MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES 8192
#define MAX_NUM_KEYS_QUOTA_MAP 8

typedef struct TableSizeEntry TableSizeEntry;
typedef struct NamespaceSizeEntry NamespaceSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct QuotaLimitEntry QuotaLimitEntry;
typedef struct BlackMapEntry BlackMapEntry;
typedef struct LocalBlackMapEntry LocalBlackMapEntry;

/*
 * local cache of table disk size and corresponding schema and owner
 */
struct TableSizeEntry
{
	Oid		reloid;
	Oid		tablespace_oid;
	Oid		namespaceoid;
	Oid		owneroid;
	int64		totalsize;		/* table size including fsm, visibility map
								 * etc. */
	bool		is_exist;		/* flag used to check whether table is already
								 * dropped */
	bool		need_flush;		/* whether need to flush to table table_size */
};

struct QuotaMapEntry {
	Oid keys[MAX_NUM_KEYS_QUOTA_MAP];
	int64 size;
	int64 limit;
};

struct QuotaInfo {
	char *map_name;
	unsigned int num_keys;
	Oid *sys_cache;
	HTAB *map;
};

struct QuotaInfo quota_info[NUM_QUOTA_TYPES] = {
	[NAMESPACE_QUOTA] = {
		.map_name = "Namespace map",
		.num_keys = 1,
		.sys_cache = (Oid[]){ NAMESPACEOID },
		.map = NULL
	},
	[ROLE_QUOTA] = {
		.map_name = "Role map",
		.num_keys = 1,
		.sys_cache = (Oid[]){ AUTHOID },
		.map = NULL
	},
	[NAMESPACE_TABLESPACE_QUOTA] = {
		.map_name = "Tablespace-namespace map",
		.num_keys = 2,
		.sys_cache = (Oid[]){ NAMESPACEOID, TABLESPACEOID },
		.map = NULL
	},
	[ROLE_TABLESPACE_QUOTA] = {
		.map_name = "Tablespace-role map",
		.num_keys = 2,
		.sys_cache = (Oid[]){ AUTHOID, TABLESPACEOID },
		.map = NULL
	}
};

/* global blacklist for which exceed their quota limit */
struct BlackMapEntry
{
	Oid		targetoid;
	Oid		databaseoid;
	Oid 		tablespace_oid;
	uint32		targettype;
};

/* local blacklist for which exceed their quota limit */
struct LocalBlackMapEntry
{
	BlackMapEntry 	keyitem;
	bool		isexceeded;
};

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;

/* black list for database objects which exceed their quota limit */
static HTAB *disk_quota_black_map = NULL;
static HTAB *local_disk_quota_black_map = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* functions to maintain the quota maps */
static void init_all_quota_maps(void);
static void update_size_for_quota(int64 size, QuotaType type, Oid* keys);
static void update_limit_for_quota(int64 limit, QuotaType type, Oid* keys);
static void remove_quota(QuotaType type, Oid* keys);
static void add_quota_to_blacklist(QuotaType type, Oid targetOid, Oid tablespace_oid);
static void check_quota_map(QuotaType type);
static void clear_all_quota_maps(void);
static void vacuum_all_quota_maps(void);
static void transfer_table_for_quota(int64 totalsize, QuotaType type, Oid* old_keys, Oid* new_keys);

/* functions to refresh disk quota model*/
static void refresh_disk_quota_usage(bool is_init);
static void calculate_table_disk_usage(bool is_init);
static void flush_to_table_size(void);
static void flush_local_black_map(void);
static bool load_quotas(void);
static void do_load_quotas(void);
static bool do_check_diskquota_state_is_ready(void);

static Size DiskQuotaShmemSize(void);
static void disk_quota_shmem_startup(void);
static void init_lwlocks(void);

static void truncateStringInfo(StringInfo str, int nchars);
static void export_exceeded_error(BlackMapEntry *blackentry);

static void
init_all_quota_maps(void)
{
	HASHCTL hash_ctl = {0};
	hash_ctl.entrysize = sizeof(struct QuotaMapEntry);
	hash_ctl.hcxt = TopMemoryContext;
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		hash_ctl.keysize = quota_info[type].num_keys * sizeof(Oid);
		if (quota_info[type].num_keys == 1)
		{
			hash_ctl.hash = oid_hash;
		}
		else
		{
			hash_ctl.hash = tag_hash;
		}
		if (quota_info[type].map != NULL)
		{
			hash_destroy(quota_info[type].map);
		}
		quota_info[type].map = hash_create(
			quota_info[type].map_name, 1024L, &hash_ctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	}
}

/* add a new entry quota or update the old entry quota */
static void
update_size_for_quota(int64 size, QuotaType type, Oid* keys)
{
	bool found;
	struct QuotaMapEntry *entry = hash_search(
		quota_info[type].map, keys, HASH_ENTER, &found);
	if (!found)
	{
		entry->size = size;
		entry->limit = -1;
		memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
	}
	else
	{
		entry->size += size;
	}
}

/* add a new entry quota or update the old entry limit */
static void
update_limit_for_quota(int64 limit, QuotaType type, Oid* keys)
{
	bool found;
	struct QuotaMapEntry *entry = hash_search(
		quota_info[type].map, keys, HASH_ENTER, &found);
	if (!found)
	{
		entry->size = 0;
		memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
	}
	entry->limit = limit;
}

/* remove a entry quota from the map */
static void
remove_quota(QuotaType type, Oid* keys)
{
	hash_search(quota_info[type].map, keys, HASH_REMOVE, NULL);
}

/*
 * Compare the disk quota limit and current usage of a database object.
 * Put them into local blacklist if quota limit is exceeded.
 */
static void
add_quota_to_blacklist(QuotaType type, Oid targetOid, Oid tablespace_oid)
{
	LocalBlackMapEntry *localblackentry;
	BlackMapEntry keyitem = {0};

	keyitem.targetoid = targetOid;
	keyitem.databaseoid = MyDatabaseId;
	keyitem.tablespace_oid = tablespace_oid;
	keyitem.targettype = (uint32) type;
	ereport(DEBUG1, (errmsg("[diskquota] Put object %u to blacklist", targetOid)));
	localblackentry = (LocalBlackMapEntry *) hash_search(local_disk_quota_black_map,
															&keyitem,
															HASH_ENTER, NULL);
	localblackentry->isexceeded = true;

}

/*
 * Check the quota map, if the entry doesn't exist any more,
 * remove it from the map. Otherwise, check if it has hit
 * the quota limit, if it does, add it to the black list.
 */
static void
check_quota_map(QuotaType type)
{
	HeapTuple tuple;
	HASH_SEQ_STATUS iter;
	struct QuotaMapEntry *entry;

	hash_seq_init(&iter, quota_info[type].map);

	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		bool removed = false;
		for (int i = 0; i < quota_info[type].num_keys; ++i)
		{
			tuple = SearchSysCache1(quota_info[type].sys_cache[i], ObjectIdGetDatum(entry->keys[i]));
			if (!HeapTupleIsValid(tuple))
			{
				remove_quota(type, entry->keys);
				removed = true;
				break;
			}
			ReleaseSysCache(tuple);
		}
		if (!removed)
		{
			if (entry->limit >= 0 && entry->size >= entry->limit)
			{
				Oid targetOid = entry->keys[0];
				Oid tablespace_oid =
					(type == NAMESPACE_TABLESPACE_QUOTA) || (type == ROLE_TABLESPACE_QUOTA) ? entry->keys[1] : InvalidOid;
				/* when quota type is not NAMESPACE_TABLESPACE_QUOTA or ROLE_TABLESPACE_QUOTA, the tablespace_oid
				 * is set to be InvalidOid, so when we get it from map, also set it to be InvalidOid
				 */
				add_quota_to_blacklist(type, targetOid, tablespace_oid);
			}
		}
	}
}

/* transfer one table's size from one quota to another quota */
static void
transfer_table_for_quota(int64 totalsize, QuotaType type, Oid* old_keys, Oid* new_keys)
{
	update_size_for_quota(-totalsize, type, old_keys);
	update_size_for_quota(totalsize, type, new_keys);
}

static void
clear_all_quota_maps(void)
{
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		HASH_SEQ_STATUS iter = {0};
		hash_seq_init(&iter, quota_info[type].map);
		struct QuotaMapEntry *entry = NULL;
		while ((entry = hash_seq_search(&iter)) != NULL)
		{
			 entry->limit = -1;
		}
	}
}

static void
vacuum_all_quota_maps(void) {
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		HASH_SEQ_STATUS iter = {0};
		hash_seq_init(&iter, quota_info[type].map);
		struct QuotaMapEntry *entry = NULL;
		while ((entry = hash_seq_search(&iter)) != NULL)
		{
			if (entry->limit == -1)
			{
				remove_quota(type, entry->keys);
			}
		}

	}

}
/* ---- Functions for disk quota shared memory ---- */
/*
 * DiskQuotaShmemInit
 *		Allocate and initialize diskquota-related shared memory
 *		This function is called in _PG_init().
 */
void
init_disk_quota_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(DiskQuotaShmemSize());
	/* locks for diskquota refer to init_lwlocks() for details */
	RequestAddinLWLocks(DiskQuotaLocksItemNumber);

	/* Install startup hook to initialize our shared memory. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = disk_quota_shmem_startup;
}

/*
 * DiskQuotaShmemInit hooks.
 * Initialize shared memory data and locks.
 */
static void
disk_quota_shmem_startup(void)
{
	bool		found;
	HASHCTL		hash_ctl;

	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook) ();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	init_lwlocks();

	/*
	 * Three shared memory data. extension_ddl_message is used to handle
	 * diskquota extension create/drop command. disk_quota_black_map is used
	 * to store out-of-quota blacklist. active_tables_map is used to store
	 * active tables whose disk usage is changed.
	 */
	extension_ddl_message = ShmemInitStruct("disk_quota_extension_ddl_message",
											sizeof(ExtensionDDLMessage),
											&found);
	if (!found)
		memset((void *) extension_ddl_message, 0, sizeof(ExtensionDDLMessage));

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

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hash = oid_hash;

	monitoring_dbid_cache = ShmemInitHash("table oid cache which shoud tracking",
			MAX_NUM_MONITORED_DB,
			MAX_NUM_MONITORED_DB,
			&hash_ctl,
			HASH_ELEM | HASH_FUNCTION);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Initialize four shared memory locks.
 * active_table_lock is used to access active table map.
 * black_map_lock is used to access out-of-quota blacklist.
 * extension_ddl_message_lock is used to access content of
 * extension_ddl_message.
 * extension_ddl_lock is used to avoid concurrent diskquota
 * extension ddl(create/drop) command.
 * monitoring_dbid_cache_lock is used to shared `monitoring_dbid_cache` on segment process.
 */
static void
init_lwlocks(void)
{
	diskquota_locks.active_table_lock = LWLockAssign();
	diskquota_locks.black_map_lock = LWLockAssign();
	diskquota_locks.extension_ddl_message_lock = LWLockAssign();
	diskquota_locks.extension_ddl_lock = LWLockAssign();
	diskquota_locks.monitoring_dbid_cache_lock = LWLockAssign();
}

/*
 * DiskQuotaShmemSize
 * Compute space needed for diskquota-related shared memory
 */
static Size
DiskQuotaShmemSize(void)
{
	Size		size;

	size = sizeof(ExtensionDDLMessage);
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_BLACK_ENTRIES, sizeof(BlackMapEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaActiveTableEntry)));
	size = add_size(size, hash_estimate_size(MAX_NUM_MONITORED_DB, sizeof(Oid)));
	return size;
}


/* ---- Functions for disk quota model ---- */
/*
 * Init disk quota model when the worker process firstly started.
 */
void
init_disk_quota_model(void)
{
	HASHCTL		hash_ctl;

	/* initialize hash table for table/schema/role etc. */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = oid_hash;

	table_size_map = hash_create("TableSizeEntry map",
								 1024 * 8,
								 &hash_ctl,
								 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	init_all_quota_maps();

	/*
	 * local diskquota black map is used to reduce the lock hold time of
	 * blackmap in shared memory
	 */
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
 * Check whether the diskquota state is ready
*/
bool
check_diskquota_state_is_ready(void)
{
	bool		is_ready = false;
	bool		connected = false;
	bool		pushed_active_snap = false;
	bool		ret = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		is_ready = do_check_diskquota_state_is_ready();
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected)
		SPI_finish();
	if (pushed_active_snap)
		PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	return is_ready;
}

/*
 * Check whether the diskquota state is ready
 * For empty database, the diskquota state would
 * be ready after 'create extension diskquota' and
 * it's ready to use. But for non-empty database,
 * user need to run UDF diskquota.init_table_size_table()
 * manually to get all the table size information and
 * store them into table diskquota.table_size
 */
static bool
do_check_diskquota_state_is_ready(void)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	StringInfoData sql_command;

	/* Add the dbid to watching list, so the hook can catch the table change*/
	initStringInfo(&sql_command);
	appendStringInfo(&sql_command, "select gp_segment_id, diskquota.update_diskquota_db_list(%u, 0) from gp_dist_random('gp_id');",
				MyDatabaseId);
	ret = SPI_execute(sql_command.data, true, 0);
        if (ret != SPI_OK_SELECT)
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                                                errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));
	pfree(sql_command.data);
	/*
	 * check diskquota state from table diskquota.state errors will be catch
	 * at upper level function.
	 */
	ret = SPI_execute("select state from diskquota.state", true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 1 ||
		((tupdesc)->attrs[0])->atttypid != INT4OID)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("[diskquota] table \"state\" is corrupted in database \"%s\","
							   " please recreate diskquota extension",
							   get_database_name(MyDatabaseId))));
	}

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		dat;
		int			state;
		bool		isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			continue;
		state = DatumGetInt64(dat);

		if (state == DISKQUOTA_READY_STATE)
		{
			return true;
		}
	}
	ereport(WARNING, (errmsg("Diskquota is not in ready state. "
							 "please run UDF init_table_size_table()")));

	return false;
}

/*
 * Diskquota worker will refresh disk quota model
 * periodically. It will reload quota setting and
 * recalculate the changed disk usage.
 */
void
refresh_disk_quota_model(bool is_init)
{
	if (is_init)
		ereport(LOG, (errmsg("[diskquota] initialize quota model started")));
	/* skip refresh model when load_quotas failed */
	if (load_quotas())
	{
		refresh_disk_quota_usage(is_init);
	}
	if (is_init)
		ereport(LOG, (errmsg("[diskquota] initialize quota model finished")));
}

/*
 * Update the disk usage of namespace, role and tablespace.
 * Put the exceeded namespace and role into shared black map.
 * Parameter 'is_init' is true when it's the first time that worker
 * process is constructing quota model.
 */
static void
refresh_disk_quota_usage(bool is_init)
{
	bool		connected = false;
	bool		pushed_active_snap = false;
	bool		ret = true;

	elog(LOG, "refresh diskquota usage...");
	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		/* recalculate the disk usage of table, schema and role */
		calculate_table_disk_usage(is_init);
		for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type) {
			check_quota_map(type);
		}
		/* flush local table_size_map to user table table_size */
		flush_to_table_size();
		/* copy local black map back to shared black map */
		flush_local_black_map();
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected)
		SPI_finish();
	if (pushed_active_snap)
		PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	return;
}

/*
 *  Incremental way to update the disk quota of every database objects
 *  Recalculate the table's disk usage when it's a new table or active table.
 *  Detect the removed table if it's no longer in pg_class.
 *  If change happens, no matter size change or owner change,
 *  update namespace_size_map and role_size_map correspondingly.
 *  Parameter 'is_init' set to true at initialization stage to fetch tables
 *  size from table table_size
 */

/* FIXME: we should only care about the tables whose role, schema, or tablespace
 * has quota, this may improve the performance especially when too many tables
 * in the database
 */
static void
calculate_table_disk_usage(bool is_init)
{
	bool		table_size_map_found;
	bool		active_tbl_found;
	int64		updated_total_size;
	Relation	classRel;
	HeapTuple	tuple;
	HeapScanDesc relScan;
	TableSizeEntry *tsentry = NULL;
	Oid			relOid;
	HASH_SEQ_STATUS iter;
	HTAB	   *local_active_table_stat_map;
	DiskQuotaActiveTableEntry *active_table_entry;

	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	/*
	 * initialization stage all the tables are active. later loop, only the
	 * tables whose disk size changed will be treated as active
	 */
	local_active_table_stat_map = gp_fetch_active_tables(is_init);

	/*
	 * unset is_exist flag for tsentry in table_size_map this is used to
	 * detect tables which have been dropped.
	 */
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

		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;
		relOid = HeapTupleGetOid(tuple);

		/* ignore system table */
		if (relOid < FirstNormalObjectId)
			continue;

		tsentry = (TableSizeEntry *) hash_search(table_size_map,
												 &relOid,
												 HASH_ENTER, &table_size_map_found);

		if (!table_size_map_found)
		{
			tsentry->reloid = relOid;
			tsentry->totalsize = 0;
			tsentry->owneroid = InvalidOid;
			tsentry->namespaceoid = InvalidOid;
			tsentry->tablespace_oid = InvalidOid;
			tsentry->need_flush = true;
		}

		/* mark tsentry is_exist */
		if (tsentry)
			tsentry->is_exist = true;

		active_table_entry = (DiskQuotaActiveTableEntry *) hash_search(local_active_table_stat_map, &relOid, HASH_FIND, &active_tbl_found);

		/* skip to recalculate the tables which are not in active list */
		if (active_tbl_found)
		{
			/* pretend process as utility mode, and append the table size on master */
			Gp_role = GP_ROLE_UTILITY;

			/* DirectFunctionCall1 may fail, since table maybe dropped by other backend */
			PG_TRY();
			{
				/* call pg_total_relation_size to get the active table size */
				active_table_entry->tablesize += (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size, ObjectIdGetDatum(relOid)));
			}
			PG_CATCH();
			{
				HOLD_INTERRUPTS();
				FlushErrorState();
				RESUME_INTERRUPTS();
			}
			PG_END_TRY();

			Gp_role = GP_ROLE_DISPATCH;

			/* firstly calculate the updated total size of a table */
			updated_total_size = active_table_entry->tablesize - tsentry->totalsize;

			/* update the table_size entry */
			tsentry->totalsize = (int64) active_table_entry->tablesize;
			tsentry->need_flush = true;

			/* update the disk usage, there may be entries in the map whose keys are InvlidOid as the tsentry does not exist in the table_size_map */
			update_size_for_quota(updated_total_size, NAMESPACE_QUOTA, (Oid[]){tsentry->namespaceoid});
			update_size_for_quota(updated_total_size, ROLE_QUOTA, (Oid[]){tsentry->owneroid});
			update_size_for_quota(updated_total_size, ROLE_TABLESPACE_QUOTA, (Oid[]){tsentry->owneroid, tsentry->tablespace_oid});
			update_size_for_quota(updated_total_size, NAMESPACE_TABLESPACE_QUOTA, (Oid[]){tsentry->namespaceoid, tsentry->tablespace_oid});
		}

		/* table size info doesn't need to flush at init quota model stage */
		if (is_init)
		{
			tsentry->need_flush = false;
		}

		/* if schema change, transfer the file size */
		if (tsentry->namespaceoid != classForm->relnamespace)
		{
			transfer_table_for_quota(
				tsentry->totalsize,
				NAMESPACE_QUOTA,
				(Oid[]){tsentry->namespaceoid},
				(Oid[]){classForm->relnamespace});
			transfer_table_for_quota(
				tsentry->totalsize,
				NAMESPACE_TABLESPACE_QUOTA,
				(Oid[]){tsentry->namespaceoid, tsentry->tablespace_oid},
				(Oid[]){classForm->relnamespace, tsentry->tablespace_oid});
			tsentry->namespaceoid = classForm->relnamespace;
		}
		/* if owner change, transfer the file size */
		if (tsentry->owneroid != classForm->relowner)
		{
			transfer_table_for_quota(
				tsentry->totalsize,
				ROLE_QUOTA,
				(Oid[]){tsentry->owneroid},
				(Oid[]){classForm->relowner}
			);
			transfer_table_for_quota(
				tsentry->totalsize,
				ROLE_TABLESPACE_QUOTA,
				(Oid[]){tsentry->owneroid, tsentry->tablespace_oid},
				(Oid[]){classForm->relowner, tsentry->tablespace_oid}
			);
			tsentry->owneroid = classForm->relowner;
		}

		if (tsentry->tablespace_oid != classForm->reltablespace)
		{
			transfer_table_for_quota(
				tsentry->totalsize,
				NAMESPACE_TABLESPACE_QUOTA,
				(Oid[]){tsentry->namespaceoid, tsentry->tablespace_oid},
				(Oid[]){tsentry->namespaceoid, classForm->reltablespace}
			);
			transfer_table_for_quota(
				tsentry->totalsize,
				ROLE_TABLESPACE_QUOTA,
				(Oid[]){tsentry->owneroid, tsentry->tablespace_oid},
				(Oid[]){tsentry->owneroid, classForm->reltablespace}
			);
			tsentry->tablespace_oid = classForm->reltablespace;
		}
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);
	hash_destroy(local_active_table_stat_map);

	/*
	 * Process removed tables. Reduce schema and role size firstly. Remove
	 * table from table_size_map in flush_to_table_size() function later.
	 */
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		if (tsentry->is_exist == false)
		{
			update_size_for_quota(-tsentry->totalsize, NAMESPACE_QUOTA, (Oid[]){tsentry->namespaceoid});
			update_size_for_quota(-tsentry->totalsize, ROLE_QUOTA, (Oid[]){tsentry->owneroid});
			update_size_for_quota(-tsentry->totalsize, ROLE_TABLESPACE_QUOTA, (Oid[]){tsentry->owneroid, tsentry->tablespace_oid});
			update_size_for_quota(-tsentry->totalsize, NAMESPACE_TABLESPACE_QUOTA, (Oid[]){tsentry->namespaceoid, tsentry->tablespace_oid});
		}
	}
}

/*
 * Flush the table_size_map to user table diskquota.table_size
 * To improve update performance, we first delete all the need_to_flush
 * entries in table table_size. And then insert new table size entries into
 * table table_size.
 */
static void
flush_to_table_size(void)
{
	HASH_SEQ_STATUS iter;
	TableSizeEntry *tsentry = NULL;
	StringInfoData delete_statement;
	StringInfoData insert_statement;
	bool		delete_statement_flag = false;
	bool		insert_statement_flag = false;
	int			ret;

	/* TODO: Add flush_size_interval to avoid flushing size info in every loop */

	/* concatenate all the need_to_flush table to SQL string */
	initStringInfo(&delete_statement);
	appendStringInfo(&delete_statement, "delete from diskquota.table_size where tableid in (");
	initStringInfo(&insert_statement);
	appendStringInfo(&insert_statement, "insert into diskquota.table_size values ");
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		/* delete dropped table from both table_size_map and table table_size */
		if (tsentry->is_exist == false)
		{
			appendStringInfo(&delete_statement, "%u, ", tsentry->reloid);
			delete_statement_flag = true;

			hash_search(table_size_map,
						&tsentry->reloid,
						HASH_REMOVE, NULL);
		}
		/* update the table size by delete+insert in table table_size */
		else if (tsentry->need_flush == true)
		{
			tsentry->need_flush = false;
			appendStringInfo(&delete_statement, "%u, ", tsentry->reloid);
			appendStringInfo(&insert_statement, "(%u,%ld), ", tsentry->reloid, tsentry->totalsize);
			delete_statement_flag = true;
			insert_statement_flag = true;
		}
	}
	truncateStringInfo(&delete_statement, delete_statement.len - strlen(", "));
	truncateStringInfo(&insert_statement, insert_statement.len - strlen(", "));
	appendStringInfo(&delete_statement, ");");
	appendStringInfo(&insert_statement, ";");

	if (delete_statement_flag)
	{
		ret = SPI_execute(delete_statement.data, false, 0);
		if (ret != SPI_OK_DELETE)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("[diskquota] flush_to_table_size SPI_execute failed: error code %d", ret)));
	}
	if (insert_statement_flag)
	{
		ret = SPI_execute(insert_statement.data, false, 0);
		if (ret != SPI_OK_INSERT)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("[diskquota] flush_to_table_size SPI_execute failed: error code %d", ret)));
	}
}

/*
 * Generate the new shared blacklist from the local_black_list which
 * exceed the quota limit.
 * local_black_list is used to reduce the lock contention.
 */
static void
flush_local_black_map(void)
{
	HASH_SEQ_STATUS iter;
	LocalBlackMapEntry *localblackentry;
	BlackMapEntry *blackentry;
	bool		found;

	LWLockAcquire(diskquota_locks.black_map_lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, local_disk_quota_black_map);
	while ((localblackentry = hash_seq_search(&iter)) != NULL)
	{
		if (localblackentry->isexceeded)
		{
			blackentry = (BlackMapEntry *) hash_search(disk_quota_black_map,
													   (void *) &localblackentry->keyitem,
													   HASH_ENTER_NULL, &found);
			if (blackentry == NULL)
			{
				ereport(WARNING, (errmsg("[diskquota] Shared disk quota black map size limit reached."
										 "Some out-of-limit schemas or roles will be lost"
										 "in blacklist.")));
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
	LWLockRelease(diskquota_locks.black_map_lock);
}

/*
 * Make sure a StringInfo's string is no longer than 'nchars' characters.
 */
static void
truncateStringInfo(StringInfo str, int nchars)
{
	if (str &&
		str->len > nchars)
	{
		Assert(str->data != NULL &&
			   str->len <= str->maxlen);
		str->len = nchars;
		str->data[nchars] = '\0';
	}
}

/*
 * Interface to load quotas from diskquota configuration table(quota_config).
 */
static bool
load_quotas(void)
{
	bool		connected = false;
	bool		pushed_active_snap = false;
	bool		ret = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		do_load_quotas();
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected)
		SPI_finish();
	if (pushed_active_snap)
		PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	return ret;
}

/*
 * Load quotas from diskquota configuration table(quota_config).
*/
static void
do_load_quotas(void)
{
	int		ret;
	TupleDesc	tupdesc;
	int		i;

	/*
	 * TODO: we should skip to reload quota config when there is no change in
	 * quota.config. A flag in shared memory could be used to detect the quota
	 * config change.
	 */
	clear_all_quota_maps();
	const unsigned int NUM_ATTRIBUTES = 4;

	/*
	 * read quotas from diskquota.quota_config and target table
	 */

	Oid nsoid = get_namespace_oid("diskquota", false);
	if (nsoid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] diskquota schema doesn't exist in database \"%s\","
					" please recreate diskquota extension",
					get_database_name(MyDatabaseId))));
	Oid targetTableOid = get_relname_relid("target", nsoid);
	/* 
	 * For diskquota 1.0, there is no target table in diskquota schema. 
	 * Why do we need this?
	 * As when we upgrade diskquota extension from 1.0 to another version,
	 * we need firstly reload the new diskquota.so and then execute the
	 * upgrade SQL. However, between the 2 steps, the new diskquota.so
	 * needs to work with the old version diskquota sql file, otherwise,
	 * the init work will fail and diskquota can not work correctly.
	 * Maybe this is not the best sulotion, only a work arround. Optimizing
	 * the init procedure is a better solution.
	 */ 
	if (targetTableOid == InvalidOid)
	{
		ret = SPI_execute("select targetoid, quotatype, quotalimitMB, 0 as tablespaceoid from diskquota.quota_config", true, 0);
	}
	else
	{
		ret = SPI_execute(
				"SELECT targetOid, c.quotaType, quotalimitMB, COALESCE(tablespaceoid, 0)"
				"FROM diskquota.quota_config c LEFT OUTER JOIN diskquota.target t "
				"ON c.targetOid = t.primaryOid and c.quotatype = t.quotatype", true, 0);
	}
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] load_quotas SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != NUM_ATTRIBUTES ||
			((tupdesc)->attrs[0])->atttypid != OIDOID ||
			((tupdesc)->attrs[1])->atttypid != INT4OID ||
			((tupdesc)->attrs[2])->atttypid != INT8OID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] configuration table is corrupted in database \"%s\","
						" please recreate diskquota extension",
						get_database_name(MyDatabaseId))));
	}

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		vals[NUM_ATTRIBUTES];
		bool		isnull[NUM_ATTRIBUTES];

		for (int i = 0; i < NUM_ATTRIBUTES; ++i)
		{
			vals[i] = SPI_getbinval(tup, tupdesc, i + 1, &(isnull[i]));
			if (i <= 2 && isnull[i])
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("[diskquota] attibutes in configuration table MUST NOT be NULL")));
			}
		}

		Oid 	targetOid = DatumGetObjectId(vals[0]);
		int	quotaType = (QuotaType) DatumGetInt32(vals[1]);
		int64	quota_limit_mb = DatumGetInt64(vals[2]);
		Oid	spcOid = DatumGetObjectId(vals[3]);

		if (spcOid == InvalidOid)
		{
			if (quota_info[quotaType].num_keys != 1) {
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("[diskquota] tablespace Oid MUST NOT be NULL for quota type: %d", quotaType)));
			}
			update_limit_for_quota(quota_limit_mb * (1 << 20), quotaType, (Oid[]){targetOid});
		}
		else
		{
			update_limit_for_quota(quota_limit_mb * (1 << 20), quotaType, (Oid[]){targetOid, spcOid});
		}
	}

	vacuum_all_quota_maps();
	return;
}

/*
 * Given table oid, search for namespace and owner.
 */
static bool
get_rel_owner_schema_tablespace(Oid relid, Oid *ownerOid, Oid *nsOid, Oid *tablespace_oid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	bool found = HeapTupleIsValid(tp);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		*ownerOid = reltup->relowner;
		*nsOid = reltup->relnamespace;
		*tablespace_oid = reltup->reltablespace;
		ReleaseSysCache(tp);
	}
	return found;
}

/*
 * Given table oid, check whether quota limit
 * of table's schema or table's owner are reached.
 * Do enforcement if quota exceeds.
 */
bool
quota_check_common(Oid reloid)
{
	Oid			ownerOid = InvalidOid;
	Oid			nsOid = InvalidOid;
	Oid 		tablespace_oid = InvalidOid;
	bool		found;
	BlackMapEntry keyitem;

	if (!IsTransactionState())
	{
		return true;
	}
	
	bool found_rel = get_rel_owner_schema_tablespace(reloid, &ownerOid, &nsOid, &tablespace_oid);
	if (!found_rel)
	{
		return true;
	}
	LWLockAcquire(diskquota_locks.black_map_lock, LW_SHARED);
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		if (type == ROLE_QUOTA || type == ROLE_TABLESPACE_QUOTA)
		{
			keyitem.targetoid = ownerOid;
		}
		else if (type == NAMESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		{
			keyitem.targetoid = nsOid;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[diskquota] unknown quota type: %d", type)));
		}
		if (type == ROLE_TABLESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		{
			keyitem.tablespace_oid = tablespace_oid;
		}
		else
		{
			/* refer to add_quota_to_blacklist */
			keyitem.tablespace_oid = InvalidOid;
		}
		keyitem.databaseoid = MyDatabaseId;
		keyitem.targettype = type;
		hash_search(disk_quota_black_map,
					&keyitem,
					HASH_FIND, &found);
		if (found)
		{
			LWLockRelease(diskquota_locks.black_map_lock);
			export_exceeded_error(&keyitem);
			return false;
		}
	}
	LWLockRelease(diskquota_locks.black_map_lock);
	return true;
}

/*
 * invalidate all black entry with a specific dbid in SHM
 */
void
invalidate_database_blackmap(Oid dbid)
{
	BlackMapEntry *entry;
	HASH_SEQ_STATUS iter;

	LWLockAcquire(diskquota_locks.black_map_lock, LW_EXCLUSIVE);
	hash_seq_init(&iter, disk_quota_black_map);
	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		if (entry->databaseoid == dbid)
		{
			hash_search(disk_quota_black_map, entry, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(diskquota_locks.black_map_lock);
}

static void
export_exceeded_error(BlackMapEntry *blackentry)
{
	switch(blackentry->targettype)
	{
		case NAMESPACE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("schema's disk space quota exceeded with name:%s", get_namespace_name(blackentry->targetoid))));
			break;
		case ROLE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("role's disk space quota exceeded with name:%s", GetUserNameFromId(blackentry->targetoid))));
			break;
		case NAMESPACE_TABLESPACE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("tablespace:%s schema:%s diskquota exceeded", get_tablespace_name(blackentry->tablespace_oid), get_namespace_name(blackentry->targetoid))));
			break;
		case ROLE_TABLESPACE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("tablespace: %s role: %s diskquota exceeded", get_tablespace_name(blackentry->tablespace_oid), GetUserNameFromId(blackentry->targetoid))));
			break;
		default :
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("diskquota exceeded, unknown quota type")));
	}

}
