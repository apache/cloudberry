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
#include "diskquota.h"
#include "gp_activetable.h"
#include "relation_cache.h"

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "port/atomics.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "libpq-fe.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbutil.h"

/* cluster level max size of black list */
#define MAX_DISK_QUOTA_BLACK_ENTRIES (1024 * 1024)
/* cluster level init size of black list */
#define INIT_DISK_QUOTA_BLACK_ENTRIES 8192
/* per database level max size of black list */
#define MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES 8192
#define MAX_NUM_KEYS_QUOTA_MAP 8
/* Number of attributes in quota configuration records. */
#define NUM_QUOTA_CONFIG_ATTRS 5

typedef struct TableSizeEntry TableSizeEntry;
typedef struct NamespaceSizeEntry NamespaceSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct QuotaLimitEntry QuotaLimitEntry;
typedef struct BlackMapEntry BlackMapEntry;
typedef struct GlobalBlackMapEntry GlobalBlackMapEntry;
typedef struct LocalBlackMapEntry LocalBlackMapEntry;


int 	SEGCOUNT = 0;
/*
 * local cache of table disk size and corresponding schema and owner
 */
struct TableSizeEntry
{
	Oid 		reloid;
	int16 		segid;
	Oid		tablespaceoid;
	Oid		namespaceoid;
	Oid		owneroid;
	int64		totalsize;		/* table size including fsm, visibility map
								 * etc. */
	bool		is_exist;		/* flag used to check whether table is already
								 * dropped */
	bool		need_flush;		/* whether need to flush to table table_size */
};

struct QuotaMapEntryKey {
	Oid keys[MAX_NUM_KEYS_QUOTA_MAP];
	int16 segid;
};

struct QuotaMapEntry {
	Oid keys[MAX_NUM_KEYS_QUOTA_MAP];
	int16 segid;
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
	Oid 		tablespaceoid;
	uint32		targettype;
	/*
	 * QD index the blackmap by (targetoid, databaseoid, tablespaceoid, targettype).
	 * QE index the blackmap by (relfilenode).
	 */
	RelFileNode		relfilenode;
};

struct GlobalBlackMapEntry
{
	BlackMapEntry 	keyitem;
	bool            segexceeded;
	/*
	 * When the quota limit is exceeded on segment servers,
	 * we need an extra auxiliary field to preserve the quota
	 * limitation information for error message on segment
	 * servers, e.g., targettype, targetoid. This field is
	 * useful on segment servers.
	 */
	BlackMapEntry	auxblockinfo;
};

/* local blacklist for which exceed their quota limit */
struct LocalBlackMapEntry
{
	BlackMapEntry 	keyitem;
	bool		isexceeded;
	bool		segexceeded;
};

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;

/* black list for database objects which exceed their quota limit */
static HTAB *disk_quota_black_map = NULL;
static HTAB *local_disk_quota_black_map = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* functions to maintain the quota maps */
static void init_all_quota_maps(void);
static void update_size_for_quota(int64 size, QuotaType type, Oid* keys, int16 segid);
static void update_limit_for_quota(int64 limit, float segratio, QuotaType type, Oid* keys);
static void remove_quota(QuotaType type, Oid* keys, int16 segid);
static void add_quota_to_blacklist(QuotaType type, Oid targetOid, Oid tablespaceoid, bool segexceeded);
static void check_quota_map(QuotaType type);
static void clear_all_quota_maps(void);
static void transfer_table_for_quota(int64 totalsize, QuotaType type, Oid* old_keys, Oid* new_keys, int16 segid);

/* functions to refresh disk quota model*/
static void refresh_disk_quota_usage(bool is_init);
static void calculate_table_disk_usage(bool is_init, HTAB *local_active_table_stat_map);
static void flush_to_table_size(void);
static void flush_local_black_map(void);
static void dispatch_blackmap(HTAB *local_active_table_stat_map);
static bool load_quotas(void);
static void do_load_quotas(void);
static bool do_check_diskquota_state_is_ready(void);

static Size DiskQuotaShmemSize(void);
static void disk_quota_shmem_startup(void);
static void init_lwlocks(void);

static void export_exceeded_error(GlobalBlackMapEntry *entry, bool skip_name);
void truncateStringInfo(StringInfo str, int nchars);

static void
init_all_quota_maps(void)
{
	HASHCTL hash_ctl = {0};
	hash_ctl.entrysize = sizeof(struct QuotaMapEntry);
	hash_ctl.hcxt = TopMemoryContext;
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		hash_ctl.keysize = sizeof(struct QuotaMapEntryKey);
		hash_ctl.hash = tag_hash;
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
update_size_for_quota(int64 size, QuotaType type, Oid* keys, int16 segid)
{
	bool found;
	struct QuotaMapEntryKey key = {0};
	memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
	key.segid = segid;
	struct QuotaMapEntry *entry = hash_search(
		quota_info[type].map, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->size = 0;
		entry->limit = -1;
		memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
		entry->segid = key.segid;
	}
	entry->size += size;
}

/* add a new entry quota or update the old entry limit */
static void
update_limit_for_quota(int64 limit, float segratio, QuotaType type, Oid* keys)
{
	bool found;
	for ( int i = -1; i < SEGCOUNT ; i++)
	{
		struct QuotaMapEntryKey key = {0};
		memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
		key.segid = i;
		struct QuotaMapEntry *entry = hash_search(
				quota_info[type].map, &key, HASH_ENTER, &found);
		if (!found)
		{
			entry->size = 0;
			memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
			entry->segid = key.segid;
		}
		if (key.segid == -1)
		{
			entry->limit = limit;
		}
		else
		{
			entry->limit = round((limit / SEGCOUNT) * segratio);
		}
	}
}

/* remove a entry quota from the map */
static void
remove_quota(QuotaType type, Oid* keys, int16 segid)
{
	struct QuotaMapEntryKey key = {0};
	memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
	key.segid = segid;
	hash_search(quota_info[type].map, &key, HASH_REMOVE, NULL);
}

/*
 * Compare the disk quota limit and current usage of a database object.
 * Put them into local blacklist if quota limit is exceeded.
 */
static void
add_quota_to_blacklist(QuotaType type, Oid targetOid, Oid tablespaceoid, bool segexceeded)
{

	LocalBlackMapEntry *localblackentry;
	BlackMapEntry keyitem = {0};

	keyitem.targetoid = targetOid;
	keyitem.databaseoid = MyDatabaseId;
	keyitem.tablespaceoid = tablespaceoid;
	keyitem.targettype = (uint32) type;
	ereport(DEBUG1, (errmsg("[diskquota] Put object %u to blacklist", targetOid)));
	localblackentry = (LocalBlackMapEntry *) hash_search(local_disk_quota_black_map,
			&keyitem,
			HASH_ENTER, NULL);
	localblackentry->isexceeded = true;
	localblackentry->segexceeded = segexceeded;

}

/*
 * Check the quota map, if the entry doesn't exist anymore,
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
				remove_quota(type, entry->keys, entry->segid);
				removed = true;
				break;
			}
			ReleaseSysCache(tuple);
		}
		if (!removed && entry->limit > 0)
		{
			if (entry->size >= entry->limit)
			{
				Oid targetOid = entry->keys[0];
				/* when quota type is not NAMESPACE_TABLESPACE_QUOTA or ROLE_TABLESPACE_QUOTA, the tablespaceoid
				 * is set to be InvalidOid, so when we get it from map, also set it to be InvalidOid
				 */
				Oid tablespaceoid =
						(type == NAMESPACE_TABLESPACE_QUOTA) || (type == ROLE_TABLESPACE_QUOTA) ? entry->keys[1] : InvalidOid;

				bool segmentExceeded = entry->segid == -1 ? false : true;
				add_quota_to_blacklist(type, targetOid, tablespaceoid, segmentExceeded);
			}
		}
	}
}

/* transfer one table's size from one quota to another quota */
static void
transfer_table_for_quota(int64 totalsize, QuotaType type, Oid* old_keys, Oid* new_keys, int16 segid)
{
	update_size_for_quota(-totalsize, type, old_keys, segid);
	update_size_for_quota(totalsize, type, new_keys, segid);
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
	 * Four shared memory data. extension_ddl_message is used to handle
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
	hash_ctl.entrysize = sizeof(GlobalBlackMapEntry);
	hash_ctl.hash = tag_hash;

	disk_quota_black_map = ShmemInitHash("blackmap whose quota limitation is reached",
										 INIT_DISK_QUOTA_BLACK_ENTRIES,
										 MAX_DISK_QUOTA_BLACK_ENTRIES,
										 &hash_ctl,
										 HASH_ELEM | HASH_FUNCTION);

	init_shm_worker_active_tables();

	init_shm_worker_relation_cache();

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hash = oid_hash;

	monitoring_dbid_cache = ShmemInitHash("table oid cache which shoud tracking",
			MAX_NUM_MONITORED_DB,
			MAX_NUM_MONITORED_DB,
			&hash_ctl,
			HASH_ELEM | HASH_FUNCTION);

	/* use disk_quota_worker_map to manage diskquota worker processes. */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(DiskQuotaWorkerEntry);
	hash_ctl.hash = oid_hash;

	disk_quota_worker_map = ShmemInitHash("disk quota worker map",
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
	diskquota_locks.relation_cache_lock = LWLockAssign();
	diskquota_locks.worker_map_lock = LWLockAssign();
	diskquota_locks.altered_reloid_cache_lock = LWLockAssign();
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
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_BLACK_ENTRIES, sizeof(GlobalBlackMapEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaActiveTableEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaRelationCacheEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaRelidCacheEntry)));
	size = add_size(size, hash_estimate_size(MAX_NUM_MONITORED_DB, sizeof(Oid)));
	size = add_size(size, hash_estimate_size(MAX_NUM_MONITORED_DB, sizeof(DiskQuotaWorkerEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(Oid)));
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
	hash_ctl.keysize = sizeof(TableEntryKey);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	hash_ctl.hash = tag_hash;

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

	initStringInfo(&sql_command);
	/* Add current database to the monitored db cache on all segments */
	appendStringInfo(&sql_command, 
					"SELECT diskquota.diskquota_fetch_table_stat(%d, ARRAY[]::oid[]) "
					"FROM gp_dist_random('gp_id');", ADD_DB_TO_MONITOR);
	ret = SPI_execute(sql_command.data, true, 0);
	if (ret != SPI_OK_SELECT) {
		pfree(sql_command.data);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));
	}
	pfree(sql_command.data);
	/* Add current database to the monitored db cache on coordinator */
	update_diskquota_db_list(MyDatabaseId, HASH_ENTER);
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
	SEGCOUNT = getgpsegmentCount();
	if (SEGCOUNT <= 0 )
	{
		ereport(ERROR,
				(errmsg("[diskquota] there is no active segment, SEGCOUNT is %d", SEGCOUNT)));
	}

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
	HTAB	   *local_active_table_stat_map = NULL;

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
		/*
		 * initialization stage all the tables are active. later loop, only the
		 * tables whose disk size changed will be treated as active
		 */
		local_active_table_stat_map = gp_fetch_active_tables(is_init);
		/* recalculate the disk usage of table, schema and role */
		calculate_table_disk_usage(is_init, local_active_table_stat_map);
		for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type) {
			check_quota_map(type);
		}
		/* flush local table_size_map to user table table_size */
		flush_to_table_size();
		/* copy local black map back to shared black map */
		flush_local_black_map();
		/* Dispatch blackmap entries to segments to perform hard-limit. */
		if (diskquota_hardlimit)
			dispatch_blackmap(local_active_table_stat_map);
		hash_destroy(local_active_table_stat_map);
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

static List*
merge_uncommitted_table_to_oidlist(List *oidlist)
{
	HASH_SEQ_STATUS 			 iter;
	DiskQuotaRelationCacheEntry *entry;

	if (relation_cache == NULL)
	{
		return oidlist;
	}

	remove_committed_relation_from_cache();

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	hash_seq_init(&iter, relation_cache);
	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		if (entry->primary_table_relid == entry->relid)
		{
			oidlist = lappend_oid(oidlist, entry->relid);
		}
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return oidlist;
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

static void
calculate_table_disk_usage(bool is_init, HTAB *local_active_table_stat_map)
{
	bool		table_size_map_found;
	bool		active_tbl_found;
	int64		updated_total_size;
	TableSizeEntry *tsentry = NULL;
	Oid			relOid;
	HASH_SEQ_STATUS iter;
	DiskQuotaActiveTableEntry *active_table_entry;
	TableEntryKey	key;
	List		*oidlist;
	ListCell        *l;

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
	 * scan pg_class to detect table event: drop, reset schema, reset owner.
	 * calculate the file size for active table and update namespace_size_map
	 * and role_size_map
	 */
	oidlist = get_rel_oid_list();

	oidlist = merge_uncommitted_table_to_oidlist(oidlist);
	
	foreach(l, oidlist)
	{
		HeapTuple	classTup;
		Form_pg_class classForm = NULL;
		Oid relnamespace = InvalidOid;
		Oid relowner = InvalidOid;
		Oid reltablespace = InvalidOid;
		relOid = lfirst_oid(l);

		classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
		if (HeapTupleIsValid(classTup))
		{
			classForm = (Form_pg_class) GETSTRUCT(classTup);
			relnamespace = classForm->relnamespace;
			relowner = classForm->relowner;
			reltablespace = classForm->reltablespace;

			if (!OidIsValid(reltablespace))
			{
				reltablespace = MyDatabaseTableSpace;
			}
		}
		else
		{
			LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
			DiskQuotaRelationCacheEntry *relation_entry = hash_search(relation_cache, &relOid, HASH_FIND, NULL);
			if (relation_entry == NULL)
			{
				elog(WARNING, "cache lookup failed for relation %u", relOid);
				LWLockRelease(diskquota_locks.relation_cache_lock);
				continue;
			}
			relnamespace = relation_entry->namespaceoid;
			relowner = relation_entry->owneroid;
			reltablespace = relation_entry->rnode.node.spcNode;
			LWLockRelease(diskquota_locks.relation_cache_lock);
		}

		/*
		 * The segid is the same as the content id in gp_segment_configuration
		 * and the content id is continuous, so it's safe to use SEGCOUNT
		 * to get segid.
		 */
		for (int i = -1; i < SEGCOUNT; i++)
		{
			key.segid = i;
			key.reloid = relOid;
			tsentry = (TableSizeEntry *) hash_search(table_size_map,
					&key,
					HASH_ENTER, &table_size_map_found);

			if (!table_size_map_found)
			{
				tsentry->reloid = relOid;
				tsentry->segid = key.segid;
				tsentry->totalsize = 0;
				tsentry->owneroid = InvalidOid;
				tsentry->namespaceoid = InvalidOid;
				tsentry->tablespaceoid = InvalidOid;
				tsentry->need_flush = true;
			}

			/* mark tsentry is_exist */
			if (tsentry)
				tsentry->is_exist = true;
			active_table_entry = (DiskQuotaActiveTableEntry *) hash_search(local_active_table_stat_map, &key, HASH_FIND, &active_tbl_found);

			/* skip to recalculate the tables which are not in active list */
			if (active_tbl_found)
			{
				if (key.segid == -1)
				{
					/* pretend process as utility mode, and append the table size on master */
					Gp_role = GP_ROLE_UTILITY;

					active_table_entry->tablesize += calculate_table_size(relOid);

					Gp_role = GP_ROLE_DISPATCH;

				}
				/* firstly calculate the updated total size of a table */
				updated_total_size = active_table_entry->tablesize - tsentry->totalsize;

				/* update the table_size entry */
				tsentry->totalsize = (int64) active_table_entry->tablesize;
				tsentry->need_flush = true;

				/* update the disk usage, there may be entries in the map whose keys are InvlidOid as the tsentry does not exist in the table_size_map */
				update_size_for_quota(updated_total_size, NAMESPACE_QUOTA, (Oid[]){tsentry->namespaceoid}, key.segid);
				update_size_for_quota(updated_total_size, ROLE_QUOTA, (Oid[]){tsentry->owneroid}, key.segid);
				update_size_for_quota(updated_total_size, ROLE_TABLESPACE_QUOTA, (Oid[]){tsentry->owneroid, tsentry->tablespaceoid}, key.segid);
				update_size_for_quota(updated_total_size, NAMESPACE_TABLESPACE_QUOTA, (Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid}, key.segid);

			}
			/* table size info doesn't need to flush at init quota model stage */
			if (is_init)
			{
				tsentry->need_flush = false;
			}

			/* if schema change, transfer the file size */
			if (tsentry->namespaceoid != relnamespace)
			{
				transfer_table_for_quota(
						tsentry->totalsize,
						NAMESPACE_QUOTA,
						(Oid[]){tsentry->namespaceoid},
						(Oid[]){relnamespace},
						key.segid);
				transfer_table_for_quota(
						tsentry->totalsize,
						NAMESPACE_TABLESPACE_QUOTA,
						(Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid},
						(Oid[]){relnamespace, tsentry->tablespaceoid},
						key.segid);
				tsentry->namespaceoid = relnamespace;
			}
			/* if owner change, transfer the file size */
			if (tsentry->owneroid != relowner)
			{
				transfer_table_for_quota(
						tsentry->totalsize,
						ROLE_QUOTA,
						(Oid[]){tsentry->owneroid},
						(Oid[]){relowner},
						key.segid
						);
				transfer_table_for_quota(
						tsentry->totalsize,
						ROLE_TABLESPACE_QUOTA,
						(Oid[]){tsentry->owneroid, tsentry->tablespaceoid},
						(Oid[]){relowner, tsentry->tablespaceoid},
						key.segid
						);
				tsentry->owneroid = relowner;
			}

			if (tsentry->tablespaceoid != reltablespace)
			{
				transfer_table_for_quota(
						tsentry->totalsize,
						NAMESPACE_TABLESPACE_QUOTA,
						(Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid},
						(Oid[]){tsentry->namespaceoid, reltablespace},
						key.segid
						);
				transfer_table_for_quota(
						tsentry->totalsize,
						ROLE_TABLESPACE_QUOTA,
						(Oid[]){tsentry->owneroid, tsentry->tablespaceoid},
						(Oid[]){tsentry->owneroid, reltablespace},
						key.segid
						);
				tsentry->tablespaceoid = reltablespace;
			}
		}
		if (HeapTupleIsValid(classTup))
		{
			heap_freetuple(classTup);
		}
	}

	list_free(oidlist);

	/*
	 * Process removed tables. Reduce schema and role size firstly. Remove
	 * table from table_size_map in flush_to_table_size() function later.
	 */
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		if (tsentry->is_exist == false)
		{
			update_size_for_quota(-tsentry->totalsize, NAMESPACE_QUOTA, (Oid[]){tsentry->namespaceoid}, tsentry->segid);
			update_size_for_quota(-tsentry->totalsize, ROLE_QUOTA, (Oid[]){tsentry->owneroid}, tsentry->segid);
			update_size_for_quota(-tsentry->totalsize, ROLE_TABLESPACE_QUOTA, (Oid[]){tsentry->owneroid, tsentry->tablespaceoid}, tsentry->segid);
			update_size_for_quota(-tsentry->totalsize, NAMESPACE_TABLESPACE_QUOTA, (Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid}, tsentry->segid);
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
	StringInfoData deleted_table_expr;
	bool		delete_statement_flag = false;
	bool		insert_statement_flag = false;
	int		ret;
	int		extMajorVersion= get_ext_major_version();

	/* TODO: Add flush_size_interval to avoid flushing size info in every loop */

	/* Disable ORCA since it does not support non-scalar subqueries. */
	bool old_optimizer = optimizer;
	optimizer = false;

	initStringInfo(&deleted_table_expr);
	appendStringInfo(&deleted_table_expr, "WITH deleted_table AS ( VALUES ");

	initStringInfo(&insert_statement);
	appendStringInfo(&insert_statement, "insert into diskquota.table_size values ");
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		/* delete dropped table from both table_size_map and table table_size */
		if (tsentry->is_exist == false)
		{
			switch (extMajorVersion)
			{
				case 1:
					appendStringInfo(&deleted_table_expr, "%u, ", tsentry->reloid);
					break;
				case 2:
					appendStringInfo(&deleted_table_expr, "(%u,%d), ", tsentry->reloid, tsentry->segid);
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("[diskquota] unknown diskquota extension version: %d", extMajorVersion)));
			}
			delete_statement_flag = true;

			hash_search(table_size_map,
						&tsentry->reloid,
						HASH_REMOVE, NULL);
		}
		/* update the table size by delete+insert in table table_size */
		else if (tsentry->need_flush == true)
		{
			tsentry->need_flush = false;
			switch (extMajorVersion)
			{
				case 1:
					if (tsentry->segid == -1)
					{
						appendStringInfo(&deleted_table_expr, "%u, ", tsentry->reloid);
						appendStringInfo(&insert_statement, "(%u,%ld), ", tsentry->reloid, tsentry->totalsize);
						delete_statement_flag = true;
						insert_statement_flag = true;
					}
					break;
				case 2:
					appendStringInfo(&deleted_table_expr, "(%u,%d), ", tsentry->reloid, tsentry->segid);
					appendStringInfo(&insert_statement, "(%u,%ld,%d), ", tsentry->reloid, tsentry->totalsize, tsentry->segid);
					delete_statement_flag = true;
					insert_statement_flag = true;
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("[diskquota] unknown diskquota extension version: %d", extMajorVersion)));
			}
		}
	}
	truncateStringInfo(&deleted_table_expr, deleted_table_expr.len - strlen(", "));
	truncateStringInfo(&insert_statement, insert_statement.len - strlen(", "));
	appendStringInfo(&deleted_table_expr, ")");
	appendStringInfo(&insert_statement, ";");

	if (delete_statement_flag)
	{
		/* concatenate all the need_to_flush table to SQL string */
		initStringInfo(&delete_statement);
		appendStringInfoString(&delete_statement, (const char *) deleted_table_expr.data);
		switch (extMajorVersion)
		{
			case 1:
				appendStringInfo(&delete_statement, "delete from diskquota.table_size where tableid in ( SELECT * FROM deleted_table );");
				break;
			case 2:
				appendStringInfo(&delete_statement, "delete from diskquota.table_size where (tableid, segid) in ( SELECT * FROM deleted_table );");
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("[diskquota] unknown diskquota extension version: %d", extMajorVersion)));
		}
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

	optimizer = old_optimizer;
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
	GlobalBlackMapEntry *blackentry;
	bool		found;

	LWLockAcquire(diskquota_locks.black_map_lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, local_disk_quota_black_map);
	while ((localblackentry = hash_seq_search(&iter)) != NULL)
	{
		if (localblackentry->isexceeded)
		{
			blackentry = (GlobalBlackMapEntry *) hash_search(disk_quota_black_map,
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
					blackentry->keyitem.targetoid = localblackentry->keyitem.targetoid;
					blackentry->keyitem.databaseoid = MyDatabaseId;
					blackentry->keyitem.targettype = localblackentry->keyitem.targettype;
					blackentry->keyitem.tablespaceoid = localblackentry->keyitem.tablespaceoid;
					blackentry->segexceeded = localblackentry->segexceeded;
				}
			}
			blackentry->segexceeded = localblackentry->segexceeded;
			localblackentry->isexceeded = false;
			localblackentry->segexceeded = false;
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
 * Dispatch blackmap to segment servers.
 */
static void
dispatch_blackmap(HTAB *local_active_table_stat_map)
{
	HASH_SEQ_STATUS					hash_seq;
	GlobalBlackMapEntry			   *blackmap_entry;
	DiskQuotaActiveTableEntry	   *active_table_entry;
	int								num_entries, count = 0;
	CdbPgResults					cdb_pgresults = {NULL, 0};
	StringInfoData					rows;
	StringInfoData					active_oids;
	StringInfoData					sql;

	initStringInfo(&rows);
	initStringInfo(&active_oids);
	initStringInfo(&sql);

	LWLockAcquire(diskquota_locks.black_map_lock, LW_SHARED);
	num_entries = hash_get_num_entries(disk_quota_black_map);
	hash_seq_init(&hash_seq, disk_quota_black_map);
	while ((blackmap_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		appendStringInfo(&rows,
						 "ROW(%d, %d, %d, %d, %s)",
						 blackmap_entry->keyitem.targetoid,
						 blackmap_entry->keyitem.databaseoid,
						 blackmap_entry->keyitem.tablespaceoid,
						 blackmap_entry->keyitem.targettype,
						 blackmap_entry->segexceeded ? "true" : "false");

		if (++count != num_entries)
			appendStringInfo(&rows, ",");
	}
	LWLockRelease(diskquota_locks.black_map_lock);

	count = 0;
	num_entries = hash_get_num_entries(local_active_table_stat_map);
	hash_seq_init(&hash_seq, local_active_table_stat_map);
	while ((active_table_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		appendStringInfo(&active_oids,
						 "%d", active_table_entry->reloid);

		if (++count != num_entries)
			appendStringInfo(&active_oids, ",");
	}

	appendStringInfo(&sql,
					 "select diskquota.refresh_blackmap("
					 "ARRAY[%s]::diskquota.blackmap_entry[], "
					 "ARRAY[%s]::oid[])", rows.data, active_oids.data);
	CdbDispatchCommand(sql.data, DF_NONE, &cdb_pgresults);

	pfree(rows.data);
	pfree(active_oids.data);
	pfree(sql.data);
}

/*
 * Make sure a StringInfo's string is no longer than 'nchars' characters.
 */
void
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
	int		extMajorVersion;

	/*
	 * TODO: we should skip to reload quota config when there is no change in
	 * quota.config. A flag in shared memory could be used to detect the quota
	 * config change.
	 */
	clear_all_quota_maps();
	extMajorVersion = get_ext_major_version();

	/*
	 * read quotas from diskquota.quota_config and target table
	 */

	/* 
	 * We need to check the extension version.
	 * Why do we need this?
	 * As when we upgrade diskquota extension from an old to a new version,
	 * we need firstly reload the new diskquota.so and then execute the
	 * upgrade SQL. However, between the 2 steps, the new diskquota.so
	 * needs to work with the old version diskquota sql file, otherwise,
	 * the init work will fail and diskquota can not work correctly.
	 * Maybe this is not the best sulotion, only a work arround. Optimizing
	 * the init procedure is a better solution.
	 */ 
	switch (extMajorVersion)
	{
		case 1:
			ret = SPI_execute("select targetoid, quotatype, quotalimitMB, 0 as segratio, 0 as tablespaceoid from diskquota.quota_config", true, 0);
			break;
		case 2:
			ret = SPI_execute(
					"SELECT c.targetOid, c.quotaType, c.quotalimitMB, COALESCE(c.segratio, 0) AS segratio, COALESCE(t.tablespaceoid, 0) AS tablespaceoid "
					"FROM diskquota.quota_config AS c LEFT OUTER JOIN diskquota.target AS t "
					"ON c.targetOid = t.primaryOid and c.quotaType = t.quotaType", true, 0);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[diskquota] unknown diskquota extension version: %d", extMajorVersion)));
	}
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] load_quotas SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != NUM_QUOTA_CONFIG_ATTRS ||
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
		Datum		vals[NUM_QUOTA_CONFIG_ATTRS];
		bool		isnull[NUM_QUOTA_CONFIG_ATTRS];

		for (int i = 0; i < NUM_QUOTA_CONFIG_ATTRS; ++i)
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
		float	segratio = DatumGetFloat4(vals[3]);
		Oid	spcOid = DatumGetObjectId(vals[4]);

		if (spcOid == InvalidOid)
		{
			if (quota_info[quotaType].num_keys != 1) {
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("[diskquota] tablespace Oid MUST NOT be NULL for quota type: %d", quotaType)));
			}
			update_limit_for_quota(quota_limit_mb * (1 << 20), segratio, quotaType, (Oid[]){targetOid});
		}
		else
		{
			update_limit_for_quota(quota_limit_mb * (1 << 20), segratio, quotaType, (Oid[]){targetOid, spcOid});
		}
	}

	return;
}

/*
 * Given table oid, search for namespace and owner.
 */
static bool
get_rel_owner_schema_tablespace(Oid relid, Oid *ownerOid, Oid *nsOid, Oid *tablespaceoid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	bool found = HeapTupleIsValid(tp);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		*ownerOid = reltup->relowner;
		*nsOid = reltup->relnamespace;
		*tablespaceoid = reltup->reltablespace;

		if (!OidIsValid(*tablespaceoid))
		{
			*tablespaceoid = MyDatabaseTableSpace;
		}

		ReleaseSysCache(tp);
	}
	return found;
}

static bool
check_blackmap_by_relfilenode(RelFileNode relfilenode)
{
	bool					found;
	BlackMapEntry			keyitem;
	GlobalBlackMapEntry	   *entry;

	SIMPLE_FAULT_INJECTOR("check_blackmap_by_relfilenode");

	memset(&keyitem, 0, sizeof(keyitem));
	memcpy(&keyitem.relfilenode, &relfilenode, sizeof(RelFileNode));

	LWLockAcquire(diskquota_locks.black_map_lock, LW_SHARED);
	entry = hash_search(disk_quota_black_map,
						&keyitem, HASH_FIND, &found);

	if (found && entry)
	{
		GlobalBlackMapEntry segblackentry;
		memcpy(&segblackentry.keyitem, &entry->auxblockinfo, sizeof(BlackMapEntry));
		segblackentry.segexceeded = entry->segexceeded;
		LWLockRelease(diskquota_locks.black_map_lock);

		export_exceeded_error(&segblackentry, true /*skip_name*/);
		return false;
	}
	LWLockRelease(diskquota_locks.black_map_lock);
	return true;
}

/*
 * This function takes relowner, relnamespace, reltablespace as arguments,
 * prepares the searching key of the global blackmap for us.
 */
static void
prepare_blackmap_search_key(BlackMapEntry *keyitem, QuotaType type,
							Oid relowner, Oid relnamespace, Oid reltablespace)
{
	Assert(keyitem != NULL);
	memset(keyitem, 0, sizeof(BlackMapEntry));
	if (type == ROLE_QUOTA || type == ROLE_TABLESPACE_QUOTA)
		keyitem->targetoid = relowner;
	else if (type == NAMESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		keyitem->targetoid = relnamespace;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("[diskquota] unknown quota type: %d", type)));

	if (type == ROLE_TABLESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		keyitem->tablespaceoid = reltablespace;
	else
	{
		/* refer to add_quota_to_blacklist */
		keyitem->tablespaceoid = InvalidOid;
	}
	keyitem->databaseoid = MyDatabaseId;
	keyitem->targettype = type;
}

/*
 * Given table oid, check whether quota limit
 * of table's schema or table's owner are reached.
 * Do enforcement if quota exceeds.
 */
static bool
check_blackmap_by_reloid(Oid reloid)
{
	Oid			ownerOid = InvalidOid;
	Oid			nsOid = InvalidOid;
	Oid 		tablespaceoid = InvalidOid;
	bool		found;
	BlackMapEntry keyitem;
	GlobalBlackMapEntry *entry;

	bool found_rel = get_rel_owner_schema_tablespace(reloid, &ownerOid, &nsOid, &tablespaceoid);
	if (!found_rel)
	{
		return true;
	}

	LWLockAcquire(diskquota_locks.black_map_lock, LW_SHARED);
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		prepare_blackmap_search_key(&keyitem, type, ownerOid, nsOid, tablespaceoid);
		entry = hash_search(disk_quota_black_map,
					&keyitem,
					HASH_FIND, &found);
		if (found)
		{
			LWLockRelease(diskquota_locks.black_map_lock);
			export_exceeded_error(entry, false /*skip_name*/);
			return false;
		}
	}
	LWLockRelease(diskquota_locks.black_map_lock);
	return true;
}

/*
 * Given relation's oid or relfilenode, check whether the
 * quota limits of schema or owner are reached. Do enforcement
 * if the quota exceeds.
 */
bool
quota_check_common(Oid reloid, RelFileNode *relfilenode)
{
	bool	enable_hardlimit;

	if (!IsTransactionState())
		return true;

	if (diskquota_is_paused())
		return true;

	if (OidIsValid(reloid))
		return check_blackmap_by_reloid(reloid);

	enable_hardlimit = diskquota_hardlimit;

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("enable_check_quota_by_relfilenode") == FaultInjectorTypeSkip)
		enable_hardlimit = true;
#endif

	if (relfilenode && enable_hardlimit)
		return check_blackmap_by_relfilenode(*relfilenode);

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

static char *
GetNamespaceName(Oid spcid, bool skip_name)
{
	if (skip_name)
	{
		NameData	spcstr;
		pg_ltoa(spcid, spcstr.data);
		return pstrdup(spcstr.data);
	}
	return get_namespace_name(spcid);
}

static char *
GetTablespaceName(Oid spcid, bool skip_name)
{
	if (skip_name)
	{
		NameData	spcstr;
		pg_ltoa(spcid, spcstr.data);
		return pstrdup(spcstr.data);
	}
	return get_tablespace_name(spcid);
}

static char *
GetUserName(Oid relowner, bool skip_name)
{
	if (skip_name)
	{
		NameData	namestr;
		pg_ltoa(relowner, namestr.data);
		return pstrdup(namestr.data);
	}
	return GetUserNameFromId(relowner);
}

static void
export_exceeded_error(GlobalBlackMapEntry *entry, bool skip_name)
{
	BlackMapEntry *blackentry = &entry->keyitem;
	switch(blackentry->targettype)
	{
		case NAMESPACE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("schema's disk space quota exceeded with name:%s", GetNamespaceName(blackentry->targetoid, skip_name))));
			break;
		case ROLE_QUOTA:
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("role's disk space quota exceeded with name:%s", GetUserName(blackentry->targetoid, skip_name))));
			break;
		case NAMESPACE_TABLESPACE_QUOTA:
			if (entry->segexceeded)
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						 errmsg("tablespace:%s schema:%s diskquota exceeded per segment quota", GetTablespaceName(blackentry->tablespaceoid, skip_name), GetNamespaceName(blackentry->targetoid, skip_name))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						 errmsg("tablespace:%s schema:%s diskquota exceeded", GetTablespaceName(blackentry->tablespaceoid, skip_name), GetNamespaceName(blackentry->targetoid, skip_name))));
			break;
		case ROLE_TABLESPACE_QUOTA:
			if (entry->segexceeded)
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						 errmsg("tablespace:%s role:%s diskquota exceeded per segment quota", GetTablespaceName(blackentry->tablespaceoid, skip_name), GetUserName(blackentry->targetoid, skip_name))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						 errmsg("tablespace:%s role:%s diskquota exceeded", GetTablespaceName(blackentry->tablespaceoid, skip_name), GetUserName(blackentry->targetoid, skip_name))));
			break;
		default :
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("diskquota exceeded, unknown quota type")));
	}
}

/*
 * refresh_blackmap() takes two arguments.
 * The first argument is an array of blackmap entries on QD.
 * The second argument is an array of active relations' oid.
 *
 * The basic idea is that, we iterate over the active relations' oid, check that
 * whether the relation's owner/tablespace/namespace is in one of the blackmap
 * entries dispatched from diskquota worker from QD. If the relation should be
 * blocked, we then add its relfilenode together with the toast, toast index,
 * appendonly, appendonly index relations' relfilenodes to the global blackmap.
 * Note that, this UDF is called on segment servers by diskquota worker on QD and
 * the global blackmap on segment servers is indexed by relfilenode.
 */
PG_FUNCTION_INFO_V1(refresh_blackmap);
Datum
refresh_blackmap(PG_FUNCTION_ARGS)
{
	ArrayType			   *blackmap_array_type = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			   *active_oid_array_type = PG_GETARG_ARRAYTYPE_P(1);
	Oid						blackmap_elem_type = ARR_ELEMTYPE(blackmap_array_type);
	Oid						active_oid_elem_type = ARR_ELEMTYPE(active_oid_array_type);
	Datum				   *datums;
	bool				   *nulls;
	int16					elem_width;
	bool					elem_type_by_val;
	char					elem_alignment_code;
	int						count;
	HeapTupleHeader			lt;
	bool					segexceeded;
	GlobalBlackMapEntry	   *blackmapentry;
	HASH_SEQ_STATUS			hash_seq;
	HTAB				   *local_blackmap;
	HASHCTL					hashctl;

	if (!superuser())
		errmsg("must be superuser to update blackmap");

	if (ARR_NDIM(blackmap_array_type) > 1 || ARR_NDIM(active_oid_array_type) > 1)
		ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("1-dimensional array needed")));

	/* Firstly, clear the blackmap entries. */
	LWLockAcquire(diskquota_locks.black_map_lock, LW_EXCLUSIVE);
	hash_seq_init(&hash_seq, disk_quota_black_map);
	while ((blackmapentry = hash_seq_search(&hash_seq)) != NULL)
		hash_search(disk_quota_black_map, &blackmapentry->keyitem, HASH_REMOVE, NULL);
	LWLockRelease(diskquota_locks.black_map_lock);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to connect to execute internal query")));

	/*
	 * Secondly, iterate over blackmap entries and add these entries to the local black map
	 * on segment servers so that we are able to check whether the given relation (by oid)
	 * should be blacked in O(1) time complexity in third step.
	 */
	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(BlackMapEntry);
	hashctl.entrysize = sizeof(GlobalBlackMapEntry);
	hashctl.hcxt = CurrentMemoryContext;
	hashctl.hash = tag_hash;

	/*
	 * Since uncommitted relations' information and the global blackmap entries
	 * are cached in shared memory. The memory regions are guarded by lightweight
	 * locks. In order not to hold multiple locks at the same time, We add blackmap
	 * entries into the local_blackmap below and then flush the content of the
	 * local_blackmap to the global blackmap at the end of this UDF.
	 */
	local_blackmap = hash_create("local_blackmap",
								 1024, &hashctl,
								 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
	get_typlenbyvalalign(blackmap_elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
	deconstruct_array(blackmap_array_type, blackmap_elem_type, elem_width,
					  elem_type_by_val, elem_alignment_code, &datums, &nulls, &count);
	for (int i = 0; i < count; ++i)
	{
		BlackMapEntry			keyitem;
		bool					isnull;

		if (nulls[i])
			continue;

		memset(&keyitem, 0, sizeof(BlackMapEntry));
		lt = DatumGetHeapTupleHeader(datums[i]);
		keyitem.targetoid     = DatumGetObjectId(GetAttributeByNum(lt, 1, &isnull));
		keyitem.databaseoid   = DatumGetObjectId(GetAttributeByNum(lt, 2, &isnull));
		keyitem.tablespaceoid = DatumGetObjectId(GetAttributeByNum(lt, 3, &isnull));
		keyitem.targettype    = DatumGetInt32(GetAttributeByNum(lt, 4, &isnull));
		/* blackmap entries from QD should have the real tablespace oid */
		if ((keyitem.targettype == NAMESPACE_TABLESPACE_QUOTA || keyitem.targettype == ROLE_TABLESPACE_QUOTA))
		{
			Assert(OidIsValid(keyitem.tablespaceoid));
		}
		segexceeded           = DatumGetBool(GetAttributeByNum(lt, 5, &isnull));

		blackmapentry = hash_search(local_blackmap, &keyitem, HASH_ENTER_NULL, NULL);
		if (blackmapentry)
			blackmapentry->segexceeded = segexceeded;
	}

	/*
	 * Thirdly, iterate over the active oid list. Check that if the relation should be blocked.
	 * If the relation should be blocked, we insert the toast, toast index, appendonly, appendonly
	 * index relations to the global black map.
	 */
	get_typlenbyvalalign(active_oid_elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
	deconstruct_array(active_oid_array_type, active_oid_elem_type, elem_width,
					  elem_type_by_val, elem_alignment_code, &datums, &nulls, &count);
	for (int i = 0; i < count; ++i)
	{
		Oid				active_oid = InvalidOid;
		HeapTuple		tuple;
		if (nulls[i])
			continue;

		active_oid = DatumGetObjectId(datums[i]);
		if (!OidIsValid(active_oid))
			continue;

		tuple = SearchSysCacheCopy1(RELOID, active_oid);
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_class	form = (Form_pg_class) GETSTRUCT(tuple);
			Oid				relnamespace = form->relnamespace;
			Oid				reltablespace = OidIsValid(form->reltablespace) ?
												form->reltablespace : MyDatabaseTableSpace;
			Oid				relowner = form->relowner;
			BlackMapEntry	keyitem;
			bool			found;

			for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
			{
				/* Check that if the current relation should be blocked. */
				prepare_blackmap_search_key(&keyitem, type, relowner, relnamespace, reltablespace);
				blackmapentry = hash_search(local_blackmap,
											&keyitem, HASH_FIND, &found);
				if (found && blackmapentry)
				{
					/*
					 * If the current relation is blocked, we should add the relfilenode
					 * of itself together with the relfilenodes of its toast relation and
					 * appendonly relations to the global black map.
					 */
					List	   *oid_list = NIL;
					ListCell   *cell = NULL;
					Oid			toastrelid = form->reltoastrelid;
					Oid			aosegrelid = InvalidOid;
					Oid			aoblkdirrelid = InvalidOid;
					Oid			aovisimaprelid = InvalidOid;
					oid_list = lappend_oid(oid_list, active_oid);

					/* Append toast relation and toast index to the oid_list if any. */
					if (OidIsValid(toastrelid))
					{
						oid_list = lappend_oid(oid_list, toastrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(toastrelid));
					}

					/* Append ao auxiliary relations and their indexes to the oid_list if any. */
					diskquota_get_appendonly_aux_oid_list(active_oid, &aosegrelid,
														&aoblkdirrelid, &aovisimaprelid);
					if (OidIsValid(aosegrelid))
					{
						oid_list = lappend_oid(oid_list, aosegrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aosegrelid));
					}
					if (OidIsValid(aoblkdirrelid))
					{
						oid_list = lappend_oid(oid_list, aoblkdirrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aoblkdirrelid));
					}
					if (OidIsValid(aovisimaprelid))
					{
						oid_list = lappend_oid(oid_list, aovisimaprelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aovisimaprelid));
					}

					/* Iterate over the oid_list and add their relfilenodes to the blackmap. */
					foreach(cell, oid_list)
					{
						Oid		curr_oid = lfirst_oid(cell);
						HeapTuple curr_tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(curr_oid));
						if (HeapTupleIsValid(curr_tuple))
						{
							Form_pg_class				curr_form = (Form_pg_class) GETSTRUCT(curr_tuple);
							Oid							curr_reltablespace =
								OidIsValid(curr_form->reltablespace) ?
								curr_form->reltablespace : MyDatabaseTableSpace;
							RelFileNode					relfilenode =
								{ .dbNode = MyDatabaseId,
								  .relNode = curr_form->relfilenode,
								  .spcNode = curr_reltablespace };
							bool						found;
							GlobalBlackMapEntry		   *blocked_filenode_entry;
							BlackMapEntry				blocked_filenode_keyitem;

							memset(&blocked_filenode_keyitem, 0, sizeof(BlackMapEntry));
							memcpy(&blocked_filenode_keyitem.relfilenode, &relfilenode, sizeof(RelFileNode));

							blocked_filenode_entry = hash_search(local_blackmap,
																 &blocked_filenode_keyitem,
																 HASH_ENTER_NULL, &found);
							if (!found && blocked_filenode_entry)
							{
								memcpy(&blocked_filenode_entry->auxblockinfo, &keyitem, sizeof(BlackMapEntry));
								blocked_filenode_entry->segexceeded = blackmapentry->segexceeded;
							}
						}
					}
					/*
					 * The current relation may satisfy multiple blocking conditions,
					 * we only add it once.
					 */
					break;
				}
			}
		}
		else
		{
			/*
			 * We cannot fetch the relation from syscache. It may be an uncommitted relation.
			 * Let's try to fetch it from relation_cache.
			 */
			DiskQuotaRelationCacheEntry		   *relation_cache_entry;
			bool								found;
			LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
			relation_cache_entry = hash_search(relation_cache, &active_oid,
											   HASH_FIND, &found);
			if (found && relation_cache_entry)
			{
				Oid				relnamespace = relation_cache_entry->namespaceoid;
				Oid				reltablespace = relation_cache_entry->rnode.node.spcNode;
				Oid				relowner = relation_cache_entry->owneroid;
				BlackMapEntry	keyitem;
				for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
				{
					/* Check that if the current relation should be blocked. */
					prepare_blackmap_search_key(&keyitem, type, relowner, relnamespace, reltablespace);
					blackmapentry = hash_search(local_blackmap, &keyitem, HASH_FIND, &found);

					if (found && blackmapentry)
					{
						List	   *oid_list = NIL;
						ListCell   *cell = NULL;

						/* Collect the relation oid together with its auxiliary relations' oid. */
						oid_list = lappend_oid(oid_list, active_oid);
						for (int auxoidcnt = 0; auxoidcnt < relation_cache_entry->auxrel_num; ++auxoidcnt)
							oid_list = lappend_oid(oid_list, relation_cache_entry->auxrel_oid[auxoidcnt]);

						foreach(cell, oid_list)
						{
							bool						found;
							GlobalBlackMapEntry		   *blocked_filenode_entry;
							BlackMapEntry				blocked_filenode_keyitem;
							Oid							curr_oid = lfirst_oid(cell);

							relation_cache_entry = hash_search(relation_cache,
															   &curr_oid, HASH_FIND, &found);
							if (found && relation_cache_entry)
							{
								memset(&blocked_filenode_keyitem, 0, sizeof(BlackMapEntry));
								memcpy(&blocked_filenode_keyitem.relfilenode,
									   &relation_cache_entry->rnode.node, sizeof(RelFileNode));

								blocked_filenode_entry = hash_search(local_blackmap,
																	 &blocked_filenode_keyitem,
																	 HASH_ENTER_NULL, &found);
								if (!found && blocked_filenode_entry)
								{
									memcpy(&blocked_filenode_entry->auxblockinfo, &keyitem, sizeof(BlackMapEntry));
									blocked_filenode_entry->segexceeded = blackmapentry->segexceeded;
								}
							}
						}
					}
				}
			}
			LWLockRelease(diskquota_locks.relation_cache_lock);
		}
	}

	/* Flush the content of local_blackmap to the global blackmap. */
	LWLockAcquire(diskquota_locks.black_map_lock, LW_EXCLUSIVE);
	hash_seq_init(&hash_seq, local_blackmap);
	while ((blackmapentry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool					found;
		GlobalBlackMapEntry	   *new_entry;
		new_entry = hash_search(disk_quota_black_map, &blackmapentry->keyitem,
								HASH_ENTER_NULL, &found);
		/*
		 * We don't perform soft-limit on segment servers, so we don't flush the
		 * blackmap entry with a valid targetoid to the global blackmap on segment
		 * servers.
		 */
		if (!found && new_entry && !OidIsValid(blackmapentry->keyitem.targetoid))
			memcpy(new_entry, blackmapentry, sizeof(GlobalBlackMapEntry));
	}
	LWLockRelease(diskquota_locks.black_map_lock);

	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * show_blackmap() provides developers or users to dump the blackmap in shared
 * memory on a single server. If you want to query blackmap on segment servers,
 * you should dispatch this query to segments.
 */
PG_FUNCTION_INFO_V1(show_blackmap);
Datum
show_blackmap(PG_FUNCTION_ARGS)
{
	FuncCallContext			   *funcctx;
	GlobalBlackMapEntry		   *blackmap_entry;
	struct BlackMapCtx {
		HASH_SEQ_STATUS			blackmap_seq;
		HTAB				   *blackmap;
	} *blackmap_ctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc					tupdesc;
		MemoryContext				oldcontext;
		HASHCTL						hashctl;
		HASH_SEQ_STATUS				hash_seq;

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(9, false /*hasoid*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "target_type", TEXTOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "target_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "database_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "tablespace_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "seg_exceeded", BOOLOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "dbnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "spcnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "relnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "segid", INT4OID, -1 /*typmod*/, 0 /*attdim*/);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Create a local hash table and fill it with entries from shared memory. */
		memset(&hashctl, 0, sizeof(hashctl));
		hashctl.keysize = sizeof(BlackMapEntry);
		hashctl.entrysize = sizeof(GlobalBlackMapEntry);
		hashctl.hcxt = CurrentMemoryContext;
		hashctl.hash = tag_hash;

		blackmap_ctx = (struct BlackMapCtx *) palloc(sizeof(struct BlackMapCtx));
		blackmap_ctx->blackmap = hash_create("blackmap_ctx blackmap",
											 1024, &hashctl,
											 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

		LWLockAcquire(diskquota_locks.black_map_lock, LW_SHARED);
		hash_seq_init(&hash_seq, disk_quota_black_map);
		while ((blackmap_entry = hash_seq_search(&hash_seq)) != NULL)
		{
			GlobalBlackMapEntry		   *local_blackmap_entry = NULL;
			local_blackmap_entry = hash_search(blackmap_ctx->blackmap,
											   &blackmap_entry->keyitem,
											   HASH_ENTER_NULL, NULL);
			if (local_blackmap_entry)
			{
				memcpy(&local_blackmap_entry->keyitem,
					   &blackmap_entry->keyitem, sizeof(BlackMapEntry));
				local_blackmap_entry->segexceeded = blackmap_entry->segexceeded;
				memcpy(&local_blackmap_entry->auxblockinfo,
					   &blackmap_entry->auxblockinfo, sizeof(BlackMapEntry));
			}
		}
		LWLockRelease(diskquota_locks.black_map_lock);

		/* Setup first calling context. */
		hash_seq_init(&(blackmap_ctx->blackmap_seq),
					  blackmap_ctx->blackmap);
		funcctx->user_fctx = (void *) blackmap_ctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	blackmap_ctx = (struct BlackMapCtx *) funcctx->user_fctx;

	while ((blackmap_entry = hash_seq_search(&(blackmap_ctx->blackmap_seq))) != NULL)
	{
		Datum			result;
		Datum			values[9];
		bool			nulls[9];
		HeapTuple		tuple;
		BlackMapEntry	keyitem;
		char			targettype_str[32];
		RelFileNode		blocked_relfilenode;

		memcpy(&blocked_relfilenode,
			   &blackmap_entry->keyitem.relfilenode, sizeof(RelFileNode));
		/*
		 * If the blackmap entry is indexed by relfilenode, we dump the blocking
		 * condition from auxblockinfo.
		 */
		if (!OidIsValid(blocked_relfilenode.relNode))
			memcpy(&keyitem, &blackmap_entry->keyitem, sizeof(keyitem));
		else
			memcpy(&keyitem, &blackmap_entry->auxblockinfo, sizeof(keyitem));
		memset(targettype_str, 0, sizeof(targettype_str));

		switch ((QuotaType) keyitem.targettype)
		{
		case ROLE_QUOTA:
			strncpy(targettype_str, "ROLE_QUOTA", 10);
			break;
		case NAMESPACE_QUOTA:
			strncpy(targettype_str, "NAMESPACE_QUOTA", 15);
			break;
		case ROLE_TABLESPACE_QUOTA:
			strncpy(targettype_str, "ROLE_TABLESPACE_QUOTA", 21);
			break;
		case NAMESPACE_TABLESPACE_QUOTA:
			strncpy(targettype_str, "NAMESPACE_TABLESPACE_QUOTA", 26);
			break;
		default:
			strncpy(targettype_str, "UNKNOWN", 7);
			break;
		}

		values[0] = CStringGetTextDatum(targettype_str);
		values[1] = ObjectIdGetDatum(keyitem.targetoid);
		values[2] = ObjectIdGetDatum(keyitem.databaseoid);
		values[3] = ObjectIdGetDatum(keyitem.tablespaceoid);
		values[4] = BoolGetDatum(blackmap_entry->segexceeded);
		values[5] = ObjectIdGetDatum(blocked_relfilenode.dbNode);
		values[6] = ObjectIdGetDatum(blocked_relfilenode.spcNode);
		values[7] = ObjectIdGetDatum(blocked_relfilenode.relNode);
		values[8] = Int32GetDatum(GpIdentity.segindex);

		memset(nulls, false, sizeof(nulls));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
