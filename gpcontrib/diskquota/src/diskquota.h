/* -------------------------------------------------------------------------
 *
 * diskquota.h
 *
 * Copyright (c) 2018-2020 Pivotal Software, Inc.
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates
 *
 * IDENTIFICATION
 *		diskquota/diskquota.c
 *
 * -------------------------------------------------------------------------
 */
#ifndef DISK_QUOTA_H
#define DISK_QUOTA_H

#include "c.h"
#include "postgres.h"
#include "port/atomics.h"

#include "catalog/pg_class.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "postmaster/bgworker.h"

#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

#include <signal.h>

/* init number of TableSizeEntry in table_size_map */
#define INIT_NUM_TABLE_SIZE_ENTRIES 128
/* max number of TableSizeEntry in table_size_map */
#define MAX_NUM_TABLE_SIZE_ENTRIES (diskquota_max_table_segments / SEGMENT_SIZE_ARRAY_LENGTH)
/* length of segment size array in TableSizeEntry */
#define SEGMENT_SIZE_ARRAY_LENGTH 100
typedef enum
{
	DISKQUOTA_TAG_HASH = 0,
	DISKQUOTA_OID_HASH,
	DISKQUOTA_STRING_HASH,
} DiskquotaHashFunction;

/* max number of monitored database with diskquota enabled */
#define MAX_NUM_MONITORED_DB 50
#define LAUNCHER_SCHEMA "diskquota_utility"
#define EXTENSION_SCHEMA "diskquota"
extern int diskquota_worker_timeout;

#if GP_VERSION_NUM < 70000
#define TableIsHeap(relstorage, relam) ((bool)(relstorage == RELSTORAGE_HEAP))
#define TableIsAoRows(relstorage, relam) ((bool)(relstorage == RELSTORAGE_AOROWS))
#define TableIsAoCols(relstorage, relam) ((bool)(relstorage == RELSTORAGE_AOCOLS))
#define DiskquotaCreateTemplateTupleDesc(natts) CreateTemplateTupleDesc(natts, false /*hasoid*/)
#define DiskquotaWaitLatch(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout)
#define DiskquotaGetRelstorage(classForm) (classForm->relstorage)
#else
#define TableIsHeap(relstorage, relam) \
	((bool)(relam != 0 && relam != AO_ROW_TABLE_AM_OID && relam != AO_COLUMN_TABLE_AM_OID))
#define TableIsAoRows(relstorage, relam) ((bool)(relam == AO_ROW_TABLE_AM_OID))
#define TableIsAoCols(relstorage, relam) ((bool)(relam == AO_COLUMN_TABLE_AM_OID))
#define DiskquotaCreateTemplateTupleDesc(natts) CreateTemplateTupleDesc(natts);
#define DiskquotaWaitLatch(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout, WAIT_EVENT_PG_SLEEP)
#define DiskquotaGetRelstorage(classForm) (0)
#endif /* GP_VERSION_NUM */

typedef enum
{
	NAMESPACE_QUOTA = 0,
	ROLE_QUOTA,
	NAMESPACE_TABLESPACE_QUOTA,
	ROLE_TABLESPACE_QUOTA,
	/*
	 * TABLESPACE_QUOTA
	 * used in `quota_config` table,
	 * when set_per_segment_quota("xx",1.0) is called
	 * to set per segment quota to '1.0', the config
	 * will be:
	 * quotatype = 4 (TABLESPACE_QUOTA)
	 * quotalimitMB = 0 (invalid quota confined)
	 * segratio = 1.0
	 */
	TABLESPACE_QUOTA,

	NUM_QUOTA_TYPES,
} QuotaType;

typedef enum
{
	FETCH_ACTIVE_OID,  /* fetch active table list */
	FETCH_ACTIVE_SIZE, /* fetch size for active tables */
	ADD_DB_TO_MONITOR,
	REMOVE_DB_FROM_BEING_MONITORED,
	PAUSE_DB_TO_MONITOR,
	RESUME_DB_TO_MONITOR,
} FetchTableStatType;

typedef enum
{
	DISKQUOTA_UNKNOWN_STATE,
	DISKQUOTA_READY_STATE
} DiskQuotaState;

struct DiskQuotaLocks
{
	LWLock *active_table_lock;
	LWLock *reject_map_lock;
	LWLock *extension_ddl_message_lock;
	LWLock *extension_ddl_lock; /* ensure create diskquota extension serially */
	LWLock *monitored_dbid_cache_lock;
	LWLock *relation_cache_lock;
	/* dblist_lock is used to protect a DiskquotaDBEntry's content */
	LWLock *dblist_lock;
	LWLock *workerlist_lock;
	LWLock *altered_reloid_cache_lock;
};
typedef struct DiskQuotaLocks DiskQuotaLocks;
#define DiskQuotaLocksItemNumber (sizeof(DiskQuotaLocks) / sizeof(void *))

/*
 * MessageBox is used to store a message for communication between
 * the diskquota launcher process and backends.
 * When backend create an extension, it send a message to launcher
 * to start the diskquota worker process and write the corresponding
 *
 * dbOid into diskquota database_list table in postgres database.
 * When backend drop an extension, it will send a message to launcher
 * to stop the diskquota worker process and remove the dbOid from diskquota
 * database_list table as well.
 */
struct ExtensionDDLMessage
{
	int launcher_pid; /* diskquota launcher pid */
	int req_pid;      /* pid of the QD process which create/drop
	                   * diskquota extension */
	int cmd;          /* message command type, see MessageCommand */
	int result;       /* message result writen by launcher, see
	                   * MessageResult */
	int dbid;         /* dbid of create/drop diskquota
	                   * extensionstatement */
};

enum MessageCommand
{
	CMD_CREATE_EXTENSION = 1,
	CMD_DROP_EXTENSION,
};

enum MessageResult
{
	ERR_PENDING = 0,
	ERR_OK,
	/* the number of database exceeds the maximum */
	ERR_EXCEED,
	/* add the dbid to diskquota_namespace.database_list failed */
	ERR_ADD_TO_DB,
	/* delete dbid from diskquota_namespace.database_list failed */
	ERR_DEL_FROM_DB,
	/* cann't start worker process */
	ERR_START_WORKER,
	/* invalid dbid */
	ERR_INVALID_DBID,
	ERR_UNKNOWN,
};

typedef struct ExtensionDDLMessage ExtensionDDLMessage;
typedef enum MessageCommand        MessageCommand;
typedef enum MessageResult         MessageResult;

extern DiskQuotaLocks       diskquota_locks;
extern ExtensionDDLMessage *extension_ddl_message;

typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;
typedef struct DiskquotaDBEntry     DiskquotaDBEntry;

/*
 * disk quota worker info used by launcher to manage the worker processes
 * used in DiskquotaLauncherShmem->{freeWorkers, runningWorkers}
 */
struct DiskQuotaWorkerEntry
{
	dlist_node node; // the double linked list header

	int      id;     // starts from 0, -1 means invalid
	NameData dbname; // the database name. It does not need to be reset, when dbEntry == NULL, dbname is not valid.
	DiskquotaDBEntry *dbEntry; // pointer to shared memory. DiskquotaLauncherShmem->dbArray
};

typedef struct
{
	dlist_head        freeWorkers;    // a list of DiskQuotaWorkerEntry
	dlist_head        runningWorkers; // a list of DiskQuotaWorkerEntry
	DiskquotaDBEntry *dbArray;        // size == MAX_NUM_MONITORED_DB
	DiskquotaDBEntry *dbArrayTail;
	volatile bool     isDynamicWorker;
	/*
	DiskQuotaWorkerEntry worker[diskquota_max_workers]; // the hidden memory to store WorkerEntry
	DiskquotaDBEntry     dbentry[MAX_NUM_MONITORED_DB]; // the hidden memory for dbentry
	*/
} DiskquotaLauncherShmemStruct;

/* In shmem, only used on master */
struct DiskquotaDBEntry
{
	int id;   // the index of DiskquotaLauncherShmem->dbArray, start from 0
	Oid dbid; // the database oid in postgres catalog

#define INVALID_WORKER_ID -1
	int         workerId; // the id of the worker which is running for the (current DB?), 0 means no worker for it.
	TimestampTz next_run_time;
	TimestampTz last_run_time;
	int16       cost; // ms

	bool inited; // this entry is inited, will set to true after the worker finish the frist run.
	bool in_use; // this slot is in using. AKA dbid != 0

	TimestampTz last_log_time; // the last time log current database info.
};

typedef enum MonitorDBStatus
{
#define DB_STATUS(id, str) id,
#include "diskquota_enum.h"
#undef DB_STATUS
	DB_STATUS_MAX
} MonitorDBStatus;
/* used in monitored_dbid_cache, in shmem, both on master and segments */

typedef struct MonitorDBEntryStruct *MonitorDBEntry;
struct MonitorDBEntryStruct
{
	Oid              dbid;   // the key
	pg_atomic_uint32 status; // enum MonitorDBStatus
	bool             paused;
	bool             is_readiness_logged; /* true if we have logged the error message for not ready */
	pg_atomic_uint32 epoch;               /* this counter will be increased after each worker loop */
};
extern HTAB *disk_quota_worker_map;

/* drop extension hook */
extern void register_diskquota_object_access_hook(void);

/* enforcement interface*/
extern void init_disk_quota_enforcement(void);
extern void invalidate_database_rejectmap(Oid dbid);

/* quota model interface*/
extern void init_disk_quota_shmem(void);
extern void init_disk_quota_model(uint32 id);
extern void refresh_disk_quota_model(bool force);
extern bool check_diskquota_state_is_ready(void);
extern bool quota_check_common(Oid reloid, RelFileNode *relfilenode);

/* quotaspi interface */
extern void init_disk_quota_hook(void);

extern Datum diskquota_fetch_table_stat(PG_FUNCTION_ARGS);
extern int   diskquota_naptime;
extern int   diskquota_max_active_tables;
extern bool  diskquota_hardlimit;

extern int      SEGCOUNT;
extern int      worker_spi_get_extension_version(int *major, int *minor);
extern void     truncateStringInfo(StringInfo str, int nchars);
extern List    *get_rel_oid_list(void);
extern int64    calculate_relation_size_all_forks(RelFileNodeBackend *rnode, char relstorage, Oid relam);
extern Relation diskquota_relation_open(Oid relid);
extern bool     get_rel_name_namespace(Oid relid, Oid *nsOid, char *relname);
extern List    *diskquota_get_index_list(Oid relid);
extern void     diskquota_get_appendonly_aux_oid_list(Oid reloid, Oid *segrelid, Oid *blkdirrelid, Oid *visimaprelid);
extern Oid      diskquota_parse_primary_table_oid(Oid namespace, char *relname);

extern bool         worker_increase_epoch(Oid dbid);
extern unsigned int worker_get_epoch(Oid dbid);
extern bool         diskquota_is_paused(void);
extern bool         do_check_diskquota_state_is_ready(void);
extern bool         diskquota_is_readiness_logged(void);
extern void         diskquota_set_readiness_logged(void);
extern Size         diskquota_launcher_shmem_size(void);
extern void         init_launcher_shmem(void);
extern void         vacuum_disk_quota_model(uint32 id);
extern void         update_monitor_db(Oid dbid, FetchTableStatType action);
extern void         update_monitor_db_mpp(Oid dbid, FetchTableStatType action, const char *schema);
extern void         diskquota_stop_worker(void);
extern void         update_monitordb_status(Oid dbid, uint32 status);
extern HTAB        *diskquota_hash_create(const char *tabname, long nelem, HASHCTL *info, int flags,
                                          DiskquotaHashFunction hashFunction);
extern HTAB *DiskquotaShmemInitHash(const char *name, long init_size, long max_size, HASHCTL *infoP, int hash_flags,
                                    DiskquotaHashFunction hash_function);
extern void  refresh_monitored_dbid_cache(void);
#endif
