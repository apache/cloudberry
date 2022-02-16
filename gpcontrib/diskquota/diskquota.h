#ifndef DISK_QUOTA_H
#define DISK_QUOTA_H

#include "c.h"
#include "postgres.h"
#include "port/atomics.h"

#include "fmgr.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "postmaster/bgworker.h"

#include "utils/hsearch.h"
#include "utils/relcache.h"

#include <signal.h>

/* max number of monitored database with diskquota enabled */
#define MAX_NUM_MONITORED_DB 10

typedef enum
{
	NAMESPACE_QUOTA = 0,
	ROLE_QUOTA,
	NAMESPACE_TABLESPACE_QUOTA,
	ROLE_TABLESPACE_QUOTA,

	NUM_QUOTA_TYPES
}			QuotaType;

typedef enum
{
	FETCH_ACTIVE_OID,			/* fetch active table list */
	FETCH_ACTIVE_SIZE			/* fetch size for active tables */
}			FetchTableStatType;

typedef enum
{
	DISKQUOTA_UNKNOWN_STATE,
	DISKQUOTA_READY_STATE
}			DiskQuotaState;

struct DiskQuotaLocks
{
	LWLock	   *active_table_lock;
	LWLock	   *black_map_lock;
	LWLock	   *extension_ddl_message_lock;
	LWLock	   *extension_ddl_lock; /* ensure create diskquota extension serially */
	LWLock	   *monitoring_dbid_cache_lock;
	LWLock	   *relation_cache_lock;
	LWLock	   *worker_map_lock;
	LWLock	   *altered_reloid_cache_lock;
};
typedef struct DiskQuotaLocks DiskQuotaLocks;
#define DiskQuotaLocksItemNumber (sizeof(DiskQuotaLocks) / sizeof(void*))

/*
 * MessageBox is used to store a message for communication between
 * the diskquota launcher process and backends.
 * When backend create an extension, it send a message to launcher
 * to start the diskquota worker process and write the corresponding
 * dbOid into diskquota database_list table in postgres database.
 * When backend drop an extension, it will send a message to launcher
 * to stop the diskquota worker process and remove the dbOid from diskquota
 * database_list table as well.
 */
struct ExtensionDDLMessage
{
	int			launcher_pid;	/* diskquota launcher pid */
	int			req_pid;		/* pid of the QD process which create/drop
								 * diskquota extension */
	int			cmd;			/* message command type, see MessageCommand */
	int			result;			/* message result writen by launcher, see
								 * MessageResult */
	int			dbid;			/* dbid of create/drop diskquota
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
typedef enum MessageCommand MessageCommand;
typedef enum MessageResult MessageResult;

extern DiskQuotaLocks diskquota_locks;
extern ExtensionDDLMessage *extension_ddl_message;
extern pg_atomic_uint32 *diskquota_hardlimit;

typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;

/* disk quota worker info used by launcher to manage the worker processes. */
struct DiskQuotaWorkerEntry
{
	Oid			dbid;
	pid_t		pid;			/* worker pid */
	pg_atomic_uint32 epoch; 		/* this counter will be increased after each worker loop */
	bool is_paused; 			/* true if this worker is paused */
	BackgroundWorkerHandle *handle;
};

extern HTAB *disk_quota_worker_map;

/* drop extension hook */
extern void register_diskquota_object_access_hook(void);

/* enforcement interface*/
extern void init_disk_quota_enforcement(void);
extern void invalidate_database_blackmap(Oid dbid);

/* quota model interface*/
extern void init_disk_quota_shmem(void);
extern void init_disk_quota_model(void);
extern void refresh_disk_quota_model(bool force);
extern bool check_diskquota_state_is_ready(void);
extern bool quota_check_common(Oid reloid, RelFileNode *relfilenode);

/* quotaspi interface */
extern void init_disk_quota_hook(void);

extern Datum diskquota_fetch_table_stat(PG_FUNCTION_ARGS);
extern int	diskquota_naptime;
extern int	diskquota_max_active_tables;

extern int 	SEGCOUNT;
extern int  get_ext_major_version(void);
extern void truncateStringInfo(StringInfo str, int nchars);
extern List *get_rel_oid_list(void);
extern int64 calculate_relation_size_all_forks(RelFileNodeBackend *rnode, char relstorage);
extern Relation diskquota_relation_open(Oid relid, LOCKMODE mode);
extern List* diskquota_get_index_list(Oid relid);
extern void diskquota_get_appendonly_aux_oid_list(Oid reloid, Oid *segrelid, Oid *blkdirrelid, Oid *visimaprelid);
extern Oid diskquota_parse_primary_table_oid(Oid namespace, char *relname);

extern bool worker_increase_epoch(Oid database_oid);
extern unsigned int worker_get_epoch(Oid database_oid);
extern bool diskquota_is_paused(void);

#endif
