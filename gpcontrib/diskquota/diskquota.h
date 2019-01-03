#ifndef DISK_QUOTA_H
#define DISK_QUOTA_H

#include "storage/lwlock.h"

typedef enum
{
	NAMESPACE_QUOTA,
	ROLE_QUOTA
}			QuotaType;

typedef enum
{
	FETCH_ALL_SIZE,				/* fetch size for all the tables */
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
	LWLock	   *message_box_lock;
};
typedef struct DiskQuotaLocks DiskQuotaLocks;

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
struct MessageBox
{
	int			launcher_pid;
	int			req_pid;		/* pid of the request process */
	int			cmd;			/* message command type, see MessageCommand */
	int			result;			/* message result writen by launcher, see
								 * MessageResult */
	int			data[4];		/* for create/drop extension diskquota,
								 * data[0] is dbid */
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
	/* cann't start worker process */
	ERR_START_WORKER,
	/* invalid dbid */
	ERR_INVALID_DBID,
	ERR_UNKNOWN,
};

typedef struct MessageBox MessageBox;
typedef enum MessageCommand MessageCommand;
typedef enum MessageResult MessageResult;

extern DiskQuotaLocks diskquota_locks;
extern volatile MessageBox *message_box;

/* enforcement interface*/
extern void init_disk_quota_enforcement(void);
extern void diskquota_invalidate_db(Oid dbid);

/* quota model interface*/
extern void init_disk_quota_shmem(void);
extern void init_disk_quota_model(void);
extern void refresh_disk_quota_model(bool force);
extern bool check_diskquota_state_is_ready(void);
extern bool quota_check_common(Oid reloid);

/* quotaspi interface */
extern void init_disk_quota_hook(void);

extern Datum diskquota_fetch_table_stat(PG_FUNCTION_ARGS);
extern int	diskquota_naptime;
extern int	diskquota_max_active_tables;

#endif
