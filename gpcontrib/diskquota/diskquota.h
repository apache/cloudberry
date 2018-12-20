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

typedef struct
{
	LWLock	   *lock;			/* protects shared memory of blackMap */
}			disk_quota_shared_state;

/* enforcement interface*/
extern void init_disk_quota_enforcement(void);

/* quota model interface*/
extern void init_disk_quota_shmem(void);
extern void init_disk_quota_model(void);
extern void refresh_disk_quota_model(bool force);
extern bool quota_check_common(Oid reloid);

/* quotaspi interface */
extern void init_disk_quota_hook(void);

extern int	diskquota_naptime;
extern char *diskquota_monitored_database_list;
extern int	diskquota_max_active_tables;

extern Datum diskquota_fetch_table_stat(PG_FUNCTION_ARGS);

#endif
