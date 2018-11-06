#ifndef ACTIVE_TABLE_H
#define ACTIVE_TABLE_H

#include "storage/lwlock.h"
#include "diskquota.h"

/* Cache to detect the active table list */
typedef struct DiskQuotaActiveTableFileEntry
{
	Oid         dbid;
	Oid         relfilenode;
	Oid         tablespaceoid;
} DiskQuotaActiveTableFileEntry;

typedef struct DiskQuotaActiveTableEntry
{
	Oid     tableoid;
	Size    tablesize;
} DiskQuotaActiveTableEntry;


extern HTAB* get_active_tables(void);
extern void init_active_table_hook(void);
extern void init_relfilenode_key(void);
extern void init_shm_worker_active_tables(void);
extern void init_lock_active_tables(void);

extern HTAB *active_tables_map;
extern disk_quota_shared_state *active_table_shm_lock;
#endif
