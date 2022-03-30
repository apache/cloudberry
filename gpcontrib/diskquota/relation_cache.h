#ifndef RELATION_CACHE_H
#define RELATION_CACHE_H

#include "c.h"
#include "utils/hsearch.h"
#include "storage/relfilenode.h"

typedef struct DiskQuotaRelationCacheEntry
{
	Oid                relid;
	Oid                primary_table_relid;
	Oid                auxrel_oid[10];
	Oid                auxrel_num;
	Oid                owneroid;
	Oid                namespaceoid;
	char               relstorage;
	RelFileNodeBackend rnode;
} DiskQuotaRelationCacheEntry;

typedef struct DiskQuotaRelidCacheEntry
{
	Oid relfilenode;
	Oid relid;
} DiskQuotaRelidCacheEntry;

extern HTAB *relation_cache;

extern void init_shm_worker_relation_cache(void);
extern Oid  get_relid_by_relfilenode(RelFileNode relfilenode);
extern void remove_cache_entry(Oid relid, Oid relfilenode);
extern Oid  get_uncommitted_table_relid(Oid relfilenode);
extern void update_relation_cache(Oid relid);
extern Oid  get_primary_table_oid(Oid relid, bool on_bgworker);
extern void remove_committed_relation_from_cache(void);
extern Size calculate_table_size(Oid relid);

#endif
