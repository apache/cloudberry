#include "postgres.h"

#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/objectaccess.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relfilenodemap.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "funcapi.h"

#include "relation_cache.h"
#include "diskquota.h"

HTAB	   *relation_cache = NULL;
HTAB	   *relid_cache = NULL;

static void update_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *relation_entry, DiskQuotaRelidCacheEntry *relid_entry);

PG_FUNCTION_INFO_V1(show_relation_cache);

void
init_shm_worker_relation_cache(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaRelationCacheEntry);
	ctl.hash = tag_hash;

	relation_cache = ShmemInitHash("relation_cache",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaRelidCacheEntry);
	ctl.hash = tag_hash;

	relid_cache = ShmemInitHash("relid_cache",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);
}

Oid
get_relid_by_relfilenode(RelFileNode relfilenode)
{
	Oid relid;

	relid = RelidByRelfilenode(relfilenode.spcNode, relfilenode.relNode);
	if(OidIsValid(relid))
	{
		remove_cache_entry(InvalidOid, relfilenode.relNode);
		return relid;
	}

	relid = get_uncommitted_table_relid(relfilenode.relNode);
	return relid;
}

void
remove_cache_entry(Oid relid, Oid relfilenode)
{
	DiskQuotaRelationCacheEntry *relation_entry;
	DiskQuotaRelidCacheEntry *relid_entry;

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	if (OidIsValid(relid))
	{
		relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
		if (relation_entry)
		{
			hash_search(relid_cache, &relation_entry->rnode.node.relNode, HASH_REMOVE, NULL);
			hash_search(relation_cache, &relid, HASH_REMOVE, NULL);
		}
	}

	if (OidIsValid(relfilenode))
	{
		relid_entry = hash_search(relid_cache, &relfilenode, HASH_FIND, NULL);
		if (relid_entry)
		{
			hash_search(relation_cache, &relid_entry->relid, HASH_REMOVE, NULL);
			hash_search(relid_cache, &relfilenode, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);
}

Oid
get_uncommitted_table_relid(Oid relfilenode)
{
	Oid relid = InvalidOid;
	DiskQuotaRelidCacheEntry *entry;
	
	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	entry = hash_search(relid_cache, &relfilenode, HASH_FIND, NULL);
	if (entry)
	{
		relid = entry->relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return relid;
}

static void
add_auxrelid_to_relation_entry(DiskQuotaRelationCacheEntry *entry, Oid relid)
{
	int i;
	
	for (i = 0; i < entry->auxrel_num; i++)
	{
		if (entry->auxrel_oid[i] == relid)
		{
			return;
		}
	}
	entry->auxrel_oid[entry->auxrel_num++] = relid;
}

static void
update_relation_entry(Oid relid, DiskQuotaRelationCacheEntry *relation_entry, DiskQuotaRelidCacheEntry *relid_entry)
{
	Relation rel;

	rel = diskquota_relation_open(relid, NoLock);
	if (rel == NULL)
	{
		return;
	}

	if (relation_entry)
	{
		relation_entry->relid = relid;
		relation_entry->rnode.node = rel->rd_node;
		relation_entry->rnode.backend = rel->rd_backend;
		relation_entry->owneroid = rel->rd_rel->relowner;
		relation_entry->namespaceoid = rel->rd_rel->relnamespace;
		relation_entry->relstorage = rel->rd_rel->relstorage;
	}

	if (relid_entry)
	{
		relid_entry->relfilenode = rel->rd_node.relNode;
		relid_entry->relid = relid;
	}

	relation_entry->primary_table_relid = relid;

	relation_close(rel, NoLock);
}

void
update_relation_cache(Oid relid)
{
	DiskQuotaRelationCacheEntry relation_entry_data = {0};
	DiskQuotaRelationCacheEntry *relation_entry;
	DiskQuotaRelidCacheEntry relid_entry_data = {0};
	DiskQuotaRelidCacheEntry *relid_entry;
	Oid prelid;

	update_relation_entry(relid, &relation_entry_data, &relid_entry_data);

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
	relation_entry = hash_search(relation_cache, &relation_entry_data.relid, HASH_ENTER, NULL);
	memcpy(relation_entry, &relation_entry_data, sizeof(DiskQuotaRelationCacheEntry));

	relid_entry = hash_search(relid_cache, &relid_entry_data.relfilenode, HASH_ENTER, NULL);
	memcpy(relid_entry, &relid_entry_data, sizeof(DiskQuotaRelidCacheEntry));
	LWLockRelease(diskquota_locks.relation_cache_lock);

	prelid = get_primary_table_oid(relid);
	if (OidIsValid(prelid) && prelid != relid)
	{
		LWLockAcquire(diskquota_locks.relation_cache_lock, LW_EXCLUSIVE);
		relation_entry->primary_table_relid = prelid;
		relation_entry = hash_search(relation_cache, &prelid, HASH_FIND, NULL);
		if (relation_entry)
		{
			add_auxrelid_to_relation_entry(relation_entry, relid);
		}
		LWLockRelease(diskquota_locks.relation_cache_lock);
	}
}

static Oid
parse_primary_table_oid(Oid relid)
{
	Relation rel;
	Oid namespace;
	char relname[NAMEDATALEN];

	rel = diskquota_relation_open(relid, NoLock);
	if (rel == NULL)
	{
		return InvalidOid;
	}

	namespace = rel->rd_rel->relnamespace;
	memcpy(relname, rel->rd_rel->relname.data, NAMEDATALEN);
	relation_close(rel, NoLock);

	switch (namespace)
	{
		case PG_TOAST_NAMESPACE:
			if (strncmp(relname, "pg_toast", 8) == 0)
				return atoi(&relname[9]);
		break;
		case PG_AOSEGMENT_NAMESPACE:
		{
			if (strncmp(relname, "pg_aoseg", 8) == 0)
				return atoi(&relname[9]);
			else if (strncmp(relname, "pg_aovisimap", 12) == 0)
				return atoi(&relname[13]);
			else if (strncmp(relname, "pg_aocsseg", 10) == 0)
				return atoi(&relname[11]);
		}
		break;
	}
	return relid;
}

Oid
get_primary_table_oid(Oid relid)
{
	DiskQuotaRelationCacheEntry *relation_entry;
	Oid cached_prelid = relid;
	Oid parsed_prelid;

	parsed_prelid = parse_primary_table_oid(relid);
	if (OidIsValid(parsed_prelid))
	{
		return parsed_prelid;
	}

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	relation_entry = hash_search(relation_cache, &relid, HASH_FIND, NULL);
	if (relation_entry)
	{
		cached_prelid = relation_entry->primary_table_relid;
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return cached_prelid;
}

void
remove_committed_relation_from_cache(void)
{
	HASH_SEQ_STATUS iter = {0};
	DiskQuotaRelationCacheEntry *entry = NULL;
	DiskQuotaRelationCacheEntry *local_entry = NULL;
	HTAB *local_relation_cache;
	HASHCTL	ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaRelationCacheEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_relation_cache = hash_create("local relation cache",
											 1024,
											 &ctl,
											 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	hash_seq_init(&iter, relation_cache);
	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		local_entry = hash_search(local_relation_cache, &entry->relid, HASH_ENTER, NULL);
		memcpy(local_entry, entry, sizeof(DiskQuotaRelationCacheEntry));
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);
	
	hash_seq_init(&iter, local_relation_cache);
	while ((local_entry = hash_seq_search(&iter)) != NULL)
	{
		if (SearchSysCacheExists1(RELOID, local_entry->relid))
		{
			remove_cache_entry(local_entry->relid, InvalidOid);
		}
	}
	hash_destroy(local_relation_cache);
}

Datum
show_relation_cache(PG_FUNCTION_ARGS)
{
	DiskQuotaRelationCacheEntry *entry;
	FuncCallContext			   *funcctx;
	struct RelationCacheCtx {
		HASH_SEQ_STATUS			iter;
		HTAB				   *relation_cache;
	} *relation_cache_ctx;

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

		tupdesc = CreateTemplateTupleDesc(11, false /*hasoid*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "RELID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "PRIMARY_TABLE_OID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "AUXREL_NUM", INT4OID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "OWNEROID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "NAMESPACEOID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "BACKENDID", INT4OID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "SPCNODE", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "DBNODE", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "RELNODE", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "RELSTORAGE", CHAROID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "AUXREL_OID", OIDARRAYOID, -1 /*typmod*/, 0 /*attdim*/);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Create a local hash table and fill it with entries from shared memory. */
		memset(&hashctl, 0, sizeof(hashctl));
		hashctl.keysize = sizeof(Oid);
		hashctl.entrysize = sizeof(DiskQuotaRelationCacheEntry);
		hashctl.hcxt = CurrentMemoryContext;
		hashctl.hash = tag_hash;

		relation_cache_ctx = (struct RelationCacheCtx *) palloc(sizeof(struct RelationCacheCtx));
		relation_cache_ctx->relation_cache = hash_create("relation_cache_ctx->relation_cache",
											 1024, &hashctl,
											 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

		LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
		hash_seq_init(&hash_seq, relation_cache);
		while ((entry = (DiskQuotaRelationCacheEntry *) hash_seq_search(&hash_seq)) != NULL)
		{
			DiskQuotaRelationCacheEntry *local_entry = hash_search(relation_cache_ctx->relation_cache,
											   					   &entry->relid, HASH_ENTER_NULL, NULL);
			if (local_entry)
			{
				memcpy(local_entry, entry, sizeof(DiskQuotaRelationCacheEntry));
			}
		}
		LWLockRelease(diskquota_locks.relation_cache_lock);

		/* Setup first calling context. */
		hash_seq_init(&(relation_cache_ctx->iter), relation_cache_ctx->relation_cache);
		funcctx->user_fctx = (void *) relation_cache_ctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	relation_cache_ctx = (struct RelationCacheCtx *) funcctx->user_fctx;

	while ((entry = (DiskQuotaRelationCacheEntry *)hash_seq_search(&(relation_cache_ctx->iter))) != NULL)
	{
		Datum			result;
		Datum			values[11];
		Datum			auxrel_oid[10];
		bool			nulls[11];
		HeapTuple		tuple;
		ArrayType 	   *array;
		int				i;

		for (i = 0; i < entry->auxrel_num; i++)
		{
			auxrel_oid[i] = ObjectIdGetDatum(entry->auxrel_oid[i]);
		}
		array = construct_array(auxrel_oid, entry->auxrel_num, OIDOID, sizeof(Oid), true, 'i');

		values[0] = ObjectIdGetDatum(entry->relid);
		values[1] = ObjectIdGetDatum(entry->primary_table_relid);
		values[2] = Int32GetDatum(entry->auxrel_num);
		values[3] = ObjectIdGetDatum(entry->owneroid);
		values[4] = ObjectIdGetDatum(entry->namespaceoid);
		values[5] = Int32GetDatum(entry->rnode.backend);
		values[6] = ObjectIdGetDatum(entry->rnode.node.spcNode);
		values[7] = ObjectIdGetDatum(entry->rnode.node.dbNode);
		values[8] = ObjectIdGetDatum(entry->rnode.node.relNode);
		values[9] = CharGetDatum(entry->relstorage);
		values[10] = PointerGetDatum(array);

		memset(nulls, false, sizeof(nulls));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
