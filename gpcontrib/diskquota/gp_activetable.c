/* -------------------------------------------------------------------------
 *
 * gp_activetable.c
 *
 * This code is responsible for detecting active table for databases
 * quotamodel will call gp_fetch_active_tables() to fetch the active tables
 * and their size information in each loop.
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		diskquota/gp_activetable.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relfilenodemap.h"

#include "gp_activetable.h"
#include "diskquota.h"

PG_FUNCTION_INFO_V1(diskquota_fetch_table_stat);

/* The results set cache for SRF call*/
typedef struct DiskQuotaSetOFCache
{
	HTAB	   *result;
	HASH_SEQ_STATUS pos;
}			DiskQuotaSetOFCache;

HTAB	   *active_tables_map = NULL;

/* active table hooks which detect the disk file size change. */
static file_create_hook_type prev_file_create_hook = NULL;
static file_extend_hook_type prev_file_extend_hook = NULL;
static file_truncate_hook_type prev_file_truncate_hook = NULL;

static void active_table_hook_smgrcreate(RelFileNodeBackend rnode);
static void active_table_hook_smgrextend(RelFileNodeBackend rnode);
static void active_table_hook_smgrtruncate(RelFileNodeBackend rnode);

static HTAB *get_active_tables_stats(ArrayType *array);
static HTAB *get_active_tables_oid(void);
static HTAB *pull_active_list_from_seg(void);
static void pull_active_table_size_from_seg(HTAB *local_table_stats_map, char *active_oid_array);
static StringInfoData convert_map_to_string(HTAB *active_list);
static void load_table_size(HTAB *local_table_stats_map);
static void report_active_table_helper(const RelFileNodeBackend *relFileNode);

void		init_active_table_hook(void);
void		init_shm_worker_active_tables(void);
void		init_lock_active_tables(void);
HTAB	   *gp_fetch_active_tables(bool is_init);

/*
 * Init active_tables_map shared memory
 */
void
init_shm_worker_active_tables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.hash = tag_hash;

	active_tables_map = ShmemInitHash("active_tables",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);
}

/*
 * Register disk file size change hook to detect active table.
 */
void
init_active_table_hook(void)
{
	prev_file_create_hook = file_create_hook;
	file_create_hook = active_table_hook_smgrcreate;

	prev_file_extend_hook = file_extend_hook;
	file_extend_hook = active_table_hook_smgrextend;

	prev_file_truncate_hook = file_truncate_hook;
	file_truncate_hook = active_table_hook_smgrtruncate;
}

/*
 * File create hook is used to monitor a new file create event
 */
static void
active_table_hook_smgrcreate(RelFileNodeBackend rnode)
{
	if (prev_file_create_hook)
		(*prev_file_create_hook) (rnode);

	report_active_table_helper(&rnode);
}

/*
 * File extend hook is used to monitor file size extend event
 * it could be extending a page for heap table or just monitoring
 * file write for an append-optimize table.
 */
static void
active_table_hook_smgrextend(RelFileNodeBackend rnode)
{
	if (prev_file_extend_hook)
		(*prev_file_extend_hook) (rnode);

	report_active_table_helper(&rnode);
}

/*
 * File truncate hook is used to monitor a new file truncate event
 */
static void
active_table_hook_smgrtruncate(RelFileNodeBackend rnode)
{
	if (prev_file_truncate_hook)
		(*prev_file_truncate_hook) (rnode);

	report_active_table_helper(&rnode);
}

/*
 * Common function for reporting active tables
 * Currently, any file events(create, extend. truncate) are
 * treated the same and report_active_table_helper just put
 * the corresponding relFileNode into the active_tables_map
 */
static void
report_active_table_helper(const RelFileNodeBackend *relFileNode)
{
	DiskQuotaActiveTableFileEntry *entry;
	DiskQuotaActiveTableFileEntry item;
	bool		found = false;

	
	/* We do not collect the active table in either master or mirror segments  */
	if (IS_QUERY_DISPATCHER() || IsRoleMirror())
	{
		return;
	}
	
	MemSet(&item, 0, sizeof(DiskQuotaActiveTableFileEntry));
	item.dbid = relFileNode->node.dbNode;
	item.relfilenode = relFileNode->node.relNode;
	item.tablespaceoid = relFileNode->node.spcNode;

	LWLockAcquire(diskquota_locks.active_table_lock, LW_EXCLUSIVE);
	entry = hash_search(active_tables_map, &item, HASH_ENTER_NULL, &found);
	if (entry && !found)
		*entry = item;

	if (!found && entry == NULL)
	{
		/*
		 * We may miss the file size change of this relation at current
		 * refresh interval.
		 */
		ereport(WARNING, (errmsg("Share memory is not enough for active tables.")));
	}
	LWLockRelease(diskquota_locks.active_table_lock);
}

/*
 * Interface of activetable module
 * This function is called by quotamodel module.
 * Disk quota worker process need to collect
 * active table disk usage from all the segments.
 * And aggregate the table size on each segment
 * to get the real table size at cluster level.
 */
HTAB *
gp_fetch_active_tables(bool is_init)
{
	HTAB	   *local_table_stats_map = NULL;
	HASHCTL		ctl;
	HTAB	   *local_active_table_oid_maps;
	StringInfoData active_oid_list;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_table_stats_map = hash_create("local active table map with relfilenode info",
										1024,
										&ctl,
										HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	if (is_init)
	{
		load_table_size(local_table_stats_map);
	}
	else
	{
		/* step 1: fetch active oids from all the segments */
		local_active_table_oid_maps = pull_active_list_from_seg();
		active_oid_list = convert_map_to_string(local_active_table_oid_maps);

		/* step 2: fetch active table sizes based on active oids */
		pull_active_table_size_from_seg(local_table_stats_map, active_oid_list.data);

		hash_destroy(local_active_table_oid_maps);
		pfree(active_oid_list.data);
	}
	return local_table_stats_map;
}

/*
 * Function to get the table size from each segments
 * There are three mode:
 * 1. gather active table oid from all the segments, since table may only
 * be modified on a subset of the segments, we need to firstly gather the
 * active table oid list from all the segments.
 * 2. calculate the active table size based on the active table oid list.
 */
Datum
diskquota_fetch_table_stat(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int32		mode = PG_GETARG_INT32(0);
	AttInMetadata *attinmeta;
	bool		isFirstCall = true;

	HTAB	   *localCacheTable = NULL;
	DiskQuotaSetOFCache *cache = NULL;
	DiskQuotaActiveTableEntry *results_entry = NULL;

	/* Init the container list in the first call and get the results back */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		{
			ereport(ERROR, (errmsg("This function must not be called on master or by user")));
		}

		switch (mode)
		{
			case FETCH_ACTIVE_OID:
				localCacheTable = get_active_tables_oid();
				break;
			case FETCH_ACTIVE_SIZE:
				localCacheTable = get_active_tables_stats(PG_GETARG_ARRAYTYPE_P(1));
				break;
			default:
				ereport(ERROR, (errmsg("Unused mode number, transaction will be aborted")));
				break;

		}

		/*
		 * total number of active tables to be returned, each tuple contains
		 * one active table stat
		 */
		funcctx->max_calls = (uint32) hash_get_num_entries(localCacheTable);

		/*
		 * prepare attribute metadata for next calls that generate the tuple
		 */

		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "TABLE_OID",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "TABLE_SIZE",
						   INT8OID, -1, 0);

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Prepare SetOf results HATB */
		cache = (DiskQuotaSetOFCache *) palloc(sizeof(DiskQuotaSetOFCache));
		cache->result = localCacheTable;
		hash_seq_init(&(cache->pos), localCacheTable);

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		isFirstCall = false;
	}

	funcctx = SRF_PERCALL_SETUP();

	if (isFirstCall)
	{
		funcctx->user_fctx = (void *) cache;
	}
	else
	{
		cache = (DiskQuotaSetOFCache *) funcctx->user_fctx;
	}

	/* return the results back to SPI caller */
	while ((results_entry = (DiskQuotaActiveTableEntry *) hash_seq_search(&(cache->pos))) != NULL)
	{
		Datum		result;
		Datum		values[2];
		bool		nulls[2];
		HeapTuple	tuple;

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));

		values[0] = ObjectIdGetDatum(results_entry->tableoid);
		values[1] = Int64GetDatum(results_entry->tablesize);

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);

		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	/* finished, do the clear staff */
	hash_destroy(cache->result);
	pfree(cache);
	SRF_RETURN_DONE(funcctx);
}

/*
 * Call pg_total_relation_size to calcualte the
 * active table size on each segments.
 */
static HTAB *
get_active_tables_stats(ArrayType *array)
{
	int			ndim = ARR_NDIM(array);
	int		   *dims = ARR_DIMS(array);
	int			nitems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char	   *ptr;
	bits8	   *bitmap;
	int			bitmask;
	int			i;
	Oid			relOid;
	HTAB	   *local_table = NULL;
	HASHCTL		ctl;
	DiskQuotaActiveTableEntry *entry;

	Assert(ARR_ELEMTYPE(array) == OIDOID);

	nitems = ArrayGetNItems(ndim, dims);

	get_typlenbyvalalign(ARR_ELEMTYPE(array),
						 &typlen, &typbyval, &typalign);


	ptr = ARR_DATA_PTR(array);
	bitmap = ARR_NULLBITMAP(array);
	bitmask = 1;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_table = hash_create("local table map",
							  1024,
							  &ctl,
							  HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	for (i = 0; i < nitems; i++)
	{
		/*
		 * handle array containing NULL case for general inupt, but the active
		 * table oid array would not contain NULL in fact
		 */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			continue;
		}
		else
		{
			relOid = DatumGetObjectId(fetch_att(ptr, typbyval, typlen));

			entry = (DiskQuotaActiveTableEntry *) hash_search(local_table, &relOid, HASH_ENTER, NULL);
			entry->tableoid = relOid;

			/*
			 * avoid to generate ERROR if relOid is not existed (i.e. table
			 * has been droped)
			 */
			PG_TRY();
			{
				/* call pg_total_relation_size to get the active table size */
				entry->tablesize = (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
																			ObjectIdGetDatum(relOid)));
			}
			PG_CATCH();
			{
				FlushErrorState();
				entry->tablesize = 0;
			}
			PG_END_TRY();

			ptr = att_addlength_pointer(ptr, typlen, ptr);
			ptr = (char *) att_align_nominal(ptr, typalign);

		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	return local_table;
}

/*
 * Get local active table with table oid and table size info.
 * This function first copies active table map from shared memory
 * to local active table map with refilenode info. Then traverses
 * the local map and find corresponding table oid and table file
 * size. Finally stores them into local active table map and return.
 */
static HTAB *
get_active_tables_oid(void)
{
	HASHCTL		ctl;
	HTAB	   *local_active_table_file_map = NULL;
	HTAB	   *local_active_table_stats_map = NULL;
	HASH_SEQ_STATUS iter;
	DiskQuotaActiveTableFileEntry *active_table_file_entry;
	DiskQuotaActiveTableEntry *active_table_entry;

	Oid			relOid;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = tag_hash;

	local_active_table_file_map = hash_create("local active table map with relfilenode info",
											  1024,
											  &ctl,
											  HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/* Move active table from shared memory to local active table map */
	LWLockAcquire(diskquota_locks.active_table_lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, active_tables_map);

	/* copy active table from shared memory into local memory */
	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool		found;
		DiskQuotaActiveTableFileEntry *entry;

		if (active_table_file_entry->dbid != MyDatabaseId)
		{
			continue;
		}

		/* Add the active table entry into local hash table */
		entry = hash_search(local_active_table_file_map, active_table_file_entry, HASH_ENTER, &found);
		if (entry)
			*entry = *active_table_file_entry;
		hash_search(active_tables_map, active_table_file_entry, HASH_REMOVE, NULL);
	}
	LWLockRelease(diskquota_locks.active_table_lock);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_active_table_stats_map = hash_create("local active table map with relfilenode info",
											   1024,
											   &ctl,
											   HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/*
	 * scan whole local map, get the oid of each table and calculate the size
	 * of them
	 */
	hash_seq_init(&iter, local_active_table_file_map);

	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool		found;

		relOid = RelidByRelfilenode(active_table_file_entry->tablespaceoid, active_table_file_entry->relfilenode);
		if (relOid != InvalidOid)
		{
			active_table_entry = hash_search(local_active_table_stats_map, &relOid, HASH_ENTER, &found);
			if (active_table_entry)
			{
				active_table_entry->tableoid = relOid;
				active_table_entry->tablesize = 0;
			}
			hash_search(local_active_table_file_map, active_table_file_entry, HASH_REMOVE, NULL);
		}
	}

	/*
	 * If cannot convert relfilenode to relOid, put them back to shared memory
	 * and wait for the next check.
	 */
	if (hash_get_num_entries(local_active_table_file_map) > 0)
	{
		bool		found;
		DiskQuotaActiveTableFileEntry *entry;

		hash_seq_init(&iter, local_active_table_file_map);
		LWLockAcquire(diskquota_locks.active_table_lock, LW_EXCLUSIVE);
		while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
		{
			entry = hash_search(active_tables_map, active_table_file_entry, HASH_ENTER_NULL, &found);
			if (entry)
				*entry = *active_table_file_entry;
		}
		LWLockRelease(diskquota_locks.active_table_lock);
	}
	hash_destroy(local_active_table_file_map);
	return local_active_table_stats_map;
}

/*
 * Load table size info from diskquota.table_size table.
 * This is called when system startup, disk quota black list
 * and other shared memory will be warmed up by table_size table.
*/
static void
load_table_size(HTAB *local_table_stats_map)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	bool		found;
	DiskQuotaActiveTableEntry *quota_entry;

	ret = SPI_execute("select tableid, size from diskquota.table_size", true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("[diskquota] load_table_size SPI_execute failed: error code %d", errno)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 2 ||
		((tupdesc)->attrs[0])->atttypid != OIDOID ||
		((tupdesc)->attrs[1])->atttypid != INT8OID)
	{
		ereport(ERROR, (errmsg("[diskquota] table \"table_size\" is corrupted in database \"%s\","
							   " please recreate diskquota extension",
							   get_database_name(MyDatabaseId))));
	}

	/* push the table oid and size into local_table_stats_map */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		dat;
		Oid			tableOid;
		int64		size;
		bool		isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			continue;
		tableOid = DatumGetObjectId(dat);

		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		if (isnull)
			continue;
		size = DatumGetInt64(dat);


		quota_entry = (DiskQuotaActiveTableEntry *) hash_search(
																local_table_stats_map,
																&tableOid,
																HASH_ENTER, &found);
		quota_entry->tableoid = tableOid;
		quota_entry->tablesize = size;
	}
	return;
}

/*
 * Convert a hash map with oids into a string array
 * This function is used to prepare the second array parameter
 * of function diskquota_fetch_table_stat.
 */
static StringInfoData
convert_map_to_string(HTAB *local_active_table_oid_maps)
{
	HASH_SEQ_STATUS iter;
	StringInfoData buffer;
	DiskQuotaActiveTableEntry *entry;
	uint32		count = 0;
	uint32		nitems = hash_get_num_entries(local_active_table_oid_maps);

	initStringInfo(&buffer);
	appendStringInfo(&buffer, "{");

	hash_seq_init(&iter, local_active_table_oid_maps);

	while ((entry = (DiskQuotaActiveTableEntry *) hash_seq_search(&iter)) != NULL)
	{
		count++;
		if (count != nitems)
		{
			appendStringInfo(&buffer, "%d,", entry->tableoid);
		}
		else
		{
			appendStringInfo(&buffer, "%d", entry->tableoid);
		}
	}
	appendStringInfo(&buffer, "}");

	return buffer;
}


/*
 * Get active table size from all the segments based on
 * active table oid list.
 * Function diskquota_fetch_table_stat is called to calculate
 * the table size on the fly.
 */
static HTAB *
pull_active_list_from_seg(void)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i,
				j;
	char	   *sql = NULL;
	HTAB	   *local_active_table_oid_map = NULL;
	HASHCTL		ctl;
	DiskQuotaActiveTableEntry *entry;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_active_table_oid_map = hash_create("local active table map with relfilenode info",
											 1024,
											 &ctl,
											 HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);


	/* first get all oid of tables which are active table on any segment */
	sql = "select * from diskquota.diskquota_fetch_table_stat(0, '{}'::oid[])";

	/* any errors will be catch in upper level */
	CdbDispatchCommand(sql, DF_NONE, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		Oid			tableOid;
		bool		found;

		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("[diskquota] fetching active tables, encounter unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}

		/* push the active table oid into local_active_table_oid_map */
		for (j = 0; j < PQntuples(pgresult); j++)
		{
			tableOid = atooid(PQgetvalue(pgresult, j, 0));

			entry = (DiskQuotaActiveTableEntry *) hash_search(local_active_table_oid_map, &tableOid, HASH_ENTER, &found);

			if (!found)
			{
				entry->tableoid = tableOid;
				entry->tablesize = 0;
			}
		}
	}
	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return local_active_table_oid_map;
}

/*
 * Get active table list from all the segments.
 * Since when loading data, there is case where only subset for
 * segment doing the real loading. As a result, the same table
 * maybe active on some segments while not active on others. We
 * haven't store the table size for each segment on master(to save
 * memory), so when re-calculate the table size, we need to sum the
 * table size on all of the segments.
 */
static void
pull_active_table_size_from_seg(HTAB *local_table_stats_map, char *active_oid_array)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	StringInfoData sql_command;
	int			i;
	int			j;

	initStringInfo(&sql_command);
	appendStringInfo(&sql_command, "select * from diskquota.diskquota_fetch_table_stat(1, '%s'::oid[])",
					 active_oid_array);
	CdbDispatchCommand(sql_command.data, DF_NONE, &cdb_pgresults);
	pfree(sql_command.data);

	/* sum table size from each segment into local_table_stats_map */
	for (i = 0; i < cdb_pgresults.numResults; i++)
	{

		Size		tableSize;
		bool		found;
		Oid			tableOid;
		DiskQuotaActiveTableEntry *entry;

		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("[diskquota] fetching active tables, encounter unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}

		for (j = 0; j < PQntuples(pgresult); j++)
		{
			tableOid = atooid(PQgetvalue(pgresult, j, 0));
			tableSize = (Size) atoll(PQgetvalue(pgresult, j, 1));

			entry = (DiskQuotaActiveTableEntry *) hash_search(
															  local_table_stats_map, &tableOid, HASH_ENTER, &found);

			if (!found)
			{
				/* receive table size info from the first segment */
				entry->tableoid = tableOid;
				entry->tablesize = tableSize;
			}
			else
			{
				/* sum table size from all the segments */
				entry->tablesize = entry->tablesize + tableSize;
			}

		}
	}
	cdbdisp_clearCdbPgResults(&cdb_pgresults);
	return;
}
