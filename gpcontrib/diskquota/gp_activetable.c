/* -------------------------------------------------------------------------
 *
 * activetable.c
 *
 * This code is responsible for detecting active table for databases
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		gpcontrib/gp_diskquota/gp_activetable.c
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


/* The results set cache for SRF call*/
typedef struct DiskQuotaSetOFCache
{
	HTAB	   *result;
	HASH_SEQ_STATUS pos;
}			DiskQuotaSetOFCache;

HTAB	   *active_tables_map = NULL;

/* active table hooks*/
static BufferedAppendWrite_hook_type prev_BufferedAppendWrite_hook = NULL;
static smgrcreate_hook_type prev_smgrcreate_hook = NULL;
static smgrextend_hook_type prev_smgrextend_hook = NULL;
static smgrtruncate_hook_type prev_smgrtruncate_hook = NULL;
static void active_table_hook_smgrcreate(SMgrRelation reln,
							 ForkNumber forknum,
							 bool isRedo);
static void active_table_hook_smgrextend(SMgrRelation reln,
							 ForkNumber forknum,
							 BlockNumber blocknum,
							 char *buffer,
							 bool skipFsync);
static void active_table_hook_smgrtruncate(SMgrRelation reln,
							   ForkNumber forknum,
							   BlockNumber blocknum);

PG_FUNCTION_INFO_V1(diskquota_fetch_table_stat);

static HTAB *get_active_tables_stats(ArrayType *array);
static HTAB *get_all_tables_size(void);
static HTAB *get_active_tables(void);
static StringInfoData convert_map_to_string(HTAB *active_list);
static HTAB *pull_active_list_from_seg(void);
static void report_active_table_SmgrStat(SMgrRelation reln);
static void report_active_table_AO(BufferedAppend * bufferedAppend);
static void load_table_size(HTAB *local_table_stats_map);

void		init_active_table_hook(void);
void		init_shm_worker_active_tables(void);
void		init_lock_active_tables(void);
HTAB	   *gp_fetch_active_tables(bool is_init);

/*
 * Register smgr hook to detect active table.
 */
void
init_active_table_hook(void)
{
	prev_smgrcreate_hook = smgrcreate_hook;
	smgrcreate_hook = active_table_hook_smgrcreate;

	prev_smgrextend_hook = smgrextend_hook;
	smgrextend_hook = active_table_hook_smgrextend;

	prev_smgrtruncate_hook = smgrtruncate_hook;
	smgrtruncate_hook = active_table_hook_smgrtruncate;

	prev_BufferedAppendWrite_hook = BufferedAppendWrite_hook;
	BufferedAppendWrite_hook = report_active_table_AO;
}

static void
active_table_hook_smgrcreate(SMgrRelation reln,
							 ForkNumber forknum,
							 bool isRedo)
{
	if (prev_smgrcreate_hook)
		(*prev_smgrcreate_hook) (reln, forknum, isRedo);

	report_active_table_SmgrStat(reln);
}

static void
active_table_hook_smgrextend(SMgrRelation reln,
							 ForkNumber forknum,
							 BlockNumber blocknum,
							 char *buffer,
							 bool skipFsync)
{
	if (prev_smgrextend_hook)
		(*prev_smgrextend_hook) (reln, forknum, blocknum, buffer, skipFsync);

	report_active_table_SmgrStat(reln);
}

static void
active_table_hook_smgrtruncate(SMgrRelation reln,
							   ForkNumber forknum,
							   BlockNumber blocknum)
{
	if (prev_smgrtruncate_hook)
		(*prev_smgrtruncate_hook) (reln, forknum, blocknum);

	report_active_table_SmgrStat(reln);
}

/*
 * Init active_tables_map shared memory
 */
void
init_shm_worker_active_tables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hash = tag_hash;

	active_tables_map = ShmemInitHash("active_tables",
									  diskquota_max_active_tables,
									  diskquota_max_active_tables,
									  &ctl,
									  HASH_ELEM | HASH_FUNCTION);
}

/*
 * Common function for reporting active tables, used by smgr and ao
 */

static void
report_active_table_helper(const RelFileNodeBackend *relFileNode)
{
	DiskQuotaActiveTableFileEntry *entry;
	DiskQuotaActiveTableFileEntry item;
	bool		found = false;

	MemSet(&item, 0, sizeof(DiskQuotaActiveTableFileEntry));
	item.dbid = relFileNode->node.dbNode;
	item.relfilenode = relFileNode->node.relNode;
	item.tablespaceoid = relFileNode->node.spcNode;

	LWLockAcquire(diskquota_locks.active_table_lock, LW_EXCLUSIVE);
	entry = hash_search(active_tables_map, &item, HASH_ENTER_NULL, &found);
	if (entry && !found)
		*entry = item;
	LWLockRelease(diskquota_locks.active_table_lock);

	if (!found && entry == NULL)
	{
		/*
		 * We may miss the file size change of this relation at current
		 * refresh interval.
		 */
		ereport(WARNING, (errmsg("Share memory is not enough for active tables.")));
	}
}

/*
 *  Hook function in smgr to report the active table
 *  information and store them in active table shared memory
 *  diskquota worker will consuming these active tables and
 *  recalculate their file size to update diskquota model.
 */
static void
report_active_table_SmgrStat(SMgrRelation reln)
{
	report_active_table_helper(&reln->smgr_rnode);
}

/*
 * Hook function in BufferedAppendWrite to report the active table, used by
 * diskquota
 */
static void
report_active_table_AO(BufferedAppend * bufferedAppend)
{
	if (prev_BufferedAppendWrite_hook)
		(*prev_BufferedAppendWrite_hook) (bufferedAppend);
	report_active_table_helper(&bufferedAppend->relFileNode);
}

/*
 * Function to get the table size from each segments
 * There are two mode: 1. calcualte disk usage for all
 * the tables, which is called when init the disk quota model.
 * 2. calculate the active table size when refreshing the
 * disk quota model.
 */
Datum
diskquota_fetch_table_stat(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int32		model = PG_GETARG_INT32(0);
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

		switch (model)
		{
			case FETCH_ALL_SIZE:
				localCacheTable = get_all_tables_size();
				break;
			case FETCH_ACTIVE_OID:
				localCacheTable = get_active_tables();
				break;
			case FETCH_ACTIVE_SIZE:
				localCacheTable = get_active_tables_stats(PG_GETARG_ARRAYTYPE_P(1));
				break;
			default:
				ereport(ERROR, (errmsg("Unused model number, transaction will be aborted")));
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


HTAB *
get_all_tables_size(void)
{
	HTAB	   *local_table_stats_map = NULL;
	HASHCTL		ctl;
	HeapTuple	tuple;
	Relation	classRel;
	HeapScanDesc relScan;


	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_table_stats_map = hash_create("local active table map with relfilenode info",
										1024,
										&ctl,
										HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);


	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);


	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Oid			relOid;
		DiskQuotaActiveTableEntry *entry;

		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;
		relOid = HeapTupleGetOid(tuple);

		/* ignore system table */
		if (relOid < FirstNormalObjectId)
			continue;

		entry = (DiskQuotaActiveTableEntry *) hash_search(local_table_stats_map, &relOid, HASH_ENTER, NULL);

		entry->tableoid = relOid;
		entry->tablesize = (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
																	ObjectIdGetDatum(relOid)));

	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	return local_table_stats_map;
}


/*
 * Get local active table with table oid and table size info.
 * This function first copies active table map from shared memory
 * to local active table map with refilenode info. Then traverses
 * the local map and find corresponding table oid and table file
 * size. Finnaly stores them into local active table map and return.
 */
HTAB *
get_active_tables(void)
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

	/* traverse local active table map and calculate their file size. */
	hash_seq_init(&iter, local_active_table_file_map);

	/*
	 * scan whole local map, get the oid of each table and calculate the size
	 * of them
	 */
	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool		found;

		relOid = RelidByRelfilenode(active_table_file_entry->tablespaceoid, active_table_file_entry->relfilenode);

		active_table_entry = hash_search(local_active_table_stats_map, &relOid, HASH_ENTER, &found);
		if (active_table_entry)
		{
			active_table_entry->tableoid = relOid;
			active_table_entry->tablesize = 0;
		}
	}
	elog(DEBUG1, "active table number is:%ld", hash_get_num_entries(local_active_table_file_map));
	hash_destroy(local_active_table_file_map);
	return local_active_table_stats_map;
}

/*
 * Load table size info from diskquota.table_size table.
*/
static void
load_table_size(HTAB *local_table_stats_map)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	bool		found;
	DiskQuotaActiveTableEntry *quota_entry;

	RangeVar   *rv;
	Relation	rel;

	rv = makeRangeVar("diskquota", "table_size", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("table \"table_size\" is missing in database \"%s\", please recreate diskquota extension", get_database_name(MyDatabaseId))));
	}
	heap_close(rel, NoLock);



	ret = SPI_execute("select tableid, size from diskquota.table_size", true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 2 ||
		((tupdesc)->attrs[0])->atttypid != OIDOID ||
		((tupdesc)->attrs[1])->atttypid != INT8OID)
	{
		ereport(ERROR, (errmsg("table \"table_size\" is corrupted in database \"%s\","
							   " please recreate diskquota extension",
							   get_database_name(MyDatabaseId))));
	}

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
 * Worker process at master need to collect
 * active table disk usage from all the segments.
 * And aggregate the table size on each segment
 * to obtainer the real table size at cluster level.
 */
HTAB *
gp_fetch_active_tables(bool is_init)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i,
				j;
	char	   *sql;
	HTAB	   *local_table_stats_map = NULL;
	HASHCTL		ctl;
	HTAB	   *local_active_table_maps;
	StringInfoData buffer;
	StringInfoData map_string;

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
		local_active_table_maps = pull_active_list_from_seg();
		map_string = convert_map_to_string(local_active_table_maps);
		initStringInfo(&buffer);
		appendStringInfo(&buffer, "select * from diskquota.diskquota_fetch_table_stat(2, '%s'::oid[])",
						 map_string.data);
		sql = buffer.data;

		elog(DEBUG1, "CHECK SPI QUERY is %s", sql);

		CdbDispatchCommand(sql, DF_NONE, &cdb_pgresults);

		/* collect data from each segment */
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
						(errmsg("unexpected result from segment: %d",
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
					entry->tableoid = tableOid;
					entry->tablesize = tableSize;
				}
				else
				{
					entry->tablesize = entry->tablesize + tableSize;
				}

			}
		}
		cdbdisp_clearCdbPgResults(&cdb_pgresults);
	}
	return local_table_stats_map;
}


/*
 * Convert a hash map with oids into a string array
 * This function is used to prepare the second array parameter
 * of function diskquota_fetch_table_stat.
 */
static StringInfoData
convert_map_to_string(HTAB *active_list)
{
	HASH_SEQ_STATUS iter;
	StringInfoData buffer;
	DiskQuotaActiveTableEntry *entry;
	uint32		count = 0;
	uint32		nitems = hash_get_num_entries(active_list);

	initStringInfo(&buffer);
	appendStringInfo(&buffer, "{");
	elog(DEBUG1, "Try to convert size of active table is %ld", hash_get_num_entries(active_list));

	hash_seq_init(&iter, active_list);

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
 * Get active table list from all the segments.
 * Since when loading data, there is case where only subset for
 * segment doing the real loading. As a result, the same table
 * maybe active on some segemnts while not active on others. We
 * haven't store the table size for each segment on master(to save
 * memory), so when re-calcualte the table size, we need to sum the
 * table size on all of the segments.
 */
static HTAB *
pull_active_list_from_seg(void)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i,
				j;
	char	   *sql;
	HTAB	   *local_table_stats_map = NULL;
	HASHCTL		ctl;
	DiskQuotaActiveTableEntry *entry;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_table_stats_map = hash_create("local active table map with relfilenode info",
										1024,
										&ctl,
										HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);


	sql = "select * from diskquota.diskquota_fetch_table_stat(1, '{}'::oid[])";

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
					(errmsg("unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}

		for (j = 0; j < PQntuples(pgresult); j++)
		{
			tableOid = atooid(PQgetvalue(pgresult, j, 0));

			entry = (DiskQuotaActiveTableEntry *) hash_search(local_table_stats_map, &tableOid, HASH_ENTER, &found);

			if (!found)
			{
				entry->tableoid = tableOid;
				entry->tablesize = 0;
			}

		}
	}
	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	elog(DEBUG1, "The number of active table is %ld", hash_get_num_entries(local_table_stats_map));
	return local_table_stats_map;
}
