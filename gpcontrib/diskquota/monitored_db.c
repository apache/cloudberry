#include "diskquota.h"
#include "postgres.h"

#include "funcapi.h"
#include "port/atomics.h"
#include "commands/dbcommands.h"
#include "storage/proc.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(show_worker_epoch);
PG_FUNCTION_INFO_V1(db_status);
PG_FUNCTION_INFO_V1(wait_for_worker_new_epoch);

HTAB       *monitored_dbid_cache      = NULL; // Map<Oid, MonitorDBEntryStruct>
const char *MonitorDBStatusToString[] = {
#define DB_STATUS(id, str) str,
#include "diskquota_enum.h"
#undef DB_STATUS
};

static bool           check_for_timeout(TimestampTz start_time);
static MonitorDBEntry dump_monitored_dbid_cache(long *nitems);
// Returns the worker epoch for the current database.
// An epoch marks a new iteration of refreshing quota usage by a bgworker.
// An epoch is a 32-bit unsigned integer and there is NO invalid value.
// Therefore, the UDF must throw an error if something unexpected occurs.
Datum
show_worker_epoch(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT32(worker_get_epoch(MyDatabaseId));
}

Datum
db_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	struct StatusCtx
	{
		MonitorDBEntry entries;
		long           nitems;
		int            index;
	} * status_ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc     tupdesc;

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(5, false /*hasoid*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)1, "DBID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)2, "DATNAME", TEXTOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)3, "STATUS", TEXTOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)4, "EPOCH", INT8OID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)5, "PAUSED", BOOLOID, -1 /*typmod*/, 0 /*attdim*/);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		status_ctx = (struct StatusCtx *)palloc(sizeof(struct StatusCtx));

		/* Setup first calling context. */
		funcctx->user_fctx = (void *)status_ctx;
		LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
		status_ctx->nitems = hash_get_num_entries(monitored_dbid_cache);
		/*
		 * As we need acquire lock monitored_dbid_cache_lock to access
		 * monitored_dbid_cache hash table, but it's unsafe to acquire lock
		 * in the function, when the function fails the lock can not be
		 * released correctly. So dump the hash table into a array in the
		 * local memory. The hash table is small, it doesn't consume much
		 * memory.
		 */
		status_ctx->entries = dump_monitored_dbid_cache(&status_ctx->nitems);
		status_ctx->index   = 0;
		LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx    = SRF_PERCALL_SETUP();
	status_ctx = (struct StatusCtx *)funcctx->user_fctx;

	while (status_ctx->index < status_ctx->nitems)
	{
		MonitorDBEntry entry = &status_ctx->entries[status_ctx->index];
		status_ctx->index++;
		Datum     result;
		Datum     values[5];
		bool      nulls[5];
		HeapTuple tuple;

		values[0]  = ObjectIdGetDatum(entry->dbid);
		values[1]  = CStringGetTextDatum(get_database_name(entry->dbid));
		int status = Int32GetDatum(pg_atomic_read_u32(&(entry->status)));
		status     = status >= DB_STATUS_MAX ? DB_STATUS_UNKNOWN : status;
		values[2]  = CStringGetTextDatum(MonitorDBStatusToString[status]);
		values[3]  = UInt32GetDatum(pg_atomic_read_u32(&(entry->epoch)));
		values[4]  = BoolGetDatum(entry->paused);

		memset(nulls, false, sizeof(nulls));
		tuple  = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	pfree(status_ctx->entries);
	SRF_RETURN_DONE(funcctx);
}

// Checks if the bgworker for the current database works as expected.
// 1. If it returns successfully in `diskquota.naptime`, the bgworker works as expected.
// 2. If it does not terminate, there must be some issues with the bgworker.
//    In this case, we must ensure this UDF can be interrupted by the user.
Datum
wait_for_worker_new_epoch(PG_FUNCTION_ARGS)
{
	TimestampTz start_time    = GetCurrentTimestamp();
	uint32      current_epoch = worker_get_epoch(MyDatabaseId);
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (check_for_timeout(start_time)) start_time = GetCurrentTimestamp();
		uint32 new_epoch = worker_get_epoch(MyDatabaseId);
		/* Unsigned integer underflow is OK */
		if (new_epoch - current_epoch >= 2u)
		{
			PG_RETURN_BOOL(true);
		}
		/* Sleep for naptime to reduce CPU usage */
		(void)WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT, diskquota_naptime ? diskquota_naptime : 1);
		ResetLatch(&MyProc->procLatch);
	}
	PG_RETURN_BOOL(false);
}

bool
diskquota_is_paused()
{
	Assert(MyDatabaseId != InvalidOid);
	bool           paused = false;
	bool           found;
	MonitorDBEntry entry;

	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	entry = hash_search(monitored_dbid_cache, &MyDatabaseId, HASH_FIND, &found);
	if (found)
	{
		paused = entry->paused;
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
	return paused;
}

bool
diskquota_is_readiness_logged()
{
	Assert(MyDatabaseId != InvalidOid);
	bool is_readiness_logged;

	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	{
		MonitorDBEntry hash_entry;
		bool           found;

		hash_entry = (MonitorDBEntry)hash_search(monitored_dbid_cache, (void *)&MyDatabaseId, HASH_FIND, &found);
		is_readiness_logged = found ? hash_entry->is_readiness_logged : false;
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);

	return is_readiness_logged;
}

void
diskquota_set_readiness_logged()
{
	Assert(MyDatabaseId != InvalidOid);

	/*
	 * We actually need ROW EXCLUSIVE lock here. Given that the current worker
	 * is the the only process that modifies the entry, it is safe to only take
	 * the shared lock.
	 */
	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	{
		MonitorDBEntry hash_entry;
		bool           found;

		hash_entry = (MonitorDBEntry)hash_search(monitored_dbid_cache, (void *)&MyDatabaseId, HASH_FIND, &found);
		hash_entry->is_readiness_logged = true;
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
}

bool
worker_increase_epoch(Oid dbid)
{
	bool           found = false;
	MonitorDBEntry entry;
	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	entry = hash_search(monitored_dbid_cache, &dbid, HASH_FIND, &found);

	if (found)
	{
		pg_atomic_fetch_add_u32(&(entry->epoch), 1);
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
	return found;
}

uint32
worker_get_epoch(Oid dbid)
{
	bool           found = false;
	uint32         epoch = 0;
	MonitorDBEntry entry;
	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	entry = hash_search(monitored_dbid_cache, &dbid, HASH_FIND, &found);
	if (found)
	{
		epoch = pg_atomic_read_u32(&(entry->epoch));
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
	if (!found)
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
		                  errmsg("[diskquota] database \"%s\" not found", get_database_name(dbid))));
	}
	return epoch;
}

/*
 * Function to update the db list on each segment
 * Will print a WARNING to log if out of memory
 */
void
update_monitor_db(Oid dbid, FetchTableStatType action)
{
	bool found = false;

	// add/remove the dbid to monitoring database cache to filter out table not under
	// monitoring in hook functions

	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_EXCLUSIVE);
	if (action == ADD_DB_TO_MONITOR)
	{
		MonitorDBEntry entry = hash_search(monitored_dbid_cache, &dbid, HASH_ENTER_NULL, &found);
		if (entry == NULL)
		{
			ereport(WARNING, (errmsg("can't alloc memory on dbid cache, there ary too many databases to monitor")));
		}
		entry->paused = false;
		pg_atomic_init_u32(&(entry->epoch), 0);
		pg_atomic_init_u32(&(entry->status), DB_INIT);
	}
	else if (action == REMOVE_DB_FROM_BEING_MONITORED)
	{
		hash_search(monitored_dbid_cache, &dbid, HASH_REMOVE, &found);
	}
	else if (action == PAUSE_DB_TO_MONITOR)
	{
		MonitorDBEntry entry = hash_search(monitored_dbid_cache, &dbid, HASH_FIND, &found);
		if (found)
		{
			entry->paused = true;
		}
	}
	else if (action == RESUME_DB_TO_MONITOR)
	{
		MonitorDBEntry entry = hash_search(monitored_dbid_cache, &dbid, HASH_FIND, &found);

		if (found)
		{
			entry->paused = false;
		}
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
}

void
update_monitordb_status(Oid dbid, uint32 status)
{
	MonitorDBEntry entry;
	bool           found;
	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	{
		entry = hash_search(monitored_dbid_cache, &dbid, HASH_FIND, &found);
	}
	if (found)
	{
		Assert(status < DB_STATUS_MAX);
		pg_atomic_write_u32(&(entry->status), status);
	}
	else
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] database %u not found", dbid)));
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);
}

static bool
check_for_timeout(TimestampTz start_time)
{
	long diff_secs  = 0;
	int  diff_usecs = 0;
	TimestampDifference(start_time, GetCurrentTimestamp(), &diff_secs, &diff_usecs);
	if (diff_secs >= 60)
	{
		ereport(NOTICE, (errmsg("[diskquota] timeout when waiting for worker"),
		                 errhint("please check if the bgworker is still alive.")));
		return true;
	}
	return false;
}

static MonitorDBEntry
dump_monitored_dbid_cache(long *nitems)
{
	HASH_SEQ_STATUS seq;
	MonitorDBEntry  curEntry;
	int             count = *nitems = hash_get_num_entries(monitored_dbid_cache);
	MonitorDBEntry  entries = curEntry = (MonitorDBEntry)palloc(sizeof(struct MonitorDBEntryStruct) * count);

	hash_seq_init(&seq, monitored_dbid_cache);
	MonitorDBEntry entry;
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		Assert(count > 0);
		memcpy(curEntry, entry, sizeof(struct MonitorDBEntryStruct));
		curEntry++;
		count--;
	}
	Assert(count == 0);
	return entries;
}
