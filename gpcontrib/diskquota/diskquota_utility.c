/* -------------------------------------------------------------------------
 *
 * diskquota_utility.c
 *
 * Diskquota utility contains some help functions for diskquota.
 * set_schema_quota and set_role_quota is used by user to set quota limit.
 * init_table_size_table is used to initialize table 'diskquota.table_size'
 * diskquota_start_worker is used when 'create extension' DDL. It will start
 * the corresponding worker process immediately.
 *
 * Copyright (c) 2018-2020 Pivotal Software, Inc.
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates
 *
 * IDENTIFICATION
 *		diskquota/diskquota_utility.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/aomd.h"
#include "access/xact.h"
#include "access/heapam.h"
#if GP_VERSION_NUM >= 70000
#include "access/genam.h"
#include "common/hashfn.h"
#endif /* GP_VERSION_NUM */
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/numeric.h"
#include "libpq-fe.h"
#include "funcapi.h"

#include <cdb/cdbvars.h>
#include <cdb/cdbdisp_query.h>
#include <cdb/cdbdispatchresult.h>

#include "diskquota.h"
#include "gp_activetable.h"

/* disk quota helper function */

PG_FUNCTION_INFO_V1(init_table_size_table);
PG_FUNCTION_INFO_V1(diskquota_start_worker);
PG_FUNCTION_INFO_V1(diskquota_pause);
PG_FUNCTION_INFO_V1(diskquota_resume);
PG_FUNCTION_INFO_V1(set_schema_quota);
PG_FUNCTION_INFO_V1(set_role_quota);
PG_FUNCTION_INFO_V1(set_schema_tablespace_quota);
PG_FUNCTION_INFO_V1(set_role_tablespace_quota);
PG_FUNCTION_INFO_V1(set_per_segment_quota);
PG_FUNCTION_INFO_V1(relation_size_local);
PG_FUNCTION_INFO_V1(pull_all_table_size);

/* timeout count to wait response from launcher process, in 1/10 sec */
#define WAIT_TIME_COUNT 1200
/*
 * three types values for "quota" column in "quota_config" table:
 * 1) more than 0: valid value
 * 2) 0: meaningless value, rejected by diskquota UDF
 * 3) less than 0: to delete the quota config in the table
 *
 * the values for segratio column are the same as quota column
 *
 * In quota_config table,
 * 1) when quota type is "TABLESPACE_QUOTA",
 *    the quota column value is always INVALID_QUOTA
 * 2) when quota type is "NAMESPACE_TABLESPACE_QUOTA" or "ROLE_TABLESPACE_QUOTA"
 *    and no segratio configed for the tablespace, the segratio value is
 *    INVALID_SEGRATIO.
 * 3) when quota type is "NAMESPACE_QUOTA" or "ROLE_QUOTA", the segratio is
 *    always INVALID_SEGRATIO.
 */
#define INVALID_SEGRATIO 0.0
#define INVALID_QUOTA 0

#define report_ddl_err(ddl_msg, prefix)                                                      \
	do                                                                                       \
	{                                                                                        \
		MessageResult ddl_result_ = (MessageResult)ddl_msg->result;                          \
		const char   *ddl_err_;                                                              \
		const char   *ddl_hint_;                                                             \
		ddl_err_code_to_err_message(ddl_result_, &ddl_err_, &ddl_hint_);                     \
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s: %s", prefix, ddl_err_), \
		                ddl_hint_ ? errhint("%s", ddl_hint_) : 0));                          \
	} while (0)

static bool  is_database_empty(void);
static void  ddl_err_code_to_err_message(MessageResult code, const char **err_msg, const char **hint_msg);
static int64 get_size_in_mb(char *str);
static void set_quota_config_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type, float4 segratio, Oid spcoid);
static int  set_target_internal(Oid primaryoid, Oid spcoid, int64 quota_limit_mb, QuotaType type);
static float4 get_per_segment_ratio(Oid spcoid);
static bool   to_delete_quota(QuotaType type, int64 quota_limit_mb, float4 segratio);
static void   check_role(Oid roleoid, char *rolname, int64 quota_limit_mb);

List *get_rel_oid_list(void);

/* ---- Help Functions to set quota limit. ---- */
/*
 * Initialize table diskquota.table_size.
 * calculate table size by UDF pg_table_size
 * This function is called by user, errors should not
 * be catch, and should be sent back to user
 */
Datum
init_table_size_table(PG_FUNCTION_ARGS)
{
	int ret;

	RangeVar *rv;
	Relation  rel;
	/*
	 * If error happens in init_table_size_table, just return error messages
	 * to the client side. So there is no need to catch the error.
	 */

	/* ensure table diskquota.state exists */
	rv  = makeRangeVar("diskquota", "state", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		elog(ERROR,
		     "table \"diskquota.state\" is missing in database \"%s\","
		     " please recreate diskquota extension",
		     get_database_name(MyDatabaseId));
	}
	heap_close(rel, NoLock);

	/*
	 * Why don't use insert into diskquota.table_size select from pg_table_size here?
	 *
	 * insert into foo select oid, pg_table_size(oid), -1 from pg_class where
	 * oid >= 16384 and (relkind='r' or relkind='m');
	 * ERROR:  This query is not currently supported by GPDB.  (entry db 127.0.0.1:6000 pid=61114)
	 *
	 * Some functions are peculiar in that they do their own dispatching.
	 * Such as pg_table_size.
	 * They do not work on entry db since we do not support dispatching
	 * from entry-db currently.
	 */
	SPI_connect();

	/* delete all the table size info in table_size if exist. */
	ret = SPI_execute("truncate table diskquota.table_size", false, 0);
	if (ret != SPI_OK_UTILITY) elog(ERROR, "cannot truncate table_size table: error code %d", ret);

	ret = SPI_execute(
	        "INSERT INTO "
	        "  diskquota.table_size "
	        "WITH all_size AS "
	        "  ("
	        "    SELECT diskquota.pull_all_table_size() AS a FROM gp_dist_random('gp_id')"
	        "  ) "
	        "SELECT (a).* FROM all_size",
	        false, 0);
	if (ret != SPI_OK_INSERT) elog(ERROR, "cannot insert into table_size table: error code %d", ret);

	/* size is the sum of size on master and on all segments when segid == -1. */
	ret = SPI_execute(
	        "INSERT INTO "
	        "  diskquota.table_size "
	        "WITH total_size AS "
	        "  ("
	        "    SELECT * from diskquota.pull_all_table_size()"
	        "    UNION ALL "
	        "    SELECT tableid, size, segid FROM diskquota.table_size"
	        "  ) "
	        "SELECT tableid, sum(size) as size, -1 as segid FROM total_size GROUP BY tableid;",
	        false, 0);
	if (ret != SPI_OK_INSERT) elog(ERROR, "cannot insert into table_size table: error code %d", ret);

	/* set diskquota state to ready. */
	ret = SPI_execute_with_args("update diskquota.state set state = $1", 1,
	                            (Oid[]){
	                                    INT4OID,
	                            },
	                            (Datum[]){
	                                    Int32GetDatum(DISKQUOTA_READY_STATE),
	                            },
	                            NULL, false, 0);
	if (ret != SPI_OK_UPDATE) elog(ERROR, "cannot update state table: error code %d", ret);

	SPI_finish();
	PG_RETURN_VOID();
}

static HTAB *
calculate_all_table_size()
{
	Relation  classRel;
	HeapTuple tuple;
#if GP_VERSION_NUM < 70000
	HeapScanDesc relScan;
#else
	TableScanDesc relScan;
#endif /* GP_VERSION_NUM */
	Oid                        relid;
	Oid                        prelid;
	Size                       tablesize;
	RelFileNodeBackend         rnode;
	TableEntryKey              keyitem;
	HTAB                      *local_table_size_map;
	HASHCTL                    hashctl;
	DiskQuotaActiveTableEntry *entry;
	bool                       found;
	char                       relstorage;

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize   = sizeof(TableEntryKey);
	hashctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	hashctl.hcxt      = CurrentMemoryContext;

	local_table_size_map =
	        diskquota_hash_create("local_table_size_map", 1024, &hashctl, HASH_ELEM | HASH_CONTEXT, DISKQUOTA_TAG_HASH);
	classRel = heap_open(RelationRelationId, AccessShareLock);
#if GP_VERSION_NUM < 70000
	relScan = heap_beginscan_catalog(classRel, 0, NULL);
#else
	relScan = table_beginscan_catalog(classRel, 0, NULL);
#endif /* GP_VERSION_NUM */

	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);
		if (classForm->relkind != RELKIND_RELATION && classForm->relkind != RELKIND_MATVIEW &&
		    classForm->relkind != RELKIND_INDEX && classForm->relkind != RELKIND_AOSEGMENTS &&
		    classForm->relkind != RELKIND_AOBLOCKDIR && classForm->relkind != RELKIND_AOVISIMAP &&
		    classForm->relkind != RELKIND_TOASTVALUE)
			continue;

#if GP_VERSION_NUM < 70000
		relid = HeapTupleGetOid(tuple);
#else
		relid = classForm->oid;
#endif /* GP_VERSION_NUM */
		/* ignore system table */
		if (relid < FirstNormalObjectId) continue;

		rnode.node.dbNode  = MyDatabaseId;
		rnode.node.relNode = classForm->relfilenode;
		rnode.node.spcNode = OidIsValid(classForm->reltablespace) ? classForm->reltablespace : MyDatabaseTableSpace;
		rnode.backend      = classForm->relpersistence == RELPERSISTENCE_TEMP ? TempRelBackendId : InvalidBackendId;
		relstorage         = DiskquotaGetRelstorage(classForm);

		tablesize = calculate_relation_size_all_forks(&rnode, relstorage, classForm->relam);

		keyitem.reloid = relid;
		keyitem.segid  = GpIdentity.segindex;

		prelid = diskquota_parse_primary_table_oid(classForm->relnamespace, classForm->relname.data);
		if (OidIsValid(prelid))
		{
			keyitem.reloid = prelid;
		}

		entry = hash_search(local_table_size_map, &keyitem, HASH_ENTER, &found);
		if (!found)
		{
			entry->tablesize = 0;
		}
		entry->tablesize += tablesize;
	}
	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	return local_table_size_map;
}

Datum
pull_all_table_size(PG_FUNCTION_ARGS)
{
	DiskQuotaActiveTableEntry *entry;
	FuncCallContext           *funcctx;
	struct PullAllTableSizeCtx
	{
		HASH_SEQ_STATUS iter;
		HTAB           *local_table_size_map;
	} * table_size_ctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc     tupdesc;
		MemoryContext oldcontext;

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		tupdesc    = DiskquotaCreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber)1, "TABLEID", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)2, "SIZE", INT8OID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)3, "SEGID", INT2OID, -1 /*typmod*/, 0 /*attdim*/);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Create a local hash table and fill it with entries from shared memory. */
		table_size_ctx                       = (struct PullAllTableSizeCtx *)palloc(sizeof(struct PullAllTableSizeCtx));
		table_size_ctx->local_table_size_map = calculate_all_table_size();

		/* Setup first calling context. */
		hash_seq_init(&(table_size_ctx->iter), table_size_ctx->local_table_size_map);
		funcctx->user_fctx = (void *)table_size_ctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx        = SRF_PERCALL_SETUP();
	table_size_ctx = (struct PullAllTableSizeCtx *)funcctx->user_fctx;

	while ((entry = hash_seq_search(&(table_size_ctx->iter))) != NULL)
	{
		Datum     result;
		Datum     values[3];
		bool      nulls[3];
		HeapTuple tuple;

		values[0] = ObjectIdGetDatum(entry->reloid);
		values[1] = Int64GetDatum(entry->tablesize);
		values[2] = Int16GetDatum(entry->segid);

		memset(nulls, false, sizeof(nulls));
		tuple  = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
/*
 * Trigger to start diskquota worker when create extension diskquota.
 * This function is called at backend side, and will send message to
 * diskquota launcher. Launcher process is responsible for starting the real
 * diskquota worker process.
 */
Datum
diskquota_start_worker(PG_FUNCTION_ARGS)
{
	int rc, launcher_pid;

	/*
	 * Lock on extension_ddl_lock to avoid multiple backend create diskquota
	 * extension at the same time.
	 */
	LWLockAcquire(diskquota_locks.extension_ddl_lock, LW_EXCLUSIVE);
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	extension_ddl_message->req_pid = MyProcPid;
	extension_ddl_message->cmd     = CMD_CREATE_EXTENSION;
	extension_ddl_message->result  = ERR_PENDING;
	extension_ddl_message->dbid    = MyDatabaseId;
	launcher_pid                   = extension_ddl_message->launcher_pid;
	/* setup sig handler to diskquota launcher process */
	rc = kill(launcher_pid, SIGUSR2);
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	if (rc == 0)
	{
		int count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			CHECK_FOR_INTERRUPTS();
			rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 100L);
			if (rc & WL_POSTMASTER_DEATH) break;
			ResetLatch(&MyProc->procLatch);

			ereportif(kill(launcher_pid, 0) == -1 && errno == ESRCH, // do existence check
			          ERROR,
			          (errmsg("[diskquota] diskquota launcher pid = %d no longer exists", launcher_pid),
			           errhint("The diskquota launcher process has been terminated for some reasons. Consider to "
			                   "restart the cluster to start it.")));

			LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
			if (extension_ddl_message->result != ERR_PENDING)
			{
				LWLockRelease(diskquota_locks.extension_ddl_message_lock);
				break;
			}
			LWLockRelease(diskquota_locks.extension_ddl_message_lock);
		}
	}
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
	if (extension_ddl_message->result != ERR_OK)
	{
		LWLockRelease(diskquota_locks.extension_ddl_message_lock);
		LWLockRelease(diskquota_locks.extension_ddl_lock);
		report_ddl_err(extension_ddl_message, "[diskquota] failed to create diskquota extension");
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_ddl_lock);

	/* notify DBA to run init_table_size_table() when db is not empty */
	if (!is_database_empty())
	{
		ereport(WARNING, (errmsg("[diskquota] diskquota is not ready because current database is not empty"),
		                  errhint("please run 'SELECT diskquota.init_table_size_table();' to initialize diskquota")));
	}
	PG_RETURN_VOID();
}

/*
 * this function is called by user.
 * pause diskquota in current or specific database.
 * After this function being called, diskquota doesn't emit an error when the disk usage limit is exceeded.
 */
Datum
diskquota_pause(PG_FUNCTION_ARGS)
{
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to pause diskquota")));
	}

	Oid dbid = MyDatabaseId;
	if (PG_NARGS() == 1)
	{
		dbid = PG_GETARG_OID(0);
	}
	if (IS_QUERY_DISPATCHER())
	{
		// pause current worker
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		update_monitor_db_mpp(dbid, PAUSE_DB_TO_MONITOR, EXTENSION_SCHEMA);
		SPI_finish();
	}
	PG_RETURN_VOID();
}

/*
 * this function is called by user.
 * active diskquota in current or specific database
 */
Datum
diskquota_resume(PG_FUNCTION_ARGS)
{
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to resume diskquota")));
	}

	Oid dbid = MyDatabaseId;
	if (PG_NARGS() == 1)
	{
		dbid = PG_GETARG_OID(0);
	}

	// active current worker
	if (IS_QUERY_DISPATCHER())
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		update_monitor_db_mpp(dbid, RESUME_DB_TO_MONITOR, EXTENSION_SCHEMA);
		SPI_finish();
	}

	PG_RETURN_VOID();
}

/*
 * Check whether database is empty (no user table created)
 */
static bool
is_database_empty(void)
{
	int       ret;
	TupleDesc tupdesc;
	bool      is_empty = false;

	/*
	 * If error happens in is_database_empty, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */
	SPI_connect();

	ret = SPI_execute(
	        "SELECT (count(relname) = 0) "
	        "FROM "
	        "  pg_class AS c, "
	        "  pg_namespace AS n "
	        "WHERE c.oid > 16384 and relnamespace = n.oid and nspname != 'diskquota'"
	        " and relkind not in ('v', 'c', 'f')",
	        true, 0);
	if (ret != SPI_OK_SELECT)
	{
		int saved_errno = errno;
		elog(ERROR, "cannot select pg_class and pg_namespace table, reason: %s.", strerror(saved_errno));
	}

	tupdesc = SPI_tuptable->tupdesc;
	/* check sql return value whether database is empty */
	if (SPI_processed > 0)
	{
		HeapTuple tup = SPI_tuptable->vals[0];
		Datum     dat;
		bool      isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (!isnull)
		{
			/* check whether condition `count(relname) = 0` is true */
			is_empty = DatumGetBool(dat);
		}
	}

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	return is_empty;
}

void
diskquota_stop_worker(void)
{
	int rc, launcher_pid;

	if (!IS_QUERY_DISPATCHER())
	{
		return;
	}

	/*
	 * Lock on extension_ddl_lock to avoid multiple backend create diskquota
	 * extension at the same time.
	 */
	LWLockAcquire(diskquota_locks.extension_ddl_lock, LW_EXCLUSIVE);
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	extension_ddl_message->req_pid = MyProcPid;
	extension_ddl_message->cmd     = CMD_DROP_EXTENSION;
	extension_ddl_message->result  = ERR_PENDING;
	extension_ddl_message->dbid    = MyDatabaseId;
	launcher_pid                   = extension_ddl_message->launcher_pid;
	rc                             = kill(launcher_pid, SIGUSR2);
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	if (rc == 0)
	{
		int count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			CHECK_FOR_INTERRUPTS();
			rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 100L);
			if (rc & WL_POSTMASTER_DEATH) break;
			ResetLatch(&MyProc->procLatch);

			ereportif(kill(launcher_pid, 0) == -1 && errno == ESRCH, // do existence check
			          ERROR,
			          (errmsg("[diskquota] diskquota launcher pid = %d no longer exists", launcher_pid),
			           errhint("The diskquota launcher process has been terminated for some reasons. Consider to "
			                   "restart the cluster to start it.")));

			LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
			if (extension_ddl_message->result != ERR_PENDING)
			{
				LWLockRelease(diskquota_locks.extension_ddl_message_lock);
				break;
			}
			LWLockRelease(diskquota_locks.extension_ddl_message_lock);
		}
	}
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
	if (extension_ddl_message->result != ERR_OK)
	{
		LWLockRelease(diskquota_locks.extension_ddl_message_lock);
		LWLockRelease(diskquota_locks.extension_ddl_lock);
		report_ddl_err(extension_ddl_message, "[diskquota] failed to drop diskquota extension");
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_ddl_lock);
}

/*
 * For extension DDL('create extension/drop extension')
 * Using this function to convert error code from diskquota
 * launcher to error message and return it to client.
 */
static void
ddl_err_code_to_err_message(MessageResult code, const char **err_msg, const char **hint_msg)
{
	*hint_msg = NULL;
	switch (code)
	{
		case ERR_PENDING:
			*err_msg  = "no response from diskquota launcher, check whether launcher process exists";
			*hint_msg = "Create \"diskquota\" database and restart the cluster.";
			break;
		case ERR_OK:
			*err_msg = "succeeded";
			break;
		case ERR_EXCEED:
			*err_msg = "too many databases to monitor";
			break;
		case ERR_ADD_TO_DB:
			*err_msg = "add dbid to database_list failed";
			break;
		case ERR_DEL_FROM_DB:
			*err_msg = "delete dbid from database_list failed";
			break;
		case ERR_START_WORKER:
			*err_msg = "start diskquota worker failed";
			break;
		case ERR_INVALID_DBID:
			*err_msg = "invalid dbid";
			break;
		default:
			*err_msg = "unknown error";
			break;
	}
}

static Datum
__get_oid_auto_case_convert(Oid (*f)(const char *name, bool missing_ok), const char *name)
{
	char *b   = NULL;
	int   l   = strlen(name);
	Oid   ret = InvalidOid;

	if (l > 2 && name[0] == '"' && name[l - 1] == '"')
	{
		// object name wrapped by '"'. eg: "foo"
		b = palloc(l);
		StrNCpy(b, name + 1, l - 1); // trim the '"'. unlike strncpy, StrNCpy will ensure b[l-1] = '\0'
	}
	else
	{
		// lower the object name if not wrapped by '"'
		b = str_tolower(name, strlen(name), DEFAULT_COLLATION_OID);
	}

	ret = f(b, false);

	pfree(b);
	return ret;
}

/*
 * Set disk quota limit for role.
 */
Datum
set_role_quota(PG_FUNCTION_ARGS)
{
	Oid   roleoid;
	char *rolname;
	char *sizestr;
	int64 quota_limit_mb;

	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set disk quota limit")));
	}

	rolname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	roleoid = __get_oid_auto_case_convert(get_role_oid, rolname);

	sizestr        = text_to_cstring(PG_GETARG_TEXT_PP(1));
	sizestr        = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);

	if (quota_limit_mb == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("disk quota can not be set to 0 MB")));
	}
	check_role(roleoid, rolname, quota_limit_mb);

	SPI_connect();
	set_quota_config_internal(roleoid, quota_limit_mb, ROLE_QUOTA, INVALID_SEGRATIO, InvalidOid);
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * Set disk quota limit for schema.
 */
Datum
set_schema_quota(PG_FUNCTION_ARGS)
{
	Oid   namespaceoid;
	char *nspname;
	char *sizestr;
	int64 quota_limit_mb;

	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set disk quota limit")));
	}

	nspname      = text_to_cstring(PG_GETARG_TEXT_PP(0));
	namespaceoid = __get_oid_auto_case_convert(get_namespace_oid, nspname);

	sizestr        = text_to_cstring(PG_GETARG_TEXT_PP(1));
	sizestr        = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);

	if (quota_limit_mb == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("disk quota can not be set to 0 MB")));
	}
	SPI_connect();
	set_quota_config_internal(namespaceoid, quota_limit_mb, NAMESPACE_QUOTA, INVALID_SEGRATIO, InvalidOid);
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * Set disk quota limit for tablepace role.
 */
Datum
set_role_tablespace_quota(PG_FUNCTION_ARGS)
{
	/*
	 * Write the quota limit info into target and quota_config table under
	 * 'diskquota' schema of the current database.
	 */
	Oid   spcoid;
	char *spcname;
	Oid   roleoid;
	char *rolname;
	char *sizestr;
	int64 quota_limit_mb;
	int   row_id;

	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set disk quota limit")));
	}

	rolname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	roleoid = __get_oid_auto_case_convert(get_role_oid, rolname);

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	spcoid  = __get_oid_auto_case_convert(get_tablespace_oid, spcname);

	sizestr        = text_to_cstring(PG_GETARG_TEXT_PP(2));
	sizestr        = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);

	if (quota_limit_mb == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("disk quota can not be set to 0 MB")));
	}
	check_role(roleoid, rolname, quota_limit_mb);

	SPI_connect();
	row_id = set_target_internal(roleoid, spcoid, quota_limit_mb, ROLE_TABLESPACE_QUOTA);
	set_quota_config_internal(row_id, quota_limit_mb, ROLE_TABLESPACE_QUOTA, INVALID_SEGRATIO, spcoid);
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * Set disk quota limit for tablepace schema.
 */
Datum
set_schema_tablespace_quota(PG_FUNCTION_ARGS)
{
	/*
	 * Write the quota limit info into target and quota_config table under
	 * 'diskquota' schema of the current database.
	 */
	Oid   spcoid;
	char *spcname;
	Oid   namespaceoid;
	char *nspname;
	char *sizestr;
	int64 quota_limit_mb;
	int   row_id;

	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set disk quota limit")));
	}

	nspname      = text_to_cstring(PG_GETARG_TEXT_PP(0));
	namespaceoid = __get_oid_auto_case_convert(get_namespace_oid, nspname);

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	spcoid  = __get_oid_auto_case_convert(get_tablespace_oid, spcname);

	sizestr        = text_to_cstring(PG_GETARG_TEXT_PP(2));
	sizestr        = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);
	if (quota_limit_mb == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("disk quota can not be set to 0 MB")));
	}

	SPI_connect();
	row_id = set_target_internal(namespaceoid, spcoid, quota_limit_mb, NAMESPACE_TABLESPACE_QUOTA);
	set_quota_config_internal(row_id, quota_limit_mb, NAMESPACE_TABLESPACE_QUOTA, INVALID_SEGRATIO, spcoid);
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * set_quota_config_intenral - insert/update/delete quota_config table
 *
 * If the segratio is valid, query the segratio from
 * the table "quota_config" by spcoid.
 *
 * DELETE doesn't need the segratio
 */
static void
set_quota_config_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type, float4 segratio, Oid spcoid)
{
	int ret;

	/* Report error if diskquota is not ready. */
	do_check_diskquota_state_is_ready();

	/*
	 * If error happens in set_quota_config_internal, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */

	ret = SPI_execute_with_args("select true from diskquota.quota_config where targetoid = $1 and quotatype = $2", 2,
	                            (Oid[]){
	                                    OIDOID,
	                                    INT4OID,
	                            },
	                            (Datum[]){
	                                    ObjectIdGetDatum(targetoid),
	                                    Int32GetDatum(type),
	                            },
	                            NULL, true, 0);
	if (ret != SPI_OK_SELECT) elog(ERROR, "cannot select quota setting table: error code %d", ret);

	if (to_delete_quota(type, quota_limit_mb, segratio))
	{
		if (SPI_processed > 0)
		{
			ret = SPI_execute_with_args("delete from diskquota.quota_config where targetoid = $1 and quotatype = $2", 2,
			                            (Oid[]){
			                                    OIDOID,
			                                    INT4OID,
			                            },
			                            (Datum[]){
			                                    ObjectIdGetDatum(targetoid),
			                                    Int32GetDatum(type),
			                            },
			                            NULL, false, 0);
			if (ret != SPI_OK_DELETE) elog(ERROR, "cannot delete item from quota setting table, error code %d", ret);
		}
		// else do nothing
	}
	// to upsert quota_config
	else
	{
		if (SPI_processed == 0)
		{
			if (segratio == INVALID_SEGRATIO && !(type == ROLE_QUOTA || type == NAMESPACE_QUOTA))
				segratio = get_per_segment_ratio(spcoid);
			ret = SPI_execute_with_args("insert into diskquota.quota_config values($1, $2, $3, $4)", 4,
			                            (Oid[]){
			                                    OIDOID,
			                                    INT4OID,
			                                    INT8OID,
			                                    FLOAT4OID,
			                            },
			                            (Datum[]){
			                                    ObjectIdGetDatum(targetoid),
			                                    Int32GetDatum(type),
			                                    Int64GetDatum(quota_limit_mb),
			                                    Float4GetDatum(segratio),
			                            },
			                            NULL, false, 0);
			if (ret != SPI_OK_INSERT) elog(ERROR, "cannot insert into quota setting table, error code %d", ret);
		}
		else
		{
			// no need to update segratio
			if (segratio == INVALID_SEGRATIO)
			{
				ret = SPI_execute_with_args(
				        "update diskquota.quota_config set quotalimitMB = $1 where targetoid= $2 and quotatype = $3", 3,
				        (Oid[]){
				                INT8OID,
				                OIDOID,
				                INT4OID,
				        },
				        (Datum[]){
				                Int64GetDatum(quota_limit_mb),
				                ObjectIdGetDatum(targetoid),
				                Int32GetDatum(type),
				        },
				        NULL, false, 0);
			}
			else
			{
				ret = SPI_execute_with_args(
				        "update diskquota.quota_config set quotalimitMb = $1, segratio = $2 where targetoid= $3 and "
				        "quotatype = $4",
				        4,
				        (Oid[]){
				                INT8OID,
				                FLOAT4OID,
				                OIDOID,
				                INT4OID,
				        },
				        (Datum[]){
				                Int64GetDatum(quota_limit_mb),
				                Float4GetDatum(segratio),
				                ObjectIdGetDatum(targetoid),
				                Int32GetDatum(type),
				        },
				        NULL, false, 0);
			}
			if (ret != SPI_OK_UPDATE) elog(ERROR, "cannot update quota setting table, error code %d", ret);
		}
	}

	return;
}

static int
set_target_internal(Oid primaryoid, Oid spcoid, int64 quota_limit_mb, QuotaType type)
{
	int   ret;
	int   row_id  = -1;
	bool  is_null = false;
	Datum v;

	/*
	 * If error happens in set_target_internal, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */

	ret = SPI_execute_with_args(
	        "select t.rowId from diskquota.quota_config as q, diskquota.target as t"
	        " where t.primaryOid = $1"
	        " and t.tablespaceOid = $2"
	        " and t.quotaType = $3"
	        " and t.quotaType = q.quotaType"
	        " and t.rowId = q.targetOid",
	        3,
	        (Oid[]){
	                OIDOID,
	                OIDOID,
	                INT4OID,
	        },
	        (Datum[]){
	                ObjectIdGetDatum(primaryoid),
	                ObjectIdGetDatum(spcoid),
	                Int32GetDatum(type),
	        },
	        NULL, true, 0);
	if (ret != SPI_OK_SELECT) elog(ERROR, "cannot select target setting table: error code %d", ret);

	if (SPI_processed > 0)
	{
		is_null = false;
		v       = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
		Assert(is_null == false);
		row_id = DatumGetInt32(v);
	}

	/* if the schema or role's quota has not been set before */
	if (SPI_processed == 0 && quota_limit_mb > 0)
	{
		ret = SPI_execute_with_args(
		        "insert into diskquota.target (quotatype, primaryOid, tablespaceOid) values($1, $2, $3) returning "
		        "rowId",
		        3,
		        (Oid[]){
		                INT4OID,
		                OIDOID,
		                OIDOID,
		        },
		        (Datum[]){
		                Int32GetDatum(type),
		                ObjectIdGetDatum(primaryoid),
		                ObjectIdGetDatum(spcoid),
		        },
		        NULL, false, 0);
		if (ret != SPI_OK_INSERT_RETURNING) elog(ERROR, "cannot insert into quota setting table, error code %d", ret);

		is_null = false;
		v       = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
		Assert(is_null == false);
		row_id = DatumGetInt32(v);
	}
	else if (SPI_processed > 0 && quota_limit_mb < 0)
	{
		ret = SPI_execute_with_args(
		        "delete from diskquota.target where primaryOid = $1 and tablespaceOid = $2 returning rowId", 2,
		        (Oid[]){
		                OIDOID,
		                OIDOID,
		        },
		        (Datum[]){
		                ObjectIdGetDatum(primaryoid),
		                ObjectIdGetDatum(spcoid),
		        },
		        NULL, false, 0);
		if (ret != SPI_OK_DELETE_RETURNING)
			elog(ERROR, "cannot delete item from target setting table, error code %d", ret);

		is_null = false;
		v       = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
		Assert(is_null == false);
		row_id = DatumGetInt32(v);
	}
	/* No need to update the target table */

	return row_id;
}

/*
 * Convert a human-readable size to a size in MB.
 */
static int64
get_size_in_mb(char *str)
{
	char   *strptr, *endptr;
	char    saved_char;
	Numeric num;
	int64   result;
	bool    have_digits = false;

	/* Skip leading whitespace */
	strptr = str;
	while (isspace((unsigned char)*strptr)) strptr++;

	/* Check that we have a valid number and determine where it ends */
	endptr = strptr;

	/* Part (1): sign */
	if (*endptr == '-' || *endptr == '+') endptr++;

	/* Part (2): main digit string */
	if (isdigit((unsigned char)*endptr))
	{
		have_digits = true;
		do endptr++;
		while (isdigit((unsigned char)*endptr));
	}

	/* Part (3): optional decimal point and fractional digits */
	if (*endptr == '.')
	{
		endptr++;
		if (isdigit((unsigned char)*endptr))
		{
			have_digits = true;
			do endptr++;
			while (isdigit((unsigned char)*endptr));
		}
	}

	/* Complain if we don't have a valid number at this point */
	if (!have_digits) ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid size: \"%s\"", str)));

	/* Part (4): optional exponent */
	if (*endptr == 'e' || *endptr == 'E')
	{
		long  exponent;
		char *cp;

		/*
		 * Note we might one day support EB units, so if what follows 'E'
		 * isn't a number, just treat it all as a unit to be parsed.
		 */
		exponent = strtol(endptr + 1, &cp, 10);
		(void)exponent; /* Silence -Wunused-result warnings */
		if (cp > endptr + 1) endptr = cp;
	}

	/*
	 * Parse the number, saving the next character, which may be the first
	 * character of the unit string.
	 */
	saved_char = *endptr;
	*endptr    = '\0';

	num = DatumGetNumeric(
	        DirectFunctionCall3(numeric_in, CStringGetDatum(strptr), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));

	*endptr = saved_char;

	/* Skip whitespace between number and unit */
	strptr = endptr;
	while (isspace((unsigned char)*strptr)) strptr++;

	/* Handle possible unit */
	if (*strptr != '\0')
	{
		int64 multiplier = 0;

		/* Trim any trailing whitespace */
		endptr = str + strlen(str) - 1;

		while (isspace((unsigned char)*endptr)) endptr--;

		endptr++;
		*endptr = '\0';

		/* Parse the unit case-insensitively */
		if (pg_strcasecmp(strptr, "mb") == 0)
			multiplier = ((int64)1);

		else if (pg_strcasecmp(strptr, "gb") == 0)
			multiplier = ((int64)1024);

		else if (pg_strcasecmp(strptr, "tb") == 0)
			multiplier = ((int64)1024) * 1024;
		else if (pg_strcasecmp(strptr, "pb") == 0)
			multiplier = ((int64)1024) * 1024 * 1024;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid size: \"%s\"", str),
			                errdetail("Invalid size unit: \"%s\".", strptr),
			                errhint("Valid units are \"MB\", \"GB\", \"TB\", and \"PB\".")));

		if (multiplier > 1)
		{
			Numeric mul_num;

			mul_num = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(multiplier)));

			num = DatumGetNumeric(DirectFunctionCall2(numeric_mul, NumericGetDatum(mul_num), NumericGetDatum(num)));
		}
	}

	result = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(num)));

	return result;
}

/*
 * Function to set disk quota ratio for per-segment
 */
Datum
set_per_segment_quota(PG_FUNCTION_ARGS)
{
	int    ret;
	Oid    spcoid;
	char  *spcname;
	float4 ratio;

	ereportif(!superuser(), ERROR,
	          (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set disk quota limit")));

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	spcoid  = __get_oid_auto_case_convert(get_tablespace_oid, spcname);

	ratio = PG_GETARG_FLOAT4(1);

	ereportif(ratio == 0, ERROR,
	          (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("per segment quota ratio can not be set to 0")));

	if (SPI_OK_CONNECT != SPI_connect())
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unable to connect to execute internal query")));
	}
	/*
	 * lock table quota_config table in exlusive mode
	 *
	 * Firstly insert the segratio with TABLESPACE_QUOTA
	 * row into the table(ROWSHARE lock), then udpate the
	 * segratio for TABLESPACE_SHCEMA/ROLE_QUOTA rows
	 * (EXLUSIZE lock), if we don't lock the table in
	 * exlusive mode first, deadlock will heappen.
	 */
	ret = SPI_execute("LOCK TABLE diskquota.quota_config IN EXCLUSIVE MODE", false, 0);
	if (ret != SPI_OK_UTILITY) elog(ERROR, "cannot lock quota_config table, error code %d", ret);

	/*
	 * insert/update/detele tablespace ratio config in the quota_config table
	 * for TABLESPACE_QUOTA, it doesn't store any quota info, just used to
	 * store the ratio for the tablespace.
	 */
	set_quota_config_internal(spcoid, INVALID_QUOTA, TABLESPACE_QUOTA, ratio, InvalidOid);

	/*
	 * UPDATEA NAMESPACE_TABLESPACE_PERSEG_QUOTA AND ROLE_TABLESPACE_PERSEG_QUOTA config for this tablespace
	 */

	/* set to invalid ratio value if the tablespace per segment quota deleted */
	if (ratio < 0)
	{
		ratio = INVALID_SEGRATIO;
	}

	ret = SPI_execute_with_args(
	        "UPDATE diskquota.quota_config AS q set segratio = $1 FROM diskquota.target AS t WHERE "
	        "q.targetOid = t.rowId AND (t.quotaType = $2 OR t.quotaType = $3) AND t.quotaType = "
	        "q.quotaType And t.tablespaceOid = $4",
	        4,
	        (Oid[]){
	                FLOAT4OID,
	                INT4OID,
	                INT4OID,
	                OIDOID,
	        },
	        (Datum[]){
	                Float4GetDatum(ratio),
	                Int32GetDatum(NAMESPACE_TABLESPACE_QUOTA),
	                Int32GetDatum(ROLE_TABLESPACE_QUOTA),
	                ObjectIdGetDatum(spcoid),
	        },
	        NULL, false, 0);
	if (ret != SPI_OK_UPDATE) elog(ERROR, "cannot update item from quota setting table, error code %d", ret);

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	PG_RETURN_VOID();
}

int
worker_spi_get_extension_version(int *major, int *minor)
{
	StartTransactionCommand();
	int ret = SPI_connect();
	Assert(ret = SPI_OK_CONNECT);
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute("select extversion from pg_extension where extname = 'diskquota'", true, 0);

	if (SPI_processed == 0)
	{
		ret = -1;
		goto out;
	}

	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		ereport(WARNING,
		        (errmsg("[diskquota] when reading installed version lines %ld code = %d", SPI_processed, ret)));
		ret = -1;
		goto out;
	}

	bool  is_null = false;
	Datum v       = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	Assert(is_null == false);

	char *version = TextDatumGetCString(v);
	if (version == NULL)
	{
		ereport(WARNING,
		        (errmsg("[diskquota] 'extversion' is empty in pg_class.pg_extension. catalog might be corrupted")));
		ret = -1;
		goto out;
	}

	ret = sscanf(version, "%d.%d", major, minor);

	if (ret != 2)
	{
		ereport(WARNING, (errmsg("[diskquota] 'extversion' is '%s' in pg_class.pg_extension which is not valid format. "
		                         "catalog might be corrupted",
		                         version)));
		ret = -1;
		goto out;
	}

	ret = 0;

out:
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	return ret;
}

/*
 * Get the list of oids of the tables which diskquota
 * needs to care about in the database.
 * Firstly the all the table oids which relkind is 'r'
 * or 'm' and not system table.
 * Then, fetch the indexes of those tables.
 */

List *
get_rel_oid_list(void)
{
	List *oidlist = NIL;
	int   ret;

	ret = SPI_execute_with_args("select oid from pg_class where oid >= $1 and (relkind='r' or relkind='m')", 1,
	                            (Oid[]){
	                                    OIDOID,
	                            },
	                            (Datum[]){
	                                    ObjectIdGetDatum(FirstNormalObjectId),
	                            },
	                            NULL, false, 0);
	if (ret != SPI_OK_SELECT) elog(ERROR, "cannot fetch in pg_class. error code %d", ret);

	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	for (int i = 0; i < SPI_processed; i++)
	{
		HeapTuple tup;
		bool      isnull;
		Oid       oid;
		ListCell *l;

		tup = SPI_tuptable->vals[i];
		oid = DatumGetObjectId(SPI_getbinval(tup, tupdesc, 1, &isnull));
		if (!isnull)
		{
			List *indexIds;
			oidlist  = lappend_oid(oidlist, oid);
			indexIds = diskquota_get_index_list(oid);
			if (indexIds != NIL)
			{
				foreach (l, indexIds)
				{
					oidlist = lappend_oid(oidlist, lfirst_oid(l));
				}
			}
			list_free(indexIds);
		}
	}
	return oidlist;
}

typedef struct
{
	char *relation_path;
	int64 size;
} RelationFileStatCtx;

static bool
relation_file_stat(int segno, void *ctx)
{
	RelationFileStatCtx *stat_ctx             = (RelationFileStatCtx *)ctx;
	char                 file_path[MAXPGPATH] = {0};
	if (segno == 0)
		snprintf(file_path, MAXPGPATH, "%s", stat_ctx->relation_path);
	else
		snprintf(file_path, MAXPGPATH, "%s.%u", stat_ctx->relation_path, segno);
	struct stat fst;
#ifdef DISKQUOTA_FAULT_INJECTOR
	SIMPLE_FAULT_INJECTOR("diskquota_before_stat_relfilenode");
#endif
	if (stat(file_path, &fst) < 0)
	{
		if (errno != ENOENT)
		{
			int saved_errno = errno;
			ereport(WARNING, (errcode_for_file_access(), errmsg("[diskquota] could not stat file %s: %s", file_path, strerror(saved_errno))));
		}
		return false;
	}
	stat_ctx->size += fst.st_size;
	return true;
}

/*
 * calculate size of (all forks of) a relation in transaction
 * This function is following calculate_relation_size()
 */
int64
calculate_relation_size_all_forks(RelFileNodeBackend *rnode, char relstorage, Oid relam)
{
	int64        totalsize = 0;
	ForkNumber   forkNum;
	unsigned int segno = 0;

	if (TableIsHeap(relstorage, relam))
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		{
			RelationFileStatCtx ctx = {0};
			ctx.relation_path       = relpathbackend(rnode->node, rnode->backend, forkNum);
			ctx.size                = 0;
			for (segno = 0;; segno++)
			{
				if (!relation_file_stat(segno, &ctx)) break;
			}
			totalsize += ctx.size;
		}
		return totalsize;
	}
	else if (TableIsAoRows(relstorage, relam) || TableIsAoCols(relstorage, relam))
	{
		RelationFileStatCtx ctx = {0};
		ctx.relation_path       = relpathbackend(rnode->node, rnode->backend, MAIN_FORKNUM);
		ctx.size                = 0;
		/*
		 * Since the extension file with (segno=0, column=1) is not traversed by
		 * ao_foreach_extent_file(), we need to handle the size of it additionally.
		 * See comments in ao_foreach_extent_file() for details.
		 */
		relation_file_stat(0, &ctx);
		ao_foreach_extent_file(relation_file_stat, &ctx);
		return ctx.size;
	}
	else
	{
		return 0;
	}
}

Datum
relation_size_local(PG_FUNCTION_ARGS)
{
	Oid                reltablespace  = PG_GETARG_OID(0);
	Oid                relfilenode    = PG_GETARG_OID(1);
	char               relpersistence = PG_GETARG_CHAR(2);
	char               relstorage     = PG_GETARG_CHAR(3);
	Oid                relam          = PG_GETARG_OID(4);
	RelFileNodeBackend rnode          = {0};
	int64              size           = 0;

	rnode.node.dbNode  = MyDatabaseId;
	rnode.node.relNode = relfilenode;
	rnode.node.spcNode = OidIsValid(reltablespace) ? reltablespace : MyDatabaseTableSpace;
	rnode.backend      = relpersistence == RELPERSISTENCE_TEMP ? TempRelBackendId : InvalidBackendId;

	size = calculate_relation_size_all_forks(&rnode, relstorage, relam);

	PG_RETURN_INT64(size);
}

Relation
diskquota_relation_open(Oid relid, LOCKMODE mode)
{
	Relation rel;
	bool     success_open               = false;
	int32    SavedInterruptHoldoffCount = InterruptHoldoffCount;

	PG_TRY();
	{
		rel          = relation_open(relid, mode);
		success_open = true;
	}
	PG_CATCH();
	{
		InterruptHoldoffCount = SavedInterruptHoldoffCount;
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	return success_open ? rel : NULL;
}

List *
diskquota_get_index_list(Oid relid)
{
	Relation    indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple   htup;
	List       *result = NIL;

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, relid);

	indrel  = heap_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);

		/*
		 * Ignore any indexes that are currently being dropped. This will
		 * prevent them from being searched, inserted into, or considered in
		 * HOT-safety decisions. It's unsafe to touch such an index at all
		 * since its catalog entries could disappear at any instant.
		 */
		if (!index->indislive) continue;

		/* Add index's OID to result list in the proper order */
		result = lappend_oid(result, index->indexrelid);
	}

	systable_endscan(indscan);

	heap_close(indrel, AccessShareLock);

	return result;
}

/*
 * Get auxiliary relations oid by searching the pg_appendonly table.
 */
void
diskquota_get_appendonly_aux_oid_list(Oid reloid, Oid *segrelid, Oid *blkdirrelid, Oid *visimaprelid)
{
	ScanKeyData skey;
	SysScanDesc scan;
	TupleDesc   tupDesc;
	Relation    aorel;
	HeapTuple   htup;
	Datum       auxoid;
	bool        isnull;

	ScanKeyInit(&skey, Anum_pg_appendonly_relid, BTEqualStrategyNumber, F_OIDEQ, reloid);
	aorel   = heap_open(AppendOnlyRelationId, AccessShareLock);
	tupDesc = RelationGetDescr(aorel);
	scan = systable_beginscan(aorel, AppendOnlyRelidIndexId, true /*indexOk*/, NULL /*snapshot*/, 1 /*nkeys*/, &skey);
	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		if (segrelid)
		{
			auxoid = heap_getattr(htup, Anum_pg_appendonly_segrelid, tupDesc, &isnull);
			if (!isnull) *segrelid = DatumGetObjectId(auxoid);
		}

		if (blkdirrelid)
		{
			auxoid = heap_getattr(htup, Anum_pg_appendonly_blkdirrelid, tupDesc, &isnull);
			if (!isnull) *blkdirrelid = DatumGetObjectId(auxoid);
		}

		if (visimaprelid)
		{
			auxoid = heap_getattr(htup, Anum_pg_appendonly_visimaprelid, tupDesc, &isnull);
			if (!isnull) *visimaprelid = DatumGetObjectId(auxoid);
		}
	}

	systable_endscan(scan);
	heap_close(aorel, AccessShareLock);
}

Oid
diskquota_parse_primary_table_oid(Oid namespace, char *relname)
{
	switch (namespace)
	{
		case PG_TOAST_NAMESPACE:
			if (strncmp(relname, "pg_toast", 8) == 0) return atoi(&relname[9]);
			break;
		case PG_AOSEGMENT_NAMESPACE: {
			if (strncmp(relname, "pg_aoseg", 8) == 0)
				return atoi(&relname[9]);
			else if (strncmp(relname, "pg_aovisimap", 12) == 0)
				return atoi(&relname[13]);
			else if (strncmp(relname, "pg_aocsseg", 10) == 0)
				return atoi(&relname[11]);
			else if (strncmp(relname, "pg_aoblkdir", 11) == 0)
				return atoi(&relname[12]);
		}
		break;
	}
	return InvalidOid;
}

static float4
get_per_segment_ratio(Oid spcoid)
{
	int    ret;
	float4 segratio = INVALID_SEGRATIO;

	if (!OidIsValid(spcoid)) return segratio;

	/*
	 * using row share lock to lock TABLESPACE_QUTAO
	 * row to avoid concurrently updating the segratio
	 */
	ret = SPI_execute_with_args(
	        "select segratio from diskquota.quota_config where targetoid = $1 and quotatype = $2 for share", 2,
	        (Oid[]){
	                OIDOID,
	                INT4OID,
	        },
	        (Datum[]){
	                ObjectIdGetDatum(spcoid),
	                Int32GetDatum(TABLESPACE_QUOTA),
	        },
	        NULL, false, 0);
	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "cannot get per segment ratio for the tablepace: error code %d", ret);
	}

	if (SPI_processed == 1)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		HeapTuple tup     = SPI_tuptable->vals[0];
		Datum     dat;
		bool      isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (!isnull)
		{
			segratio = DatumGetFloat4(dat);
		}
	}
	return segratio;
}

/*
 * For quota type: TABLESPACE_QUOTA, it only stores
 * segratio not quota info. So when segratio is
 * negtive, we can just delete it.
 */
static bool
to_delete_quota(QuotaType type, int64 quota_limit_mb, float4 segratio)
{
	if (quota_limit_mb < 0)
		return true;
	else if (segratio < 0 && type == TABLESPACE_QUOTA)
		return true;
	return false;
}

static void
check_role(Oid roleoid, char *rolname, int64 quota_limit_mb)
{
	/* reject setting quota for super user, but deletion is allowed */
	if (roleoid == BOOTSTRAP_SUPERUSERID && quota_limit_mb >= 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("Can not set disk quota for system owner: %s", rolname)));
}

HTAB *
diskquota_hash_create(const char *tabname, long nelem, HASHCTL *info, int flags, DiskquotaHashFunction hashFunction)
{
#if GP_VERSION_NUM < 70000
	if (hashFunction == DISKQUOTA_TAG_HASH)
		info->hash = tag_hash;
	else if (hashFunction == DISKQUOTA_OID_HASH)
		info->hash = oid_hash;
	else
		info->hash = string_hash;
	return hash_create(tabname, nelem, info, flags | HASH_FUNCTION);
#else
	return hash_create(tabname, nelem, info, flags | HASH_BLOBS);
#endif /* GP_VERSION_NUM */
}

HTAB *
DiskquotaShmemInitHash(const char           *name,       /* table string name for shmem index */
                       long                  init_size,  /* initial table size */
                       long                  max_size,   /* max size of the table */
                       HASHCTL              *infoP,      /* info about key and bucket size */
                       int                   hash_flags, /* info about infoP */
                       DiskquotaHashFunction hashFunction)
{
#if GP_VERSION_NUM < 70000
	if (hashFunction == DISKQUOTA_TAG_HASH)
		infoP->hash = tag_hash;
	else if (hashFunction == DISKQUOTA_OID_HASH)
		infoP->hash = oid_hash;
	else
		infoP->hash = string_hash;
	return ShmemInitHash(name, init_size, max_size, infoP, hash_flags | HASH_FUNCTION);
#else
	return ShmemInitHash(name, init_size, max_size, infoP, hash_flags | HASH_BLOBS);
#endif /* GP_VERSION_NUM */
}
