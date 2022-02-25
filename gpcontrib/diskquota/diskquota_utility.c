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
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		diskquota/diskquota_utility.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/aomd.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/numeric.h"
#include "libpq-fe.h"

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

/* timeout count to wait response from launcher process, in 1/10 sec */
#define WAIT_TIME_COUNT  1200

static object_access_hook_type next_object_access_hook;
static bool is_database_empty(void);
static void dq_object_access_hook(ObjectAccessType access, Oid classId,
					  Oid objectId, int subId, void *arg);
static const char *ddl_err_code_to_err_message(MessageResult code);
static int64 get_size_in_mb(char *str);
static void set_quota_config_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type);
static void set_target_internal(Oid primaryoid, Oid spcoid, int64 quota_limit_mb, QuotaType type);
static bool generate_insert_table_size_sql(StringInfoData *buf, int extMajorVersion);
static char *convert_oidlist_to_string(List *oidlist);

int get_ext_major_version(void);
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
	int		ret;
	StringInfoData	buf;
	StringInfoData	insert_buf;

	RangeVar   	*rv;
	Relation	rel;
	int 		extMajorVersion;
	bool		insert_flag;
	/*
	 * If error happens in init_table_size_table, just return error messages
	 * to the client side. So there is no need to catch the error.
	 */

	/* ensure table diskquota.state exists */
	rv = makeRangeVar("diskquota", "state", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		elog(ERROR, "table \"diskquota.state\" is missing in database \"%s\","
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
	extMajorVersion = get_ext_major_version();
	char *oids = convert_oidlist_to_string(get_rel_oid_list());

	/* delete all the table size info in table_size if exist. */
	initStringInfo(&buf);
	initStringInfo(&insert_buf);
	appendStringInfo(&buf, "delete from diskquota.table_size;");
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "cannot delete table_size table: error code %d", ret);

	/* fetch table size for master*/
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "select oid, pg_table_size(oid), -1"
					 " from pg_class"
					 " where oid in (%s);",
					 oids);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot fetch in pg_table_size. error code %d", ret);

	/* fill table_size table with table oid and size info for master. */
	appendStringInfo(&insert_buf,
	                 "insert into diskquota.table_size values");
	insert_flag = generate_insert_table_size_sql(&insert_buf, extMajorVersion);
	/* fetch table size on segments*/
	resetStringInfo(&buf);
	appendStringInfo(&buf,
			"select oid, pg_table_size(oid), gp_segment_id"
			" from gp_dist_random('pg_class')"
			" where oid in (%s);",
			oids);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot fetch in pg_table_size. error code %d", ret);

	/* fill table_size table with table oid and size info for segments. */
	insert_flag = insert_flag | generate_insert_table_size_sql(&insert_buf, extMajorVersion);
	if (insert_flag)
	{
		truncateStringInfo(&insert_buf, insert_buf.len - strlen(","));
		appendStringInfo(&insert_buf, ";");
		ret = SPI_execute(insert_buf.data, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "cannot insert table_size_per_segment table: error code %d", ret);
	}

	/* set diskquota state to ready. */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "update diskquota.state set state = %u;",
					 DISKQUOTA_READY_STATE);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "cannot update state table: error code %d", ret);

	SPI_finish();
	PG_RETURN_VOID();
}

/* last_part is true means there is no other set of values to be inserted to table_size */
static bool
generate_insert_table_size_sql(StringInfoData *insert_buf, int extMajorVersion)
{
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	bool insert_flag = false;
	for(int i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup;
		bool        	isnull;
		Oid         	oid;
		int64       	sz;
		int16		segid;

		tup = SPI_tuptable->vals[i];
		oid = SPI_getbinval(tup,tupdesc, 1, &isnull);
		sz = SPI_getbinval(tup,tupdesc, 2, &isnull);
		segid = SPI_getbinval(tup,tupdesc, 3, &isnull);
		switch (extMajorVersion)
		{
			case 1:
				/* for version 1.0, only insert the values from master */
				if (segid == -1)
				{
					appendStringInfo(insert_buf, " ( %u, %ld),", oid, sz);
					insert_flag = true;
				}
				break;
			case 2:
				appendStringInfo(insert_buf, " ( %u, %ld, %d),", oid, sz, segid);
				insert_flag = true;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("[diskquota] unknown diskquota extension version: %d", extMajorVersion)));

		}
	}
	return insert_flag;
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
	extension_ddl_message->cmd = CMD_CREATE_EXTENSION;
	extension_ddl_message->result = ERR_PENDING;
	extension_ddl_message->dbid = MyDatabaseId;
	launcher_pid = extension_ddl_message->launcher_pid;
	/* setup sig handler to diskquota launcher process */
	rc = kill(launcher_pid, SIGUSR1);
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	if (rc == 0)
	{
		int			count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			CHECK_FOR_INTERRUPTS();
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   100L);
			if (rc & WL_POSTMASTER_DEATH)
				break;
			ResetLatch(&MyProc->procLatch);

			ereportif(kill(launcher_pid, 0) == -1 && errno == ESRCH, // do existence check
					ERROR,
					(errmsg("[diskquota] diskquota launcher pid = %d no longer exists", launcher_pid)));

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
		elog(ERROR, "[diskquota] failed to create diskquota extension: %s", ddl_err_code_to_err_message((MessageResult) extension_ddl_message->result));
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_ddl_lock);

	/* notify DBA to run init_table_size_table() when db is not empty */
	if (!is_database_empty())
	{
		ereport(WARNING, (errmsg("database is not empty, please run `select diskquota.init_table_size_table()` to initialize table_size information for diskquota extension. Note that for large database, this function may take a long time.")));
	}
	PG_RETURN_VOID();
}

/*
 * Dispatch pausing/resuming command to segments.
 */
static void
dispatch_pause_or_resume_command(Oid dbid, bool pause_extension)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT diskquota.%s", pause_extension ? "pause" : "resume");
	if (dbid == InvalidOid) {
		appendStringInfo(&sql, "()");
	} else {
		appendStringInfo(&sql, "(%d)", dbid);
	}
	CdbDispatchCommand(sql.data, DF_NONE, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; ++i)
	{
		PGresult *pgresult = cdb_pgresults.pg_results[i];
		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
				(errmsg("[diskquota] %s extension on segments, encounter unexpected result from segment: %d",
						pause_extension ? "pausing" : "resuming",
						PQresultStatus(pgresult))));
		}
	}
	cdbdisp_clearCdbPgResults(&cdb_pgresults);
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
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to pause diskquota")));
	}

	Oid dbid = MyDatabaseId;
	if (PG_NARGS() == 1) {
		dbid = PG_GETARG_OID(0);
	}

	// pause current worker
	LWLockAcquire(diskquota_locks.worker_map_lock, LW_EXCLUSIVE);
	{
		bool found;
		DiskQuotaWorkerEntry *hentry;

		hentry = (DiskQuotaWorkerEntry*) hash_search(disk_quota_worker_map,
													(void*)&dbid,
													// segment dose not boot the worker
													// this will add new element on segment
													// delete this element in diskquota_resume()
													HASH_ENTER,
													&found);

		hentry->is_paused = true;
	}
	LWLockRelease(diskquota_locks.worker_map_lock);

	if (IS_QUERY_DISPATCHER())
		dispatch_pause_or_resume_command(PG_NARGS() == 0 ? InvalidOid : dbid,
										 true /* pause_extension */);

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
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to resume diskquota")));
	}

	Oid dbid = MyDatabaseId;
	if (PG_NARGS() == 1) {
		dbid = PG_GETARG_OID(0);
	}

	// active current worker
	LWLockAcquire(diskquota_locks.worker_map_lock, LW_EXCLUSIVE);
	{
		bool found;
		DiskQuotaWorkerEntry *hentry;

		hentry = (DiskQuotaWorkerEntry*) hash_search(disk_quota_worker_map,
													(void*)&dbid,
													HASH_FIND,
													&found);
		if (found) {
			hentry->is_paused = false;
		}

		// remove the element since we do not need any more
		// ref diskquota_pause()
		if (found && hentry->handle == NULL) {
			hash_search(disk_quota_worker_map, (void*)&dbid, HASH_REMOVE, &found);
		}
	}
	LWLockRelease(diskquota_locks.worker_map_lock);

	if (IS_QUERY_DISPATCHER())
		dispatch_pause_or_resume_command(PG_NARGS() == 0 ? InvalidOid : dbid,
										 false /* pause_extension */);

	PG_RETURN_VOID();
}

/*
 * Check whether database is empty (no user table created)
 */
static bool
is_database_empty(void)
{
	int			ret;
	StringInfoData buf;
	TupleDesc	tupdesc;
	bool		is_empty = false;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT (count(relname) = 0)  FROM pg_class AS c, pg_namespace AS n WHERE c.oid > 16384 and relnamespace = n.oid and nspname != 'diskquota'");

	/*
	 * If error happens in is_database_empty, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */
	SPI_connect();

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot select pg_class and pg_namespace table: error code %d", errno);
	tupdesc = SPI_tuptable->tupdesc;
	/* check sql return value whether database is empty */
	if (SPI_processed > 0)
	{
		HeapTuple	tup = SPI_tuptable->vals[0];
		Datum		dat;
		bool		isnull;

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

/*
 * Add dq_object_access_hook to handle drop extension event.
 */
void
register_diskquota_object_access_hook(void)
{
	next_object_access_hook = object_access_hook;
	object_access_hook = dq_object_access_hook;
}

/*
 * This hook is used to handle drop extension diskquota event
 * It will send CMD_DROP_EXTENSION message to diskquota laucher.
 * Laucher will terminate the corresponding worker process and
 * remove the dbOid from the database_list table.
 */
static void
dq_object_access_hook(ObjectAccessType access, Oid classId,
					  Oid objectId, int subId, void *arg)
{
	Oid			oid;
	int			rc, launcher_pid;

	if (access != OAT_DROP || classId != ExtensionRelationId)
		goto out;
	oid = get_extension_oid("diskquota", true);
	if (oid != objectId)
		goto out;

	/* 
	 * Remove the current database from monitored db cache 
	 * on all segments and on coordinator.
	 */
	update_diskquota_db_list(MyDatabaseId, HASH_REMOVE);

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
	extension_ddl_message->cmd = CMD_DROP_EXTENSION;
	extension_ddl_message->result = ERR_PENDING;
	extension_ddl_message->dbid = MyDatabaseId;
	launcher_pid = extension_ddl_message->launcher_pid;
	rc = kill(launcher_pid, SIGUSR1);
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	if (rc == 0)
	{
		int			count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			CHECK_FOR_INTERRUPTS();
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   100L);
			if (rc & WL_POSTMASTER_DEATH)
				break;
			ResetLatch(&MyProc->procLatch);

			ereportif(kill(launcher_pid, 0) == -1 && errno == ESRCH, // do existence check
					ERROR,
					(errmsg("[diskquota] diskquota launcher pid = %d no longer exists", launcher_pid)));

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
		elog(ERROR, "[diskquota launcher] failed to drop diskquota extension: %s", ddl_err_code_to_err_message((MessageResult) extension_ddl_message->result));
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_ddl_lock);
out:
	if (next_object_access_hook)
		(*next_object_access_hook) (access, classId, objectId,
									subId, arg);
}

/*
 * For extension DDL('create extension/drop extension')
 * Using this function to convert error code from diskquota
 * launcher to error message and return it to client.
 */
static const char *
ddl_err_code_to_err_message(MessageResult code)
{
	switch (code)
	{
		case ERR_PENDING:
			return "no response from diskquota launcher, check whether launcher process exists";
		case ERR_OK:
			return "succeeded";
		case ERR_EXCEED:
			return "too many databases to monitor";
		case ERR_ADD_TO_DB:
			return "add dbid to database_list failed";
		case ERR_DEL_FROM_DB:
			return "delete dbid from database_list failed";
		case ERR_START_WORKER:
			return "start diskquota worker failed";
		case ERR_INVALID_DBID:
			return "invalid dbid";
		default:
			return "unknown error";
	}
}


/*
 * Set disk quota limit for role.
 */
Datum
set_role_quota(PG_FUNCTION_ARGS)
{
	Oid			roleoid;
	char	   *rolname;
	char	   *sizestr;
	int64		quota_limit_mb;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	rolname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	rolname = str_tolower(rolname, strlen(rolname), DEFAULT_COLLATION_OID);
	roleoid = get_role_oid(rolname, false);

	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	sizestr = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);

	if (quota_limit_mb == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("disk quota can not be set to 0 MB")));
	}
	set_quota_config_internal(roleoid, quota_limit_mb, ROLE_QUOTA);
	PG_RETURN_VOID();
}

/*
 * Set disk quota limit for schema.
 */
Datum
set_schema_quota(PG_FUNCTION_ARGS)
{
	Oid			namespaceoid;
	char	   *nspname;
	char	   *sizestr;
	int64		quota_limit_mb;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	nspname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	nspname = str_tolower(nspname, strlen(nspname), DEFAULT_COLLATION_OID);
	namespaceoid = get_namespace_oid(nspname, false);

	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	sizestr = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);

	if (quota_limit_mb == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("disk quota can not be set to 0 MB")));
	}
	set_quota_config_internal(namespaceoid, quota_limit_mb, NAMESPACE_QUOTA);
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
	Oid	spcoid;
	char	*spcname;
	Oid	roleoid;
	char   	*rolname;
	char   	*sizestr;
	int64	quota_limit_mb;
	
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	rolname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	rolname = str_tolower(rolname, strlen(rolname), DEFAULT_COLLATION_OID);
	roleoid = get_role_oid(rolname, false);

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	spcname = str_tolower(spcname, strlen(spcname), DEFAULT_COLLATION_OID);
	spcoid = get_tablespace_oid(spcname, false);


	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(2));
	sizestr = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);
	if (quota_limit_mb == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("disk quota can not be set to 0 MB")));
	}

	set_target_internal(roleoid, spcoid, quota_limit_mb, ROLE_TABLESPACE_QUOTA);
	set_quota_config_internal(roleoid, quota_limit_mb, ROLE_TABLESPACE_QUOTA);
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
	Oid	spcoid;
	char	*spcname;
	Oid	namespaceoid;
	char 	*nspname;
	char   	*sizestr;
	int64	quota_limit_mb;
	
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	nspname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	nspname = str_tolower(nspname, strlen(nspname), DEFAULT_COLLATION_OID);
	namespaceoid = get_namespace_oid(nspname, false);

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	spcname = str_tolower(spcname, strlen(spcname), DEFAULT_COLLATION_OID);
	spcoid = get_tablespace_oid(spcname, false);


	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(2));
	sizestr = str_tolower(sizestr, strlen(sizestr), DEFAULT_COLLATION_OID);
	quota_limit_mb = get_size_in_mb(sizestr);
	if (quota_limit_mb == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("disk quota can not be set to 0 MB")));
	}

	set_target_internal(namespaceoid, spcoid, quota_limit_mb, NAMESPACE_TABLESPACE_QUOTA);
	set_quota_config_internal(namespaceoid, quota_limit_mb, NAMESPACE_TABLESPACE_QUOTA);
	PG_RETURN_VOID();
}

static void
set_quota_config_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type)
{
	int			ret;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select true from diskquota.quota_config where targetoid = %u"
					 " and quotatype =%d",
					 targetoid, type);

	/*
	 * If error happens in set_quota_config_internal, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */
	SPI_connect();

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot select quota setting table: error code %d", ret);

	/* if the schema or role's quota has been set before */
	if (SPI_processed == 0 && quota_limit_mb > 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "insert into diskquota.quota_config values(%u,%d,%ld);",
						 targetoid, type, quota_limit_mb);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "cannot insert into quota setting table, error code %d", ret);
	}
	else if (SPI_processed > 0 && quota_limit_mb < 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "delete from diskquota.quota_config where targetoid=%u"
						 " and quotatype=%d;",
						 targetoid, type);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_DELETE)
			elog(ERROR, "cannot delete item from quota setting table, error code %d", ret);
	}
	else if (SPI_processed > 0 && quota_limit_mb > 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "update diskquota.quota_config set quotalimitMB = %ld where targetoid=%u"
						 " and quotatype=%d;",
						 quota_limit_mb, targetoid, type);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UPDATE)
			elog(ERROR, "cannot update quota setting table, error code %d", ret);
	}

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	return;
}

static void
set_target_internal(Oid primaryoid, Oid spcoid, int64 quota_limit_mb, QuotaType type)
{
	int			ret;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select true from diskquota.quota_config as q, diskquota.target as t"
					 " where t.primaryOid = %u"
					 " and t.tablespaceOid=%u"
					 " and t.quotaType=%d"
					 " and t.quotaType=q.quotaType"
					 " and t.primaryOid=q.targetOid;",
					 primaryoid, spcoid, type);

	/*
	 * If error happens in set_quota_config_internal, just return error messages to
	 * the client side. So there is no need to catch the error.
	 */
	SPI_connect();

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot select target setting table: error code %d", ret);

	/* if the schema or role's quota has been set before */
	if (SPI_processed == 0 && quota_limit_mb > 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "insert into diskquota.target values(%d,%u,%u)",
						  type, primaryoid, spcoid);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "cannot insert into quota setting table, error code %d", ret);
	}
	else if (SPI_processed > 0 && quota_limit_mb < 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "delete from diskquota.target where primaryOid=%u"
						 " and tablespaceOid=%u;",
						 primaryoid, spcoid);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_DELETE)
			elog(ERROR, "cannot delete item from target setting table, error code %d", ret);
	}

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	return;
}

/*
 * Convert a human-readable size to a size in MB.
 */
static int64
get_size_in_mb(char *str)
{
	char	   *strptr,
			   *endptr;
	char		saved_char;
	Numeric		num;
	int64		result;
	bool		have_digits = false;

	/* Skip leading whitespace */
	strptr = str;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Check that we have a valid number and determine where it ends */
	endptr = strptr;

	/* Part (1): sign */
	if (*endptr == '-' || *endptr == '+')
		endptr++;

	/* Part (2): main digit string */
	if (isdigit((unsigned char) *endptr))
	{
		have_digits = true;
		do
			endptr++;
		while (isdigit((unsigned char) *endptr));
	}

	/* Part (3): optional decimal point and fractional digits */
	if (*endptr == '.')
	{
		endptr++;
		if (isdigit((unsigned char) *endptr))
		{
			have_digits = true;
			do
				endptr++;
			while (isdigit((unsigned char) *endptr));
		}
	}

	/* Complain if we don't have a valid number at this point */
	if (!have_digits)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size: \"%s\"", str)));

	/* Part (4): optional exponent */
	if (*endptr == 'e' || *endptr == 'E')
	{
		long		exponent;
		char	   *cp;

		/*
		 * Note we might one day support EB units, so if what follows 'E'
		 * isn't a number, just treat it all as a unit to be parsed.
		 */
		exponent = strtol(endptr + 1, &cp, 10);
		(void) exponent;		/* Silence -Wunused-result warnings */
		if (cp > endptr + 1)
			endptr = cp;
	}

	/*
	 * Parse the number, saving the next character, which may be the first
	 * character of the unit string.
	 */
	saved_char = *endptr;
	*endptr = '\0';

	num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
											  CStringGetDatum(strptr),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(-1)));

	*endptr = saved_char;

	/* Skip whitespace between number and unit */
	strptr = endptr;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Handle possible unit */
	if (*strptr != '\0')
	{
		int64		multiplier = 0;

		/* Trim any trailing whitespace */
		endptr = str + strlen(str) - 1;

		while (isspace((unsigned char) *endptr))
			endptr--;

		endptr++;
		*endptr = '\0';

		/* Parse the unit case-insensitively */
		if (pg_strcasecmp(strptr, "mb") == 0)
			multiplier = ((int64) 1);

		else if (pg_strcasecmp(strptr, "gb") == 0)
			multiplier = ((int64) 1024);

		else if (pg_strcasecmp(strptr, "tb") == 0)
			multiplier = ((int64) 1024) * 1024;
		else if (pg_strcasecmp(strptr, "pb") == 0)
			multiplier = ((int64) 1024) * 1024 * 1024;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid size: \"%s\"", str),
					 errdetail("Invalid size unit: \"%s\".", strptr),
					 errhint("Valid units are \"MB\", \"GB\", \"TB\", and \"PB\".")));

		if (multiplier > 1)
		{
			Numeric		mul_num;

			mul_num = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
														  Int64GetDatum(multiplier)));

			num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(mul_num),
													  NumericGetDatum(num)));
		}
	}

	result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
											   NumericGetDatum(num)));

	return result;
}

/*
 * Function to update the db list on each segment
 * Will print a WARNING to log if out of memory
 */
void
update_diskquota_db_list(Oid dbid, HASHACTION action)
{
	bool	found = false;

	/* add/remove the dbid to monitoring database cache to filter out table not under
	* monitoring in hook functions
	*/

	LWLockAcquire(diskquota_locks.monitoring_dbid_cache_lock, LW_EXCLUSIVE);
	if (action == HASH_ENTER)
	{	
		Oid *entry = NULL;
		entry = hash_search(monitoring_dbid_cache, &dbid, HASH_ENTER_NULL, &found);
		if (entry == NULL)
		{
			ereport(WARNING,
				(errmsg("can't alloc memory on dbid cache, there ary too many databases to monitor")));
		}
	}
	else if (action == HASH_REMOVE)
	{
		hash_search(monitoring_dbid_cache, &dbid, HASH_REMOVE, &found);
		if (!found)
		{
			ereport(WARNING,
				(errmsg("cannot remove the database from db list, dbid not found")));
		}
	}
	LWLockRelease(diskquota_locks.monitoring_dbid_cache_lock);
}

/*
 * Function to set disk quota ratio for per-segment
 */
Datum
set_per_segment_quota(PG_FUNCTION_ARGS)
{
	int		ret;
	Oid		spcoid;
	char	   	*spcname;
	float4		ratio;
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	spcname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	spcname = str_tolower(spcname, strlen(spcname), DEFAULT_COLLATION_OID);
	spcoid = get_tablespace_oid(spcname, false);

	ratio = PG_GETARG_FLOAT4(1);

	if (ratio == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("per segment quota ratio can not be set to 0")));
	}
	StringInfoData buf;

	if (SPI_OK_CONNECT != SPI_connect())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to connect to execute internal query")));
	}

	/* Get all targetOid which are related to this tablespace, and saved into rowIds */
	initStringInfo(&buf);
	appendStringInfo(&buf,
			"SELECT true FROM diskquota.target as t, diskquota.quota_config as q WHERE tablespaceOid = %u AND (t.quotaType = %d OR t.quotaType = %d) AND t.primaryOid = q.targetOid AND t.quotaType = q.quotaType", spcoid, NAMESPACE_TABLESPACE_QUOTA, ROLE_TABLESPACE_QUOTA);

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot select target and quota setting table: error code %d", ret);
	if (SPI_processed <= 0)
	{
		ereport(ERROR,
				(errmsg("there are no roles or schema quota configed for this tablespace: %s, can't config per segment ratio for it", spcname)));
	}
	resetStringInfo(&buf);
	appendStringInfo(&buf,
			"UPDATE diskquota.quota_config AS q set segratio = %f FROM diskquota.target AS t WHERE q.targetOid = t.primaryOid AND (t.quotaType = %d OR t.quotaType = %d) AND t.quotaType = q.quotaType And t.tablespaceOid = %d", ratio, NAMESPACE_TABLESPACE_QUOTA, ROLE_TABLESPACE_QUOTA, spcoid);
	/*
	 * UPDATEA NAMESPACE_TABLESPACE_PERSEG_QUOTA AND ROLE_TABLESPACE_PERSEG_QUOTA config for this tablespace
	 */
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "cannot update item from quota setting table, error code %d", ret);
	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	PG_RETURN_VOID();
}

/*
 * Get major version from extversion, and convert it to int
 * 0 means an invalid major version.
 */
int
get_ext_major_version(void)
{
	int		ret;
	TupleDesc	tupdesc;
	HeapTuple	tup;
	Datum		dat;
	bool		isnull;
	char		*extversion;

	ret = SPI_execute("select COALESCE(extversion,'') from pg_extension where extname = 'diskquota'", true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 1 ||
		((tupdesc)->attrs[0])->atttypid != TEXTOID || SPI_processed != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] can not get diskquota extesion version")));
	}

	tup = SPI_tuptable->vals[0];
	dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("[diskquota] can not get diskquota extesion version")));
	extversion =  TextDatumGetCString(dat);
	if (extversion)
	{
		return (int)strtol(extversion, (char **) NULL, 10);
	}
	return 0;
}

static char *
convert_oidlist_to_string(List *oidlist)
{
	StringInfoData 	buf;
	bool		hasOid = false;
	ListCell   *l;
	initStringInfo(&buf);

	foreach(l, oidlist)
	{
		Oid	oid = lfirst_oid(l);
		appendStringInfo(&buf, "%u, ", oid);
		hasOid = true;
	}
	if (hasOid)
		truncateStringInfo(&buf, buf.len - strlen(", "));
	return buf.data;
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
	List   		*oidlist = NIL;
	StringInfoData	buf;
	int             ret;

	initStringInfo(&buf);
	appendStringInfo(&buf,
			"select oid "
			" from pg_class"
			" where oid >= %u and (relkind='r' or relkind='m')",
			FirstNormalObjectId);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot fetch in pg_class. error code %d", ret);
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	for(int i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup;
		bool        	isnull;
		Oid         	oid;
		ListCell   	*l;

		tup = SPI_tuptable->vals[i];
		oid = DatumGetObjectId(SPI_getbinval(tup,tupdesc, 1, &isnull));
		if (!isnull)
		{
			List *indexIds;
			oidlist = lappend_oid(oidlist, oid);
			indexIds = diskquota_get_index_list(oid);
			if (indexIds != NIL )
			{
				foreach(l, indexIds)
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
	RelationFileStatCtx *stat_ctx = (RelationFileStatCtx *)ctx;
	char file_path[MAXPGPATH] = {0};
	if (segno == 0)
		snprintf(file_path, MAXPGPATH, "%s", stat_ctx->relation_path);
	else
		snprintf(file_path, MAXPGPATH, "%s.%u", stat_ctx->relation_path, segno);
	struct stat fst;
	SIMPLE_FAULT_INJECTOR("diskquota_before_stat_relfilenode");
	if (stat(file_path, &fst) < 0)
	{
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					errmsg("[diskquota] could not stat file %s: %m", file_path)));
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
calculate_relation_size_all_forks(RelFileNodeBackend *rnode, char relstorage)
{
	int64		totalsize = 0;
	ForkNumber	forkNum;
	unsigned int segno = 0;

	if (relstorage == RELSTORAGE_HEAP)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		{
			RelationFileStatCtx ctx = {0};
			ctx.relation_path = relpathbackend(rnode->node, rnode->backend, forkNum);
			ctx.size = 0;
			for (segno = 0; ; segno++)
			{
				if (!relation_file_stat(segno, &ctx))
					break;
			}
			totalsize += ctx.size;
		}
		return totalsize;
	} 
	else if (relstorage == RELSTORAGE_AOROWS || relstorage == RELSTORAGE_AOCOLS)
	{
		RelationFileStatCtx ctx = {0};
		ctx.relation_path = relpathbackend(rnode->node, rnode->backend, MAIN_FORKNUM);
		ctx.size = 0;
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
	Oid reltablespace = PG_GETARG_OID(0);
	Oid relfilenode = PG_GETARG_OID(1);
	char relpersistence = PG_GETARG_CHAR(2);
	char relstorage = PG_GETARG_CHAR(3);
	RelFileNodeBackend rnode = {0};
	int64 size = 0;

	rnode.node.dbNode = MyDatabaseId;
	rnode.node.relNode = relfilenode;
	rnode.node.spcNode = OidIsValid(reltablespace) ? reltablespace : MyDatabaseTableSpace;
	rnode.backend = relpersistence == RELPERSISTENCE_TEMP ? TempRelBackendId : InvalidBackendId;

	size = calculate_relation_size_all_forks(&rnode, relstorage);

	PG_RETURN_INT64(size);
}

Relation
diskquota_relation_open(Oid relid, LOCKMODE mode)
{
	Relation rel;
	bool success_open = false;
    int32 SavedInterruptHoldoffCount = InterruptHoldoffCount;

	PG_TRY();
	{
		rel = relation_open(relid, mode);
		success_open = true;
	}
	PG_CATCH();
	{
        InterruptHoldoffCount = SavedInterruptHoldoffCount;
		HOLD_INTERRUPTS();
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	return success_open ? rel : NULL;
}

List*
diskquota_get_index_list(Oid relid)
{
	Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				relid);

	indrel = heap_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

		/*
		 * Ignore any indexes that are currently being dropped. This will
		 * prevent them from being searched, inserted into, or considered in
		 * HOT-safety decisions. It's unsafe to touch such an index at all
		 * since its catalog entries could disappear at any instant.
		 */
		if (!IndexIsLive(index))
			continue;

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
	ScanKeyData			skey;
	SysScanDesc			scan;
	TupleDesc			tupDesc;
	Relation			aorel;
	HeapTuple			htup;
	Datum  				auxoid;
	bool				isnull;

	ScanKeyInit(&skey, Anum_pg_appendonly_relid,
				BTEqualStrategyNumber, F_OIDEQ, reloid);
	aorel = heap_open(AppendOnlyRelationId, AccessShareLock);
	tupDesc = RelationGetDescr(aorel);
	scan = systable_beginscan(aorel, AppendOnlyRelidIndexId,
							  true /*indexOk*/, NULL /*snapshot*/,
							  1 /*nkeys*/, &skey);
	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		if (segrelid)
		{
			auxoid = heap_getattr(htup,
								  Anum_pg_appendonly_segrelid,
								  tupDesc, &isnull);
			if (!isnull)
				*segrelid = DatumGetObjectId(auxoid);
		}

		if (blkdirrelid)
		{
			auxoid = heap_getattr(htup,
								  Anum_pg_appendonly_blkdirrelid,
								  tupDesc, &isnull);
			if (!isnull)
				*blkdirrelid = DatumGetObjectId(auxoid);
		}

		if (visimaprelid)
		{
			auxoid = heap_getattr(htup,
								  Anum_pg_appendonly_visimaprelid,
								  tupDesc, &isnull);
			if (!isnull)
				*visimaprelid = DatumGetObjectId(auxoid);
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
			else if (strncmp(relname, "pg_aoblkdir", 11) == 0)
				return atoi(&relname[12]);
		}
		break;
	}
	return InvalidOid;
}
