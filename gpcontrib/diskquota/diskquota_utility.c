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

#include <unistd.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"
#include "libpq-fe.h"

#include <cdb/cdbvars.h>
#include <cdb/cdbutil.h>
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
PG_FUNCTION_INFO_V1(update_diskquota_db_list);
PG_FUNCTION_INFO_V1(set_per_segment_quota);

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
	int			rc;

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
	/* setup sig handler to diskquota launcher process */
	rc = kill(extension_ddl_message->launcher_pid, SIGUSR1);
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
dispatch_pause_or_resume_command(bool pause_extension)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT diskquota.%s", pause_extension ? "pause()" : "resume()");
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
 * Set diskquota_paused to true.
 * This function is called by user. After this function being called, diskquota
 * keeps counting the disk usage but doesn't emit an error when the disk usage
 * limit is exceeded.
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

	LWLockAcquire(diskquota_locks.paused_lock, LW_EXCLUSIVE);
	*diskquota_paused = true;
	LWLockRelease(diskquota_locks.paused_lock);

	if (IS_QUERY_DISPATCHER())
		dispatch_pause_or_resume_command(true /* pause_extension */);

	PG_RETURN_VOID();
}

/*
 * Set diskquota_paused to false.
 * This function is called by user. After this function being called, diskquota
 * resume to emit an error when the disk usage limit is exceeded.
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

	LWLockAcquire(diskquota_locks.paused_lock, LW_EXCLUSIVE);
	*diskquota_paused = false;
	LWLockRelease(diskquota_locks.paused_lock);

	if (IS_QUERY_DISPATCHER())
		dispatch_pause_or_resume_command(false /* pause_extension */);

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
	int			rc;

	if (access != OAT_DROP || classId != ExtensionRelationId)
		goto out;
	oid = get_extension_oid("diskquota", true);
	if (oid != objectId)
		goto out;

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
	rc = kill(extension_ddl_message->launcher_pid, SIGUSR1);
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
 */
Datum
update_diskquota_db_list(PG_FUNCTION_ARGS)
{
	Oid	dbid = PG_GETARG_OID(0);
	int	mode = PG_GETARG_INT32(1);
	bool	found = false;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to update db list")));
	}

	/* add/remove the dbid to monitoring database cache to filter out table not under
	* monitoring in hook functions
	*/

	LWLockAcquire(diskquota_locks.monitoring_dbid_cache_lock, LW_EXCLUSIVE);
	if (mode == 0)
	{	
		Oid *entry = NULL;
		entry = hash_search(monitoring_dbid_cache, &dbid, HASH_ENTER, &found);
		elog(WARNING, "add dbid %u into SHM", dbid);
		if (!found && entry == NULL)
		{
			ereport(WARNING,
				(errmsg("can't alloc memory on dbid cache, there ary too many databases to monitor")));
		}
	}
	else if (mode == 1)
	{
		hash_search(monitoring_dbid_cache, &dbid, HASH_REMOVE, &found);
		if (!found)
		{
			ereport(WARNING,
				(errmsg("cannot remove the database from db list, dbid not found")));
		}
	}
	LWLockRelease(diskquota_locks.monitoring_dbid_cache_lock);

	PG_RETURN_VOID();

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
			Relation	relation;
			List	   	*indexIds;
			relation = try_relation_open(oid, AccessShareLock, false);
			if (!relation)
				continue;

			oidlist = lappend_oid(oidlist, oid);
			indexIds = RelationGetIndexList(relation);
			if (indexIds != NIL )
			{
				foreach(l, indexIds)
				{
					oidlist = lappend_oid(oidlist, lfirst_oid(l));
				}
			}
		        relation_close(relation, NoLock);
			list_free(indexIds);
		}
	}
	return oidlist;
}
