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

#include "diskquota.h"

/* disk quota helper function */

PG_FUNCTION_INFO_V1(init_table_size_table);
PG_FUNCTION_INFO_V1(diskquota_start_worker);
PG_FUNCTION_INFO_V1(set_schema_quota);
PG_FUNCTION_INFO_V1(set_role_quota);

/* timeout count to wait response from launcher process, in 1/10 sec */
#define WAIT_TIME_COUNT  1200

static object_access_hook_type next_object_access_hook;

static void dq_object_access_hook(ObjectAccessType access, Oid classId,
					  Oid objectId, int subId, void *arg);
static const char *ddl_err_code_to_err_message(MessageResult code);
static int64 get_size_in_mb(char *str);
static void set_quota_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type);

/* ---- Help Functions to set quota limit. ---- */
/*
 * Initialize table diskquota.table_size.
 * calculate table size by UDF pg_total_relation_size
 * This function is called by user, errors should not
 * be catch, and should be sent back to user
 */
Datum
init_table_size_table(PG_FUNCTION_ARGS)
{
	int			ret;
	StringInfoData buf;

	RangeVar   *rv;
	Relation	rel;

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

	SPI_connect();

	/* delete all the table size info in table_size if exist. */
	initStringInfo(&buf);
	appendStringInfo(&buf, "delete from diskquota.table_size;");
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "cannot delete table_size table: error code %d", ret);

	/* fill table_size table with table oid and size info. */
	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "insert into diskquota.table_size "
					 "select oid, pg_total_relation_size(oid) from pg_class "
					 "where oid>= %u and (relkind='r' or relkind='m');",
					 FirstNormalObjectId);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "cannot insert table_size table: error code %d", ret);

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
	 * Lock on extension_lock to avoid multiple backend create diskquota
	 * extension at the same time.
	 */
	LWLockAcquire(diskquota_locks.extension_lock, LW_EXCLUSIVE);
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
		LWLockRelease(diskquota_locks.extension_lock);
		elog(ERROR, "[diskquota] failed to create diskquota extension: %s", ddl_err_code_to_err_message((MessageResult) extension_ddl_message->result));
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_lock);
	PG_RETURN_VOID();
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
	 * Lock on extension_lock to avoid multiple backend create diskquota
	 * extension at the same time.
	 */
	LWLockAcquire(diskquota_locks.extension_lock, LW_EXCLUSIVE);
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
		LWLockRelease(diskquota_locks.extension_lock);
		elog(ERROR, "[diskquota launcher] failed to drop diskquota extension: %s", ddl_err_code_to_err_message((MessageResult) extension_ddl_message->result));
	}
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	LWLockRelease(diskquota_locks.extension_lock);
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

	set_quota_internal(roleoid, quota_limit_mb, ROLE_QUOTA);
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

	set_quota_internal(namespaceoid, quota_limit_mb, NAMESPACE_QUOTA);
	PG_RETURN_VOID();
}

/*
 * Write the quota limit info into quota_config table under
 * 'diskquota' schema of the current database.
 */
static void
set_quota_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type)
{
	int			ret;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select true from diskquota.quota_config where targetoid = %u"
					 " and quotatype =%d",
					 targetoid, type);

	/*
	 * If error happens in set_quota_internal, just return error messages to
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
	else if (SPI_processed > 0 && quota_limit_mb <= 0)
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
