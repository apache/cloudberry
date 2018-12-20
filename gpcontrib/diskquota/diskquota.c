/* -------------------------------------------------------------------------
 *
 * diskquota.c
 *
 * Diskquota is used to limit the amount of disk space that a schema or a role
 * can use. Diskquota is based on background worker framework. It contains a
 * launcher process which is responsible for starting/refreshing the diskquota
 * worker processes which monitor given databases.
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		gpcontrib/gp_diskquota/diskquota.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/numeric.h"

#include "gp_activetable.h"
#include "diskquota.h"
PG_MODULE_MAGIC;

/* disk quota helper function */
PG_FUNCTION_INFO_V1(set_schema_quota);
PG_FUNCTION_INFO_V1(set_role_quota);

/* max number of monitored database with diskquota enabled */
#define MAX_NUM_MONITORED_DB 10

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
int			diskquota_naptime = 0;
char	   *diskquota_monitored_database_list = NULL;
int			diskquota_max_active_tables = 0;

typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;

/* disk quota worker info used by launcher to manage the worker processes. */
struct DiskQuotaWorkerEntry
{
	char		dbname[NAMEDATALEN];
	BackgroundWorkerHandle *handle;
};

/* using hash table to support incremental update the table size entry.*/
static HTAB *disk_quota_worker_map = NULL;

/* functions of disk quota*/
void		_PG_init(void);
void		_PG_fini(void);
void		disk_quota_worker_main(Datum);
void		disk_quota_launcher_main(Datum);

static void disk_quota_sigterm(SIGNAL_ARGS);
static void disk_quota_sighup(SIGNAL_ARGS);
static List *get_database_list(bool *is_refresh);
static int64 get_size_in_mb(char *str);
static void refresh_worker_list(void);
static void set_quota_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type);
static int	start_worker(char *dbname);

/*
 * Entrypoint of diskquota module.
 *
 * Init shared memory and hooks.
 * Define GUCs.
 * start diskquota launcher process.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* diskquota.so must be in shared_preload_libraries to init SHM. */
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "diskquota.so not in shared_preload_libraries.");

	init_disk_quota_shmem();
	init_disk_quota_enforcement();
	init_active_table_hook();

	/* get the configuration */
	DefineCustomIntVariable("diskquota.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&diskquota_naptime,
							5,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("diskquota.monitor_databases",
							   gettext_noop("database list with disk quota monitored."),
							   NULL,
							   &diskquota_monitored_database_list,
							   "",
							   PGC_SIGHUP, GUC_LIST_INPUT,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("diskquota.max_active_tables",
							"max number of active tables monitored by disk-quota",
							NULL,
							&diskquota_max_active_tables,
							1 * 1024 * 1024,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	/* start disk quota launcher only on master */
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		return;
	}

	/* set up common data for diskquota launcher worker */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "diskquota");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "disk_quota_launcher_main");
	worker.bgw_notify_pid = 0;

	snprintf(worker.bgw_name, BGW_MAXLEN, "disk quota launcher");

	RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
disk_quota_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
disk_quota_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}


/* ---- Functions for disk quota worker process ---- */

/*
 * Disk quota worker process will refresh disk quota model periodically.
 * Refresh logic is defined in quotamodel.c
 */
void
disk_quota_worker_main(Datum main_arg)
{
	char	   *dbname = MyBgworkerEntry->bgw_name;

	ereport(LOG,
			(errmsg("start disk quota worker process to monitor database:%s",
					dbname)));

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL);

	/*
	 * Initialize diskquota related local hash map and refresh model
	 * immediately
	 */
	init_disk_quota_model();
	refresh_disk_quota_model(true);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* Do the work */
		refresh_disk_quota_model(false);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	proc_exit(1);
}

/* ---- Functions for lancher process ---- */
/*
 * Launcher process manages the worker processes based on
 * GUC diskquota.monitor_databases in configuration file.
 */
void
disk_quota_launcher_main(Datum main_arg)
{
	List	   *dblist;
	ListCell   *cell;
	HASHCTL		hash_ctl;
	bool		is_refresh = false;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(DiskQuotaWorkerEntry);

	disk_quota_worker_map = hash_create("disk quota worker map",
										1024,
										&hash_ctl,
										HASH_ELEM);

	ereport(LOG,
			(errmsg("diskquota launcher started")));

	dblist = get_database_list(&is_refresh);
	if (is_refresh)
	{
		foreach(cell, dblist)
		{
			char	   *db_name;

			db_name = (char *) lfirst(cell);
			if (db_name == NULL || *db_name == '\0')
			{
				ereport(LOG,
						(errmsg("invalid db name='%s' in diskquota.monitor_databases", db_name)));
				continue;
			}
			start_worker(db_name);
		}
	}
	/* free dblist */
	list_free(dblist);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * terminate not monitored worker process and start new worker
			 * process
			 */
			refresh_worker_list();
		}

	}

	proc_exit(1);
}

/*
 * Extract database list in GUC diskquota.monitored_database_list
 * Parameter is_refresh is used to indicate whether to refresh the
 * monitored database list when GUC monitored_database_list changed.
 * If GUC contains more than 10 databases, is_refresh is set to false.
 */
static List *
get_database_list(bool *is_refresh)
{
	List	   *monitor_db_list = NIL;
	char	   *dbstr;

	*is_refresh = true;
	dbstr = pstrdup(diskquota_monitored_database_list);

	if (!SplitIdentifierString(dbstr, ',', &monitor_db_list))
	{
		ereport(WARNING,
				(errmsg("GUC monitor_databases:'%s' is invalid, GUC should be"
						"separated by comma",
						diskquota_monitored_database_list)));
		pfree(dbstr);
		return NULL;
	}

	/*
	 * We only allow to minitor at most 10 databases truncate the list if
	 * there are more than 10 databases in list.
	 */
	if (list_length(monitor_db_list) > MAX_NUM_MONITORED_DB)
	{
		*is_refresh = false;
		ereport(WARNING,
				(errmsg("Currently diskquota could monitor at most 10 databases."
						"GUC monitor_databases:'%s' contains more than"
						" 10 databases, additional databases will be ignored.",
						diskquota_monitored_database_list)));
		monitor_db_list = list_truncate(monitor_db_list, MAX_NUM_MONITORED_DB);
	}

	pfree(dbstr);
	/* dblist should be list_free by the caller */
	return monitor_db_list;
}

/*
 * When launcher receive SIGHUP, it will call refresh_worker_list()
 * to terminate worker processes whose connected database no longer need
 * to be monitored, and start new worker processes to watch new database.
 */
static void
refresh_worker_list(void)
{
	List	   *monitor_dblist;
	ListCell   *cell;
	bool		flag = false;
	bool		is_refresh = false;
	bool		found;
	DiskQuotaWorkerEntry *hash_entry;
	HASH_SEQ_STATUS status;

	monitor_dblist = get_database_list(&is_refresh);
	if (!is_refresh)
	{
		ereport(WARNING,
				(errmsg("Failed to refresh monitored database. GUC "
						"monitor_databases:'%s' should contain less than "
						"10 databases.",
						diskquota_monitored_database_list)));
		return;
	}

	/*
	 * refresh the worker process based on the configuration file change. step
	 * 1 is to terminate worker processes whose connected database not in
	 * monitor database list.
	 */
	ereport(LOG,
			(errmsg("Refresh monitored database list.")));
	hash_seq_init(&status, disk_quota_worker_map);

	while ((hash_entry = (DiskQuotaWorkerEntry *) hash_seq_search(&status)) != NULL)
	{
		flag = false;
		foreach(cell, monitor_dblist)
		{
			char	   *db_name;

			db_name = (char *) lfirst(cell);
			if (db_name == NULL || *db_name == '\0')
			{
				continue;
			}
			if (strcmp(db_name, hash_entry->dbname) == 0)
			{
				flag = true;
				break;
			}
		}
		if (!flag)
		{
			TerminateBackgroundWorker(hash_entry->handle);
			(DiskQuotaWorkerEntry *) hash_search(disk_quota_worker_map,
												 (void *) hash_entry->dbname,
												 HASH_REMOVE, NULL);
		}
	}

	/* step 2: start new worker which first appears in monitor database list. */
	foreach(cell, monitor_dblist)
	{
		DiskQuotaWorkerEntry *workerentry;
		char	   *db_name;
		pid_t		pid;

		db_name = (char *) lfirst(cell);
		if (db_name == NULL || *db_name == '\0')
		{
			continue;
		}
		workerentry = (DiskQuotaWorkerEntry *) hash_search(disk_quota_worker_map,
														   (void *) db_name,
														   HASH_FIND, &found);
		if (found)
		{
			/* in case worker is not in BGWH_STARTED mode, restart it. */
			if (GetBackgroundWorkerPid(workerentry->handle, &pid) != BGWH_STARTED)
				start_worker(db_name);
		}
		else
		{
			start_worker(db_name);
		}
	}

	/* free monitor_dblist */
	list_free(monitor_dblist);
}

/*
 * Dynamically launch an disk quota worker process.
 */
static int
start_worker(char *dbname)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	bool		found;
	DiskQuotaWorkerEntry *workerentry;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "diskquota");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "disk_quota_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", dbname);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = (Datum) 0;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return -1;

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	/* put the worker handle into the worker map */
	workerentry = (DiskQuotaWorkerEntry *) hash_search(disk_quota_worker_map,
													   (void *) dbname,
													   HASH_ENTER, &found);
	if (!found)
	{
		workerentry->handle = handle;
	}

	return pid;
}

/* ---- Help Functions to set quota limit. ---- */
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

	SPI_connect();

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "cannot select quota setting table: error code %d", ret);

	/* if the schema or role's quota has been set before */
	if (SPI_processed == 0 && quota_limit_mb > 0)
	{
		resetStringInfo(&buf);
		initStringInfo(&buf);
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
		initStringInfo(&buf);
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
		initStringInfo(&buf);
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
