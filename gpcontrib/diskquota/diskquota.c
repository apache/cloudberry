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

#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "gp_activetable.h"
#include "diskquota.h"
PG_MODULE_MAGIC;

/* disk quota helper function */
PG_FUNCTION_INFO_V1(set_schema_quota);
PG_FUNCTION_INFO_V1(set_role_quota);
PG_FUNCTION_INFO_V1(diskquota_start_worker);
PG_FUNCTION_INFO_V1(init_table_size_table);

/* timeout count to wait response from launcher process, in 1/10 sec */
#define WAIT_TIME_COUNT  120

/* max number of monitored database with diskquota enabled */
#define MAX_NUM_MONITORED_DB 10

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

/* GUC variables */
int			diskquota_naptime = 0;
int			diskquota_max_active_tables = 0;

typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;

/* disk quota worker info used by launcher to manage the worker processes. */
struct DiskQuotaWorkerEntry
{
	Oid			dbid;
	pid_t		pid;			/* worker pid */
	BackgroundWorkerHandle *handle;
};

DiskQuotaLocks diskquota_locks;
volatile	MessageBox *message_box = NULL;

/* using hash table to support incremental update the table size entry.*/
static HTAB *disk_quota_worker_map = NULL;
static object_access_hook_type next_object_access_hook;
static int	num_db = 0;

/* functions of disk quota*/
void		_PG_init(void);
void		_PG_fini(void);
void		disk_quota_worker_main(Datum);
void		disk_quota_launcher_main(Datum);

static void disk_quota_sigterm(SIGNAL_ARGS);
static void disk_quota_sighup(SIGNAL_ARGS);
static int64 get_size_in_mb(char *str);
static void set_quota_internal(Oid targetoid, int64 quota_limit_mb, QuotaType type);
static int	start_worker_by_dboid(Oid dbid);
static void create_monitor_db_table();
static inline void exec_simple_utility(const char *sql);
static void exec_simple_spi(const char *sql, int expected_code);
static bool add_db_to_config(Oid dbid);
static void del_db_from_config(Oid dbid);
static void process_message_box(void);
static void process_message_box_internal(MessageResult * code);
static void dq_object_access_hook(ObjectAccessType access, Oid classId,
					  Oid objectId, int subId, void *arg);
static const char *err_code_to_err_message(MessageResult code);
extern void diskquota_invalidate_db(Oid dbid);

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
	/* Add dq_object_access_hook to handle drop extension event. */
	next_object_access_hook = object_access_hook;
	object_access_hook = dq_object_access_hook;

	/* set up common data for diskquota launcher worker */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "diskquota");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "disk_quota_launcher_main");
	worker.bgw_notify_pid = 0;

	snprintf(worker.bgw_name, BGW_MAXLEN, "[diskquota] - launcher");

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

/*
 * Signal handler for SIGUSR1
 * 		Set a flag to tell the launcher to handle message box
 */
static void
disk_quota_sigusr1(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigusr1 = true;

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
	pqsignal(SIGUSR1, disk_quota_sigusr1);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL);

	/*
	 * Initialize diskquota related local hash map and refresh model
	 * immediately
	 */
	init_disk_quota_model();
	/* sleep 2 seconds to wait create extension statement finished */
	sleep(2);
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Check whether the state is in ready mode. The state would be
		 * unknown, when you `create extension diskquota` at the first time.
		 * After running UDF init_table_size_table() The state will changed to
		 * be ready.
		 */
		if (check_diskquota_state_is_ready())
		{
			break;
		}
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);
	}
	refresh_disk_quota_model(true);

	/*
	 * Set ps display name of the worker process of diskquota, so we can
	 * distinguish them quickly. Note: never mind parameter name of the
	 * function `init_ps_display`, we only want the ps name looks like
	 * 'bgworker: [diskquota] <dbname> ...'
	 */
	init_ps_display("bgworker:", "[diskquota]", dbname, "");

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

	diskquota_invalidate_db(MyDatabaseId);
	proc_exit(0);
}

/**
 * create table to record the list of monitored databases
 * we need a place to store the database with diskquota enabled
 * (via CREATE EXTENSION diskquota). Currently, we store them into
 * heap table in diskquota_namespace schema of postgres database.
 * When database restarted, diskquota laucher will start worker processes
 * for these databases.
 */
static void
create_monitor_db_table()
{
	const char *sql;

	sql = "create schema if not exists diskquota_namespace;"
		"create table if not exists diskquota_namespace.database_list(dbid oid not null unique);";
	exec_simple_utility(sql);
}

static inline void
exec_simple_utility(const char *sql)
{
	debug_query_string = sql;
	StartTransactionCommand();
	exec_simple_spi(sql, SPI_OK_UTILITY);
	CommitTransactionCommand();
	debug_query_string = NULL;
}

static void
exec_simple_spi(const char *sql, int expected_code)
{
	int			ret;

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "connect error, code=%d", ret);
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_execute(sql, false, 0);
	if (ret != expected_code)
		elog(ERROR, "sql:'%s', code %d", sql, ret);
	SPI_finish();
	PopActiveSnapshot();
}

static bool
is_valid_dbid(Oid dbid)
{
	HeapTuple	tuple;

	if (dbid == InvalidOid)
		return false;
	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}

/*
 * in early stage, start all worker processes of diskquota-enabled databases
 * from diskquota_namespace.database_list
 */
static void
start_workers_from_dblist()
{
	TupleDesc	tupdesc;
	Oid			fake_dbid[128];
	int			fake_count = 0;
	int			num = 0;
	int			ret;
	int			i;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "connect error, code=%d", ret);
	ret = SPI_execute("select dbid from diskquota_namespace.database_list;", false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "select diskquota_namespace.database_list");
	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 1 || tupdesc->attrs[0]->atttypid != OIDOID)
		elog(ERROR, "[diskquota] table database_list corrupt, laucher will exit");

	for (i = 0; num < SPI_processed; i++)
	{
		HeapTuple	tup;
		Oid			dbid;
		Datum		dat;
		bool		isnull;

		tup = SPI_tuptable->vals[i];
		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
		{
			elog(ERROR, "dbid cann't be null");
		}
		dbid = DatumGetObjectId(dat);
		if (!is_valid_dbid(dbid))
		{
			fake_dbid[fake_count++] = dbid;
			continue;
		}
		if (start_worker_by_dboid(dbid) < 1)
		{
			elog(WARNING, "[diskquota]: start worker process of database(%d) failed", dbid);
		}
		num++;
	}
	num_db = num;
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* TODO: clean invalid database */

}

static bool
add_db_to_config(Oid dbid)
{
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, "insert into diskquota_namespace.database_list values(%d);", dbid);
	exec_simple_spi(str.data, SPI_OK_INSERT);
	return true;
}

static void
del_db_from_config(Oid dbid)
{
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, "delete from diskquota_namespace.database_list where dbid=%d;", dbid);
	exec_simple_spi(str.data, SPI_OK_DELETE);
}

/*
 * When drop exention database, diskquota laucher will receive a message
 * to kill the diskquota worker process which monitoring the target database.
 */
static void
try_kill_db_worker(Oid dbid)
{
	DiskQuotaWorkerEntry *hash_entry;
	bool		found;

	hash_entry = (DiskQuotaWorkerEntry *) hash_search(disk_quota_worker_map,
													  (void *) &dbid,
													  HASH_REMOVE, &found);
	if (found)
	{
		BackgroundWorkerHandle *handle;

		handle = hash_entry->handle;
		TerminateBackgroundWorker(handle);
		pfree(handle);
	}
}

/*
 * handle create extension diskquota
 * if we know the exact error which caused failure,
 * we set it, and error out
 */
static void
on_add_db(Oid dbid, MessageResult * code)
{
	if (num_db >= MAX_NUM_MONITORED_DB)
	{
		*code = ERR_EXCEED;
		elog(ERROR, "[diskquota] too database to monitor");
	}
	if (!is_valid_dbid(dbid))
	{
		*code = ERR_INVALID_DBID;
		elog(ERROR, "[diskquota] invalid database oid");
	}

	/*
	 * add dbid to diskquota_namespace.database_list set *code to
	 * ERR_ADD_TO_DB if any error occurs
	 */
	PG_TRY();
	{
		add_db_to_config(dbid);
	}
	PG_CATCH();
	{
		*code = ERR_ADD_TO_DB;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (start_worker_by_dboid(dbid) < 1)
	{
		*code = ERR_START_WORKER;
		elog(ERROR, "[diskquota] failed to start worker - dbid=%d", dbid);
	}
}

/*
 * handle message: drop extension diskquota
 * do our best to:
 * 1. kill the associated worker process
 * 2. delete dbid from diskquota_namespace.database_list
 * 3. invalidate black-map entries from shared memory
 */
static void
on_del_db(Oid dbid)
{
	if (dbid == InvalidOid)
		return;
	try_kill_db_worker(dbid);
	del_db_from_config(dbid);
}

/* ---- Functions for lancher process ---- */
/*
 * Launcher process manages the worker processes based on
 * GUC diskquota.monitor_databases in configuration file.
 */
void
disk_quota_launcher_main(Datum main_arg)
{
	HASHCTL		hash_ctl;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);
	pqsignal(SIGUSR1, disk_quota_sigusr1);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	message_box->launcher_pid = MyProcPid;
	/* Connect to our database */
	BackgroundWorkerInitializeConnection("diskquota", NULL);
	create_monitor_db_table();

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(DiskQuotaWorkerEntry);
	hash_ctl.hash = oid_hash;

	disk_quota_worker_map = hash_create("disk quota worker map",
										1024,
										&hash_ctl,
										HASH_ELEM | HASH_FUNCTION);

	start_workers_from_dblist();

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
		/* process message box, now someone is holding message_box_lock */
		if (got_sigusr1)
		{
			got_sigusr1 = false;
			process_message_box();
		}

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

/*
 * Dynamically launch an disk quota worker process.
 */
static int
start_worker_by_dboid(Oid dbid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	MemoryContext old_ctx;
	char	   *dbname;
	pid_t		pid;
	bool		found;
	bool		ok;
	DiskQuotaWorkerEntry *workerentry;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "diskquota");
	sprintf(worker.bgw_function_name, "disk_quota_worker_main");

	dbname = get_database_name(dbid);
	Assert(dbname != NULL);
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "%s", dbname);
	pfree(dbname);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = (Datum) 0;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	ok = RegisterDynamicBackgroundWorker(&worker, &handle);
	MemoryContextSwitchTo(old_ctx);
	if (!ok)
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
													   (void *) &dbid,
													   HASH_ENTER, &found);
	if (!found)
	{
		workerentry->handle = handle;
		workerentry->pid = pid;
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
 * init table diskquota.table_size.
 * calculate table size by UDF pg_total_relation_size
 */
Datum
init_table_size_table(PG_FUNCTION_ARGS)
{
	int			ret;
	StringInfoData buf;

	RangeVar   *rv;
	Relation	rel;

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
					 "where oid> %u and (relkind='r' or relkind='m');",
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

/*
 * trigger start diskquota worker when create extension diskquota
 * This function is called at backend side, and will send message to
 * diskquota launcher. Luacher process is responsible for starting the real
 * diskquota worker process.
 */
Datum
diskquota_start_worker(PG_FUNCTION_ARGS)
{
	int			rc;

	elog(LOG, "[diskquota]:DB = %d, MyProc=%p launcher pid=%d", MyDatabaseId, MyProc, message_box->launcher_pid);
	LWLockAcquire(diskquota_locks.message_box_lock, LW_EXCLUSIVE);
	message_box->req_pid = MyProcPid;
	message_box->cmd = CMD_CREATE_EXTENSION;
	message_box->result = ERR_PENDING;
	message_box->data[0] = MyDatabaseId;
	/* setup sig handler to receive message */
	rc = kill(message_box->launcher_pid, SIGUSR1);
	if (rc == 0)
	{
		int			count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   100L);
			if (rc & WL_POSTMASTER_DEATH)
				break;
			ResetLatch(&MyProc->procLatch);
			if (message_box->result != ERR_PENDING)
				break;
		}
	}
	message_box->req_pid = 0;
	LWLockRelease(diskquota_locks.message_box_lock);
	if (message_box->result != ERR_OK)
		elog(ERROR, "%s", err_code_to_err_message((MessageResult) message_box->result));
	PG_RETURN_VOID();
}

static void
process_message_box_internal(MessageResult * code)
{
	Assert(message_box->launcher_pid == MyProcPid);
	switch (message_box->cmd)
	{
		case CMD_CREATE_EXTENSION:
			on_add_db(message_box->data[0], code);
			num_db++;
			break;
		case CMD_DROP_EXTENSION:
			on_del_db(message_box->data[0]);
			num_db--;
			break;
		default:
			elog(LOG, "[diskquota]:unsupported message cmd=%d", message_box->cmd);
			*code = ERR_UNKNOWN;
			break;
	}
}

/*
 * this function is called by launcher process to handle message from other backend
 * processes which call CREATE/DROP EXTENSION diskquota; It must be able to catch errors,
 * and return an error code back to the backend process.
 */
static void
process_message_box()
{
	MessageResult code = ERR_UNKNOWN;
	int			old_num_db = num_db;

	if (message_box->req_pid == 0)
		return;
	elog(LOG, "[launcher]: received message");
	PG_TRY();
	{
		StartTransactionCommand();
		process_message_box_internal(&code);
		CommitTransactionCommand();
		code = ERR_OK;
	}
	PG_CATCH();
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		AbortCurrentTransaction();
		FlushErrorState();
		RESUME_INTERRUPTS();
		num_db = old_num_db;
	}
	PG_END_TRY();

	message_box->result = (int) code;
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
	 * invoke drop extension diskquota 1. stop bgworker for MyDatabaseId 2.
	 * remove dbid from diskquota_namespace.database_list in postgres
	 */
	LWLockAcquire(diskquota_locks.message_box_lock, LW_EXCLUSIVE);
	message_box->req_pid = MyProcPid;
	message_box->cmd = CMD_DROP_EXTENSION;
	message_box->result = ERR_PENDING;
	message_box->data[0] = MyDatabaseId;
	rc = kill(message_box->launcher_pid, SIGUSR1);
	if (rc == 0)
	{
		int			count = WAIT_TIME_COUNT;

		while (count-- > 0)
		{
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   100L);
			if (rc & WL_POSTMASTER_DEATH)
				break;
			ResetLatch(&MyProc->procLatch);
			if (message_box->result != ERR_PENDING)
				break;
		}
	}
	message_box->req_pid = 0;
	LWLockRelease(diskquota_locks.message_box_lock);
	if (message_box->result != ERR_OK)
		elog(ERROR, "[diskquota] %s", err_code_to_err_message((MessageResult) message_box->result));
	elog(LOG, "[diskquota] DROP EXTENTION diskquota; OK");

out:
	if (next_object_access_hook)
		(*next_object_access_hook) (access, classId, objectId,
									subId, arg);
}

static const char *
err_code_to_err_message(MessageResult code)
{
	switch (code)
	{
		case ERR_PENDING:
			return "no response from launcher, or timeout";
		case ERR_OK:
			return "NO ERROR";
		case ERR_EXCEED:
			return "too many database to monitor";
		case ERR_ADD_TO_DB:
			return "add dbid to database_list failed";
		case ERR_START_WORKER:
			return "start worker failed";
		case ERR_INVALID_DBID:
			return "invalid dbid";
		default:
			return "unknown error";
	}
}
