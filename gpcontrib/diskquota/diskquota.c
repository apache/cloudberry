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
 *		diskquota/diskquota.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <utils/timeout.h>

#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
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
#include "tcop/idle_resource_cleaner.h"
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

/* max number of monitored database with diskquota enabled */
#define MAX_NUM_MONITORED_DB 10

#define DISKQUOTA_DB	"diskquota"
#define DISKQUOTA_APPLICATION_NAME  "gp_reserved_gpdiskquota"

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

/* GUC variables */
int			diskquota_naptime = 0;
int			diskquota_max_active_tables = 0;
static bool diskquota_enable_hardlimit = false;

typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;

/* disk quota worker info used by launcher to manage the worker processes. */
struct DiskQuotaWorkerEntry
{
	Oid			dbid;
	pid_t		pid;			/* worker pid */
	BackgroundWorkerHandle *handle;
};

DiskQuotaLocks diskquota_locks;
ExtensionDDLMessage *extension_ddl_message = NULL;

/* using hash table to support incremental update the table size entry.*/
static HTAB *disk_quota_worker_map = NULL;
static int	num_db = 0;

/* functions of disk quota*/
void		_PG_init(void);
void		_PG_fini(void);
void		disk_quota_worker_main(Datum);
void		disk_quota_launcher_main(Datum);

static void disk_quota_sigterm(SIGNAL_ARGS);
static void disk_quota_sighup(SIGNAL_ARGS);
static bool start_worker_by_dboid(Oid dbid);
static void start_workers_from_dblist(void);
static void create_monitor_db_table(void);
static void add_dbid_to_database_list(Oid dbid);
static void del_dbid_from_database_list(Oid dbid);
static void process_extension_ddl_message(void);
static void do_process_extension_ddl_message(MessageResult * code,
								 ExtensionDDLMessage local_extension_ddl_message);
static void try_kill_db_worker(Oid dbid);
static void terminate_all_workers(void);
static void on_add_db(Oid dbid, MessageResult * code);
static void on_del_db(Oid dbid, MessageResult * code);
static bool is_valid_dbid(Oid dbid);
extern void invalidate_database_blackmap(Oid dbid);

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

	memset(&worker, 0, sizeof(BackgroundWorker));

	/* diskquota.so must be in shared_preload_libraries to init SHM. */
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR, (errmsg("diskquota.so not in shared_preload_libraries.")));

	init_disk_quota_shmem();
	init_disk_quota_enforcement();
	init_active_table_hook();

	/* get the configuration */
	DefineCustomIntVariable("diskquota.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&diskquota_naptime,
							2,
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

	DefineCustomBoolVariable("diskquota.enable_hardlimit",
							 "Use in-query diskquota enforcement",
							 NULL,
							 &diskquota_enable_hardlimit,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* start disk quota launcher only on master */
	if (!IS_QUERY_DISPATCHER())
	{
		return;
	}

	/* Add dq_object_access_hook to handle drop extension event. */
	register_diskquota_object_access_hook();

	/* set up common data for diskquota launcher worker */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/* launcher process should be restarted after pm reset. */
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
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
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
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
 * Set a flag to tell the main loop to reread the config file, and set
 * our latch to wake it up.
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
 * Set a flag to tell the launcher to handle extension ddl message
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

	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME,
	                  PGC_USERSET,PGC_S_SESSION,
	                  GUC_ACTION_SAVE, true, 0);

	/*
	 * Set ps display name of the worker process of diskquota, so we can
	 * distinguish them quickly. Note: never mind parameter name of the
	 * function `init_ps_display`, we only want the ps name looks like
	 * 'bgworker: [diskquota] <dbname> ...'
	 */
	init_ps_display("bgworker:", "[diskquota]", dbname, "");

	/* diskquota worker should has Gp_role as dispatcher */
	Gp_role = GP_ROLE_DISPATCH;

	/*
	 * Initialize diskquota related local hash map and refresh model
	 * immediately
	 */
	init_disk_quota_model();

	/* Waiting for diskquota state become ready */
	while (!got_sigterm)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

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

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	/* if received sigterm, just exit the worker process */
	if (got_sigterm)
	{
		/* clear the out-of-quota blacklist in shared memory */
		invalidate_database_blackmap(MyDatabaseId);
		proc_exit(0);
	}

	/* Refresh quota model with init mode */
	refresh_disk_quota_model(true);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

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


		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Do the work */
		refresh_disk_quota_model(false);

		if (diskquota_enable_hardlimit)
		{
			/* TODO: Add hard limit function here */
		}
	}

	/* clear the out-of-quota blacklist in shared memory */
	invalidate_database_blackmap(MyDatabaseId);
	proc_exit(0);
}

inline bool isAbnormalLoopTime(int diff_sec)
{
	int max_time = diskquota_naptime + 6;
	return diff_sec > max_time;
}

/* ---- Functions for launcher process ---- */
/*
 * Launcher process manages the worker processes based on
 * GUC diskquota.monitor_databases in configuration file.
 */
void
disk_quota_launcher_main(Datum main_arg)
{
	HASHCTL		hash_ctl;
	time_t loop_begin, loop_end;

	/* establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);
	pqsignal(SIGUSR1, disk_quota_sigusr1);

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	extension_ddl_message->launcher_pid = MyProcPid;
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);

	/*
	 * connect to our database 'diskquota'. launcher process will exit if
	 * 'diskquota' database is not existed.
	 */
	BackgroundWorkerInitializeConnection(DISKQUOTA_DB, NULL);

	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME,
	                  PGC_USERSET,PGC_S_SESSION,
	                  GUC_ACTION_SAVE, true, 0);

	/* diskquota launcher should has Gp_role as dispatcher */
	Gp_role = GP_ROLE_DISPATCH;
	
	/*
	 * use table diskquota_namespace.database_list to store diskquota enabled
	 * database.
	 */
	create_monitor_db_table();

	/* use disk_quota_worker_map to manage diskquota worker processes. */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(DiskQuotaWorkerEntry);
	hash_ctl.hash = oid_hash;

	disk_quota_worker_map = hash_create("disk quota worker map",
										1024,
										&hash_ctl,
										HASH_ELEM | HASH_FUNCTION);

	/*
	 * firstly start worker processes for each databases with diskquota
	 * enabled.
	 */
	start_workers_from_dblist();

	/* main loop: do this until the SIGTERM handler tells us to terminate. */
	EnableClientWaitTimeoutInterrupt();
	StartIdleResourceCleanupTimers();
	loop_end = time(NULL);
	while (!got_sigterm)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		/*
		 * background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* process extension ddl message */
		if (got_sigusr1)
		{
			got_sigusr1 = false;
			CancelIdleResourceCleanupTimers();
			process_extension_ddl_message();
			StartIdleResourceCleanupTimers();
		}

		/* in case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			CancelIdleResourceCleanupTimers();
			ProcessConfigFile(PGC_SIGHUP);
			StartIdleResourceCleanupTimers();
		}
		loop_begin = loop_end;
		loop_end = time(NULL);
		if (isAbnormalLoopTime(loop_end - loop_begin))
		{
			ereport(WARNING, (errmsg("[diskquota-loop] loop takes too much time %d/%d",
				(int)(loop_end - loop_begin), diskquota_naptime)));
		}
	}

	/* terminate all the diskquota worker processes before launcher exit */
	terminate_all_workers();
	proc_exit(0);
}


/*
 * Create table to record the list of monitored databases
 * we need a place to store the database with diskquota enabled
 * (via CREATE EXTENSION diskquota). Currently, we store them into
 * heap table in diskquota_namespace schema of diskquota database.
 * When database restarted, diskquota launcher will start worker processes
 * for these databases.
 */
static void
create_monitor_db_table(void)
{
	const char *sql;
	bool		connected = false;
	bool		pushed_active_snap = false;
	bool		ret = true;

	sql = "create schema if not exists diskquota_namespace;"
		"create table if not exists diskquota_namespace.database_list(dbid oid not null unique);";

	/* debug_query_string need to be set for SPI_execute utility functions. */
	debug_query_string = sql;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota launcher process should
	 * tolerate this kind of errors.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to connect to execute internal query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;

		if (SPI_execute(sql, false, 0) != SPI_OK_UTILITY)
		{
			ereport(ERROR, (errmsg("[diskquota launcher] SPI_execute error, sql:'%s', errno:%d", sql, errno)));
		}
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected)
		SPI_finish();
	if (pushed_active_snap)
		PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	debug_query_string = NULL;
}

/*
 * When launcher started, it will start all worker processes of
 * diskquota-enabled databases from diskquota_namespace.database_list
 */
static void
start_workers_from_dblist(void)
{
	TupleDesc	tupdesc;
	int			num = 0;
	int			ret;
	int			i;

	/*
	 * Don't catch errors in start_workers_from_dblist. Since this is the
	 * startup worker for diskquota launcher. If error happens, we just let
	 * launcher exits.
	 */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("[diskquota launcher] SPI connect error, errno:%d", errno)));
	ret = SPI_execute("select dbid from diskquota_namespace.database_list;", true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("select diskquota_namespace.database_list")));
	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 1 || tupdesc->attrs[0]->atttypid != OIDOID)
		ereport(ERROR, (errmsg("[diskquota launcher] table database_list corrupt, laucher will exit")));

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup;
		Oid			dbid;
		Datum		dat;
		bool		isnull;

		tup = SPI_tuptable->vals[i];
		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			ereport(ERROR, (errmsg("[diskquota launcher] dbid cann't be null in table database_list")));
		dbid = DatumGetObjectId(dat);
		if (!is_valid_dbid(dbid))
		{
			ereport(LOG, (errmsg("[diskquota launcher] database(oid:%u) in table database_list is not a valid database", dbid)));
			continue;
		}
		if (!start_worker_by_dboid(dbid))
			ereport(ERROR, (errmsg("[diskquota launcher] start worker process of database(oid:%u) failed", dbid)));
		num++;

		/*
		 * diskquota only supports to monitor at most MAX_NUM_MONITORED_DB
		 * databases
		 */
		if (num >= MAX_NUM_MONITORED_DB)
		{
			ereport(LOG, (errmsg("[diskquota launcher] diskquota monitored database limit is reached, database(oid:%u) will not enable diskquota", dbid)));
			break;
		}
	}
	num_db = num;
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* TODO: clean invalid database */
}

/*
 * This function is called by launcher process to handle message from other backend
 * processes which call CREATE/DROP EXTENSION diskquota; It must be able to catch errors,
 * and return an error code back to the backend process.
 */
static void
process_extension_ddl_message()
{
	MessageResult code = ERR_UNKNOWN;
	ExtensionDDLMessage local_extension_ddl_message;

	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
	memcpy(&local_extension_ddl_message, extension_ddl_message, sizeof(ExtensionDDLMessage));
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);

	/* create/drop extension message must be valid */
	if (local_extension_ddl_message.req_pid == 0 || local_extension_ddl_message.launcher_pid != MyProcPid)
		return;

	ereport(LOG, (errmsg("[diskquota launcher]: received create/drop extension diskquota message")));

	do_process_extension_ddl_message(&code, local_extension_ddl_message);

	/* Send createdrop extension diskquota result back to QD */
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	memset(extension_ddl_message, 0, sizeof(ExtensionDDLMessage));
	extension_ddl_message->launcher_pid = MyProcPid;
	extension_ddl_message->result = (int) code;
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
}


/*
 * Process 'create extension' and 'drop extension' message.
 * For 'create extension' message, store dbid into table
 * 'database_list' and start the diskquota worker process.
 * For 'drop extension' message, remove dbid from table
 * 'database_list' and stop the diskquota worker process.
 */
static void
do_process_extension_ddl_message(MessageResult * code, ExtensionDDLMessage local_extension_ddl_message)
{
	int			old_num_db = num_db;
	bool		connected = false;
	bool		pushed_active_snap = false;
	bool		ret = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota launcher process should
	 * tolerate this kind of errors.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to connect to execute internal query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;

		switch (local_extension_ddl_message.cmd)
		{
			case CMD_CREATE_EXTENSION:
				on_add_db(local_extension_ddl_message.dbid, code);
				num_db++;
				*code = ERR_OK;
				break;
			case CMD_DROP_EXTENSION:
				on_del_db(local_extension_ddl_message.dbid, code);
				num_db--;
				*code = ERR_OK;
				break;
			default:
				ereport(LOG, (errmsg("[diskquota launcher]:received unsupported message cmd=%d", local_extension_ddl_message.cmd)));
				*code = ERR_UNKNOWN;
				break;
		}
	}
	PG_CATCH();
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		num_db = old_num_db;
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	if (connected)
		SPI_finish();
	if (pushed_active_snap)
		PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();
}

/*
 * Handle create extension diskquota
 * if we know the exact error which caused failure,
 * we set it, and error out
 */
static void
on_add_db(Oid dbid, MessageResult * code)
{
	if (num_db >= MAX_NUM_MONITORED_DB)
	{
		*code = ERR_EXCEED;
		ereport(ERROR, (errmsg("[diskquota launcher] too many databases to monitor")));
	}
	if (!is_valid_dbid(dbid))
	{
		*code = ERR_INVALID_DBID;
		ereport(ERROR, (errmsg("[diskquota launcher] invalid database oid")));
	}

	/*
	 * add dbid to diskquota_namespace.database_list set *code to
	 * ERR_ADD_TO_DB if any error occurs
	 */
	PG_TRY();
	{
		add_dbid_to_database_list(dbid);
	}
	PG_CATCH();
	{
		*code = ERR_ADD_TO_DB;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!start_worker_by_dboid(dbid))
	{
		*code = ERR_START_WORKER;
		ereport(ERROR, (errmsg("[diskquota launcher] failed to start worker - dbid=%u", dbid)));
	}
}

/*
 * Handle message: drop extension diskquota
 * do our best to:
 * 1. kill the associated worker process
 * 2. delete dbid from diskquota_namespace.database_list
 * 3. invalidate black-map entries from shared memory
 */
static void
on_del_db(Oid dbid, MessageResult * code)
{
	if (!is_valid_dbid(dbid))
	{
		*code = ERR_INVALID_DBID;
		ereport(ERROR, (errmsg("[diskquota launcher] invalid database oid")));
	}

	/* tell postmaster to stop this bgworker */
	try_kill_db_worker(dbid);

	/*
	 * delete dbid from diskquota_namespace.database_list set *code to
	 * ERR_DEL_FROM_DB if any error occurs
	 */
	PG_TRY();
	{
		del_dbid_from_database_list(dbid);
	}
	PG_CATCH();
	{
		*code = ERR_DEL_FROM_DB;
		PG_RE_THROW();
	}
	PG_END_TRY();

}

/*
 * Add the database id into table 'database_list' in
 * database 'diskquota' to store the diskquota enabled
 * database info.
 */
static void
add_dbid_to_database_list(Oid dbid)
{
	StringInfoData str;
	int			ret;

	initStringInfo(&str);
	appendStringInfo(&str, "insert into diskquota_namespace.database_list values(%u);", dbid);

	/* errors will be cached in outer function */
	ret = SPI_execute(str.data, false, 0);
	if (ret != SPI_OK_INSERT)
	{
		ereport(ERROR, (errmsg("[diskquota launcher] SPI_execute sql:'%s', errno:%d", str.data, errno)));
	}
	return;
}

/*
 * Delete database id from table 'database_list' in
 * database 'diskquota'.
 */
static void
del_dbid_from_database_list(Oid dbid)
{
	StringInfoData str;
	int			ret;

	initStringInfo(&str);
	appendStringInfo(&str, "delete from diskquota_namespace.database_list where dbid=%u;", dbid);

	/* errors will be cached in outer function */
	ret = SPI_execute(str.data, false, 0);
	if (ret != SPI_OK_DELETE)
	{
		ereport(ERROR, (errmsg("[diskquota launcher] SPI_execute sql:'%s', errno:%d", str.data, errno)));
	}
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
		if (handle)
		{
			TerminateBackgroundWorker(handle);
			pfree(handle);
		}
	}
}

/*
 * When launcher exits, it should also terminate all the workers.
 */
static void
terminate_all_workers(void)
{
	DiskQuotaWorkerEntry *hash_entry;
	HASH_SEQ_STATUS iter;


	hash_seq_init(&iter, disk_quota_worker_map);

	/*
	 * terminate the worker processes. since launcher will exit immediately,
	 * we skip to clear the disk_quota_worker_map
	 */
	while ((hash_entry = hash_seq_search(&iter)) != NULL)
	{
		if (hash_entry->handle)
			TerminateBackgroundWorker(hash_entry->handle);
	}
}

/*
 * Dynamically launch an disk quota worker process.
 * This function is called when laucher process receive
 * a 'create extension diskquota' message.
 */
static bool
start_worker_by_dboid(Oid dbid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	MemoryContext old_ctx;
	char	   *dbname;
	pid_t		pid;
	bool		found;
	bool		ret;
	DiskQuotaWorkerEntry *workerentry;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	/*
	 * diskquota worker should not restart by bgworker framework. If
	 * postmaster reset, all the bgworkers will be terminated and diskquota
	 * launcher is restarted by postmaster. All the diskquota workers should
	 * be started by launcher process again.
	 */
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
	ret = RegisterDynamicBackgroundWorker(&worker, &handle);
	MemoryContextSwitchTo(old_ctx);
	if (!ret)
		return false;
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

	return true;
}

/*
 * Check whether db oid is valid.
 */
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
