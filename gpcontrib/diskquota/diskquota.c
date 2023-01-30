/* -------------------------------------------------------------------------
 *
 * diskquota.c
 *
 * Diskquota is used to limit the amount of disk space that a schema or a role
 * can use. Diskquota is based on background worker framework. It contains a
 * launcher process which is responsible for starting/refreshing the diskquota
 * worker processes which monitor given databases.
 *
 * Copyright (c) 2018-2020 Pivotal Software, Inc.
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates
 *
 * IDENTIFICATION
 *		diskquota/diskquota.c
 *
 * -------------------------------------------------------------------------
 */
#include "diskquota.h"
#include "gp_activetable.h"

#include "postgres.h"

#include "funcapi.h"
#include "pgstat.h"
#include "access/xact.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/sinval.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define DISKQUOTA_DB "diskquota"
#define DISKQUOTA_APPLICATION_NAME "gp_reserved_gpdiskquota"

/* clang-format off */
#if !defined(DISKQUOTA_VERSION) || \
	!defined(DISKQUOTA_MAJOR_VERSION) || \
	!defined(DISKQUOTA_PATCH_VERSION) || \
	!defined(DISKQUOTA_MINOR_VERSION) || \
	!defined(DISKQUOTA_BINARY_NAME)
	#error Version not found. Please check if the VERSION file exists.
#endif
/* clang-format on */

#include <unistd.h>                 // for useconds_t
extern int usleep(useconds_t usec); // in <unistd.h>

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup  = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;
static volatile sig_atomic_t got_sigusr2 = false;

/* GUC variables */
int  diskquota_naptime            = 0;
int  diskquota_max_active_tables  = 0;
int  diskquota_worker_timeout     = 60; /* default timeout is 60 seconds */
bool diskquota_hardlimit          = false;
int  diskquota_max_workers        = 10;
int  diskquota_max_table_segments = 0;

DiskQuotaLocks       diskquota_locks;
ExtensionDDLMessage *extension_ddl_message = NULL;

// Only access in diskquota worker, different from each worker.
// a pointer to DiskquotaLauncherShmem->workerEntries in shared memory
static DiskQuotaWorkerEntry *volatile MyWorkerInfo = NULL;

// how many database diskquota are monitoring on
static int num_db = 0;

/* how many TableSizeEntry are maintained in all the table_size_map in shared memory*/
pg_atomic_uint32 *diskquota_table_size_entry_num;

static DiskquotaLauncherShmemStruct *DiskquotaLauncherShmem;

#define MIN_SLEEPTIME 100 /* milliseconds */

/*
 * bgworker handles, in launcher local memory,
 * bgworker_handles[i] is the handle of DiskquotaLauncherShmem-><hidden memory space>[i]
 * the actually useable reference is DiskquotaLauncherShmem->{freeWorkers, runningWorkers}
 *
 * size: GUC diskquota_max_workers
 */
BackgroundWorkerHandle **bgworker_handles;

typedef enum
{
	SUCCESS,
	INVALID_DB,
	NO_FREE_WORKER,
	UNKNOWN,
} StartWorkerState;
/* functions of disk quota*/
void _PG_init(void);
void _PG_fini(void);
void disk_quota_worker_main(Datum);
void disk_quota_launcher_main(Datum);

static void             disk_quota_sigterm(SIGNAL_ARGS);
static void             disk_quota_sighup(SIGNAL_ARGS);
static void             define_guc_variables(void);
static StartWorkerState start_worker(DiskquotaDBEntry *dbEntry);
static void             create_monitor_db_table(void);
static void             add_dbid_to_database_list(Oid dbid);
static void             del_dbid_from_database_list(Oid dbid);
static void             process_extension_ddl_message(void);
static void do_process_extension_ddl_message(MessageResult *code, ExtensionDDLMessage local_extension_ddl_message);
static void terminate_all_workers(void);
static void on_add_db(Oid dbid, MessageResult *code);
static void on_del_db(Oid dbid, MessageResult *code);
static bool is_valid_dbid(Oid dbid);
extern void invalidate_database_rejectmap(Oid dbid);
static void FreeWorkerOnExit(int code, Datum arg);
static void FreeWorker(DiskQuotaWorkerEntry *worker);
static void init_database_list(void);
static DiskquotaDBEntry       *next_db(DiskquotaDBEntry *curDB);
static DiskQuotaWorkerEntry   *next_worker(void);
static DiskquotaDBEntry       *add_db_entry(Oid dbid);
static void                    release_db_entry(Oid dbid);
static char                   *get_db_name(Oid dbid);
static void                    reset_worker(DiskQuotaWorkerEntry *dq_worker);
static void                    vacuum_db_entry(DiskquotaDBEntry *db);
static void                    init_bgworker_handles(void);
static BackgroundWorkerHandle *get_bgworker_handle(uint32 worker_id);
static void                    free_bgworker_handle(uint32 worker_id);
#if GP_VERSION_NUM < 70000
/* WaitForBackgroundWorkerShutdown is copied from gpdb7 */
static BgwHandleStatus WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle);
#endif /* GP_VERSION_NUM */

/*
 * diskquota_launcher_shmem_size
 *		Compute space needed for diskquota launcher related shared memory
 */
Size
diskquota_launcher_shmem_size(void)
{
	Size size;

	size = MAXALIGN(sizeof(DiskquotaLauncherShmemStruct));
	size = add_size(size, mul_size(diskquota_max_workers,
	                               sizeof(struct DiskQuotaWorkerEntry))); // hidden memory for DiskQuotaWorkerEntry
	size = add_size(size, mul_size(MAX_NUM_MONITORED_DB, sizeof(struct DiskquotaDBEntry))); // hidden memory for dbArray
	return size;
}
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
	/* diskquota.so must be in shared_preload_libraries to init SHM. */
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("[diskquota] booting " DISKQUOTA_VERSION ", but " DISKQUOTA_BINARY_NAME
		                       " not in shared_preload_libraries. abort.")));
	}
	else
	{
		ereport(INFO, (errmsg("booting diskquota-" DISKQUOTA_VERSION)));
	}

	BackgroundWorker worker;
	memset(&worker, 0, sizeof(BackgroundWorker));

	/* values are used in later calls */
	define_guc_variables();

	init_disk_quota_shmem();
	init_disk_quota_enforcement();
	init_active_table_hook();

	/* start disk quota launcher only on master */
	if (!IS_QUERY_DISPATCHER())
	{
		return;
	}

	/* set up common data for diskquota launcher worker */
	worker.bgw_flags      = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/* launcher process should be restarted after pm reset. */
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, DISKQUOTA_BINARY_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "disk_quota_launcher_main");
	worker.bgw_notify_pid = 0;

	snprintf(worker.bgw_name, BGW_MAXLEN, "[diskquota] - launcher");

	RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{}

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
 */
static void
disk_quota_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc) SetLatch(&MyProc->procLatch);

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
	int save_errno = errno;

	got_sighup = true;
	if (MyProc) SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGUSR1
 * Set a flag to tell the launcher to handle extension ddl message
 */
static void
disk_quota_sigusr1(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigusr1 = true;

	if (MyProc) SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGUSR2
 * Set a flag to tell the launcher to handle extension ddl message
 */
static void
disk_quota_sigusr2(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigusr2 = true;

	if (MyProc) SetLatch(&MyProc->procLatch);

	errno = save_errno;
}
/*
 * Define GUC variables used by diskquota
 */
static void
define_guc_variables(void)
{
#if DISKQUOTA_DEBUG
	const int min_naptime = 0;
#else
	const int min_naptime = 1;
#endif

	DefineCustomIntVariable("diskquota.naptime", "Duration between each check (in seconds).", NULL, &diskquota_naptime,
	                        2, min_naptime, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("diskquota.max_active_tables", "Max number of active tables monitored by disk-quota.", NULL,
	                        &diskquota_max_active_tables, 300 * 1024, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("diskquota.worker_timeout", "Duration between each check (in seconds).", NULL,
	                        &diskquota_worker_timeout, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("diskquota.hard_limit", "Set this to 'on' to enable disk-quota hardlimit.", NULL,
	                         &diskquota_hardlimit, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable(
	        "diskquota.max_workers",
	        "Max number of backgroud workers to run diskquota extension, should be less than max_worker_processes.",
	        NULL, &diskquota_max_workers, 10, 1, 20, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("diskquota.max_table_segments", "Max number of tables segments on the cluster.", NULL,
	                        &diskquota_max_table_segments, 10 * 1024 * 1024,
	                        INIT_NUM_TABLE_SIZE_ENTRIES * MAX_NUM_MONITORED_DB, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL,
	                        NULL);
}

/* ---- Functions for disk quota worker process ---- */

/*
 * Disk quota worker process will refresh disk quota model periodically.
 * Refresh logic is defined in quotamodel.c
 */
void
disk_quota_worker_main(Datum main_arg)
{
	char *dbname = MyBgworkerEntry->bgw_name;

	MyProcPort                = (Port *)calloc(1, sizeof(Port));
	MyProcPort->database_name = dbname; /* To show the database in the log */

	/* Disable ORCA to avoid fallback */
	optimizer = false;

	ereport(DEBUG1, (errmsg("[diskquota] start disk quota worker process to monitor database:%s", dbname)));
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);
	pqsignal(SIGUSR1, disk_quota_sigusr1);

	MyWorkerInfo = (DiskQuotaWorkerEntry *)DatumGetPointer(MyBgworkerEntry->bgw_main_arg);
	Assert(MyWorkerInfo != NULL);
	if (!MyWorkerInfo->dbEntry->inited)
		ereport(LOG, (errmsg("[diskquota] start disk quota worker process to monitor database:%s", dbname)));
	/*
	 * The shmem exit hook is registered after registering disk_quota_sigterm.
	 * So if the SIGTERM arrives before this statement, the shmem exit hook
	 * won't be called.
	 *
	 * TODO: launcher to free the unused worker?
	 */
	on_shmem_exit(FreeWorkerOnExit, 0);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

#if GP_VERSION_NUM < 70000
	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL);
	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true,
	                  0);
#else
	BackgroundWorkerInitializeConnection(dbname, NULL, 0);
	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true,
	                  0, true);
#endif /* GP_VERSION_NUM */

	/* diskquota worker should has Gp_role as dispatcher */
	Gp_role = GP_ROLE_DISPATCH;

	/*
	 * Initialize diskquota related local hash map and refresh model
	 * immediately
	 */
	init_disk_quota_model(MyWorkerInfo->dbEntry->id);

	// FIXME: version check should be run for each starting bgworker?
	//  check current binary version and SQL DLL version are matched
	int times = 0;
	while (!got_sigterm)
	{
		CHECK_FOR_INTERRUPTS();

		int major = -1, minor = -1;
		int has_error = worker_spi_get_extension_version(&major, &minor) != 0;

		if (major == DISKQUOTA_MAJOR_VERSION && minor == DISKQUOTA_MINOR_VERSION) break;
#if GP_VERSION_NUM < 70000
		/* MemoryAccount has been removed on gpdb7 */
		MemoryAccounting_Reset();
#endif /* GP_VERSION_NUM */
		if (has_error)
		{
			static char _errfmt[] = "find issues in pg_class.pg_extension check server log. waited %d seconds",
			            _errmsg[sizeof(_errfmt) + sizeof("2147483647" /* INT_MAX */) + 1] = {};
			snprintf(_errmsg, sizeof(_errmsg), _errfmt, times * diskquota_naptime);

			init_ps_display("bgworker:", "[diskquota]", dbname, _errmsg);
		}
		else
		{
			init_ps_display("bgworker:", "[diskquota]", dbname,
			                "v" DISKQUOTA_VERSION " is not matching with current SQL. stop working");
		}

		ereportif(!has_error && times == 0, WARNING,
		          (errmsg("[diskquota] worker for \"%s\" detected the installed version is \"%d.%d\", "
		                  "but current version is %s. abort due to version not match",
		                  dbname, major, minor, DISKQUOTA_VERSION),
		           errhint("run alter extension diskquota update to \"%d.%d\"", DISKQUOTA_MAJOR_VERSION,
		                   DISKQUOTA_MINOR_VERSION)));

		int rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
		                            diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[diskquota] bgworker for \"%s\" is being terminated by postmaster death.", dbname)));
			proc_exit(-1);
		}

		times++;
	}

	/*
	 * Set ps display name of the worker process of diskquota, so we can
	 * distinguish them quickly. Note: never mind parameter name of the
	 * function `init_ps_display`, we only want the ps name looks like
	 * 'bgworker: [diskquota] <dbname> ...'
	 */
	init_ps_display("bgworker:", "[diskquota]", dbname, "");

	/* suppose the database is ready, if not, then set it to false */
	bool is_ready = true;
	/* Waiting for diskquota state become ready */
	while (!got_sigterm)
	{
		int rc;
		/* If the database has been inited before, no need to check the ready state again */
		if (MyWorkerInfo->dbEntry->inited) break;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Check whether the state is in ready mode. The state would be
		 * unknown, when you `create extension diskquota` at the first time.
		 * After running UDF init_table_size_table() The state will changed to
		 * be ready.
		 */
		if (check_diskquota_state_is_ready())
		{
			is_ready = true;
			break;
		}

#if GP_VERSION_NUM < 70000
		MemoryAccounting_Reset();
#endif /* GP_VERSION_NUM */
		if (is_ready)
		{
			update_monitordb_status(MyWorkerInfo->dbEntry->dbid, DB_UNREADY);
			is_ready = false;
		}
		rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
		                        diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		// be nice to scheduler when naptime == 0 and diskquota_is_paused() == true
		if (!diskquota_naptime) usleep(1);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[diskquota] bgworker for \"%s\" is being terminated by postmaster death.", dbname)));
			proc_exit(1);
		}

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	if (!MyWorkerInfo->dbEntry->inited) update_monitordb_status(MyWorkerInfo->dbEntry->dbid, DB_RUNNING);
	bool is_gang_destroyed = false;
	while (!got_sigterm)
	{
		int rc;
#ifdef DISKQUOTA_FAULT_INJECTOR
		SIMPLE_FAULT_INJECTOR("diskquota_worker_main");
#endif
		if (!diskquota_is_paused())
		{
			/* Refresh quota model with init mode */
			refresh_disk_quota_model(!MyWorkerInfo->dbEntry->inited);
			MyWorkerInfo->dbEntry->inited = true;
			is_gang_destroyed             = false;
		}
		else if (!is_gang_destroyed)
		{
			DisconnectAndDestroyAllGangs(false);
			is_gang_destroyed = true;
		}
		worker_increase_epoch(MyWorkerInfo->dbEntry->dbid);

		// GPDB6 opend a MemoryAccount for us without asking us.
		// and GPDB6 did not release the MemoryAccount after SPI finish.
		// Reset the MemoryAccount although we never create it.
#if GP_VERSION_NUM < 70000
		MemoryAccounting_Reset();
#endif /* GP_VERSION_NUM */
		if (DiskquotaLauncherShmem->isDynamicWorker)
		{
			break;
		}
		CHECK_FOR_INTERRUPTS();

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
		                        diskquota_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		// be nice to scheduler when naptime == 0 and diskquota_is_paused() == true
		if (!diskquota_naptime) usleep(1);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[diskquota] bgworker for \"%s\" is being terminated by postmaster death.", dbname)));
			proc_exit(1);
		}

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	if (got_sigterm)
		ereport(LOG, (errmsg("[diskquota] stop disk quota worker process to monitor database:%s", dbname)));
	ereport(DEBUG1, (errmsg("[diskquota] stop disk quota worker process to monitor database:%s", dbname)));
#if DISKQUOTA_DEBUG
	long secs;
	int  usecs;
	TimestampDifference(MyWorkerInfo->dbEntry->last_run_time, GetCurrentTimestamp(), &secs, &usecs);
	MyWorkerInfo->dbEntry->cost = secs * 1000L + usecs / 1000L;
#endif
	proc_exit(0);
}

static inline bool
isAbnormalLoopTime(int diff_sec)
{
	int max_time;
	if (diskquota_naptime > 6)
		max_time = diskquota_naptime * 2;
	else
		max_time = diskquota_naptime + 6;
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
	time_t loop_begin, loop_end;

	/* the launcher should exit when the master boots in utility mode */
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		proc_exit(0);
	}

	MemoryContextSwitchTo(TopMemoryContext);
	init_bgworker_handles();

	/* establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, disk_quota_sighup);
	pqsignal(SIGTERM, disk_quota_sigterm);
	pqsignal(SIGUSR1, disk_quota_sigusr1);
	pqsignal(SIGUSR2, disk_quota_sigusr2);
	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	extension_ddl_message->launcher_pid = MyProcPid;
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);
	/*
	 * connect to our database 'diskquota'. launcher process will exit if
	 * 'diskquota' database is not existed.
	 */

#if GP_VERSION_NUM < 70000
	/* Connect to our database */
	BackgroundWorkerInitializeConnection(DISKQUOTA_DB, NULL);
	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true,
	                  0);
#else
	BackgroundWorkerInitializeConnection(DISKQUOTA_DB, NULL, 0);
	set_config_option("application_name", DISKQUOTA_APPLICATION_NAME, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true,
	                  0, true);
#endif /* GP_VERSION_NUM */

	/*
	 * use table diskquota_namespace.database_list to store diskquota enabled
	 * database.
	 */
	create_monitor_db_table();

	init_database_list();
	DisconnectAndDestroyAllGangs(false);

	loop_end = time(NULL);

	struct timeval nap;
	nap.tv_sec  = diskquota_naptime;
	nap.tv_usec = 0;
	/* main loop: do this until the SIGTERM handler tells us to terminate. */
	ereport(LOG, (errmsg("[diskquota launcher] start main loop")));
	DiskquotaDBEntry *curDB          = NULL;
	Oid               curDBId        = 0;
	bool              advance_one_db = true;
	bool              timeout        = false;
	int               try_times      = 0;
	while (!got_sigterm)
	{
		int rc;
		CHECK_FOR_INTERRUPTS();
		/* pick a db to run */
		if (advance_one_db)
		{
			curDB     = next_db(curDB);
			timeout   = false;
			try_times = 0;
			if (curDB != NULL)
			{
				curDBId = curDB->dbid;
				elog(DEBUG1, "[diskquota] next db to run:%u", curDBId);
			}
			else
				elog(DEBUG1, "[diskquota] no db to run");
		}
		/*
		 * Modify wait time
		 *
		 * If there is no db needed to run or has exceeded the next_run_time,
		 * just sleep to wait a db or a free worker.
		 *
		 * Otherwise check the next_run_time to determin how much time to wait
		 */
		if (timeout || curDB == NULL)
		{
			nap.tv_sec  = diskquota_naptime > 0 ? diskquota_naptime : 1;
			nap.tv_usec = 0;
		}
		else
		{
			TimestampTz curTime = GetCurrentTimestamp();
			long        sec;
			int         usec;
			TimestampDifference(curTime, curDB->next_run_time, &sec, &usec);
			nap.tv_sec  = sec;
			nap.tv_usec = usec;

			/* if the sleep time is too short, just skip the sleeping */
			if (nap.tv_sec == 0 && nap.tv_usec < MIN_SLEEPTIME * 1000L)
			{
				nap.tv_usec = 0;
			}

			/* if the sleep time is too long, advance the next_run_time */
			if (nap.tv_sec > diskquota_naptime)
			{
				nap.tv_sec           = diskquota_naptime;
				nap.tv_usec          = 0;
				curDB->next_run_time = TimestampTzPlusMilliseconds(curTime, diskquota_naptime * 1000L);
			}
		}

		bool sigusr1 = false;
		bool sigusr2 = false;

		/*
		 * background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */

		if (nap.tv_sec != 0 || nap.tv_usec != 0)
		{
			elog(DEBUG1, "[diskquota] naptime sec:%ld, usec:%ld", nap.tv_sec, nap.tv_usec);
			rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
			                        (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L));
			ResetLatch(&MyProc->procLatch);

			/* Emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
			{
				ereport(LOG, (errmsg("[diskquota launcher] launcher is being terminated by postmaster death.")));
				proc_exit(1);
			}
		}
		/* process extension ddl message */
		if (got_sigusr2)
		{
			elog(DEBUG1, "[diskquota] got sigusr2");
			got_sigusr2 = false;
			process_extension_ddl_message();
			sigusr2 = true;
		}

		/* in case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			elog(DEBUG1, "[diskquota] got sighup");
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * When the bgworker for diskquota worker starts or stops,
		 * postmsater prosess will send sigusr1 to launcher as
		 * worker.bgw_notify_pid has been set to launcher pid.
		 */
		if (got_sigusr1)
		{
			elog(DEBUG1, "[diskquota] got sigusr1");
			got_sigusr1 = false;
			sigusr1     = true;
		}

		/*
		 * Try to starts a bgworker for the curDB
		 *
		 */

		/*
		 * When db list is empty, curDB is NULL.
		 * When curDB->in_use is false means dbEtnry has been romoved
		 * When curDB->dbid doesn't equtal curDBId, it means the slot has
		 * been used by another db
		 *
		 * For the above conditions, we just skip this loop and try to fetch
		 * next db to run.
		 */
		if (curDB == NULL || !curDB->in_use || curDB->dbid != curDBId)
		{
			advance_one_db = true;
			continue;
		}

		/*
		 * Try to start a worker to run the db if has exceeded the next_run_time.
		 * if start_worker fails, advance_one_db will be set to false, so in the
		 * next loop will run the db again.
		 */
		if (TimestampDifferenceExceeds(curDB->next_run_time, GetCurrentTimestamp(), MIN_SLEEPTIME))
		{
			StartWorkerState ret = start_worker(curDB);
			/* when start_worker successfully or db is invalid, pick up next db to run */
			advance_one_db = (ret == SUCCESS || ret == INVALID_DB) ? true : false;
			if (!advance_one_db)
			{
				/* has exceeded the next_run_time of current db */
				timeout = true;
				/* when start_worker return is not 2(no free worker), increase the try_times*/
				if (ret != NO_FREE_WORKER) try_times++;
				/* only try to start bgworker for a database at most 3 times */
				if (try_times >= 3) advance_one_db = true;
			}
		}
		else
		{
			advance_one_db = false;
		}

		loop_begin = loop_end;
		loop_end   = time(NULL);
		if (isAbnormalLoopTime(loop_end - loop_begin))
		{
			ereport(WARNING, (errmsg("[diskquota launcher] loop takes too much time %d/%d",
			                         (int)(loop_end - loop_begin), diskquota_naptime)));
		}
	}

	/* terminate all the diskquota worker processes before launcher exit */
	ereport(LOG, (errmsg("[diskquota launcher] launcher is being terminated by SIGTERM.")));
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
	bool        connected          = false;
	bool        pushed_active_snap = false;
	bool        ret                = true;

	/*
	 * Create function diskquota.diskquota_fetch_table_stat in launcher
	 * We need this function to distribute dbid to segments when creating
	 * a diskquota extension.
	 */
	sql = "create schema if not exists diskquota_namespace;"
	      "create table if not exists diskquota_namespace.database_list(dbid oid not null unique);"
	      "DROP SCHEMA IF EXISTS " LAUNCHER_SCHEMA
	      " CASCADE;"
	      "CREATE SCHEMA " LAUNCHER_SCHEMA
	      ";"
	      "CREATE TYPE " LAUNCHER_SCHEMA
	      ".diskquota_active_table_type AS (TABLE_OID oid, TABLE_SIZE int8, GP_SEGMENT_ID "
	      "smallint);"
	      "CREATE FUNCTION " LAUNCHER_SCHEMA ".diskquota_fetch_table_stat(int4, oid[]) RETURNS setof " LAUNCHER_SCHEMA
	      ".diskquota_active_table_type AS '$libdir/" DISKQUOTA_BINARY_NAME
	      ".so', 'diskquota_fetch_table_stat' LANGUAGE C VOLATILE;";

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota launcher process should
	 * tolerate this kind of errors.
	 */
	PG_TRY();
	{
		int ret_code = SPI_connect();
		if (ret_code != SPI_OK_CONNECT)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			                errmsg("[diskquota launcher] unable to connect to execute internal query. return code: %d.",
			                       ret_code)));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;

		/* debug_query_string need to be set for SPI_execute utility functions. */
		debug_query_string = sql;

		ret_code = SPI_execute(sql, false, 0);
		if (ret_code != SPI_OK_UTILITY)
		{
			int saved_errno = errno;
			ereport(ERROR, (errmsg("[diskquota launcher] SPI_execute error, sql: \"%s\", reason: %s, ret_code: %d.",
			                       sql, strerror(saved_errno), ret_code)));
		}
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret                = false;
		debug_query_string = NULL;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected) SPI_finish();
	if (pushed_active_snap) PopActiveSnapshot();
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
init_database_list(void)
{
	TupleDesc tupdesc;
	int       num = 0;
	int       ret;
	int       i;

	/*
	 * Don't catch errors in start_workers_from_dblist. Since this is the
	 * startup worker for diskquota launcher. If error happens, we just let
	 * launcher exits.
	 */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		int saved_errno = errno;
		ereport(ERROR, (errmsg("[diskquota launcher] SPI connect error, reason: %s, return code: %d.", strerror(saved_errno), ret)));
	}
	ret = SPI_execute("select dbid from diskquota_namespace.database_list;", true, 0);
	if (ret != SPI_OK_SELECT)
	{
		int saved_errno = errno;
		ereport(ERROR,
		        (errmsg("[diskquota launcher] 'select diskquota_namespace.database_list', reason: %s, return code: %d.",
				strerror(saved_errno),
		                ret)));
	}
	tupdesc = SPI_tuptable->tupdesc;
#if GP_VERSION_NUM < 70000
	if (tupdesc->natts != 1 || tupdesc->attrs[0]->atttypid != OIDOID)
	{
		ereport(LOG, (errmsg("[diskquota launcher], natts/atttypid: %d.",
		                     tupdesc->natts != 1 ? tupdesc->natts : tupdesc->attrs[0]->atttypid)));
		ereport(ERROR, (errmsg("[diskquota launcher] table database_list corrupt, launcher will exit. natts: ")));
	}
#else

	if (tupdesc->natts != 1 || tupdesc->attrs[0].atttypid != OIDOID)
	{
		ereport(LOG, (errmsg("[diskquota launcher], natts/atttypid: %d.",
		                     tupdesc->natts != 1 ? tupdesc->natts : tupdesc->attrs[0].atttypid)));
		ereport(ERROR, (errmsg("[diskquota launcher] table database_list corrupt, launcher will exit. natts: ")));
	}
#endif /* GP_VERSION_NUM */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple         tup;
		Oid               dbid;
		Datum             dat;
		bool              isnull;
		DiskquotaDBEntry *dbEntry;

		tup = SPI_tuptable->vals[i];
		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull) ereport(ERROR, (errmsg("[diskquota launcher] dbid cann't be null in table database_list")));
		dbid = DatumGetObjectId(dat);
		if (!is_valid_dbid(dbid))
		{
			ereport(LOG, (errmsg("[diskquota launcher] database(oid:%u) in table database_list is not a valid database",
			                     dbid)));
			continue;
		}
		dbEntry = add_db_entry(dbid);
		if (dbEntry == NULL) continue;
		num++;
		/*
		 * diskquota only supports to monitor at most MAX_NUM_MONITORED_DB
		 * databases
		 */
		if (num >= MAX_NUM_MONITORED_DB)
		{
			ereport(LOG, (errmsg("[diskquota launcher] diskquota monitored database limit is reached, database(oid:%u) "
			                     "will not enable diskquota",
			                     dbid)));
			break;
		}
	}
	num_db = num;
	/* As update_monitor_db_mpp needs to execute sql, so can not put in the loop above */
	for (int i = 0; i < MAX_NUM_MONITORED_DB; i++)
	{
		DiskquotaDBEntry *dbEntry = &DiskquotaLauncherShmem->dbArray[i];
		if (dbEntry->in_use)
		{
			update_monitor_db_mpp(dbEntry->dbid, ADD_DB_TO_MONITOR, LAUNCHER_SCHEMA);
		}
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	/* TODO: clean invalid database */
	if (num_db > diskquota_max_workers) DiskquotaLauncherShmem->isDynamicWorker = true;
}

/*
 * This function is called by launcher process to handle message from other backend
 * processes which call CREATE/DROP EXTENSION diskquota; It must be able to catch errors,
 * and return an error code back to the backend process.
 */
static void
process_extension_ddl_message()
{
	MessageResult       code = ERR_UNKNOWN;
	ExtensionDDLMessage local_extension_ddl_message;

	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_SHARED);
	memcpy(&local_extension_ddl_message, extension_ddl_message, sizeof(ExtensionDDLMessage));
	LWLockRelease(diskquota_locks.extension_ddl_message_lock);

	/* create/drop extension message must be valid */
	if (local_extension_ddl_message.req_pid == 0 || local_extension_ddl_message.launcher_pid != MyProcPid) return;

	ereport(LOG,
	        (errmsg("[diskquota launcher]: received create/drop extension diskquota message, extension launcher")));

	do_process_extension_ddl_message(&code, local_extension_ddl_message);
#if GP_VERSION_NUM < 70000
	MemoryAccounting_Reset();
#endif /* GP_VERSION_NUM */

	/* Send createdrop extension diskquota result back to QD */
	LWLockAcquire(diskquota_locks.extension_ddl_message_lock, LW_EXCLUSIVE);
	memset(extension_ddl_message, 0, sizeof(ExtensionDDLMessage));
	extension_ddl_message->launcher_pid = MyProcPid;
	extension_ddl_message->result       = (int)code;
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
do_process_extension_ddl_message(MessageResult *code, ExtensionDDLMessage local_extension_ddl_message)
{
	int  old_num_db         = num_db;
	bool connected          = false;
	bool pushed_active_snap = false;
	bool ret                = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota launcher process should
	 * tolerate this kind of errors.
	 */
	PG_TRY();
	{
		int ret_code = SPI_connect();
		if (ret_code != SPI_OK_CONNECT)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			                errmsg("unable to connect to execute internal query. return code: %d.", ret_code)));
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
				if (num_db > 0) num_db--;
				*code = ERR_OK;
				break;
			default:
				ereport(LOG, (errmsg("[diskquota launcher]:received unsupported message cmd=%d",
				                     local_extension_ddl_message.cmd)));
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
		ret    = false;
		num_db = old_num_db;
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	if (connected) SPI_finish();
	if (pushed_active_snap) PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();
	/* update something in memory after transaction committed */
	if (ret)
	{
		PG_TRY();
		{
			/* update_monitor_db_mpp runs sql to distribute dbid to segments */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			pushed_active_snap = true;
			Oid dbid           = local_extension_ddl_message.dbid;
			int ret_code       = SPI_connect();
			if (ret_code != SPI_OK_CONNECT)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				                errmsg("unable to connect to execute internal query. return code: %d.", ret_code)));
			}
			switch (local_extension_ddl_message.cmd)
			{
				case CMD_CREATE_EXTENSION:
					if (num_db > diskquota_max_workers) DiskquotaLauncherShmem->isDynamicWorker = true;
					add_db_entry(dbid);
					/* TODO: how about this failed? */
					update_monitor_db_mpp(dbid, ADD_DB_TO_MONITOR, LAUNCHER_SCHEMA);
					break;
				case CMD_DROP_EXTENSION:
					if (num_db <= diskquota_max_workers) DiskquotaLauncherShmem->isDynamicWorker = false;
					/* terminate bgworker in release_db_entry rountine */
					release_db_entry(dbid);
					update_monitor_db_mpp(dbid, REMOVE_DB_FROM_BEING_MONITORED, LAUNCHER_SCHEMA);
					/* clear the out-of-quota rejectmap in shared memory */
					invalidate_database_rejectmap(dbid);
					break;
				default:
					ereport(LOG, (errmsg("[diskquota launcher]:received unsupported message cmd=%d",
					                     local_extension_ddl_message.cmd)));
					break;
			}
			SPI_finish();
			if (pushed_active_snap) PopActiveSnapshot();
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			error_context_stack = NULL;
			HOLD_INTERRUPTS();
			EmitErrorReport();
			FlushErrorState();
			RESUME_INTERRUPTS();
		}
		PG_END_TRY();
	}
	DisconnectAndDestroyAllGangs(false);
}

/*
 * Handle create extension diskquota
 * if we know the exact error which caused failure,
 * we set it, and error out
 */
static void
on_add_db(Oid dbid, MessageResult *code)
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
}

/*
 * Handle message: drop extension diskquota
 * do:
 * 1. kill the associated worker process
 * 2. delete dbid from diskquota_namespace.database_list
 * 3. invalidate reject-map entries and monitored_dbid_cache from shared memory
 */
static void
on_del_db(Oid dbid, MessageResult *code)
{
	if (!is_valid_dbid(dbid))
	{
		*code = ERR_INVALID_DBID;
		ereport(ERROR, (errmsg("[diskquota launcher] invalid database oid")));
	}

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
	int ret;

	Oid   argt[1] = {OIDOID};
	Datum argv[1] = {ObjectIdGetDatum(dbid)};

	ret = SPI_execute_with_args("select * from diskquota_namespace.database_list where dbid = $1", 1, argt, argv, NULL,
	                            true, 0);

	if (ret != SPI_OK_SELECT)
	{
		int saved_errno = errno;
		ereport(ERROR, (errmsg("[diskquota launcher] error occured while checking database_list, "
		                       " code: %d, reason: %s.",
		                       ret,
				       strerror(saved_errno))));
	}

	if (SPI_processed == 1)
	{
		ereport(WARNING, (errmsg("[diskquota launcher] database id %d is already actived, "
		                         "skip database_list update",
		                         dbid)));
		return;
	}

	ret = SPI_execute_with_args("insert into diskquota_namespace.database_list values($1)", 1, argt, argv, NULL, false,
	                            0);

	if (ret != SPI_OK_INSERT || SPI_processed != 1)
	{
		int saved_errno = errno;
		ereport(ERROR, (errmsg("[diskquota launcher] error occured while updating database_list, "
		                       " code: %d, reason: %s.",
		                       ret,
				       strerror(saved_errno))));
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
	int ret;

	/* errors will be cached in outer function */
	ret = SPI_execute_with_args("delete from diskquota_namespace.database_list where dbid = $1", 1,
	                            (Oid[]){
	                                    OIDOID,
	                            },
	                            (Datum[]){
	                                    ObjectIdGetDatum(dbid),
	                            },
	                            NULL, false, 0);
	if (ret != SPI_OK_DELETE)
	{
		int saved_errno = errno;
		ereport(ERROR,
			(errmsg("[diskquota launcher] del_dbid_from_database_list: reason: %s, ret_code: %d.", strerror(saved_errno), ret)));
	}
}

/*
 * When launcher exits, it should also terminate all the workers.
 */
static void
terminate_all_workers(void)
{
	dlist_iter              iterdb;
	DiskQuotaWorkerEntry   *worker;
	BackgroundWorkerHandle *handle;
	LWLockAcquire(diskquota_locks.workerlist_lock, LW_SHARED);
	dlist_foreach(iterdb, &DiskquotaLauncherShmem->runningWorkers)
	{
		worker = dlist_container(DiskQuotaWorkerEntry, node, iterdb.cur);
		handle = get_bgworker_handle(worker->id);
		if (handle != NULL) TerminateBackgroundWorker(handle);
	}
	LWLockRelease(diskquota_locks.workerlist_lock);
}

/*
 * Dynamically launch an disk quota worker process.
 * This function is called when launcher process
 * schedules a database's diskquota worker to run.
 *
 * return:
 * SUCCESS means starting the bgworker sucessfully.
 * INVALID_DB means the database is invalid
 * NO_FREE_WORKER means there is no avaliable free workers
 * UNKNOWN means registering or starting the bgworker
 * failed, maybe there is no free bgworker, or
 * forking a process failed and so on.
 */

static StartWorkerState
start_worker(DiskquotaDBEntry *dbEntry)
{
	BackgroundWorker      worker;
	bool                  ret;
	DiskQuotaWorkerEntry *dq_worker;
	MemoryContext         old_ctx;
	char                 *dbname = NULL;
	int                   result = SUCCESS;

	dq_worker = next_worker();
	if (dq_worker == NULL)
	{
		elog(DEBUG1, "[diskquota] no free workers");
		result = NO_FREE_WORKER;
		return result;
	}
	/* free the BackgroundWorkerHandle used by last database */
	free_bgworker_handle(dq_worker->id);

	dbEntry->workerId  = dq_worker->id;
	dq_worker->dbEntry = dbEntry;

#if DISKQUOTA_DEBUG
	dbEntry->last_run_time = GetCurrentTimestamp();
#endif

	/* register a dynamic bgworker and wait for it to start */
	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags      = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	/*
	 * diskquota worker should not restart by bgworker framework. If
	 * postmaster reset, all the bgworkers will be terminated and diskquota
	 * launcher is restarted by postmaster. All the diskquota workers should
	 * be started by launcher process again.
	 */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, DISKQUOTA_BINARY_NAME);
	sprintf(worker.bgw_function_name, "disk_quota_worker_main");
	dbname = get_db_name(dbEntry->dbid);
	if (dbname == NULL)
	{
		result = INVALID_DB;
		goto Failed;
	}
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "%s", dbname);
	pfree(dbname);

	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg   = (Datum)PointerGetDatum(dq_worker);

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	ret     = RegisterDynamicBackgroundWorker(&worker, &(bgworker_handles[dq_worker->id]));
	MemoryContextSwitchTo(old_ctx);
	if (!ret)
	{
		elog(WARNING, "Create bgworker failed");
		result = UNKNOWN;
		goto Failed;
	}
	BgwHandleStatus status;
	pid_t           pid;
	status = WaitForBackgroundWorkerStartup(bgworker_handles[dq_worker->id], &pid);
	if (status == BGWH_STOPPED)
	{
		ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"),
		                  errhint("More details may be available in the server log.")));
		result = UNKNOWN;
		goto Failed;
	}
	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
		                  errmsg("cannot start background processes without postmaster"),
		                  errhint("Kill all remaining database processes and restart the database.")));
		result = UNKNOWN;
		goto Failed;
	}

	Assert(status == BGWH_STARTED);
	return result;
Failed:

	elog(DEBUG1, "[diskquota] diskquota, starts diskquota failed");
	FreeWorker(dq_worker);
	return result;
}

/*
 * Check whether db oid is valid.
 */
static bool
is_valid_dbid(Oid dbid)
{
	HeapTuple tuple;

	if (dbid == InvalidOid) return false;
	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
	if (!HeapTupleIsValid(tuple)) return false;
	ReleaseSysCache(tuple);
	return true;
}

static const char *
diskquota_status_check_soft_limit()
{
	// should run on coordinator only.
	Assert(IS_QUERY_DISPATCHER());

	bool           found, paused;
	MonitorDBEntry entry;
	LWLockAcquire(diskquota_locks.monitored_dbid_cache_lock, LW_SHARED);
	{
		entry  = hash_search(monitored_dbid_cache, &MyDatabaseId, HASH_FIND, &found);
		paused = found ? entry->paused : false;
	}
	LWLockRelease(diskquota_locks.monitored_dbid_cache_lock);

	// if worker no booted, aka 'CREATE EXTENSION' not called, diskquota is paused
	if (!found) return "paused";

	// if worker booted, check 'worker_map->is_paused'
	return paused ? "paused" : "on";
}

static const char *
diskquota_status_check_hard_limit()
{
	// should run on coordinator only.
	Assert(IS_QUERY_DISPATCHER());

	bool hardlimit = diskquota_hardlimit;

	bool paused = false;
	paused      = diskquota_is_paused();
	// if worker booted and 'is_paused == true' and hardlimit is enabled
	// hard limits should also paused
	if (paused && hardlimit) return "paused";

	return hardlimit ? "on" : "off";
}

static const char *
diskquota_status_binary_version()
{
	return DISKQUOTA_VERSION;
}

static const char *
diskquota_status_schema_version()
{
	static char version[64] = {0};
	memset(version, 0, sizeof(version));

	int ret = SPI_connect();
	Assert(ret = SPI_OK_CONNECT);

	ret = SPI_execute("select extversion from pg_extension where extname = 'diskquota'", true, 0);

	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		ereport(WARNING,
		        (errmsg("[diskquota] when reading installed version lines %ld code = %d", SPI_processed, ret)));
		goto out;
	}

	if (SPI_processed == 0)
	{
		goto out;
	}

	bool  is_null = false;
	Datum v       = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
	Assert(is_null == false);

	char *vv = TextDatumGetCString(v);
	if (vv == NULL)
	{
		ereport(WARNING, (errmsg("[diskquota] 'extversion' is empty in pg_class.pg_extension. may catalog corrupted")));
		goto out;
	}

	StrNCpy(version, vv, sizeof(version));

out:
	SPI_finish();
	return version;
}

PG_FUNCTION_INFO_V1(diskquota_status);
Datum
diskquota_status(PG_FUNCTION_ARGS)
{
	typedef struct Context
	{
		int index;
	} Context;

	typedef struct FeatureStatus
	{
		const char *name;
		const char *(*status)(void);
	} FeatureStatus;

	static const FeatureStatus fs[] = {
	        {.name = "soft limits", .status = diskquota_status_check_soft_limit},
	        {.name = "hard limits", .status = diskquota_status_check_hard_limit},
	        {.name = "current binary version", .status = diskquota_status_binary_version},
	        {.name = "current schema version", .status = diskquota_status_schema_version},
	};

	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();

		MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		{
			tupdesc = DiskquotaCreateTemplateTupleDesc(2);
			TupleDescInitEntry(tupdesc, 1, "name", TEXTOID, -1, 0);
			TupleDescInitEntry(tupdesc, 2, "status", TEXTOID, -1, 0);
			funcctx->tuple_desc = BlessTupleDesc(tupdesc);
			Context *context    = (Context *)palloc(sizeof(Context));
			context->index      = 0;
			funcctx->user_fctx  = context;
		}
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx          = SRF_PERCALL_SETUP();
	Context *context = (Context *)funcctx->user_fctx;

	if (context->index >= sizeof(fs) / sizeof(FeatureStatus))
	{
		SRF_RETURN_DONE(funcctx);
	}

	bool  nulls[2] = {false, false};
	Datum v[2]     = {
            DirectFunctionCall1(textin, CStringGetDatum(fs[context->index].name)),
            DirectFunctionCall1(textin, CStringGetDatum(fs[context->index].status())),
    };
	ReturnSetInfo *rsi   = (ReturnSetInfo *)fcinfo->resultinfo;
	HeapTuple      tuple = heap_form_tuple(rsi->expectedDesc, v, nulls);

	context->index++;
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

static void
FreeWorker(DiskQuotaWorkerEntry *worker)
{
	if (worker != NULL)
	{
		LWLockAcquire(diskquota_locks.dblist_lock, LW_EXCLUSIVE);
		if (worker->dbEntry != NULL)
		{
			bool in_use = worker->dbEntry->in_use;
			if (in_use && worker->dbEntry->workerId == worker->id)
			{
				worker->dbEntry->workerId = INVALID_WORKER_ID;
				worker->dbEntry->next_run_time =
				        TimestampTzPlusMilliseconds(GetCurrentTimestamp(), diskquota_naptime * 1000L);
			}
		}
		LWLockRelease(diskquota_locks.dblist_lock);
		LWLockAcquire(diskquota_locks.workerlist_lock, LW_EXCLUSIVE);
		dlist_delete(&worker->node);
		worker->dbEntry = NULL;
		dlist_push_head(&DiskquotaLauncherShmem->freeWorkers, &worker->node);
		elog(DEBUG1, "[diskquota] free worker %d", worker->id);
		LWLockRelease(diskquota_locks.workerlist_lock);
	}
}

static void
FreeWorkerOnExit(int code, Datum arg)
{
	if (MyWorkerInfo != NULL)
	{
		FreeWorker(MyWorkerInfo);
	}
}

void
init_launcher_shmem()
{
	bool found;
	DiskquotaLauncherShmem = (DiskquotaLauncherShmemStruct *)ShmemInitStruct("Diskquota launcher Data",
	                                                                         diskquota_launcher_shmem_size(), &found);
	memset(DiskquotaLauncherShmem, 0, diskquota_launcher_shmem_size());
	if (!found)
	{
		dlist_init(&DiskquotaLauncherShmem->freeWorkers);
		dlist_init(&DiskquotaLauncherShmem->runningWorkers);

		// a pointer to the start address of hidden memory
		uint8_t *hidden_memory_prt = (uint8_t *)DiskquotaLauncherShmem + MAXALIGN(sizeof(DiskquotaLauncherShmemStruct));

		// get DiskQuotaWorkerEntry from the hidden memory
		DiskQuotaWorkerEntry *worker = (DiskQuotaWorkerEntry *)hidden_memory_prt;
		hidden_memory_prt += mul_size(diskquota_max_workers, sizeof(DiskQuotaWorkerEntry));

		// get dbArray from the hidden memory
		DiskquotaDBEntry *dbArray = (DiskquotaDBEntry *)hidden_memory_prt;
		hidden_memory_prt += mul_size(MAX_NUM_MONITORED_DB, sizeof(struct DiskquotaDBEntry));

		// get the dbArrayTail from the hidden memory
		DiskquotaDBEntry *dbArrayTail = (DiskquotaDBEntry *)hidden_memory_prt;

		/* add all worker to the free worker list */
		for (int i = 0; i < diskquota_max_workers; i++)
		{
			memset(&worker[i], 0, sizeof(DiskQuotaWorkerEntry));
			worker[i].id = i;
			dlist_push_head(&DiskquotaLauncherShmem->freeWorkers, &worker[i].node);
		}

		DiskquotaLauncherShmem->dbArray     = dbArray;
		DiskquotaLauncherShmem->dbArrayTail = dbArrayTail;

		for (int i = 0; i < MAX_NUM_MONITORED_DB; i++)
		{
			memset(&DiskquotaLauncherShmem->dbArray[i], 0, sizeof(DiskquotaDBEntry));
			DiskquotaLauncherShmem->dbArray[i].id       = i;
			DiskquotaLauncherShmem->dbArray[i].workerId = INVALID_WORKER_ID;
		}
	}
	/* init TableSizeEntry counter */
	diskquota_table_size_entry_num =
	        ShmemInitStruct("diskquota TableSizeEntry counter", sizeof(pg_atomic_uint32), &found);
	if (!found) pg_atomic_init_u32(diskquota_table_size_entry_num, 0);
}

/*
 * Look for an unused slot.  If we find one, grab it.
 *
 * We always look for the slot from the lower-numbers slots
 * firstly, so that we can recycle the slots instead of using
 * the unused slots in order to recycle the shared memory
 * allocated before.
 */
static DiskquotaDBEntry *
add_db_entry(Oid dbid)
{
	DiskquotaDBEntry *result = NULL;

	LWLockAcquire(diskquota_locks.dblist_lock, LW_EXCLUSIVE);
	/* if there is already dbEntry's dbid equals dbid, returning the existing one */
	for (int i = 0; i < MAX_NUM_MONITORED_DB; i++)
	{
		DiskquotaDBEntry *dbEntry = &DiskquotaLauncherShmem->dbArray[i];
		if (!dbEntry->in_use && result == NULL)
		{
			dbEntry->dbid          = dbid;
			dbEntry->in_use        = true;
			dbEntry->next_run_time = GetCurrentTimestamp();
			result                 = dbEntry;
		}
		else if (dbEntry->in_use && dbEntry->dbid == dbid)
		{
			result = dbEntry;
			break;
		}
	}
	if (result == NULL)
		ereport(WARNING, (errmsg("[diskquota launcher] diskquota monitored database limit is reached, database(oid:%u) "
		                         "will not enable diskquota",
		                         dbid)));
	if (result != NULL) elog(DEBUG1, "[diskquota] add db entry: id: %d, %u", result->id, dbid);

	LWLockRelease(diskquota_locks.dblist_lock);
	return result;
}

static void
release_db_entry(Oid dbid)
{
	DiskquotaDBEntry *db = NULL;
	for (int i = 0; i < MAX_NUM_MONITORED_DB; i++)
	{
		DiskquotaDBEntry *dbEntry = &DiskquotaLauncherShmem->dbArray[i];
		if (dbEntry->in_use && dbEntry->dbid == dbid)
		{
			db = dbEntry;
			break;
		}
	}
	if (db == NULL)
	{
		return;
	}

	LWLockAcquire(diskquota_locks.dblist_lock, LW_EXCLUSIVE);
	if (db->workerId != INVALID_WORKER_ID)
	{
		BackgroundWorkerHandle *handle = get_bgworker_handle(db->workerId);
		TerminateBackgroundWorker(handle);
	}
	vacuum_disk_quota_model(db->id);
	/* should be called at last to set in_use to false */
	vacuum_db_entry(db);
	LWLockRelease(diskquota_locks.dblist_lock);
}

/*
 * Pick next db to run.
 * If the curDB is NULL, pick the head db to run.
 * If the dbList empty, return NULL.
 * If the picked db is in running status, skip it, pick the next one to run.
 */
static DiskquotaDBEntry *
next_db(DiskquotaDBEntry *curDB)
{
	DiskquotaDBEntry *result   = NULL;
	int               nextSlot = 0;
	if (curDB != NULL)
	{
		nextSlot = curDB->id + 1;
	}

	/*
	 * SearchSysCache should be run in a transaction
	 */
	StartTransactionCommand();
	LWLockAcquire(diskquota_locks.dblist_lock, LW_SHARED);
	for (int i = 0; i < MAX_NUM_MONITORED_DB; i++)
	{
		if (nextSlot >= MAX_NUM_MONITORED_DB) nextSlot = 0;
		DiskquotaDBEntry *dbEntry = &DiskquotaLauncherShmem->dbArray[nextSlot];
		nextSlot++;
		if (!dbEntry->in_use || dbEntry->workerId != INVALID_WORKER_ID || dbEntry->dbid == InvalidOid) continue;
		/* TODO: should release the invalid db related things */
		if (!is_valid_dbid(dbEntry->dbid)) continue;
		result = dbEntry;
		break;
	}
	LWLockRelease(diskquota_locks.dblist_lock);
	CommitTransactionCommand();
	return result;
}

static DiskQuotaWorkerEntry *
next_worker(void)
{
	DiskQuotaWorkerEntry *dq_worker = NULL;
	dlist_node           *wnode;

	/* acquire worker from worker list */
	LWLockAcquire(diskquota_locks.workerlist_lock, LW_EXCLUSIVE);
	if (dlist_is_empty(&DiskquotaLauncherShmem->freeWorkers)) goto out;
	wnode     = dlist_pop_head_node(&DiskquotaLauncherShmem->freeWorkers);
	dq_worker = dlist_container(DiskQuotaWorkerEntry, node, wnode);
	reset_worker(dq_worker);
	dlist_push_head(&DiskquotaLauncherShmem->runningWorkers, &dq_worker->node);
	elog(DEBUG1, "[diskquota] gets a worker %d", dq_worker->id);
out:
	LWLockRelease(diskquota_locks.workerlist_lock);
	return dq_worker;
}

static char *
get_db_name(Oid dbid)
{
	char	     *dbname = NULL;
	MemoryContext old_ctx;
	if (dbid == InvalidOid)
	{
		elog(WARNING, "database oid  is invalid");
		return NULL;
	}

	StartTransactionCommand();
	(void)GetTransactionSnapshot();
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	dbname  = get_database_name(dbid);
	MemoryContextSwitchTo(old_ctx);
	CommitTransactionCommand();
	return dbname;
}

static void
reset_worker(DiskQuotaWorkerEntry *dq_worker)
{
	if (dq_worker == NULL) return;
	dq_worker->dbEntry = NULL;
}

/*
 * id can not be changed
 */
static void
vacuum_db_entry(DiskquotaDBEntry *db)
{
	if (db == NULL) return;
	db->dbid     = InvalidOid;
	db->inited   = false;
	db->workerId = INVALID_WORKER_ID;
	db->in_use   = false;
}

static void
init_bgworker_handles(void)
{
	bgworker_handles = (BackgroundWorkerHandle **)(palloc(sizeof(BackgroundWorkerHandle *) * diskquota_max_workers));
	for (int i = 0; i < diskquota_max_workers; i++)
	{
		bgworker_handles[i] = NULL;
	}
	return;
}

static BackgroundWorkerHandle *
get_bgworker_handle(uint32 worker_id)
{
	if (worker_id >= 0)
		return bgworker_handles[worker_id];
	else
		return NULL;
}

static void
free_bgworker_handle(uint32 worker_id)
{
	BackgroundWorkerHandle **handle = &bgworker_handles[worker_id];
	if (*handle != NULL)
	{
		WaitForBackgroundWorkerShutdown(*handle);
		pfree(*handle);
		*handle = NULL;
	}
}

#if GP_VERSION_NUM < 70000
static BgwHandleStatus
WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle)
{
	BgwHandleStatus status;
	int             rc;

	for (;;)
	{
		pid_t pid;

		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STOPPED) break;

		rc = DiskquotaWaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			status = BGWH_POSTMASTER_DIED;
			break;
		}

		ResetLatch(&MyProc->procLatch);
	}

	return status;
}
#endif /* GP_VERSION_NUM */
