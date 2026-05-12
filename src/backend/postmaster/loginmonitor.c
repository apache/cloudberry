/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * loginmonitor.c
 *    PostgreSQL Integrated Login Monitor Daemon
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/loginmonitor.c
 * 
 * Like autovacuum, the login monitor is structured in two different
 * kinds of processes: the login monitor launcher and the login monitor
 * worker. The launcher is an always-running process, started by the
 * postmaster. It is mainly used to process user's failed login
 * authentication. It will always running in loop waiting for failed
 * login signal. The launcher will signal postmaster to fork the worker
 * process when it receives failed authentication signal from the postgres
 * process. The worker process is the process which doing the actual
 * working; as the worker process is only forked when authenticate failed,
 * one worker process is enough to finish that. It will be forked from
 * the postmaster as needed. Like normal postgres process, login monitor
 * worker is equipped with locks, transactions, read/write catalog table
 * and other functionalities.
 *
 * The login monitor launcher cannot start the worker process by itself,
 * as doing so would cause robustness issues (namely, failure to shut
 * them down on exceptional conditions, and also, since the launcher is
 * connected to shared memory and is thus subject to corruption there,
 * it is not as robust as the postmaster). So it leaves that task to the
 * postmaster.
 *
 * There is a login monitor shared memory area, where the launcher
 * stores information about its pid and latch. What's more, the worker
 * stores the current login user's name and it's latch to shared memory.
 * There is also a flag between launcher and worker to indicate whether
 * the failed login authentication signal has been processed.
 *
 * When there is a failed login authentication, the postgres process will
 * send the signal to the postmaster and wait the shared memory latch.
 * The launcher received signal from postgres process, it will set the
 * flag to true to indicate the signal is processing. And it will resend
 * a signal to postmaster and wait the latch of lm_latch in shared memory.
 * After receiving the signal from the launcher, the postmaster will fork
 * the worker to do the actual working. After the worker finishes work,
 * it will notify postgres process and the launcher by setting the latch
 * and the lm_latch in shared memory. Moreover, it will reset the flag to
 * false to indicate the work is finished.
 *
 * Only when the user is able to use profile, process will send signal to
 * login monitor and wait for the completion of worker process. Otherwise,
 * the failed login authentication of the user will be ignored and doesn't
 * need to send signal to the launcher.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_profile.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/loginmonitor.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "cdb/cdbvars.h"

/* Memory context for login monitor rewrites catalogs */
static MemoryContext LoginMonitorMemCxt;
static pid_t LoginMonitorPID;

/*
 * The main Login Monitor shmem struct. On shared memory we store it,
 * which will be set by failed proc and reset by login monitor
 * post-processing. This struct keeps:
 *
 * lm_pid		Login Monitor Launcher Process pid
 * lm_latch		Login Monitor Launcher Latch pointer
 * curr_user_name	current failed user name
 * latch		pointer of current failed proc's latch
 * login_failed_requested		whether current is handling a failed login
 *
 * curr_user_name, latch and login_failed_requested are protected by LWLock LoginFailedSharedMemoryLock
 */
typedef struct {
    pid_t lm_pid;
    Latch *lm_latch;
    char curr_user_name[NAMEDATALEN];
    Latch *latch;
    sig_atomic_t login_failed_requested;
} LoginMonitorShmemStruct;

static LoginMonitorShmemStruct *LoginMonitorShmem;

/* Flags to tell if we are in an login monitor process */
static bool am_login_monitor_launcher = false;
static bool am_login_monitor_worker = false;

static void LoginMonitorShutdown(void);

static void HandleLoginMonitorInterrupts(void);

static void LoginMonitorShmemReset(void);

#ifdef EXEC_BACKEND
static pid_t lmlauncher_forkexec(void);
static pid_t lmworker_forkexec(void);
#endif

NON_EXEC_STATIC void LoginMonitorLauncherMain(int argc, char *argv[]);

NON_EXEC_STATIC void LoginMonitorWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();

static void record_failed_login(void);


/********************************************************************
 *					  LOGIN MONITOR CODE
 ********************************************************************/


#ifdef EXEC_BACKEND
/*
 * forkexec routine for the login monitor launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
lmlauncher_forkexec(void)
{
	char	*lm[10];
	int		lc = 0;

	lm[lc++] = "postgres";
	lm[lc++] = "--forkloginmonitor";
	lm[lc++] = NULL;
	lm[lc] = NULL;

	Assert(lc < lengthof(lm));

	return postmaster_forkexec(lc, lm);
}
#endif

/*
 * Main entry point for login monitor launcher process, to be called from
 * the postmaster.
 */
int
StartLoginMonitorLauncher(void) {
#ifdef EXEC_BACKEND
	switch ((LoginMonitorPID = lmlauncher_forkexec()))
#else
	switch ((LoginMonitorPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
				(errmsg("could not fork login monitor process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postgresmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			LoginMonitorLauncherMain(0, NULL);
			break;
#endif
		default:
			return (int) LoginMonitorPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * Main loop for the login monitor process.
 */
NON_EXEC_STATIC void
LoginMonitorLauncherMain(int argc, char *argv[]) {
	sigjmp_buf local_sigjmp_buf;

	am_login_monitor_launcher = true;

	MyBackendType = B_LOGIN_MONITOR;
	init_ps_display(NULL);

	ereport(DEBUG1,
		(errmsg_internal("login monitor launcher started")));

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handler. We operate on databases much like a regular
	 * backend, so we use the same signal handling. See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */

	InitializeTimeouts();                /* established SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif
	
	/* Early initialization */
	BaseInit();


	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, false, false, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in. We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	LoginMonitorMemCxt = AllocSetContextCreate(TopMemoryContext,
						   "Login Monitor",
						   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(LoginMonitorMemCxt);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here. Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery. It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Release any other resources, for the case where we were not in a
		 * transaction.
		 */
		LWLockReleaseAll();
		UnlockBuffers();
		/* this is probably dead code, but let's be safe: */
		if (AuxProcessResourceOwner)
			ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(LoginMonitorMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(LoginMonitorMemCxt);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* reset and notify process by latch */
		if (LoginMonitorShmem->latch)
			SetLatch(LoginMonitorShmem->latch);
		ResetLatch(LoginMonitorShmem->lm_latch);
		LoginMonitorShmemReset();

		/* if in shutdown mode, no need for anything further; just go away */
		if (ShutdownRequestPending)
			LoginMonitorShutdown();

		/*
		 * Sleep at least 1 milli second after any error. We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Set always-secure search path.
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force zero_damaged_pages OFF in the login monitor process, even if it is set`
	 * in postgresql.conf. We don't really want such a dangerous option being applied
	 * non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force settable timeouts off to avoid letting these settings prevent
	 * regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("idle_in_transaction_session_timeout", "0",
			PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED. We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
			PGC_SUSET, PGC_S_OVERRIDE);

	LoginMonitorShmem->lm_pid = MyProcPid;
	LoginMonitorShmem->lm_latch = MyLatch;

	/*
	 * Main loop until shutdown request
	 */
	while (!ShutdownRequestPending) {
		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		(void) WaitLatch(MyLatch,
				 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				 5 * 1000L,
				 WAIT_EVENT_LOGIN_MONITOR_LAUNCHER_MAIN);

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleLoginMonitorInterrupts();

		/*
		 * As only one worker can run at any time, before start a worker,
		 * we should check whether there is already one worker running.
		 */
		if (LoginMonitorShmem->login_failed_requested)
		{
			SendPostmasterSignal(PMSIGNAL_START_LOGIN_MONITOR_WORKER);
			elog(DEBUG1, "Login monitor launcher is processing uesr %s and has sent starting"
	     					"worker signal to postmaster.", LoginMonitorShmem->curr_user_name);
			WaitLatch(LoginMonitorShmem->lm_latch,
				  WL_LATCH_SET | WL_POSTMASTER_DEATH, 0,
				  WAIT_EVENT_LOGINMONITOR_FINISH);
			/* Clear any already-pending wakeups */
			ResetLatch(LoginMonitorShmem->lm_latch);
		}
	}

	LoginMonitorShutdown();
}

/********************************************************************
 *				LOGIN MONITOR WORKER CODE
 ********************************************************************/
/*
 * Main entry point for login monitor process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
StartLoginMonitorWorker(void) {
	pid_t worker_pid;

#ifdef EXEC_BACKEND
	switch ((worker_pid = lmworker_forkexec()))
#else
	switch ((worker_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
				(errmsg("could not fork login monitor worker process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			LoginMonitorWorkerMain(0, NULL);
			break;
#endif
		default:
			return (int) worker_pid;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * LoginMonitorWorkerMain
 */
NON_EXEC_STATIC void
LoginMonitorWorkerMain(int argc, char *argv[]) {
	sigjmp_buf local_sigjmp_buf;

	am_login_monitor_worker = true;

	/* MPP-4990: LoginMonitor always runs as utility-mode */
	if (IS_QUERY_DISPATCHER())
		Gp_role = GP_ROLE_DISPATCH;
	else
		Gp_role = GP_ROLE_UTILITY;

	MyBackendType = B_LOGIN_MONITOR_WORKER;
	init_ps_display(NULL);

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers. We operate on databases much like a regular
	 * backend, so we use the same signal handling. See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, die);

	/* SIGQUIT handler was already set up by InitPostmasterChild */
	InitializeTimeouts();        /* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here. Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery. It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		SetLatch(LoginMonitorShmem->lm_latch);
		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * Set always-secure search path.
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force zero_damaged_pages OFF in the login monitor process, even if it is set`
	 * in postgresql.conf. We don't really want such a dangerous option being applied
	 * non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force settable timeouts off to avoid letting these settings prevent
	 * regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("idle_in_transaction_session_timeout", "0",
			PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED. We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
			PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force synchronous replication off to allow regular maintenance even if
	 * we are waiting for standbys to connect. This is important to ensure we
	 * aren't blocked from performing anti-wraparound tasks.
	 */
	if (synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
		SetConfigOption("synchronous_commit", "local",
				PGC_SUSET, PGC_S_OVERRIDE);

	if (LoginMonitorShmem->curr_user_name[0] != '\0')
	{
		InitPostgres(DB_FOR_COMMON_ACCESS, InvalidOid, NULL, InvalidOid, false, false, NULL);
		SetProcessingMode(NormalProcessing);
		set_ps_display(LoginMonitorShmem->curr_user_name);
		ereport(DEBUG1,
				(errmsg_internal("login monitor: processing user \"%s\" failed",
					 LoginMonitorShmem->curr_user_name)));

		record_failed_login();
	}

	proc_exit(0);
}

/*
 * Process any new interrupts.
 */
static void
HandleLoginMonitorInterrupts(void) {
	/* the normal shutdown case */
	if (ShutdownRequestPending)
		LoginMonitorShutdown();

	if (ConfigReloadPending) {
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	/* Process barrier events */
	if (ProcSignalBarrierPending)
		ProcessProcSignalBarrier();

	/* Perform logging of memory contexts of this process */
	if (LogMemoryContextPending)
		ProcessLogMemoryContextInterrupt();

	/* Process sinval catchup interrupts that happened while sleeping */
	ProcessCatchupInterrupt();
}

/*
 * Process a failed login authentication
 *
 * As this will only record the current failed login, we don't need
 * CHECK_FOR_INTERRUPTS during process.
 */
static void
record_failed_login(void) {
	Datum new_record[Natts_pg_authid];
	bool new_record_nulls[Natts_pg_authid];
	bool new_record_repl[Natts_pg_authid];
	Relation pg_authid_rel;
	Relation pg_profile_rel;
	TupleDesc pg_authid_dsc;
	HeapTuple profile_tuple;
	HeapTuple auth_tuple;
	HeapTuple new_tuple;
	Form_pg_profile profileform;
	int32 failed_login_attempts;
	int32 profile_failed_login_attempts;
	bool isnull;
	int32 profileid;
	TimestampTz now;

	/* Start a transaction so our commands have one to play into. */
	StartTransactionCommand();

	/* Acquire LWLock */
	LWLockAcquire(LoginFailedSharedMemoryLock, LW_EXCLUSIVE);
	elog(DEBUG1, "Login monitor worker has acquired LoginFailedSharedMemoryLock for process user %s.",
      							LoginMonitorShmem->curr_user_name);

	/*
	 * Update related catalog and check whether the account need to be locked
	 */
	pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
	pg_authid_dsc = RelationGetDescr(pg_authid_rel);
	pg_profile_rel = table_open(ProfileRelationId, AccessShareLock);

	auth_tuple = SearchSysCache1(AUTHNAME,
				     CStringGetDatum(LoginMonitorShmem->curr_user_name));

	Assert(HeapTupleIsValid(auth_tuple));

	/* get current failed_login_attempts */
	failed_login_attempts = SysCacheGetAttr(AUTHNAME, auth_tuple,
						Anum_pg_authid_rolfailedlogins, &isnull);
	Assert(!isnull);

	/* increase failed_login_attempts by one */
	failed_login_attempts++;
	elog(DEBUG1, "User %s FAILED LOGIN ATTEMPTS is %d in Login Monitor worker",
      					LoginMonitorShmem->curr_user_name, failed_login_attempts);
	/*
	 * Build an updated tuple, perusing the information just obtained
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, true, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	new_record[Anum_pg_authid_rolfailedlogins - 1] =
		Int32GetDatum(failed_login_attempts);
	new_record_nulls[Anum_pg_authid_rolfailedlogins - 1] =
		false;
	new_record_repl[Anum_pg_authid_rolfailedlogins - 1] =
		true;

	/* get the user's current profile oid */
	profileid = SysCacheGetAttr(AUTHNAME, auth_tuple,
				    Anum_pg_authid_rolprofile, &isnull);
	Assert(!isnull);

	/* get user's current profile tuple */
	profile_tuple = SearchSysCache1(PROFILEID, ObjectIdGetDatum(profileid));
	Assert(HeapTupleIsValid(profile_tuple));
	profileform = (Form_pg_profile) GETSTRUCT(profile_tuple);

	/*
	 * Transform failed_login_attempts to normal value if it's
	 * PROFILE_DEFAULT or PROFILE_UNLIMITED.
	 */
	profile_failed_login_attempts =
		tranformProfileValueToNormal(profileform->prffailedloginattempts,
					     Anum_pg_profile_prffailedloginattempts);

	/*
	 * If user's failed_login_attempts is bigger equal than current
	 * profile's failed_login_attempts, update account status to
	 * locked and lockdate to now.
	 */
	if (failed_login_attempts >= profile_failed_login_attempts) {
		new_record[Anum_pg_authid_rolaccountstatus - 1] =
			Int32GetDatum(ROLE_ACCOUNT_STATUS_LOCKED_TIMED);
		new_record_nulls[Anum_pg_authid_rolaccountstatus - 1] =
			false;
		new_record_repl[Anum_pg_authid_rolaccountstatus - 1] =
			true;

		now = GetCurrentTimestamp();
		new_record[Anum_pg_authid_rollockdate - 1] =
			Int64GetDatum(now);
		new_record_nulls[Anum_pg_authid_rollockdate - 1] =
			false;
		new_record_repl[Anum_pg_authid_rollockdate - 1] =
			true;
	}

	new_tuple = heap_modify_tuple(auth_tuple, pg_authid_dsc,
				      new_record, new_record_nulls, new_record_repl);
	CatalogTupleUpdate(pg_authid_rel, &auth_tuple->t_self, new_tuple);

	InvokeObjectPostAlterHook(AuthIdRelationId, profileid, 0);

	ReleaseSysCache(auth_tuple);
	ReleaseSysCache(profile_tuple);
	table_close(pg_profile_rel, NoLock);
	table_close(pg_authid_rel, NoLock);

	/* reset login_failed_requested to false */
	LoginMonitorShmem->login_failed_requested = false;

	/* notify process by setting latch */
	SetLatch(LoginMonitorShmem->lm_latch);
	SetLatch(LoginMonitorShmem->latch);
	LWLockRelease(LoginFailedSharedMemoryLock);
	elog(DEBUG1, "Login monitor worker has released LoginFailedSharedMemoryLock for process user %s.",
      								LoginMonitorShmem->curr_user_name);

	CommitTransactionCommand();
}

/*
 * Notify Login Monitor Launcher by LoginMonitorShmem
 */
void
LoginMonitorWorkerFailed(void) {
	SetLatch(LoginMonitorShmem->lm_latch);
}

/*
 * Perform a normal exit from the login monitor.
 */
static void
LoginMonitorShutdown(void) {
	ereport(DEBUG1,
		(errmsg_internal("login monitor shutting down")));

	LoginMonitorShmem->lm_pid = 0;
	LoginMonitorShmem->lm_latch = NULL;

	proc_exit(0);
}

/*
 * SIGUSR1: a login password authentication failed
 */
void
HandleLoginFailed(void) {
	int save_errno = errno;

	LoginMonitorShmem->login_failed_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * IsLoginMonitorLauncher functions
 * 		Return whether this is a login monitor launcher process.
 */
bool
IsLoginMonitorLauncherProcess(void) {
	return am_login_monitor_launcher;
}

/*
 * IsLoginMonitorWorker functions
 * 		Return whether this is a login monitor worker process.
 */
bool
IsLoginMonitorWorkerProcess(void) {
	return am_login_monitor_worker;
}

/*
 * LoginMonitorShmemSize
 * 		Compute space needed for login monitor-related shared memory
 */
Size
LoginMonitorShmemSize(void) {
	Size size;

	size = sizeof(LoginMonitorShmemStruct);

	return size;
}

/*
 * LoginMonitorShmemInit
 * 		  Allocate and initialize login monitor-related shared memory
 */
void
LoginMonitorShmemInit(void) {
	bool found;

	LoginMonitorShmem = (LoginMonitorShmemStruct *)
		ShmemInitStruct("Login Monitor Data",
				LoginMonitorShmemSize(),
				&found);

	if (!IsUnderPostmaster) {
		Assert(!found);
		LoginMonitorShmem->lm_pid = 0;
		LoginMonitorShmem->lm_latch = NULL;
		memset(LoginMonitorShmem->curr_user_name, 0, NAMEDATALEN);
		LoginMonitorShmem->latch = NULL;
		LoginMonitorShmem->login_failed_requested = false;
	} else
		Assert(found);
}

/*
 * SendLoginFailedSignal - signal the postmaster after failed authentication.
 */
void
SendLoginFailedSignal(const char *curr_user_name) {
	int rc;

	/* If called in a standalone backend or Login Monitor PID is 0, do nothing */
	if (!IsUnderPostmaster || !LoginMonitorPID)
		return;
	/*
	 * Before sending signal, we need to acquire lock in shared memory and set
	 * current user oid. Only when login monitor backend process has solved the
	 * current signal, the lock will be released. By this way, the login monitor
	 * solve the postgres signal serially which will avoid dead lock.
	 */
	LWLockAcquire(LoginFailedControlLock, LW_EXCLUSIVE);
	LWLockAcquire(LoginFailedSharedMemoryLock, LW_EXCLUSIVE);
	elog(DEBUG1, "User %s has acquire LoginFailedControlLock and LoginFailedSharedMemoryLock", curr_user_name);

	/* Reset login monitor shmem user name */
	memset(LoginMonitorShmem->curr_user_name, 0, NAMEDATALEN);
	/* Set current user name */
	strcpy(LoginMonitorShmem->curr_user_name, curr_user_name);

	/* Set latch to pointer to MyLatch */
	LoginMonitorShmem->latch = &MyProc->procLatch;
	ResetLatch(LoginMonitorShmem->latch);

	/* Send signal to PostMaster */
	SendPostmasterSignal(PMSIGNAL_FAILED_LOGIN);

	elog(DEBUG1, "User %s has sent failed login signal to postmaster", curr_user_name);

	LWLockRelease(LoginFailedSharedMemoryLock);
	elog(DEBUG1, "User %s has released LoginFailedSharedMemoryLock and wait latch", curr_user_name);

	rc = WaitLatch(LoginMonitorShmem->latch,
		       WL_LATCH_SET | WL_POSTMASTER_DEATH, 0,
		       WAIT_EVENT_LOGINMONITOR_FINISH);

	LWLockRelease(LoginFailedControlLock);
	elog(DEBUG1, "User %s has release LoginFailedControlLock", curr_user_name);
}

static void
LoginMonitorShmemReset(void) {
	Assert(LoginMonitorShmem);

	LWLockAcquire(LoginFailedSharedMemoryLock, LW_EXCLUSIVE);

	memset(LoginMonitorShmem->curr_user_name, 0, NAMEDATALEN);
	LoginMonitorShmem->latch = NULL;
	LoginMonitorShmem->login_failed_requested = false;

	LWLockRelease(LoginFailedSharedMemoryLock);

	return;
}
