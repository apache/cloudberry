/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/subtrans.h"
#include "access/syncscan.h"
#include "access/twophase.h"
#include "access/distributedlog.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbvars.h"
#include "access/xlogprefetcher.h"
#include "access/xlogrecovery.h"
#include "commands/async.h"
#include "commands/matview.h"
#include "crypto/kmgr.h"
#include "executor/nodeShareInputScan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/fts.h"
#include "postmaster/loginmonitor.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "task/pg_cron.h"
#include "utils/backend_cancel.h"
#include "utils/resource_manager.h"
#include "utils/faultinjector.h"
#include "utils/sharedsnapshot.h"
#include "utils/gpexpand.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbtm.h"
#include "postmaster/backoff.h"
#include "cdb/memquota.h"
#include "executor/instrument.h"
#include "executor/spi.h"
#include "utils/workfile_mgr.h"
#include "utils/session_state.h"
#include "cdb/cdbendpoint.h"
#include "replication/gp_replication.h"

/* GUCs */
int			shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;

/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This may only be called via the shmem_request_hook of a library that is
 * loaded into the postmaster via shared_preload_libraries.  Calls from
 * elsewhere will fail.
 */
void
RequestAddinShmemSpace(Size size)
{
	if (!process_shmem_requests_in_progress)
		elog(FATAL, "cannot request additional shared memory outside shmem_request_hook");
	total_addin_request = add_size(total_addin_request, size);
}

/*
 * CalculateShmemSize
 *		Calculates the amount of shared memory and number of semaphores needed.
 *
 * If num_semaphores is not NULL, it will be set to the number of semaphores
 * required.
 */
Size
CalculateShmemSize(int *num_semaphores)
{
	Size		size;
	int			numSemas;

	/* Compute number of semaphores we'll need */
	numSemas = ProcGlobalSemas();
	numSemas += SpinlockSemas();

	/* Return the number of semaphores if requested by the caller */
	if (num_semaphores)
		*num_semaphores = numSemas;

	/*
	 * Size of the Postgres shared-memory block is estimated via moderately-
	 * accurate estimates for the big hogs, plus 100K for the stuff that's too
	 * small to bother with estimating.
	 *
	 * We take some care to ensure that the total size request doesn't
	 * overflow size_t.  If this gets through, we don't need to be so careful
	 * during the actual allocation phase.
	 */
	size = 100000;
	size = add_size(size, PGSemaphoreShmemSize(numSemas));
	size = add_size(size, SpinlockSemaSize());
	size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
											 sizeof(ShmemIndexEnt)));
	size = add_size(size, dsm_estimate_size());
	size = add_size(size, BufferShmemSize());
	size = add_size(size, GpParallelDSMHashSize());
	size = add_size(size, LockShmemSize());
	size = add_size(size, PredicateLockShmemSize());
	if (IsResQueueEnabled() && (Gp_role == GP_ROLE_DISPATCH || IS_SINGLENODE()))
	{
		size = add_size(size, ResSchedulerShmemSize());
		size = add_size(size, ResPortalIncrementShmemSize());
	}
	else if (IsResGroupEnabled())
		size = add_size(size, ResGroupShmemSize());
	size = add_size(size, SharedSnapshotShmemSize());
#ifdef USE_INTERNAL_FTS
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
			size = add_size(size, FtsShmemSize());
#endif
	size = add_size(size, ProcGlobalShmemSize());
	size = add_size(size, XLogPrefetchShmemSize());
	size = add_size(size, XLOGShmemSize());
	size = add_size(size, XLogRecoveryShmemSize());
	size = add_size(size, DistributedLog_ShmemSize());
	size = add_size(size, CLOGShmemSize());
	size = add_size(size, CommitTsShmemSize());
	size = add_size(size, SUBTRANSShmemSize());
	size = add_size(size, TwoPhaseShmemSize());
	size = add_size(size, BackgroundWorkerShmemSize());
	size = add_size(size, MultiXactShmemSize());
	size = add_size(size, LWLockShmemSize());
	size = add_size(size, ProcArrayShmemSize());
	size = add_size(size, BackendStatusShmemSize());
	size = add_size(size, SInvalShmemSize());
	size = add_size(size, PMSignalShmemSize());
	size = add_size(size, ProcSignalShmemSize());
	size = add_size(size, CheckpointerShmemSize());
	size = add_size(size, AutoVacuumShmemSize());
	size = add_size(size, LoginMonitorShmemSize());
	size = add_size(size, ReplicationSlotsShmemSize());
	size = add_size(size, ReplicationOriginShmemSize());
	size = add_size(size, WalSndShmemSize());
	size = add_size(size, WalRcvShmemSize());
	size = add_size(size, PgArchShmemSize());
	size = add_size(size, ApplyLauncherShmemSize());
	size = add_size(size, FTSReplicationStatusShmemSize());
	size = add_size(size, PgCronLauncherShmemSize());
	size = add_size(size, SnapMgrShmemSize());
	size = add_size(size, BTreeShmemSize());
	size = add_size(size, SyncScanShmemSize());
	size = add_size(size, AsyncShmemSize());
	size = add_size(size, StatsShmemSize());
	size = add_size(size, KmgrShmemSize());
#ifdef EXEC_BACKEND
	size = add_size(size, ShmemBackendArraySize());
#endif
	size = add_size(size, tmShmemSize());
	size = add_size(size, CheckpointerShmemSize());
	size = add_size(size, CancelBackendMsgShmemSize());
	size = add_size(size, WorkFileShmemSize());
	size = add_size(size, ShareInputShmemSize());

#ifdef FAULT_INJECTOR
	size = add_size(size, FaultInjector_ShmemSize());
#endif

	/* This elog happens before we know the name of the log file we are supposed to use */
	elog(DEBUG1, "Size not including the buffer pool %lu",
		 (unsigned long) size);

	/* include additional requested shmem from preload libraries */
	size = add_size(size, total_addin_request);

	/* might as well round it off to a multiple of a typical page size */
	size = add_size(size, BLCKSZ - (size % BLCKSZ));

	/* Consider the size of the SessionState array */
	size = add_size(size, SessionState_ShmemSize());

	/* size of Instrumentation slots */
	size = add_size(size, InstrShmemSize());

	/* size of expand version */
	size = add_size(size, GpExpandVersionShmemSize());

	/* size of token and endpoint shared memory */
	size = add_size(size, EndpointShmemSize());
#ifndef USE_INTERNAL_FTS
	/* size of cdb etcd result cache */
	if (Gp_role != GP_ROLE_EXECUTE)
		size = add_size(size, ShmemSegmentConfigsCacheSize());

	/* size of standby promote flags */
	size = add_size(size, ShmemStandbyPromoteReadySize());
#endif
	size = add_size(size, mv_TableShmemSize());

	return size;
}

/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 */
void
CreateSharedMemoryAndSemaphores(void)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* Compute the size of the shared-memory block */
		size = CalculateShmemSize(&numSemas);
		elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

		/*
		 * Create the shmem segment
		 */
		seghdr = PGSharedMemoryCreate(size, &shim);

		InitShmemAccess(seghdr);

		/*
		 * Create semaphores
		 */
		PGReserveSemaphores(numSemas);

		/*
		 * If spinlocks are disabled, initialize emulation layer (which
		 * depends on semaphores, so the order is important here).
		 */
#ifndef HAVE_SPINLOCKS
		SpinlockSemaInit();
#endif
	}
	else
	{
		/*
		 * We are reattaching to an existing shared memory segment. This
		 * should only be reached in the EXEC_BACKEND case.
		 */
#ifndef EXEC_BACKEND
		elog(PANIC, "should be attached to shared memory already");
#endif
	}

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	dsm_shmem_init();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	XLogPrefetchShmemInit();
	XLogRecoveryShmemInit();
	CLOGShmemInit();
	DistributedLog_ShmemInit();
	CommitTsShmemInit();
	SUBTRANSShmemInit();
	MultiXactShmemInit();
#ifdef USE_INTERNAL_FTS
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		FtsShmemInit();
#endif
	tmShmemInit();
	InitBufferPool();

	InitGpParallelDSMHash();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up resource manager 
	 */
	ResManagerShmemInit();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();

	/* Initialize SessionState shared memory array */
	SessionState_ShmemInit();
	/* Initialize vmem protection */
	GPMemoryProtect_ShmemInit();

	CreateSharedProcArray();
	CreateSharedBackendStatus();
	
	/*
	 * Set up Shared snapshot slots
	 *
	 * TODO: only need to do this if we aren't the QD. for now we are just 
	 *		 doing it all the time and wasting shemem on the QD.  This is 
	 *		 because this happens at postmaster startup time when we don't
	 *		 know who we are.  
	 */
	CreateSharedSnapshotArray();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	LoginMonitorShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	PgArchShmemInit();
	ApplyLauncherShmemInit();
	FTSReplicationStatusShmemInit();
	PgCronLauncherShmemInit();

#ifdef FAULT_INJECTOR
	FaultInjector_ShmemInit();
#endif

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	BackendCancelShmemInit();
	WorkFileShmemInit();
	ShareInputShmemInit();

	/*
	 * Set up Instrumentation free list
	 */
	if (!IsUnderPostmaster)
		InstrShmemInit();

	GpExpandVersionShmemInit();
	KmgrShmemInit();
	StatsShmemInit();

#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

	if (gp_enable_resqueue_priority)
		BackoffStateInit();

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/* Initialize shared memory for parallel retrieve cursor */
	if (!IsUnderPostmaster)
		EndpointShmemInit();
#ifndef USE_INTERNAL_FTS
	/* Initialize shared memory for cdb etcd cache */
	if (Gp_role != GP_ROLE_EXECUTE)
		ShmemSegmentConfigsCacheAllocation();

	/* Initialize shared memory for standby_promote_ready */
	ShmemStandbyPromoteReadyAllocation();
#endif
	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}

/*
 * InitializeShmemGUCs
 *
 * This function initializes runtime-computed GUCs related to the amount of
 * shared memory required for the current configuration.
 */
void
InitializeShmemGUCs(void)
{
	char		buf[64];
	Size		size_b;
	Size		size_mb;
	Size		hp_size;

	/*
	 * Calculate the shared memory size and round up to the nearest megabyte.
	 */
	size_b = CalculateShmemSize(NULL);
	size_mb = add_size(size_b, (1024 * 1024) - 1) / (1024 * 1024);
	sprintf(buf, "%zu", size_mb);
	SetConfigOption("shared_memory_size", buf,
					PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);

	/*
	 * Calculate the number of huge pages required.
	 */
	GetHugePageSize(&hp_size, NULL);
	if (hp_size != 0)
	{
		Size		hp_required;

		hp_required = add_size(size_b / hp_size, 1);
		sprintf(buf, "%zu", hp_required);
		SetConfigOption("shared_memory_size_in_huge_pages", buf,
						PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
	}
}
