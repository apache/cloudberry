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
 * pg_query_state.c
 *		Core of the pg_query_state signal-dispatch layer.
 *
 * This module provides:
 *   - Shared-memory setup (shm_toc segment with params, mq, mq_req_id).
 *   - Custom ProcSignal registrations for three signals:
 *       QueryStatePollReason  -> SendQueryState()
 *       BackendInfoPollReason -> SendCdbComponents()
 *   - GUC variables: pg_query_state.enable / enable_timing / enable_buffers.
 *   - Executor hooks (start/run/finish/end), registered by this module itself,
 *     that maintain the QueryDescStack and enable instrumentation on the
 *     top-level query.
 *   - A requestor-side helper: shm_mq_receive_with_timeout().
 *
 * Per-node stats are pushed to the yagpcc UDS sink on demand, when a backend is
 * signalled to report its live query state; see signal_handler.c.
 *
 * Portions derived from pg_query_state
 * (https://github.com/postgrespro/pg_query_state), under the PostgreSQL
 * License:
 *   Portions Copyright (c) 2016-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/pg_query_state.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_query_state.h"
#include "PlanNodeEmitter.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "executor/execParallel.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/typcache.h"

/* GUC variables */
/* Master switch: disabling this suppresses all stat collection. */
bool pg_qs_enable  = true;

/* Collect timing (wall-clock) data in addition to row counts. */
bool pg_qs_timing  = true;

/* Collect buffer usage via Instrumentation.bufusage. */
bool pg_qs_buffers = true;

/*
 * Rolling counter incremented for every QueryDesc pushed onto the stack.
 * Used to generate synthetic queryId values for statements lacking one.
 */
static int qs_push_count = 0;

/* Saved hook pointer for chaining shmem_startup callbacks. */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Saved hook pointers for chaining the executor callbacks. */
static ExecutorStart_hook_type  prev_ExecutorStart_hook  = NULL;
static ExecutorRun_hook_type    prev_ExecutorRun_hook    = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type    prev_ExecutorEnd_hook    = NULL;

/* Whether pg_qs_shmem_startup has completed successfully. */
static bool module_initialized = false;

/*
 * Monotonically increasing request counter on the requestor side.
 * Compared against *mq_req_id in the reply to detect stale responses.
 */
static int reqid = 0;

/* Shared-memory variables (pointers into the shm_toc segment) */
/* Table of contents anchoring the whole shared segment. */
static shm_toc *toc = NULL;

/*
 * Signal parameters written by the requestor and read by the handler.
 * Slot 0 in the toc.
 */
pg_qs_params *params = NULL;

/*
 * Raw shared memory queue used to return data from the handler.
 * Slot 1 in the toc.
 */
shm_mq *mq = NULL;

/*
 * Shared request-id counter.  The requestor increments it before sending a
 * signal; the handler echoes it back so the requestor can detect stale
 * replies.  Slot 2 in the toc.
 */
uint32 *mq_req_id = NULL;

/*
 * Per-backend trace_id slots (toc key 3), indexed by BackendId.  The dispatcher
 * stamps the target's slot before signalling; the signaled backend reads its
 * own slot to key the batch it pushes.  See the header for the full rationale.
 */
char (*qs_trace_slots)[GPSC_TRACE_ID_LEN] = NULL;

/* Global signal-reason handles (set during pg_qs_init) */
List *QueryDescStack = NIL;

ProcSignalReason QueryStatePollReason  = INVALID_PROCSIGNAL;
ProcSignalReason BackendInfoPollReason = INVALID_PROCSIGNAL;

/* Forward declarations for module-private helpers */
static Size pg_qs_shmem_size(void);
static void pg_qs_shmem_startup(void);
static void push_query(QueryDesc *queryDesc);
static void pg_qs_pop_query(void);
static bool filter_query(QueryDesc *queryDesc);
static void pg_qs_executor_start(QueryDesc *queryDesc, int eflags);
static void pg_qs_executor_run(QueryDesc *queryDesc, ScanDirection direction,
							   uint64 count, bool execute_once);
static void pg_qs_executor_finish(QueryDesc *queryDesc);
static void pg_qs_executor_end(QueryDesc *queryDesc);
static shm_mq_result shm_mq_receive_with_timeout(shm_mq_handle *mqh, Size *nbytesp,
												 void **datap, int64 timeout);
static List *get_query_backend_info(ArrayType *array);
static shm_mq_result receive_msg_by_parts(shm_mq_handle *mqh, Size *total,
										  void **datap, int64 timeout,
										  int *rc, bool nowait);
static PG_QS_RequestResult GetRemoteBackendInfo(PGPROC *proc, List **result);
static void CollectQEQueryState(List *backendInfo, bytea *trace_id);
static void SignalEntryDbBackends(List *backendInfo, bytea *trace_id);
static bool is_querystack_empty(void);
static PG_QS_RequestResult qs_fetch_backend_info(PGPROC *proc, List **backend_info);

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void pg_qs_shmem_request(void);
#endif

/*
 * pg_qs_shmem_size -- compute the size of the shared memory segment.
 *
 * The segment holds four objects at fixed toc keys:
 *   key 0: pg_qs_params
 *   key 1: message queue of QUEUE_SIZE bytes
 *   key 2: uint32 request-id counter
 *   key 3: per-backend trace_id slots, char[GPSC_TRACE_ID_LEN] × (MaxBackends+1)
 */
static Size
pg_qs_shmem_size(void)
{
	shm_toc_estimator e;
	Size size;
	int  nkeys = 4;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(pg_qs_params));
	shm_toc_estimate_chunk(&e, (Size) QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, sizeof(uint32));
	shm_toc_estimate_chunk(&e, (Size) GPSC_TRACE_ID_LEN * (MaxBackends + 1));
	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);
	return size;
}

/*
 * pg_qs_shmem_startup -- attach to (or initialize) the shared segment.
 *
 * Called from the shmem_startup_hook chain after shared memory is mapped.
 * On first call (found == false) it initialises all sub-structures.
 * On subsequent calls it just re-attaches the toc pointers.
 */
static void
pg_qs_shmem_startup(void)
{
	bool  found;
	Size  shmem_size = pg_qs_shmem_size();
	void *shmem;
	int   num_toc = 0;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shmem = ShmemInitStruct("pg_query_state", shmem_size, &found);
	if (!found)
	{
		toc = shm_toc_create(PG_QS_MODULE_KEY, shmem, shmem_size);

		params = shm_toc_allocate(toc, sizeof(pg_qs_params));
		shm_toc_insert(toc, num_toc++, params);

		mq = shm_toc_allocate(toc, QUEUE_SIZE);
		shm_toc_insert(toc, num_toc++, mq);

		mq_req_id = shm_toc_allocate(toc, sizeof(uint32));
		shm_toc_insert(toc, num_toc++, mq_req_id);
		*mq_req_id = 0;

		qs_trace_slots = shm_toc_allocate(toc,
										  (Size) GPSC_TRACE_ID_LEN * (MaxBackends + 1));
		shm_toc_insert(toc, num_toc++, qs_trace_slots);
		memset(qs_trace_slots, 0, (Size) GPSC_TRACE_ID_LEN * (MaxBackends + 1));
	}
	else
	{
		toc = shm_toc_attach(PG_QS_MODULE_KEY, shmem);
		params         = shm_toc_lookup(toc, num_toc++, false);
		mq             = shm_toc_lookup(toc, num_toc++, false);
		mq_req_id      = shm_toc_lookup(toc, num_toc++, false);
		qs_trace_slots = shm_toc_lookup(toc, num_toc++, false);
	}
	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	module_initialized = true;
}

#if PG_VERSION_NUM >= 150000
/*
 * pg_qs_shmem_request -- hook called to request shared memory space.
 *
 * PostgreSQL 15+ separates the request phase from the startup phase.
 * This hook is installed only when building against PG15+.
 */
static void
pg_qs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pg_qs_shmem_size());
}
#endif

/*
 * pg_qs_init -- initialise the pg_query_state signal infrastructure.
 *
 * Must be called from _PG_init() while process_shared_preload_libraries_in_progress
 * is true.  Registers shared memory, custom ProcSignal handlers and GUC
 * variables.  Safe to call unconditionally for all roles.
 */
void
pg_qs_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_qs_shmem_request;
#else
	RequestAddinShmemSpace(pg_qs_shmem_size());
#endif

	QueryStatePollReason  = RegisterCustomProcSignalHandler(SendQueryState);
	BackendInfoPollReason = RegisterCustomProcSignalHandler(SendCdbComponents);

	if (QueryStatePollReason  == INVALID_PROCSIGNAL ||
		BackendInfoPollReason == INVALID_PROCSIGNAL)
	{
		ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						  errmsg("pg_query_state isn't loaded: insufficient custom ProcSignal slots")));
		return;
	}

	DefineCustomBoolVariable("pg_query_state.enable",
							 "Enable module.",
							 NULL,
							 &pg_qs_enable,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_query_state.enable_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &pg_qs_timing,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_query_state.enable_buffers",
							 "Collect buffer usage.",
							 NULL,
							 &pg_qs_buffers,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_qs_shmem_startup;

	/*
	 * Own the executor hooks rather than being called from the collector's
	 * wrappers: this module has to run outside of whatever else hooks the
	 * executor, because pg_qs_executor_start() only instruments a query whose
	 * showstatctx is still unset, and gp_stats_collector allocates one itself
	 * when gpsc.enable_analyze and gpsc.enable_cdbstats are on.  A hook is
	 * pushed onto the head of the chain, so registering last means running
	 * first -- see _PG_init() in gp_stats_collector.c.
	 */
	prev_ExecutorStart_hook  = ExecutorStart_hook;
	ExecutorStart_hook       = pg_qs_executor_start;
	prev_ExecutorRun_hook    = ExecutorRun_hook;
	ExecutorRun_hook         = pg_qs_executor_run;
	prev_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook      = pg_qs_executor_finish;
	prev_ExecutorEnd_hook    = ExecutorEnd_hook;
	ExecutorEnd_hook         = pg_qs_executor_end;

	elog(LOG, "pg_query_state: signal infrastructure initialised");
}

/* Executor lifecycle hooks */
/*
 * pg_qs_executor_start -- called at the start of executor execution.
 *
 * Enables instrumentation on the QueryDesc when:
 *   - pg_query_state is enabled
 *   - this is not an EXPLAIN-only execution
 *   - we are on a QD or QE role
 *   - there is no outer query already on the stack (top-level only)
 *   - the query passes the filter
 *   - no showstatctx is already attached
 *
 * Also assigns a synthetic queryId when the planner left it as zero.
 *
 * Parameters:
 *   queryDesc  -- the QueryDesc being started
 *   eflags     -- executor flags (EXEC_FLAG_EXPLAIN_ONLY etc.)
 */
static void
pg_qs_executor_start(QueryDesc *queryDesc, int eflags)
{
	instr_time starttime;

	if (pg_qs_enable
		&& ((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		&& (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE)
		&& is_querystack_empty()
		&& filter_query(queryDesc)
		&& queryDesc->showstatctx == NULL)
	{
		queryDesc->instrument_options |= INSTRUMENT_CDB;
		queryDesc->instrument_options |= INSTRUMENT_ROWS;
		if (pg_qs_timing)
			queryDesc->instrument_options |= INSTRUMENT_TIMER;
		if (pg_qs_buffers)
			queryDesc->instrument_options |= INSTRUMENT_BUFFERS;

		INSTR_TIME_SET_CURRENT(starttime);

		/*
		 * cdbexplain_showExecStatsBegin() aggregates QE stats on the QD and
		 * asserts Gp_role != GP_ROLE_EXECUTE, so it must run on the dispatcher
		 * only.  QE backends still get instrument_options above, which is all
		 * the per-node walker reads.
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
			queryDesc->showstatctx =
				cdbexplain_showExecStatsBegin(queryDesc, starttime);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);

		gpsc_reset_node_roll_state();
	}

	if (queryDesc->plannedstmt->queryId == 0)
		queryDesc->plannedstmt->queryId =
			((uint64) gp_command_count << 32) + qs_push_count;

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * pg_qs_executor_run -- called when the executor begins fetching tuples.
 *
 * Keeps the QueryDesc on the stack for as long as tuples are being fetched, so
 * that a poll arriving mid-run finds it.
 */
static void
pg_qs_executor_run(QueryDesc *queryDesc, ScanDirection direction,
				   uint64 count, bool execute_once)
{
	push_query(queryDesc);
	PG_TRY();
	{
		if (prev_ExecutorRun_hook)
			prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		pg_qs_pop_query();
	}
	PG_END_TRY();
}

/*
 * pg_qs_executor_finish -- called after all tuples have been fetched.
 *
 * Same push/pop as the run phase: the query stays visible to signal handlers
 * while after-triggers and the like are still running.
 */
static void
pg_qs_executor_finish(QueryDesc *queryDesc)
{
	push_query(queryDesc);
	PG_TRY();
	{
		if (prev_ExecutorFinish_hook)
			prev_ExecutorFinish_hook(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		pg_qs_pop_query();
	}
	PG_END_TRY();
}

/*
 * pg_qs_executor_end -- called when executor resources are released.
 *
 * Drops the per-node rolling state so the next query on this backend starts its
 * delta accounting clean.  It does not collect or push anything: a finish is not
 * a signalled collection and carries no trace_id to key a batch under.
 */
static void
pg_qs_executor_end(QueryDesc *queryDesc)
{
	if (queryDesc && pg_qs_enable)
		gpsc_reset_node_roll_state();

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static void
push_query(QueryDesc *queryDesc)
{
	qs_push_count++;
	QueryDescStack = lcons(queryDesc, QueryDescStack);
}

static void
pg_qs_pop_query(void)
{
	QueryDescStack = list_delete_first(QueryDescStack);
}

static bool
is_querystack_empty(void)
{
	return list_length(QueryDescStack) == 0;
}

QueryDesc *
get_toppest_query(void)
{
	return (QueryDescStack == NIL) ? NULL : (QueryDesc *) llast(QueryDescStack);
}

/*
 * filter_query -- decide whether to instrument a given QueryDesc.
 *
 * Returns false for cursor queries with non-default cursor options, and for
 * utility statements.  Returns true for SELECT, INSERT, UPDATE, DELETE.
 */
static bool
filter_query(QueryDesc *queryDesc)
{
	Portal portal;

	if (queryDesc == NULL)
		return false;

	if (queryDesc->extended_query && queryDesc->portal_name)
	{
		portal = GetPortalByName(queryDesc->portal_name);
		if (!PointerIsValid(portal) || portal->cursorOptions != CURSOR_OPT_NO_SCROLL)
			return false;
	}

	return (queryDesc->operation == CMD_SELECT  ||
			queryDesc->operation == CMD_DELETE  ||
			queryDesc->operation == CMD_INSERT  ||
			queryDesc->operation == CMD_UPDATE);
}

/*
 * LockShmem -- acquire an exclusive user-lock keyed by (PG_QS_MODULE_KEY, key).
 *
 * Used to serialise access to the shared mq between concurrent requestors
 * and between requestor and handler.
 */
static void
LockShmem(LOCKTAG *tag, uint32 key)
{
	LockAcquireResult result;

	tag->locktag_field1 = PG_QS_MODULE_KEY;
	tag->locktag_field2 = key;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type   = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;

	result = LockAcquire(tag, ExclusiveLock, false, false);
	Assert(result == LOCKACQUIRE_OK);
}

/*
 * UnlockShmem -- release the exclusive user-lock acquired by LockShmem.
 */
static void
UnlockShmem(LOCKTAG *tag)
{
	LockRelease(tag, ExclusiveLock, false);
}

/*
 * GetRemoteBackendInfo -- obtain the list of (segid, pid) pairs from QD.
 *
 * Sends BackendInfoPollReason to proc and waits for the reply.  On success,
 * *result is populated with gp_segment_pid entries (palloc'd).
 *
 * Returns the PG_QS_RequestResult code from the reply.
 */
static PG_QS_RequestResult
GetRemoteBackendInfo(PGPROC *proc, List **result)
{
	int sig_result;
	shm_mq_handle *mqh;
	shm_mq_result mq_receive_result;
	Size msg_len;
	backend_info *msg;
	LOCKTAG tag;
	int i;

	LockShmem(&tag, PG_QS_SND_KEY);
	params->reason = BackendInfoPollReason;
	mq = shm_mq_create(mq, QUEUE_SIZE);
	shm_mq_set_sender(mq, proc);
	shm_mq_set_receiver(mq, MyProc);
	*mq_req_id = reqid;
	UnlockShmem(&tag);

	sig_result = SendProcSignal(proc->pid, BackendInfoPollReason,
								proc->backendId);
	if (sig_result == -1)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not send BackendInfoPollReason signal")));

	mqh = shm_mq_attach(mq, NULL, NULL);
	mq_receive_result = shm_mq_receive_with_timeout(mqh, &msg_len,
													(void **) &msg,
													MAX_RCV_TIMEOUT);

	if (mq_receive_result != SHM_MQ_SUCCESS || msg == NULL ||
		msg->reqid != (uint32) reqid)
	{
		shm_mq_detach(mqh);
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
						  errmsg("GetRemoteBackendInfo: message not received")));
		return QUERY_NOT_RUNNING;
	}

	if (msg->result_code != QS_RETURNED)
	{
		PG_QS_RequestResult result_code = msg->result_code;
		shm_mq_detach(mqh);
		return result_code;
	}

	/* Validate the reply payload length against the reported backend count. */
	{
		int expected_len = BASE_SIZEOF_GP_BACKEND_INFO +
						   msg->number * sizeof(gp_segment_pid);
		if ((int) msg_len != expected_len)
		{
			shm_mq_detach(mqh);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("GetRemoteBackendInfo: unexpected message length")));
		}
	}

	for (i = 0; i < msg->number; i++)
	{
		gp_segment_pid *segpid = palloc(sizeof(gp_segment_pid));
		*segpid = msg->pids[i];
		*result = lcons(segpid, *result);
	}

	shm_mq_detach(mqh);
	return QS_RETURNED;
}

/*
 * CollectQEQueryState -- fan-out query-state signals to the segment QEs.
 *
 * Dispatches a cbdb_mpp_query_state() call to each segment listed in
 * backendInfo.  Results are returned as raw CdbPgResults.
 *
 * GPSC_SEGID_ENTRY_DB entries are left out: the dispatch reaches primary
 * segments only, and there the receiving cbdb_mpp_query_state() matches
 * entries against its own GpIdentity.segindex, which is never negative.
 * SignalEntryDbBackends() handles those.
 */
static void
CollectQEQueryState(List *backendInfo, bytea *trace_id)
{
	ListCell       *lc;
	StringInfoData  params_buf;
	char           *sql;
	char            trace_id_hex[2 * GPSC_TRACE_ID_LEN + 1];
	int             nsegments = 0;

	if (list_length(backendInfo) == 0)
		return;

	initStringInfo(&params_buf);

	foreach(lc, backendInfo)
	{
		gp_segment_pid *segpid = (gp_segment_pid *) lfirst(lc);

		if (segpid->segid < 0)
			continue;

		if (nsegments++ > 0)
			appendStringInfoChar(&params_buf, ',');
		appendStringInfo(&params_buf, "'(%d,%d)'", segpid->segid, segpid->pid);
	}

	if (nsegments == 0)
	{
		pfree(params_buf.data);
		return;
	}

	hex_encode(VARDATA_ANY(trace_id), GPSC_TRACE_ID_LEN, trace_id_hex);
	trace_id_hex[2 * GPSC_TRACE_ID_LEN] = '\0';
	sql = psprintf("SELECT gpsc.cbdb_mpp_query_state((ARRAY[%s])::gpsc.gp_segment_pid[], '\\x%s'::bytea)",
				   params_buf.data, trace_id_hex);

	CdbDispatchCommand(sql, DF_NONE, NULL);
	pfree(params_buf.data);
	pfree(sql);
}

/*
 * SignalEntryDbBackends -- poll the entry-db QEs listed in backendInfo.
 *
 * An entry-db reader runs the coordinator-side slice of a distributed query and
 * so holds the only instrumentation for it, but it lives in the coordinator's
 * own postmaster and no dispatch reaches it.  Since it is a local backend, the
 * QD signals it the same way it signals itself.
 */
static void
SignalEntryDbBackends(List *backendInfo, bytea *trace_id)
{
	ListCell *lc;

	foreach(lc, backendInfo)
	{
		gp_segment_pid *segpid = (gp_segment_pid *) lfirst(lc);
		PGPROC         *proc;

		if (segpid->segid != GPSC_SEGID_ENTRY_DB)
			continue;

		proc = BackendPidGetProc(segpid->pid);
		if (!proc || proc->backendId == InvalidBackendId)
			continue;

		memcpy(qs_trace_slots[proc->backendId], VARDATA_ANY(trace_id),
			   GPSC_TRACE_ID_LEN);
		if (SendProcSignal(proc->pid, QueryStatePollReason,
						   proc->backendId) == -1)
			elog(DEBUG1, "pg_query_state: failed to signal entry-db backend pid=%d",
				 segpid->pid);
	}
}

/*
 * shm_mq_receive_with_timeout -- receive from mqh, blocking up to `timeout` ms.
 *
 * Calls receive_msg_by_parts() in a loop, sleeping on the latch between
 * retries.  Returns SHM_MQ_SUCCESS, SHM_MQ_DETACHED, or SHM_MQ_WOULD_BLOCK
 * (the last meaning the timeout expired).
 *
 * On success, *nbytesp is set to the message length and *datap to a palloc'd
 * buffer containing the message.
 */
static shm_mq_result
shm_mq_receive_with_timeout(shm_mq_handle *mqh,
							Size *nbytesp,
							void **datap,
							int64 timeout)
{
	int        rc = 0;
	int64      delay = timeout;
	instr_time start_time;
	instr_time cur_time;

	INSTR_TIME_SET_CURRENT(start_time);

	for (;;)
	{
		shm_mq_result result;

		result = receive_msg_by_parts(mqh, nbytesp, datap, timeout, &rc, true);
		if (result != SHM_MQ_WOULD_BLOCK)
			return result;

		if (rc & WL_TIMEOUT || delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
					   delay, PG_WAIT_EXTENSION);

		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);
		delay = timeout - (int64) INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}
}

/*
 * receive_msg_by_parts -- reassemble a multi-chunk message from mqh.
 *
 * The wire protocol prefixes each message with its total byte count (a Size),
 * followed by one or more chunks of up to MSG_MAX_SIZE bytes.  This function
 * reads the prefix, allocates a buffer, and loops until all chunks arrive.
 *
 * Parameters:
 *   mqh     -- attached message-queue handle
 *   total   -- out: total bytes received
 *   datap   -- out: palloc'd buffer with reassembled message
 *   timeout -- caller's deadline in ms (used only for PART_RCV_DELAY retries)
 *   rc      -- out: WaitLatch flags (set to WL_TIMEOUT if we give up)
 *   nowait  -- passed through to shm_mq_receive
 */
static shm_mq_result
receive_msg_by_parts(shm_mq_handle *mqh, Size *total, void **datap,
					 int64 timeout, int *rc, bool nowait)
{
	shm_mq_result  mq_receive_result;
	shm_mq_msg    *buff;
	int            offset;
	Size          *expected;
	Size           expected_data;
	Size           len;

	/* Read the length prefix. */
	mq_receive_result = shm_mq_receive(mqh, &len, (void **) &expected, nowait);
	if (mq_receive_result != SHM_MQ_SUCCESS)
		return mq_receive_result;
	Assert(len == sizeof(Size));

	expected_data = *expected;
	Assert(expected_data < UINT32_MAX);
	*datap = palloc0(expected_data);

	/* Reassemble chunks until we have expected_data bytes. */
	for (offset = 0; offset < (int) expected_data; )
	{
		int64 delay = timeout;

		for (;;)
		{
			mq_receive_result = shm_mq_receive(mqh, &len, (void **) &buff,
											   nowait);
			if (mq_receive_result != SHM_MQ_SUCCESS)
			{
				if (nowait && mq_receive_result == SHM_MQ_WOULD_BLOCK)
				{
					if (delay > 0)
					{
						pg_usleep(PART_RCV_DELAY * 1000);
						delay -= PART_RCV_DELAY;
						continue;
					}
					if (rc)
						*rc |= WL_TIMEOUT;
				}
				return mq_receive_result;
			}
			break;
		}
		memcpy((char *) *datap + offset, buff, len);
		offset += len;
	}

	*total = offset;
	return mq_receive_result;
}

/*
 * qs_fetch_backend_info -- serialise a backend-info request and collect the
 * (segid, pid) list for the query running on `proc`.
 *
 * Holds PG_QS_RCV_KEY across the request so concurrent requestors do not clobber
 * the shared mq, releasing it on both the success and error paths.
 */
static PG_QS_RequestResult
qs_fetch_backend_info(PGPROC *proc, List **backend_info)
{
	LOCKTAG             tag;
	PG_QS_RequestResult result;

	LockShmem(&tag, PG_QS_RCV_KEY);
	PG_TRY();
	{
		reqid = *mq_req_id + 1;
		result = GetRemoteBackendInfo(proc, backend_info);
		UnlockShmem(&tag);
	}
	PG_CATCH();
	{
		UnlockShmem(&tag);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/* SQL callable functions */
/*
 * pg_query_state -- entry point for the pg_query_state() SQL function.
 *
 * Obtains the user-id and segment-backend list from the target backend,
 * then fans out cbdb_mpp_query_state() to each QE.
 */
PG_FUNCTION_INFO_V1(pg_query_state);
Datum
pg_query_state(PG_FUNCTION_ARGS)
{
	pid_t                pid = PG_GETARG_INT32(0);
	bytea               *trace_id = PG_GETARG_BYTEA_P(1);
	PGPROC              *proc;
	PG_QS_RequestResult  result;
	List                *backend_info = NIL;

	if (VARSIZE_ANY_EXHDR(trace_id) != GPSC_TRACE_ID_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size of trace_id: %zu, expected %d",
						VARSIZE_ANY_EXHDR(trace_id), GPSC_TRACE_ID_LEN)));

	if (pid == MyProcPid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot extract state of current process")));

	if (!module_initialized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_query_state must be loaded via shared_preload_libraries")));

	proc = BackendPidGetProc(pid);
	if (!proc || proc->backendId == InvalidBackendId ||
		proc->databaseId == InvalidOid || proc->roleId == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("backend with pid=%d not found", pid)));

	if (!(superuser() || GetUserId() == proc->roleId))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied")));
	}

	result = qs_fetch_backend_info(proc, &backend_info);

	switch (result)
	{
		case QUERY_NOT_RUNNING:
			elog(DEBUG1, "pg_query_state: pid=%d is not running a query", pid);
			break;

		case STAT_DISABLED:
			elog(DEBUG1, "pg_query_state: stats collection disabled");
			break;

		case WRONG_ROLE:
			/*
			 * Not the QD, so there is no participant list to fan out to and no
			 * point signalling: a QE polled directly would report a single
			 * slice that no collection is waiting for.  Stay quiet here -- the
			 * caller-facing complaint belongs to pg_query_state_backends(),
			 * which errors out on the same result code.
			 */
			elog(DEBUG1, "pg_query_state: pid=%d is a query executor, not the QD", pid);
			break;

		case QS_RETURNED:
			/*
			 * Signal all segment QEs to push their plan-node stats via UDS,
			 * carrying the trace_id so every backend's batch lands under the one
			 * key this pg_query_state() invocation owns.
			 */
			CollectQEQueryState(backend_info, trace_id);
			SignalEntryDbBackends(backend_info, trace_id);

			/*
			 * Signal the QD backend itself so it pushes coordinator-side plan
			 * nodes and the plan-doc.  SendQueryState() emits directly via UDS.
			 * Stamp the target's own trace slot before signalling, so its batch
			 * lands under this collection's key.
			 */
			memcpy(qs_trace_slots[proc->backendId], VARDATA_ANY(trace_id),
				   GPSC_TRACE_ID_LEN);
			SendProcSignal(proc->pid, QueryStatePollReason, proc->backendId);
			break;
	}

	PG_RETURN_VOID();
}

/*
 * pg_query_state_backends -- list the QE backends participating in the query
 * running on backend `pid`.
 *
 * Returns a set of (segid, pid) rows obtained from the coordinator via
 * GetRemoteBackendInfo (the same list the poll path fans out to).  A consumer
 * can use the row count as the expected number of backends that will report.
 *
 * Uses the materialize SRF mode: the whole list is built into a tuplestore in
 * one call.  Returns an empty set when the target query is not running.
 */
PG_FUNCTION_INFO_V1(pg_query_state_backends);
Datum
pg_query_state_backends(PG_FUNCTION_ARGS)
{
	pid_t                pid = PG_GETARG_INT32(0);
	ReturnSetInfo       *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc            tupdesc;
	Tuplestorestate     *tupstore;
	PGPROC              *proc;
	List                *backend_info = NIL;
	PG_QS_RequestResult  result;
	ListCell            *lc;

	InitMaterializedSRF(fcinfo, 0);
	tupdesc  = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	if (pid == MyProcPid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot extract state of current process")));

	proc = BackendPidGetProc(pid);
	if (!proc || proc->backendId == InvalidBackendId ||
		proc->databaseId == InvalidOid || proc->roleId == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("backend with pid=%d not found", pid)));

	if (!module_initialized)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_query_state must be loaded via shared_preload_libraries")));
	}

	if (!(superuser() || GetUserId() == proc->roleId))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied")));
	}

	result = qs_fetch_backend_info(proc, &backend_info);

	/*
	 * Not running / disabled are ordinary outcomes of polling a pid that has
	 * just finished: return an empty set rather than erroring.  A wrong-role
	 * target is different -- the backend is alive and will never answer, which
	 * a caller must be able to tell apart from a finished query, so that one
	 * does error out.
	 */
	switch (result)
	{
		case QUERY_NOT_RUNNING:
			elog(DEBUG1, "pg_query_state_backends: pid=%d is not running a query", pid);
			return (Datum) 0;

		case STAT_DISABLED:
			elog(DEBUG1, "pg_query_state_backends: stats collection disabled");
			return (Datum) 0;

		case WRONG_ROLE:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("backend with pid=%d is a query executor, "
							"not the session's coordinator backend", pid)));
			break;

		case QS_RETURNED:
			break;
	}

	foreach(lc, backend_info)
	{
		gp_segment_pid *segpid = (gp_segment_pid *) lfirst(lc);
		Datum           values[2];
		bool            nulls[2] = {false, false};

		values[0] = Int32GetDatum(segpid->segid);
		values[1] = Int32GetDatum(segpid->pid);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/*
	 * QD-only query (INSERT ... VALUES, catalog reads, and other coordinator-
	 * local plans): no QE gang ran, so backend_info is empty even though the
	 * coordinator is executing and will push its own per-node batch.  Report the
	 * coordinator itself so the caller does not mistake an empty QE list for a
	 * finished query and drop the QD's batch.
	 */
	if (list_length(backend_info) == 0)
	{
		Datum   values[2];
		bool    nulls[2] = {false, false};

		values[0] = Int32GetDatum(GPSC_SEGID_QD);
		values[1] = Int32GetDatum(proc->pid);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * cbdb_mpp_query_state -- QE-side entry point dispatched by CollectQEQueryState.
 *
 * Receives an array of gp_segment_pid, filters those belonging to this
 * segment, and fires QueryStatePollReason at each matching backend.
 */
PG_FUNCTION_INFO_V1(cbdb_mpp_query_state);
Datum
cbdb_mpp_query_state(PG_FUNCTION_ARGS)
{
	ListCell *iter;
	List     *alive_procs = get_query_backend_info(PG_GETARG_ARRAYTYPE_P(0));
	bytea    *trace_id = PG_GETARG_BYTEA_P(1);

	if (VARSIZE_ANY_EXHDR(trace_id) != GPSC_TRACE_ID_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size of trace_id: %zu, expected %d",
						VARSIZE_ANY_EXHDR(trace_id), GPSC_TRACE_ID_LEN)));

	if (alive_procs == NIL)
		PG_RETURN_NULL();

	foreach(iter, alive_procs)
	{
		PGPROC *proc = (PGPROC *) lfirst(iter);
		int sig_result;

		if (!proc || proc->backendId == InvalidBackendId)
			continue;

		/* Stamp the target's own trace slot before signalling it. */
		memcpy(qs_trace_slots[proc->backendId], VARDATA_ANY(trace_id),
			   GPSC_TRACE_ID_LEN);

		sig_result = SendProcSignal(proc->pid, QueryStatePollReason,
									proc->backendId);
		if (sig_result == -1)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cbdb_mpp_query_state: failed to send signal to pid %d",
								   proc->pid)));
	}
	PG_RETURN_VOID();
}

/*
 * get_query_backend_info -- convert a gp_segment_pid[] SQL array to a list
 * of PGPROC pointers for backends running on this segment.
 *
 * Skips entries for other segments and entries whose backend has exited.
 */
static List *
get_query_backend_info(ArrayType *array)
{
	int16  typlen;
	bool   typbyval;
	char   typalign;
	Oid    element_type = ARR_ELEMTYPE(array);
	Datum *data;
	bool  *nulls;
	int    nitems;
	int    len;
	List  *alive_procs = NIL;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	deconstruct_array(array, element_type, typlen, typbyval, typalign,
					  &data, &nulls, &nitems);

	len = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	for (int i = 0; i < len; i++)
	{
		if (nulls[i])
			continue;

		HeapTupleHeader td = DatumGetHeapTupleHeader(data[i]);
		TupleDesc       tupdesc;
		HeapTupleData   tmptup;
		int32           pid;
		int32           segid;
		bool            segid_isnull = false;
		bool            pid_isnull = false;
		PGPROC         *proc;

		tupdesc = lookup_rowtype_tupdesc_copy(
			HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
		tmptup.t_len  = HeapTupleHeaderGetDatumLength(td);
		tmptup.t_data = td;

		segid = DatumGetInt32(heap_getattr(&tmptup, 1, tupdesc, &segid_isnull));
		pid   = DatumGetInt32(heap_getattr(&tmptup, 2, tupdesc, &pid_isnull));
		FreeTupleDesc(tupdesc);

		if (segid_isnull || pid_isnull || segid != GpIdentity.segindex)
			continue;

		proc = BackendPidGetProc(pid);
		if (!proc)
			continue;

		alive_procs = lappend(alive_procs, proc);
	}
	return alive_procs;
}
