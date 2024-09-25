/*
 * pg_query_state.c
 *		Extract information about query state from other backend
 *
 * Copyright (c) 2016-2024, Postgres Professional
 *
 *	  contrib/pg_query_state/pg_query_state.c
 * IDENTIFICATION
 */
#include "pg_query_state.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "executor/execParallel.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shm_toc.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "libpq-int.h"

#define TEXT_CSTR_CMP(text, cstr) \
	(memcmp(VARDATA(text), (cstr), VARSIZE(text) - VARHDRSZ))
#define HEADER_LEN sizeof(int) * 2

/* GUC variables */
bool pg_qs_enable = true;
bool pg_qs_timing = false;
bool pg_qs_buffers = false;
StringInfo queryStateData = NULL;
volatile pg_atomic_uint32 *pg_qs_on;
/*
 * CachedQueryStateInfo both exists on QE and QD
 *
 * On QE, it is used by pg_query_state. When one
 * query node is finished, the query maybe still
 * running. So then pg_query_state is called to
 * fetch the whole query running state, use it for
 * the finished query node. We cached the query
 * state info at end the query. And reset it
 * when next query starts.
 *
 * On QD, it is used to cache the whole query
 * state info. And gpmon_query_info_collect_hook
 * will send it to gpsmon. Also reset it when
 * next query starts.
 */
query_state_info *CachedQueryStateInfo = NULL;
MemoryContext queryStateCtx = NULL;

/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
/* hooks defined in this module */
static void qs_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void qs_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
						   uint64 count, bool execute_once);
static void qs_ExecutorFinish(QueryDesc *queryDesc);
static void qs_ExecutorEnd(QueryDesc *queryDesc);
static void clear_queryStateInfo(void);
static void
set_CachedQueryStateInfo(int sliceIndex, StringInfo strInfo, int gp_command_count, int queryId);
static shm_mq_result receive_msg_by_parts(shm_mq_handle *mqh, Size *total,
										  void **datap, int64 timeout, int *rc, bool nowait);
/* functions added by cbdb */
static List *GetRemoteBackendInfo(PGPROC *proc);
static CdbPgResults CollectQEQueryState(List *backendInfo);
static List *get_query_backend_info(ArrayType *array);
static shm_mq_msg *GetRemoteBackendQueryStates(CdbPgResults cdb_pgresults,
										 PGPROC *proc,
										 bool verbose,
										 bool costs,
										 bool timing,
										 bool buffers,
										 bool triggers,
										 ExplainFormat format);
static void qs_print_plan(qs_query *query);
static bool filter_query_common(QueryDesc *queryDesc);
	/* functions added by cbdb */

/* important to record the info of the peer */
static void check_and_init_peer(LOCKTAG *tag, PGPROC *proc, int n_peers);
static shm_mq_msg *receive_final_query_state(void);
static bool wait_for_mq_ready(shm_mq *mq);
static List *get_cdbStateCells(CdbPgResults cdb_pgresults);
static qs_query *push_query(QueryDesc *queryDesc);
static void pop_query(void);

/* Global variables */
List 					*QueryDescStack = NIL;
static ProcSignalReason UserIdPollReason = INVALID_PROCSIGNAL;
static ProcSignalReason QueryStatePollReason = INVALID_PROCSIGNAL;
static ProcSignalReason BackendInfoPollReason = INVALID_PROCSIGNAL;
static bool 			module_initialized = false;
static const char		*be_state_str[] = {						/* BackendState -> string repr */
							"undefined",						/* STATE_UNDEFINED */
							"idle",								/* STATE_IDLE */
							"active",							/* STATE_RUNNING */
							"idle in transaction",				/* STATE_IDLEINTRANSACTION */
							"fastpath function call",			/* STATE_FASTPATH */
							"idle in transaction (aborted)",	/* STATE_IDLEINTRANSACTION_ABORTED */
							"disabled",							/* STATE_DISABLED */
						};
static int              reqid = 0;

typedef struct
{
	slock_t	 mutex;		/* protect concurrent access to `userid` */
	Oid		 userid;
	Latch	*caller;
	pg_atomic_uint32 n_peers;
} RemoteUserIdResult;

static void SendCurrentUserId(void);
//static void SendBgWorkerPids(void);
static Oid GetRemoteBackendUserId(PGPROC *proc);


/* Shared memory variables */
shm_toc			   *toc = NULL;
RemoteUserIdResult *counterpart_userid = NULL;
pg_qs_params   	   *params = NULL;
shm_mq 			   *mq = NULL;

/* Running on QE to collect query state from slices */
PG_FUNCTION_INFO_V1(pg_query_state);
PG_FUNCTION_INFO_V1(cbdb_mpp_query_state);
PG_FUNCTION_INFO_V1(query_state_pause);
PG_FUNCTION_INFO_V1(query_state_resume);
PG_FUNCTION_INFO_V1(query_state_pause_command);
PG_FUNCTION_INFO_V1(query_state_resume_command);
/*
 * Estimate amount of shared memory needed.
 */
static Size
pg_qs_shmem_size()
{
	shm_toc_estimator	e;
	Size				size;
	int					nkeys;

	shm_toc_initialize_estimator(&e);

	nkeys = 3;

	shm_toc_estimate_chunk(&e, sizeof(RemoteUserIdResult));
	shm_toc_estimate_chunk(&e, sizeof(pg_qs_params));
	shm_toc_estimate_chunk(&e, (Size) QUEUE_SIZE);

	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);


	size = MAXALIGN(size) + MAXALIGN(sizeof(pg_atomic_uint32));
	return size;
}

/*
 * Distribute shared memory.
 */
static void
pg_qs_shmem_startup(void)
{
	bool	found;
	Size	shmem_size = pg_qs_shmem_size() - MAXALIGN(sizeof(pg_atomic_uint32));
	void	*shmem;
	int		num_toc = 0;

	shmem = ShmemInitStruct("pg_query_state", shmem_size, &found);
	if (!found)
	{
		toc = shm_toc_create(PG_QS_MODULE_KEY, shmem, shmem_size);

		counterpart_userid = shm_toc_allocate(toc, sizeof(RemoteUserIdResult));
		shm_toc_insert(toc, num_toc++, counterpart_userid);
		SpinLockInit(&counterpart_userid->mutex);
		pg_atomic_init_u32(&counterpart_userid->n_peers, 0);

		params = shm_toc_allocate(toc, sizeof(pg_qs_params));
		shm_toc_insert(toc, num_toc++, params);

		mq = shm_toc_allocate(toc, QUEUE_SIZE);
		shm_toc_insert(toc, num_toc++, mq);
	}
	else
	{
		toc = shm_toc_attach(PG_QS_MODULE_KEY, shmem);

#if PG_VERSION_NUM < 100000
		counterpart_userid = shm_toc_lookup(toc, num_toc++);
		params = shm_toc_lookup(toc, num_toc++);
		mq = shm_toc_lookup(toc, num_toc++);
#else
		counterpart_userid = shm_toc_lookup(toc, num_toc++, false);
		params = shm_toc_lookup(toc, num_toc++, false);
		mq = shm_toc_lookup(toc, num_toc++, false);
#endif
	}
	pg_qs_on = (pg_atomic_uint32 *) ShmemInitStruct("pg_qs_on", sizeof(pg_atomic_uint32), &found);
	if (!found)
		pg_atomic_init_u32(pg_qs_on, 1);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	module_initialized = true;
}

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void pg_qs_shmem_request(void);
#endif

/*
 * Module load callback
 */
void
init_pg_query_state(void)
{
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_qs_shmem_request;
#else
	RequestAddinShmemSpace(pg_qs_shmem_size());
#endif

	/* Register interrupt on custom signal of polling query state */
	UserIdPollReason = RegisterCustomProcSignalHandler(SendCurrentUserId);
	QueryStatePollReason = RegisterCustomProcSignalHandler(SendQueryState);
	BackendInfoPollReason = RegisterCustomProcSignalHandler(SendCdbComponents);
	if (QueryStatePollReason == INVALID_PROCSIGNAL
		|| UserIdPollReason == INVALID_PROCSIGNAL
		|| BackendInfoPollReason == INVALID_PROCSIGNAL)
	{
		ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						  errmsg("pg_query_state isn't loaded: insufficient custom ProcSignal slots")));
		return;
	}

	/* Define custom GUC variables */
	DefineCustomBoolVariable("pg_query_state.enable",
							 "Enable module.",
							 NULL,
							 &pg_qs_enable,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("pg_query_state.enable_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &pg_qs_timing,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("pg_query_state.enable_buffers",
							 "Collect buffer usage.",
							 NULL,
							 &pg_qs_buffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	EmitWarningsOnPlaceholders("pg_query_state");

	/* Install hooks */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		prev_ExecutorStart = ExecutorStart_hook;
		ExecutorStart_hook = qs_ExecutorStart;
	}
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = qs_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = qs_ExecutorFinish;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_qs_shmem_startup;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = qs_ExecutorEnd;
}

#if PG_VERSION_NUM >= 150000
static void
pg_qs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pg_qs_shmem_size());
}
#endif

/*
 * ExecutorStart hook:
 * 		set up flags to store runtime statistics,
 * 		push current query description in global stack
 */
static void
qs_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	instr_time		starttime;
	/* Enable per-node instrumentation */
	if (enable_qs_runtime() && ((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0) &&
		Gp_role == GP_ROLE_DISPATCH && is_querystack_empty() &&
		filter_query_common(queryDesc))
	{
		queryDesc->instrument_options |= INSTRUMENT_CDB;
		queryDesc->instrument_options |= INSTRUMENT_ROWS;
		if (pg_qs_timing)
			queryDesc->instrument_options |= INSTRUMENT_TIMER;
		if (pg_qs_buffers)
			queryDesc->instrument_options |= INSTRUMENT_BUFFERS;

		INSTR_TIME_SET_CURRENT(starttime);
		queryDesc->showstatctx = cdbexplain_showExecStatsBegin(queryDesc,
															   starttime);
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
	if (enable_qs_runtime() && ((eflags & EXEC_FLAG_EXPLAIN_ONLY)) == 0 &&
		  queryDesc->totaltime == NULL && Gp_role == GP_ROLE_DISPATCH
		  && is_querystack_empty())
	{
		MemoryContext oldcxt;
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ExecutorRun:
 * 		Catch any fatal signals
 */
static void
qs_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
			   bool execute_once)
{
	/* Clear query state info if we are in a new query */
	if (is_querystack_empty() && CachedQueryStateInfo != NULL)
	{
			clear_queryStateInfo();
	}
	push_query(queryDesc);

	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM < 100000
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
#else
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#endif
		pop_query();
	}
	PG_CATCH();
	{
		pop_query();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish:
 * 		Catch any fatal signals
 */
static void
qs_ExecutorFinish(QueryDesc *queryDesc)
{
	push_query(queryDesc);
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		pop_query();
	}
	PG_CATCH();
	{
		pop_query();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Find PgBackendStatus entry
 */
static PgBackendStatus *
search_be_status(int pid)
{
	int beid;

	if (pid <= 0)
		return NULL;

	for (beid = 1; beid <= pgstat_fetch_stat_numbackends(); beid++)
	{
#if PG_VERSION_NUM >= 160000
		PgBackendStatus *be_status = pgstat_get_beentry_by_backend_id(beid);
#else
		PgBackendStatus *be_status = pgstat_fetch_stat_beentry(beid);
#endif

		if (be_status && be_status->st_procpid == pid)
			return be_status;
	}

	return NULL;
}


void
UnlockShmem(LOCKTAG *tag)
{
	LockRelease(tag, ExclusiveLock, false);
}

void
LockShmem(LOCKTAG *tag, uint32 key)
{
	LockAcquireResult result;
	tag->locktag_field1 = PG_QS_MODULE_KEY;
	tag->locktag_field2 = key;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
	result = LockAcquire(tag, ExclusiveLock, false, false);
	Assert(result == LOCKACQUIRE_OK);
	elog(DEBUG1, "LockAcquireResult is OK %d", result);
}

/*
 * Structure of stack frame of fucntion call which transfers through message queue
 */
typedef struct
{
	text	*query;
	text	*plan;
} stack_frame;

/*
 *	Convert serialized stack frame into stack_frame record
 *		Increment '*src' pointer to the next serialized stack frame
 */
static stack_frame *
deserialize_stack_frame(char **src)
{
	stack_frame *result = palloc(sizeof(stack_frame));
	text		*query = (text *) *src,
				*plan = (text *) (*src + INTALIGN(VARSIZE(query)));

	result->query = palloc(VARSIZE(query));
	memcpy(result->query, query, VARSIZE(query));
	result->plan = palloc(VARSIZE(plan));
	memcpy(result->plan, plan, VARSIZE(plan));

	*src = (char *) plan + INTALIGN(VARSIZE(plan));
	return result;
}

/*
 * Convert serialized stack frames into List of stack_frame records
 */
static List *
deserialize_stack(char *src, int stack_depth)
{
	List 	*result = NIL;
	char	*curr_ptr = src;
	int		 i;

	for (i = 0; i < stack_depth; i++)
	{
		stack_frame	*frame = deserialize_stack_frame(&curr_ptr);
		result = lappend(result, frame);
	}

	return result;
}

/*
 * Implementation of pg_query_state function
 */
Datum
pg_query_state(PG_FUNCTION_ARGS)
{
	typedef struct
	{
		PGPROC 		*proc;
		ListCell 	*frame_cursor;
		int			 frame_index;
		List		*stack;
	} proc_state;

	/* multicall context type */
	typedef struct
	{
		ListCell	*proc_cursor;
		List		*procs;
	} pg_qs_fctx;

	FuncCallContext	*funcctx;
	MemoryContext	oldcontext;
	pg_qs_fctx		*fctx;
#define		 N_ATTRS  5
	pid_t			pid = PG_GETARG_INT32(0);
	if (!enable_qs_runtime())
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_query_state is not enabled or paused")));
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_query_state can only be called on coordinator")));

	if (SRF_IS_FIRSTCALL())
	{
		LOCKTAG			 tag;
		bool			 verbose = PG_GETARG_BOOL(1),
						 costs = PG_GETARG_BOOL(2),
						 timing = PG_GETARG_BOOL(3),
						 buffers = PG_GETARG_BOOL(4),
						 triggers = PG_GETARG_BOOL(5);
		text			*format_text = PG_GETARG_TEXT_P(6);
		ExplainFormat	 format;
		PGPROC			*proc;
		shm_mq_msg		*msg;
		List			*msgs;
		List			*backendInfo;

		if (!module_initialized)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_query_state wasn't initialized yet")));

		if (pid == MyProcPid)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("attempt to extract state of current process")));

		proc = BackendPidGetProc(pid);
		if (!proc || proc->backendId == InvalidBackendId || proc->databaseId == InvalidOid || proc->roleId == InvalidOid)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("backend with pid=%d not found", pid)));

		if (TEXT_CSTR_CMP(format_text, "text") == 0)
			format = EXPLAIN_FORMAT_TEXT;
		else if (TEXT_CSTR_CMP(format_text, "xml") == 0)
			format = EXPLAIN_FORMAT_XML;
		else if (TEXT_CSTR_CMP(format_text, "json") == 0)
			format = EXPLAIN_FORMAT_JSON;
		else if (TEXT_CSTR_CMP(format_text, "yaml") == 0)
			format = EXPLAIN_FORMAT_YAML;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unrecognized 'format' argument")));
		/*
		 * init and acquire lock so that any other concurrent calls of this fuction
		 * can not occupy shared queue for transfering query state
		 */
		LockShmem(&tag, PG_QS_RCV_KEY);
		check_and_init_peer(&tag, proc, 1);

		backendInfo = GetRemoteBackendInfo(proc);
		CdbPgResults cdb_pgresults = CollectQEQueryState(backendInfo);
		AttachPeer();
		msg =  GetRemoteBackendQueryStates(cdb_pgresults,
										   proc,
										   verbose,
										   costs,
										   timing,
										   buffers,
										   triggers,
										   format);

		msgs = NIL;
		if (msg != NULL)
		{
			msgs = lappend(msgs, msg );
		}

		funcctx = SRF_FIRSTCALL_INIT();
		if (msgs == NULL || list_length(msgs) == 0)
		{
			elog(DEBUG1, "backend does not reply");
			UnlockShmem(&tag);
			SRF_RETURN_DONE(funcctx);
		}

		msg = (shm_mq_msg *) linitial(msgs);
		switch (msg->result_code)
		{
			case QUERY_NOT_RUNNING:
				{
					PgBackendStatus	*be_status = search_be_status(pid);

					if (be_status)
						elog(INFO, "state of backend is %s",
								be_state_str[be_status->st_state - STATE_UNDEFINED]);
					else
						elog(INFO, "backend is not running query");

					UnlockShmem(&tag);
					SRF_RETURN_DONE(funcctx);
				}
			case STAT_DISABLED:
				elog(INFO, "query execution statistics disabled");
				UnlockShmem(&tag);
				SRF_RETURN_DONE(funcctx);
			case QS_RETURNED:
				{
					TupleDesc	tupdesc;
					ListCell	*i;
					int64		max_calls = 0;

					/* print warnings if exist */
					if (msg->warnings & TIMINIG_OFF_WARNING)
						ereport(WARNING, (errcode(ERRCODE_WARNING),
										  errmsg("timing statistics disabled")));
					if (msg->warnings & BUFFERS_OFF_WARNING)
						ereport(WARNING, (errcode(ERRCODE_WARNING),
										  errmsg("buffers statistics disabled")));

					oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

					/* save stack of calls and current cursor in multicall context */
					fctx = (pg_qs_fctx *) palloc(sizeof(pg_qs_fctx));
					fctx->procs = NIL;
					foreach(i, msgs)
					{
						List 		*qs_stack;
						shm_mq_msg	*current_msg = (shm_mq_msg *) lfirst(i);
						proc_state	*p_state = (proc_state *) palloc(sizeof(proc_state));

						if (current_msg->result_code != QS_RETURNED)
							continue;

						Assert(current_msg->result_code == QS_RETURNED);

						qs_stack = deserialize_stack(current_msg->stack,
													 current_msg->stack_depth);

						p_state->proc = current_msg->proc;
						p_state->stack = qs_stack;
						p_state->frame_index = 0;
						p_state->frame_cursor = list_head(qs_stack);

						fctx->procs = lappend(fctx->procs, p_state);

						max_calls += list_length(qs_stack);
					}
					fctx->proc_cursor = list_head(fctx->procs);

					funcctx->user_fctx = fctx;
					funcctx->max_calls = max_calls;

					/* Make tuple descriptor */
#if PG_VERSION_NUM < 120000
					tupdesc = CreateTemplateTupleDesc(N_ATTRS, false);
#else
					tupdesc = CreateTemplateTupleDesc(N_ATTRS);
#endif
					TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid", INT4OID, -1, 0);
					TupleDescInitEntry(tupdesc, (AttrNumber) 2, "frame_number", INT4OID, -1, 0);
					TupleDescInitEntry(tupdesc, (AttrNumber) 3, "query_text", TEXTOID, -1, 0);
					TupleDescInitEntry(tupdesc, (AttrNumber) 4, "plan", TEXTOID, -1, 0);
					TupleDescInitEntry(tupdesc, (AttrNumber) 5, "leader_pid", INT4OID, -1, 0);
					funcctx->tuple_desc = BlessTupleDesc(tupdesc);

					UnlockShmem(&tag);
					MemoryContextSwitchTo(oldcontext);
				}
				break;
		}
	}

	/* restore function multicall context */
	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		HeapTuple 	 tuple;
		Datum		 values[N_ATTRS];
		bool		 nulls[N_ATTRS];
		proc_state	*p_state = (proc_state *) lfirst(fctx->proc_cursor);
		stack_frame	*frame = (stack_frame *) lfirst(p_state->frame_cursor);

		/* Make and return next tuple to caller */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(p_state->proc->pid);
		values[1] = Int32GetDatum(p_state->frame_index);
		values[2] = PointerGetDatum(frame->query);
		values[3] = PointerGetDatum(frame->plan);
		if (p_state->proc->pid == pid)
			nulls[4] = true;
		else
			values[4] = Int32GetDatum(pid);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* increment cursor */
#if PG_VERSION_NUM >= 130000
		p_state->frame_cursor = lnext(p_state->stack, p_state->frame_cursor);
#else
		p_state->frame_cursor = lnext(p_state->frame_cursor);
#endif
		p_state->frame_index++;

		if (p_state->frame_cursor == NULL)
#if PG_VERSION_NUM >= 130000
			fctx->proc_cursor = lnext(fctx->procs, fctx->proc_cursor);
#else
			fctx->proc_cursor = lnext(fctx->proc_cursor);
#endif

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
		SRF_RETURN_DONE(funcctx);
}

static void
SendCurrentUserId(void)
{
	SpinLockAcquire(&counterpart_userid->mutex);
	counterpart_userid->userid = GetUserId();
	SpinLockRelease(&counterpart_userid->mutex);

	SetLatch(counterpart_userid->caller);
}

/*
 * Extract effective user id from backend on which `proc` points.
 *
 * Assume the `proc` points on valid backend and it's not current process.
 *
 * This fuction must be called after registration of `UserIdPollReason` and
 * initialization `RemoteUserIdResult` object in shared memory.
 */
static Oid
GetRemoteBackendUserId(PGPROC *proc)
{
	Oid result;

	Assert(proc && proc->backendId != InvalidBackendId);
	Assert(UserIdPollReason != INVALID_PROCSIGNAL);
	Assert(counterpart_userid);

	counterpart_userid->userid = InvalidOid;
	counterpart_userid->caller = MyLatch;
	pg_write_barrier();

	SendProcSignal(proc->pid, UserIdPollReason, proc->backendId);
	int count = 0;
	int64 delay = 1000;
	for (;;)
	{
		SpinLockAcquire(&counterpart_userid->mutex);
		result = counterpart_userid->userid;
		SpinLockRelease(&counterpart_userid->mutex);

		if (result != InvalidOid || count == 3)
			break;

#if PG_VERSION_NUM < 100000
		WaitLatch(MyLatch, WL_LATCH_SET, 0);
#elif PG_VERSION_NUM < 120000
		WaitLatch(MyLatch, WL_LATCH_SET, 0, PG_WAIT_EXTENSION);
#else
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT, delay,
				  PG_WAIT_EXTENSION);
#endif
		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
		count ++;
	}

	return result;
}

/*
 * Receive a message from a shared message queue until timeout is exceeded.
 *
 * Parameter `*nbytes` is set to the message length and *data to point to the
 * message payload. If timeout is exceeded SHM_MQ_WOULD_BLOCK is returned.
 */
shm_mq_result
shm_mq_receive_with_timeout(shm_mq_handle *mqh,
							Size *nbytesp,
							void **datap,
							int64 timeout)
{
	int 		rc = 0;
	int64 		delay = timeout;
	instr_time	start_time;
	instr_time	cur_time;

	INSTR_TIME_SET_CURRENT(start_time);

	for (;;)
	{
		shm_mq_result mq_receive_result;

		mq_receive_result = receive_msg_by_parts(mqh, nbytesp, datap, timeout, &rc, true);
		if (mq_receive_result != SHM_MQ_WOULD_BLOCK)
			return mq_receive_result;
		if (rc & WL_TIMEOUT || delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

#if PG_VERSION_NUM < 100000
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, delay);
#elif PG_VERSION_NUM < 120000
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT,
					   delay, PG_WAIT_EXTENSION);
#else
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
					   delay, PG_WAIT_EXTENSION);
#endif

		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);

		delay = timeout - (int64) INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}
}

static shm_mq_result
receive_msg_by_parts(shm_mq_handle *mqh, Size *total, void **datap,
						int64 timeout, int *rc, bool nowait)
{
	shm_mq_result	mq_receive_result;
	shm_mq_msg	   *buff;
	int				offset;
	Size		   *expected;
	Size			expected_data;
	Size			len;

	/* Get the expected number of bytes in message */
	mq_receive_result = shm_mq_receive(mqh, &len, (void **) &expected, nowait);
	if (mq_receive_result != SHM_MQ_SUCCESS)
		return mq_receive_result;
	Assert(len == sizeof(Size));

	expected_data = *expected;
	*datap = palloc0(expected_data);
	elog(DEBUG1, "receive data len %zu " , expected_data);

	/* Get the message itself */
	for (offset = 0; offset < expected_data; )
	{
		int64 delay = timeout;
		/* Keep receiving new messages until we assemble the full message */
		for (;;)
		{
			mq_receive_result = shm_mq_receive(mqh, &len, ((void **) &buff), nowait);
			if (mq_receive_result != SHM_MQ_SUCCESS)
			{
				if (nowait && mq_receive_result == SHM_MQ_WOULD_BLOCK)
				{
					/*
					 * We can't leave this function during reading parts with
					 * error code SHM_MQ_WOULD_BLOCK because can be be error
					 * at next call receive_msg_by_parts() with continuing
					 * reading non-readed parts.
					 * So we should wait whole MAX_RCV_TIMEOUT timeout and
					 * return error after that only.
					*/
					if (delay > 0)
					{
						pg_usleep(PART_RCV_DELAY * 1000);
						delay -= PART_RCV_DELAY;
						continue;
					}
					if (rc)
					{	/* Mark that the timeout has expired: */
						*rc |= WL_TIMEOUT;
					}
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

void
AttachPeer(void)
{
	pg_atomic_add_fetch_u32(&counterpart_userid->n_peers, 1);
}

void
DetachPeer(void)
{
	int n_peers = pg_atomic_fetch_sub_u32(&counterpart_userid->n_peers, 1);
	if (n_peers <= 0)
		ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
					  errmsg("pg_query_state peer is not responding")));
}

/*
 * Extracts all QE worker running by process `proc`
 */
static List *
GetRemoteBackendInfo(PGPROC *proc)
{
	int sig_result;
	shm_mq_handle *mqh;
	shm_mq_result mq_receive_result;
	Size msg_len;
	backend_info *msg;
	int i;
	List *result = NIL;

	Assert(proc && proc->backendId != InvalidBackendId);
	Assert(BackendInfoPollReason!= INVALID_PROCSIGNAL);
	create_shm_mq(proc, MyProc);

	sig_result = SendProcSignal(proc->pid, BackendInfoPollReason, proc->backendId);
	if (sig_result == -1)
		goto signal_error;

	mqh = shm_mq_attach(mq, NULL, NULL);
	mq_receive_result = shm_mq_receive(mqh, &msg_len, (void **) &msg, false);
	if (mq_receive_result != SHM_MQ_SUCCESS || msg == NULL || msg->reqid != reqid)
		goto mq_error;
	if (msg->result_code == STAT_DISABLED)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("query execution statistics disabled")));
	if (msg->result_code == QUERY_NOT_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("backend is not running query")));
	int expect_len = BASE_SIZEOF_GP_BACKEND_INFO + msg->number * sizeof(gp_segment_pid);
	if (msg_len != expect_len)
		goto mq_error;

	for (i = 0; i < msg->number; i++)
	{
		gp_segment_pid *segpid = &(msg->pids[i]);
		elog(DEBUG1, "QE %d is running on segment %d", segpid->pid, segpid->segid);
		result = lcons(segpid, result);
	}
	shm_mq_detach(mqh);
	return result;

signal_error:
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid send signal")));
mq_error:
	shm_mq_detach(mqh);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("backend is not running query")));
}

/*
 * Dispatch sql SELECT cbdb_mpp_query_state(::gp_segment_pid[])
 * to collect QE query state
 * 
 * return: CdbPgResults contains struct {sliceIndex, gp_command_count, CdbExplain_StatHdr}
 * data from all QE workers
 */
static CdbPgResults
CollectQEQueryState(List *backendInfo)
{
	ListCell		*lc;
	int				index = 0;
	StringInfoData params;
	char *sql;
	CdbPgResults cdb_pgresults = {NULL, 0};

	if (list_length(backendInfo) <= 0)
		return cdb_pgresults;
	/* generate the below sql
	 * SELECT cbdb_mpp_query_state((ARRAY['(0,1789)','(1,2984)'])::gp_segment_pid[]);
	 * (0,1789) segid | pid
	 * segment will check the segid and find those pids of
	 * local segment to collect query state
	 */
	initStringInfo(&params);

	foreach(lc, backendInfo)
	{
			index++;
			gp_segment_pid *segpid= (gp_segment_pid *) lfirst(lc);
			appendStringInfo(&params, "'(%d,%d)'", segpid->segid, segpid->pid );
			if (index != list_length(backendInfo))
			{
				appendStringInfoChar(&params, ',');
			}
	}

	sql = psprintf("SELECT cbdb_mpp_query_state((ARRAY[%s])::gp_segment_pid[])", params.data);
	CdbDispatchCommand(sql, DF_NONE, &cdb_pgresults);
	elog(DEBUG1, "SQL FOR QUERY %s, result num is %d", sql, cdb_pgresults.numResults);
	pfree(params.data);
	pfree(sql);
	return cdb_pgresults;
}

/*
 * Signal the QD which running the current query to generate
 * the final explain result and send the cdb_pgresults to it.
 */
static shm_mq_msg*
GetRemoteBackendQueryStates(CdbPgResults cdb_pgresults,
										 PGPROC *proc,
										 bool verbose,
										 bool costs,
										 bool timing,
										 bool buffers,
										 bool triggers,
										 ExplainFormat format)
{
	int sig_result;
	shm_mq_handle  *mqh;
	ListCell *lc = NULL;
	List *pgCdbStatCells = get_cdbStateCells(cdb_pgresults);
	int resnum = list_length(pgCdbStatCells);

	/* fill in parameters of query state request */
	params->verbose = verbose;
	params->costs = costs;
	params->timing = timing;
	params->buffers = buffers;
	params->triggers = triggers;
	params->format = format;
	pg_write_barrier();
	create_shm_mq(MyProc, proc);
	elog(DEBUG1, "CREATE shm_mq sender %d, %d, sender %d", MyProc->pid, MyProcPid, proc->pid);

	mqh = shm_mq_attach(mq, NULL, NULL);
	sig_result = SendProcSignal(proc->pid,
								QueryStatePollReason,
								proc->backendId);
	if (sig_result == -1)
	{
		goto signal_error;
	}
	// write out how many cdb_pgresults.numResults
	if (send_msg_by_parts(mqh, sizeof(int), (void*)(&resnum)) != MSG_BY_PARTS_SUCCEEDED)
	{
		elog(WARNING, "pg_query_state: peer seems to have detached");
		goto mq_error;
	}
	/* send the cdb_pgresults to shm_mq */
	foreach(lc, pgCdbStatCells)
	{
		pgCdbStatCell *statcell = (pgCdbStatCell *)lfirst(lc);
		if (send_msg_by_parts(mqh, statcell->len, statcell->data) != MSG_BY_PARTS_SUCCEEDED)
		{
			elog(WARNING, "pg_query_state: peer seems to have detached");
			goto mq_error;
		}
	}
	shm_mq_detach(mqh);
	return receive_final_query_state();

signal_error:
	shm_mq_detach(mqh);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid send signal")));
mq_error:
	shm_mq_detach(mqh);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("error in message queue data transmitting")));
}

static shm_mq_msg*
receive_final_query_state(void)
{
	shm_mq_handle  *mqh;
	shm_mq_result mq_receive_result;
	Size len;
	shm_mq_msg *msg;
	if (!wait_for_mq_ready(mq))
			return NULL;
	mqh = shm_mq_attach(mq, NULL, NULL);
	mq_receive_result = shm_mq_receive_with_timeout(mqh,
													&len,
													(void **)&msg,
													MAX_RCV_TIMEOUT);
	if (!check_msg(mq_receive_result, msg, len, params->reqid))
		goto mq_error;
	shm_mq_detach(mqh);
	return msg;
mq_error:
	shm_mq_detach(mqh);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("error in message queue data transmitting")));
}
/* Running on QE to collect query state from slices */
Datum
cbdb_mpp_query_state(PG_FUNCTION_ARGS)
{
	ListCell *iter;
	LOCKTAG tag;
	shm_mq_result mq_receive_result;
	shm_mq_handle *mqh = NULL;

	/* get the {segid, pid} info of this query*/
	List *alive_procs = get_query_backend_info(PG_GETARG_ARRAYTYPE_P(0));
	if (alive_procs == NIL || list_length(alive_procs) <= 0)
		PG_RETURN_NULL();
	LockShmem(&tag, PG_QS_RCV_KEY);
	check_and_init_peer(&tag, NULL, 0);
	/*
	 * collect query instrument results from all active QE backend
	 */
	foreach (iter, alive_procs)
	{
		PGPROC *proc = (PGPROC *)lfirst(iter);
		int sig_result;
		query_state_info *msg;
		Size len;
		if (proc == NULL)
			continue;
		/*
		 * Wait for shm_mq detached as the mq will be reused here,
		 * we need to wait for the mqh->sender to detached first,
		 * then reset the mq, otherwiase it will panic
		 */
		if (mqh != NULL)
		{
			if (!wait_for_mq_detached(mqh))
				goto mq_error;
		}
		AttachPeer();
		create_shm_mq(proc, MyProc);
		mqh = shm_mq_attach(mq, NULL, NULL);
		/*
		 * send signal `QueryStatePollReason` to all processes
		 */
		sig_result = SendProcSignal(proc->pid,
									QueryStatePollReason,
									proc->backendId);
		if (sig_result == -1)
		{
			/* the gang of this sclie maybe closed*/
			if (errno != ESRCH)
				continue;

			elog(WARNING, "failed to send signal");
			goto signal_error;
		}
		mq_receive_result = shm_mq_receive_with_timeout(mqh,
														&len,
														(void **)&msg,
														MAX_RCV_TIMEOUT);
		if (!check_msg(mq_receive_result, (shm_mq_msg *) msg, len, params->reqid))
		{
			elog(DEBUG1, "invalid msg from %d", proc->pid);
			goto mq_error;
		}
		/*
		 * the query of this slice maybe closed or no query running on that backend
		 * such as create table as, some backends insert data to the table instead
		 * of running any plan nodes.
		 * "create table tt as select oid from pg_class;"
		 */
		if (msg->result_code == QUERY_NOT_RUNNING)
		{
			continue;
		}
		if (msg->result_code == STAT_DISABLED)
		{
			ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("query execution statistics disabled")));
			goto mq_error;
		}
		/* a little hack here, send the query_state_info as query stats to QD */
		StringInfoData buf;
		pq_beginmessage(&buf, 'Y');
		appendBinaryStringInfo(&buf, (char *)msg, len);
		pq_endmessage(&buf);
		elog(DEBUG1, "segment %d, sliceIndex %d send query state successfully %ld ", GpIdentity.segindex, msg->sliceIndex, len + sizeof(int));
	}
	UnlockShmem(&tag);
	PG_RETURN_VOID();
signal_error:
	DetachPeer();
	shm_mq_detach(mqh);
	UnlockShmem(&tag);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("invalid send signal")));
mq_error:
	shm_mq_detach(mqh);
	UnlockShmem(&tag);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("failed to receive query state")));
}

static List*
get_query_backend_info(ArrayType *array)
{
	int32 len = 0; /* the length of oid array */
	int16       typlen;
	int         nitems;
	bool        typbyval;
	char        typalign;
	Oid         element_type = ARR_ELEMTYPE(array);
	Datum      	*data;
	bool		*nulls;
	List 		*alive_procs = NIL;

	get_typlenbyvalalign(element_type,
			&typlen, &typbyval, &typalign);
	deconstruct_array(array, element_type, typlen, typbyval,
			 typalign, &data, &nulls,
			 &nitems);

	len = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	for (int i = 0; i < len; i++)
	{
		HeapTupleHeader td = DatumGetHeapTupleHeader(data[i]);
		TupleDesc   tupDesc;
		HeapTupleData tmptup;
		int32 pid;
		int32 segid;
		bool isnull;
		PGPROC     *proc ;
		//FIXME: check isnull
		tupDesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td),
				HeapTupleHeaderGetTypMod(td));
		tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
		tmptup.t_data = td;
		segid = heap_getattr(&tmptup, 1, tupDesc, &isnull);
		pid = heap_getattr(&tmptup, 2, tupDesc, &isnull);
		proc = BackendPidGetProc(pid);
		if (proc == NULL)
			continue;
		if(segid != GpIdentity.segindex)
		{
			continue;
		}
		alive_procs = lappend(alive_procs, proc);
	}
	return alive_procs;

}

bool
check_msg(shm_mq_result mq_receive_result, shm_mq_msg *msg, Size len, int reqid)
{
	if (mq_receive_result != SHM_MQ_SUCCESS)
	{
		elog(DEBUG1, "receive the msg from the shm_mq failed: %d", mq_receive_result);
		return false;
	}
	if (msg->reqid != reqid)
	{
		elog(WARNING, "check the msg reqid failed: msg reqid %d, reqid %d", msg->reqid, reqid);
		return false;
	}
	Assert(len == msg->length);
	return true;
}

void
create_shm_mq(PGPROC *sender, PGPROC *receiver)
{
	memset(mq, 0, QUEUE_SIZE);
	mq = shm_mq_create(mq, QUEUE_SIZE);
	shm_mq_set_sender(mq, sender);
	shm_mq_set_receiver(mq, receiver);
}

static bool
wait_for_mq_ready(shm_mq *mq)
{
	/* wait until caller sets this process as sender or receiver to message queue */
	instr_time start_time;
	instr_time cur_time;
	int64 delay = MAX_SND_TIMEOUT;
	INSTR_TIME_SET_CURRENT(start_time);
	for (;;)
	{
		if (shm_mq_get_receiver(mq) == MyProc)
			break;
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT, delay, PG_WAIT_IPC);
		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);

		delay = MAX_SND_TIMEOUT - (int64)INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
		{
			elog(WARNING, "pg_query_state: failed to receive request from leader");
			return false;
		}
		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}
return true;
}

bool
wait_for_mq_detached(shm_mq_handle *mqh)
{
	/* wait until caller sets this process as sender or receiver to message queue */
	instr_time start_time;
	instr_time cur_time;
	int64 delay = MAX_SND_TIMEOUT;
	INSTR_TIME_SET_CURRENT(start_time);
	for (;;)
	{
		if (shm_mq_wait_for_attach(mqh) == SHM_MQ_DETACHED)
			break;
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT, delay, PG_WAIT_IPC);
		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);
		delay = MAX_SND_TIMEOUT - (int64)INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
		{
			elog(WARNING, "wait for mq detached timeout");
			return false;
		}
		CHECK_FOR_INTERRUPTS();
	}
	return true;
}

static void
check_and_init_peer(LOCKTAG *tag, PGPROC *proc, int n_peers)
{
	Oid counterpart_user_id;
	instr_time start_time;
	instr_time cur_time;
	INSTR_TIME_SET_CURRENT(start_time);
	while (pg_atomic_read_u32(&counterpart_userid->n_peers) != 0)
	{
		pg_usleep(1000000); /* wait one second */
		CHECK_FOR_INTERRUPTS();

		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);

		if (INSTR_TIME_GET_MILLISEC(cur_time) > MAX_RCV_TIMEOUT)
		{
			elog(DEBUG1, "pg_query_state: last request was interrupted");
			/* reset the n_peers in shared memory */
			pg_atomic_write_u32(&counterpart_userid->n_peers, 0);
			break;
		}
	}
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		counterpart_user_id = GetRemoteBackendUserId(proc);
		if (counterpart_user_id == InvalidOid)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("query is busy, no response")));
		if (!(superuser() || GetUserId() == counterpart_user_id))
		{
			UnlockShmem(tag);
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("permission denied")));
		}
	}
	pg_atomic_write_u32(&counterpart_userid->n_peers, n_peers);
	params->reqid = ++reqid;
	pg_write_barrier();
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
qs_ExecutorEnd(QueryDesc *queryDesc)
{
	if (pg_qs_enable && is_querystack_empty() && filter_running_query(queryDesc))
	{
		qs_query *query = push_query(queryDesc);
		PG_TRY();
		{
			if (Gp_role == GP_ROLE_EXECUTE && enable_qs_runtime() &&
				(query->queryDesc->instrument_options | INSTRUMENT_ROWS))
			{
				StringInfo strInfo = cdbexplain_getExecStats_runtime(queryDesc);
				if (strInfo != NULL)
					set_CachedQueryStateInfo(LocallyExecutingSliceIndex(queryDesc->estate), strInfo,
											 gp_command_count, query->id);
			} else {
				qs_print_plan(query);
			}
			pop_query();
		}
		PG_CATCH();
		{
			pop_query();
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
static void
qs_print_plan(qs_query *query)
{
	MemoryContext oldcxt;
	QueryDesc *queryDesc = query->queryDesc;
	double msec;
	ErrorData *qeError = NULL;
	if (Gp_role == GP_ROLE_DISPATCH && queryDesc->totaltime && queryDesc->showstatctx && enable_qs_runtime())
	{
		if (queryDesc->estate->dispatcherState &&
			queryDesc->estate->dispatcherState->primaryResults)
		{
			EState *estate = queryDesc->estate;
			DispatchWaitMode waitMode = DISPATCH_WAIT_NONE;
			if (!estate->es_got_eos)
			{
				ExecSquelchNode(queryDesc->planstate, true);
			}

			/*
			 * Wait for completion of all QEs.  We send a "graceful" query
			 * finish, not cancel signal.  Since the query has succeeded,
			 * don't confuse QEs by sending erroneous message.
			 */
			if (estate->cancelUnfinished)
				waitMode = DISPATCH_WAIT_FINISH;

			cdbdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);
			cdbdisp_getDispatchResults(queryDesc->estate->dispatcherState, &qeError);
		}
		if (!qeError)
		{
			/*
			 * Make sure we operate in the per-query context, so any cruft will be
			 * discarded later during ExecutorEnd.
			 */
			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

			/*
			 * Make sure stats accumulation is done.  (Note: it's okay if several
			 * levels of hook all do this.)
			 */
			InstrEndLoop(queryDesc->totaltime);
			/* Log plan if duration is exceeded. */
			msec = queryDesc->totaltime->total;
			if (msec >= 0)
			{
				ExplainState *es = NewExplainState();
				es->analyze = true; 
				es->verbose = false;
				es->buffers = false;
				es->wal = false;
				es->timing = true;
				es->summary = false;
				es->format = EXPLAIN_FORMAT_JSON;
				es->settings = true;
				ExplainBeginOutput(es);
				ExplainQueryText(es, queryDesc);
				ExplainPrintPlan(es, queryDesc);
				if (es->costs)
					ExplainPrintJITSummary(es, queryDesc);
				if (es->analyze)
					ExplainPrintExecStatsEnd(es, queryDesc);
				ExplainEndOutput(es);

				/* Remove last line break */
				if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
					es->str->data[--es->str->len] = '\0';

				es->str->data[0] = '{';
				es->str->data[es->str->len - 1] = '}';

				/* save the qd query state, set the sliceId to be 0, it will be sent to gpsmon */
				set_CachedQueryStateInfo(0, es->str, gp_command_count, query->id);
			}
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

static void
clear_queryStateInfo(void)
{
	/*
	 * Don't process any signal when reseting the CachedQueryStateInfo
	 * so that will not leed to contention on this var
	 */
	HOLD_INTERRUPTS();
	if (CachedQueryStateInfo != NULL)
	{
		pfree(CachedQueryStateInfo);
		CachedQueryStateInfo = NULL;
	}
	RESUME_INTERRUPTS();
}

static void
set_CachedQueryStateInfo(int sliceIndex, StringInfo strInfo, int gp_command_count, int queryId)
{
	HOLD_INTERRUPTS();
	if (queryStateCtx == NULL)
	{
		queryStateCtx = AllocSetContextCreate(TopMemoryContext,
											  "save_query_state_cxt",
											  ALLOCSET_DEFAULT_SIZES);
	}
	if (CachedQueryStateInfo != NULL)
		clear_queryStateInfo();
	MemoryContext queryContext = MemoryContextSwitchTo(queryStateCtx);
	CachedQueryStateInfo = new_queryStateInfo(sliceIndex, strInfo,gp_command_count , queryId,  QS_RETURNED);
	MemoryContextSwitchTo(queryContext);
	RESUME_INTERRUPTS();
}
query_state_info*
new_queryStateInfo(int sliceIndex, StringInfo strInfo, int reqid, int queryId, PG_QS_RequestResult result_code)
{
	/* The strInfo->data[len] is \0, we need it to be included in the length */
	int dataLen = strInfo->len + 1;
	/*
	 * Don't process any signal when setting the CachedQueryStateInfo
	 * so that will not leed to contention on this var
	 */
	query_state_info *info = (query_state_info *)palloc0(dataLen + sizeof(query_state_info));
	info->sliceIndex = sliceIndex;
	info->gp_command_count = gp_command_count;
	info->queryId = queryId;
	info->length = strInfo->len + sizeof(query_state_info);
	info->reqid = reqid;
	info->proc = MyProc;
	info->result_code = result_code;
	memcpy(info->data, strInfo->data, dataLen);
	pfree(strInfo);
	return info;
}

static bool
filter_query_common(QueryDesc *queryDesc)
{
	if (queryDesc == NULL)
		return false;
	if (queryDesc->extended_query)
		return false;
	return (queryDesc->operation == CMD_SELECT || queryDesc->operation == CMD_DELETE ||
			queryDesc->operation == CMD_INSERT || queryDesc->operation == CMD_UPDATE);
}
bool filter_running_query(QueryDesc *queryDesc)
{
	if (!filter_query_common(queryDesc))
		return false;
	if (!queryDesc->instrument_options)
		return false;
	if (!queryDesc->instrument_options)
		return false;
	if ((queryDesc->instrument_options & INSTRUMENT_ROWS) == 0)
		return false;
	return true;
}

bool
enable_qs_runtime(void)
{
	if (!pg_qs_enable)
		return false;
	if (!pg_atomic_read_u32(pg_qs_on))
		return false;
	if (strcmp(GP_VERSION, "1.6.0") <= 0)
		return false;
	return true;
}

/* check and count the cbd_pgresults */
static List *
get_cdbStateCells(CdbPgResults cdb_pgresults)
{
	List *pgCdbStatCells = NIL;
	for (int i = 0; i < cdb_pgresults.numResults; i++)
	{
		PGresult *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			elog(ERROR, "cdbRelMaxSegSize: resultStatus not tuples_Ok: %s %s",
				 PQresStatus(PQresultStatus(pgresult)), PQresultErrorMessage(pgresult));
		}
		else
		{
			pgCdbStatCell *statcell;
			/* Find our statistics in list of response messages.  If none, skip. */
			/* FIXME: only one statecell per pgresult?*/
			for (statcell = pgresult->cdbstats; statcell; statcell = statcell->next)
			{
				if (!statcell)
				{
					/* should detach the mq as the cdb_pgresults.numResults sent to mq is not correct*/
					elog(WARNING, "invalid statecell");
					return NIL;
				}
				query_state_info *state = (query_state_info *)statcell->data;
				if (!IsA((Node *)(state->data), CdbExplain_StatHdr))
				{
					elog(WARNING, "not a statecell");
					continue;
				}
				pgCdbStatCells = lappend(pgCdbStatCells, statcell);
			}
		}
	}
	return pgCdbStatCells;
}

Datum
query_state_pause(PG_FUNCTION_ARGS)
{
	if (!IS_QUERY_DISPATCHER())
		ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_YET), errmsg("Only can be called on coordinator")));
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to pause query state")));
	}
	char *sql = psprintf("SELECT query_state_pause_command()");
	CdbDispatchCommand(sql, DF_NONE, NULL);
	pfree(sql);
	pg_atomic_write_u32(pg_qs_on, 0);
	PG_RETURN_NULL();
}

Datum
query_state_resume(PG_FUNCTION_ARGS)
{
	if (!IS_QUERY_DISPATCHER())
		ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_YET), errmsg("Only can be called on coordinator")));
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to pause query state")));
	}
	char *sql = psprintf("SELECT query_state_resume_command()");
	CdbDispatchCommand(sql, DF_NONE, NULL);
	pfree(sql);
	pg_atomic_write_u32(pg_qs_on, 1);
	PG_RETURN_NULL();
}
Datum
query_state_pause_command(PG_FUNCTION_ARGS)
{
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to pause query state")));
	}

	pg_atomic_write_u32(pg_qs_on, 0);
	PG_RETURN_NULL();
}


Datum
query_state_resume_command(PG_FUNCTION_ARGS)
{
	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to pause query state")));
	}
	pg_atomic_write_u32(pg_qs_on, 1);
	PG_RETURN_NULL();
}

static qs_query*
push_query(QueryDesc *queryDesc)
{
	qs_query *query = (qs_query *) palloc0(sizeof(qs_query));
	query->id = list_length(QueryDescStack) + 1;
	query->queryDesc = queryDesc;
	QueryDescStack = lcons(query, QueryDescStack);
	return query;
}

static void
pop_query(void)
{
	QueryDescStack = list_delete_first(QueryDescStack);
}

bool
is_querystack_empty(void)
{
	return list_length(QueryDescStack) == 0;
}

qs_query*
get_query(void)
{
	return QueryDescStack == NIL ? NULL : (qs_query *)llast(QueryDescStack);
}
