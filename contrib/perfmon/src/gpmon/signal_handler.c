/*
 * signal_handler.c
 *		Collect current query state and send it to requestor in custom signal handler
 *
 * Copyright (c) 2016-2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_query_state/signal_handler.c
 */

#include "pg_query_state.h"
#include "libpq-fe.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdispatchresult.h"
#include "commands/explain.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 100000
#include "pgstat.h"
#endif
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "libpq-int.h"
#include "libpq/pqmq.h"
#include "storage/lock.h"
/*
 * Structure of stack frame of fucntion call which resulted from analyze of query state
 */
typedef struct
{
	const char	*query;
	char		*plan;
} stack_frame;


msg_by_parts_result send_msg_by_parts(shm_mq_handle *mqh, Size nbytes, const void *data);

static bool QE_SendQueryState(shm_mq_handle  *mqh, PGPROC *proc);
static bool QD_SendQueryState(shm_mq_handle  *mqh, PGPROC *proc);
static CdbDispatchResults* makeDispatchResults(SliceTable *table);
static bool 
query_state_pre_check(shm_mq_handle *mqh, int reqid, shm_mq_msg *msg);
static bool
send_cdbComponents_pre_check(shm_mq_handle *mqh, int reqid, shm_mq_msg *msg);
static bool 
receive_QE_query_state(shm_mq_handle *mqh, List **query_state_info_list);
static CdbDispatchResults*
process_qe_query_state(QueryDesc *queryDesc, List *query_state_info_list);
static void 
fill_segpid(CdbComponentDatabaseInfo *segInfo ,backend_info *msg, int *index);
/*
 * Compute length of serialized stack frame
 */
static int
serialized_stack_frame_length(stack_frame *qs_frame)
{
	return 	INTALIGN(strlen(qs_frame->query) + VARHDRSZ)
		+ 	INTALIGN(strlen(qs_frame->plan) + VARHDRSZ);
}

/*
 * Compute overall length of serialized stack of function calls
 */
static int
serialized_stack_length(List *qs_stack)
{
	ListCell 	*i;
	int			result = 0;

	foreach(i, qs_stack)
	{
		stack_frame *qs_frame = (stack_frame *) lfirst(i);

		result += serialized_stack_frame_length(qs_frame);
	}

	return result;
}

/*
 * Convert stack_frame record into serialized text format version
 * 		Increment '*dest' pointer to the next serialized stack frame
 */
static void
serialize_stack_frame(char **dest, stack_frame *qs_frame)
{
	SET_VARSIZE(*dest, strlen(qs_frame->query) + VARHDRSZ);
	memcpy(VARDATA(*dest), qs_frame->query, strlen(qs_frame->query));
	*dest += INTALIGN(VARSIZE(*dest));

	SET_VARSIZE(*dest, strlen(qs_frame->plan) + VARHDRSZ);
	memcpy(VARDATA(*dest), qs_frame->plan, strlen(qs_frame->plan));
	*dest += INTALIGN(VARSIZE(*dest));
}

/*
 * Convert List of stack_frame records into serialized structures laid out sequentially
 */
static void
serialize_stack(char *dest, List *qs_stack)
{
	ListCell		*i;

	foreach(i, qs_stack)
	{
		stack_frame *qs_frame = (stack_frame *) lfirst(i);

		serialize_stack_frame(&dest, qs_frame);
	}
}

static msg_by_parts_result
shm_mq_send_nonblocking(shm_mq_handle *mqh, Size nbytes, const void *data, Size attempts)
{
	int				i;
	shm_mq_result	res;

	for(i = 0; i < attempts; i++)
	{
#if PG_VERSION_NUM < 150000
		res = shm_mq_send(mqh, nbytes, data, true);
#else
		res = shm_mq_send(mqh, nbytes, data, true, true);
#endif

		if(res == SHM_MQ_SUCCESS)
			break;
		else if (res == SHM_MQ_DETACHED)
			return MSG_BY_PARTS_FAILED;

		/* SHM_MQ_WOULD_BLOCK - sleeping for some delay */
		pg_usleep(WRITING_DELAY);
	}

	if(i == attempts)
		return MSG_BY_PARTS_FAILED;

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * send_msg_by_parts sends data through the queue as a bunch of messages
 * of smaller size
 */
msg_by_parts_result
send_msg_by_parts(shm_mq_handle *mqh, Size nbytes, const void *data)
{
	int bytes_left;
	int bytes_send;
	int offset;

	/* Send the expected message length */
	if(shm_mq_send_nonblocking(mqh, sizeof(Size), &nbytes, NUM_OF_ATTEMPTS) == MSG_BY_PARTS_FAILED)
		return MSG_BY_PARTS_FAILED;

	/* Send the message itself */
	for (offset = 0; offset < nbytes; offset += bytes_send)
	{
		bytes_left = nbytes - offset;
		bytes_send = (bytes_left < MSG_MAX_SIZE) ? bytes_left : MSG_MAX_SIZE;
		if(shm_mq_send_nonblocking(mqh, bytes_send, &(((unsigned char*)data)[offset]), NUM_OF_ATTEMPTS)
			== MSG_BY_PARTS_FAILED)
			return MSG_BY_PARTS_FAILED;
	}

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * Send state of current query to shared queue.
 * This function is called when fire custom signal QueryStatePollReason
 */
void
SendQueryState(void)
{
	shm_mq_handle  *mqh;
	int         	reqid = params->reqid;
	MemoryContext	oldctx;
	bool 			success = true;
	volatile int32	savedInterruptHoldoffCount;

	MemoryContext query_state_ctx = AllocSetContextCreate(TopMemoryContext,
														  "pg_query_state",
														  ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(query_state_ctx);
	/* in elog(ERROR), InterruptHoldoffCount will be set to 0 */
	savedInterruptHoldoffCount = InterruptHoldoffCount;
	elog(DEBUG1, "Worker %d receives pg_query_state request from %d", shm_mq_get_sender(mq)->pid, shm_mq_get_receiver(mq)->pid);

	PG_TRY();
	{
		mqh = shm_mq_attach(mq, NULL, NULL);

		/* happy path */
		elog(DEBUG1, "happy path");
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			if (reqid != params->reqid || shm_mq_get_receiver(mq) != MyProc)
			{
				success = false;
			}
			else if (!QD_SendQueryState(mqh,  MyProc))
				 success = false;
		}
		else
		{
			if (reqid != params->reqid || shm_mq_get_sender(mq) != MyProc)
			{
				success = false;
			}
			else if (!QE_SendQueryState(mqh, MyProc))
				success = false;
		}
	}
	PG_CATCH();
	{
		elog(WARNING, "Failed to send query state");
		elog_dismiss(WARNING);
		success = false;
		InterruptHoldoffCount = savedInterruptHoldoffCount;
	}
	PG_END_TRY();
	shm_mq_detach(mqh);
	DetachPeer();
	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(query_state_ctx);
	return;
}

/* Added by cbdb
*  SendCdbComponents sends the array of gp_segment_pid info
*  of current session to shm_mq
*  Only called on QD
*/
void
SendCdbComponents(void)
{
	shm_mq_handle *mqh;
	int reqid = params->reqid;
	shm_mq_result result;
	CdbComponentDatabases *cdbs;
	shm_mq_msg *pre_check_msg;
	MemoryContext oldctx;
	bool success = true;
	int index = 0;
	volatile int32	savedInterruptHoldoffCount;
	MemoryContext query_state_ctx = AllocSetContextCreate(TopMemoryContext,
														  "pg_query_state",
														  ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(query_state_ctx);
	/* in elog(ERROR), InterruptHoldoffCount will be set to 0 */
	savedInterruptHoldoffCount = InterruptHoldoffCount;
	pre_check_msg = (shm_mq_msg *)palloc0(sizeof(shm_mq_msg));
	PG_TRY();
	{
		mqh = shm_mq_attach(mq, NULL, NULL);
		if (!send_cdbComponents_pre_check(mqh, params->reqid, pre_check_msg))
		{
			success = false;
			shm_mq_send(mqh, pre_check_msg->length, pre_check_msg, false);
		}
		else
		{
			cdbs = cdbcomponent_getCdbComponents();
			/* compute the size of the msg
			 * as the struct gp_segment_pid only contains two int fields,
			 * so not calling INTALIGN here.
			 */
			int msglen = BASE_SIZEOF_GP_BACKEND_INFO + sizeof(gp_segment_pid) * cdbs->numActiveQEs;
			backend_info *msg = (backend_info *)palloc0(msglen);
			/* index for backend_info.pids array */
			msg->reqid = reqid;
			/* FIXME: add another code for it  */
			msg->result_code = QS_RETURNED;
			/* Fill the QE pid */
			for (int i = 0; i < cdbs->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *segInfo = &cdbs->segment_db_info[i];
				fill_segpid(segInfo, msg, &index);
			}
			/* Fill the entryDB pid */
			for (int i = 0; i < cdbs->total_entry_dbs; i++)
			{
				CdbComponentDatabaseInfo *segInfo = &cdbs->entry_db_info[i];
				fill_segpid(segInfo, msg, &index);
			}
			Assert(index == cdbs->numActiveQEs);
			msg->number = index;
#if PG_VERSION_NUM < 150000
			result = shm_mq_send(mqh, msglen, msg, false);
#else
			result = shm_mq_send(mqh, msglen, msg, false, true);
#endif
			/* Check for failure. */
			if (result != SHM_MQ_SUCCESS){
				shm_mq_detach(mqh);
				success = false;
			}
		}
	}
	PG_CATCH();
	{
		elog(WARNING, " SendCdbComponents failed");
		elog_dismiss(WARNING);
		success = false;
		shm_mq_detach(mqh);
		InterruptHoldoffCount = savedInterruptHoldoffCount;
	}
	PG_END_TRY();
	DetachPeer();
	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(query_state_ctx);
	if (success)
		elog(DEBUG1, "Worker %d sends response for SendCdbComponents to %d", shm_mq_get_sender(mq)->pid, shm_mq_get_receiver(mq)->pid);
	return;
}

static bool 
QD_SendQueryState(shm_mq_handle  *mqh, PGPROC *proc)
{
	QueryDesc *queryDesc;
	ExplainState *es;
	List *result = NIL;
	shm_mq_msg *msg;
	CdbDispatcherState *disp_state = NULL;
	instr_time starttime;
	List *qs_stack = NIL;
	LOCKTAG			 tag;
	volatile int32 savedInterruptHoldoffCount;
	bool success = true;
	PGPROC *sender;
	List *query_state_info_list = NIL;
	disp_state = palloc0(sizeof(CdbDispatcherState));
	shm_mq_msg *pre_check_msg = (shm_mq_msg *)palloc0(sizeof(shm_mq_msg));
	queryDesc = get_query();
	/* first receive the results, it may be empty, such as functions only run on master */
	if (!receive_QE_query_state(mqh, &query_state_info_list))
		return false;
	disp_state->primaryResults = process_qe_query_state(queryDesc, query_state_info_list);

	sender = shm_mq_get_sender(mq);
	if (!wait_for_mq_detached(mqh))
		return false;
	/* recreate shm_mq, switch the sender and receiver*/
	LockShmem(&tag, PG_QS_SND_KEY);
	create_shm_mq(MyProc, sender);
	elog(DEBUG1, "switch sender and receiver  receiver %d, sender  %d",sender->pid, MyProc->pid);
	mqh = shm_mq_attach(mq, NULL, NULL);
	if (!query_state_pre_check(mqh, params->reqid, pre_check_msg))
	{
		int sendRes = send_msg_by_parts(mqh, pre_check_msg->length, pre_check_msg);
		UnlockShmem(&tag);
		pfree(pre_check_msg);
		if (sendRes != MSG_BY_PARTS_SUCCEEDED)
		{
			elog(DEBUG1, "send cannot send query state proc %d failed", proc->pid);
			return false;
		}
		return true;
	}
	Assert(queryDesc);

	/*
	 * Save the old dispatcher state of estate.
	 * If the analyze of the query is true, the old_disp_state is not null,
	 * we need to restore it.
	 */
	CdbDispatcherState *old_disp_state = queryDesc->estate->dispatcherState;
	struct CdbExplain_ShowStatCtx *oldShowstatctx = queryDesc->showstatctx;
	savedInterruptHoldoffCount = InterruptHoldoffCount;
	PG_TRY();
	{
		/* initialize explain state with all config parameters */
		es = NewExplainState();
		es->analyze = true;
		es->verbose = params->verbose;
		es->costs = params->costs;
		es->buffers = params->buffers && pg_qs_buffers;
		es->timing = params->timing && pg_qs_timing;
		es->summary = false;
		es->format = params->format;
		es->runtime = true;
		INSTR_TIME_SET_CURRENT(starttime);
		es->showstatctx = cdbexplain_showExecStatsBegin(queryDesc,
														starttime);
		/* push the DispatchState into queryDesc->estate */
		queryDesc->estate->dispatcherState = disp_state;
		queryDesc->showstatctx = es->showstatctx;

		initStringInfo(es->str);
		ExplainBeginOutput(es);
		ExplainQueryText(es, queryDesc);
		ExplainPrintPlan(es, queryDesc);
		if (es->costs)
			ExplainPrintJITSummary(es, queryDesc);
		if (es->analyze)
			ExplainPrintExecStatsEnd(es, queryDesc);
		ExplainEndOutput(es);

		/* reset the dispatcherState in estate*/
		queryDesc->estate->dispatcherState = old_disp_state;
		queryDesc->showstatctx = oldShowstatctx;

		/* Remove last line break */
		if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
			es->str->data[--es->str->len] = '\0';

		/* Fix JSON to output an object */
		if (params->format == EXPLAIN_FORMAT_JSON)
		{
			es->str->data[0] = '{';
			es->str->data[es->str->len - 1] = '}';
		}
		stack_frame *qs_frame = palloc0(sizeof(stack_frame));

		qs_frame->plan = es->str->data;
		qs_frame->query = queryDesc->sourceText;

		qs_stack = lcons(qs_frame, result);
		success= true;
	}
	PG_CATCH();
	{
		UnlockShmem(&tag);
		elog(WARNING, " SendQueryState failed");
		/* reset the queryDesc->estate */
		queryDesc->estate->dispatcherState = old_disp_state;
		queryDesc->showstatctx = oldShowstatctx;
		elog_dismiss(WARNING);
		success = false;
		InterruptHoldoffCount = savedInterruptHoldoffCount;
	}
	PG_END_TRY();
	if (!success)
		return success;

	/* send result to pg_query_state process */
	int msglen = sizeof(shm_mq_msg) + serialized_stack_length(qs_stack);
	msg = palloc(msglen);

	msg->reqid = params->reqid;
	msg->length = msglen;
	msg->proc = MyProc;
	msg->result_code = QS_RETURNED;

	msg->warnings = 0;
	if (params->timing && !pg_qs_timing)
		msg->warnings |= TIMINIG_OFF_WARNING;
	if (params->buffers && !pg_qs_buffers)
		msg->warnings |= BUFFERS_OFF_WARNING;

	msg->stack_depth = list_length(qs_stack);
	serialize_stack(msg->stack, qs_stack);

	if (send_msg_by_parts(mqh, msglen, msg) != MSG_BY_PARTS_SUCCEEDED)
	{
		elog(WARNING, "pg_query_state: peer seems to have detached");
			UnlockShmem(&tag);
		return false;
	}
	elog(DEBUG1, "Worker %d sends response for pg_query_state to %d", shm_mq_get_sender(mq)->pid, shm_mq_get_receiver(mq)->pid);
		UnlockShmem(&tag);
	return true;
}

/*
 * Send plan with instrument to the shm_mq
 *
 * The data is format as below
 * First message:
 * Msg length | reqid | proc | result_code | warnings | stack_depth | sliceIndex
 * Second message:
 * pq_msgtype('Y') | CdbExplain_StatHdr
 */
static bool 
QE_SendQueryState(shm_mq_handle  *mqh, PGPROC *proc)
{
	QueryDesc *queryDesc;
	int sliceIndex;
	query_state_info *info = NULL;
	shm_mq_msg *pre_check_msg = (shm_mq_msg *)palloc0(sizeof(shm_mq_msg));
	volatile int32 savedInterruptHoldoffCount;
	bool success = true;
	/* cannot use the send_msg_by_parts here */
	if (!query_state_pre_check(mqh, params->reqid, pre_check_msg))
	{
		int sendRes = send_msg_by_parts(mqh, pre_check_msg->length, pre_check_msg);
		pfree(pre_check_msg);
		if (sendRes != MSG_BY_PARTS_SUCCEEDED)
		{
			elog(WARNING, "send cannot send query state proc %d failed", proc->pid);
			return false;
		}
		return true;
	}
	savedInterruptHoldoffCount = InterruptHoldoffCount;
	PG_TRY();
	{
	
		if (is_querystack_empty())
		{
			if (CachedQueryStateInfo == NULL)
				success = false;
			else
			{
				int dataLen = 0;
				info = (query_state_info *)palloc0(CachedQueryStateInfo->length);
				info->length = CachedQueryStateInfo->length;
				dataLen = CachedQueryStateInfo->length - sizeof(query_state_info);
				info->sliceIndex = CachedQueryStateInfo->sliceIndex;
				memcpy(info->data, CachedQueryStateInfo->data, dataLen);
				info->reqid = params->reqid;
				info->proc = MyProc;
				info->result_code = QS_RETURNED;
				info->queryId = CachedQueryStateInfo->queryId;
			}
		}
		else
		{
			queryDesc = get_query();
			Assert(queryDesc);
			StringInfo strInfo = cdbexplain_getExecStats_runtime(queryDesc);
			if (strInfo == NULL)
				ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cannot get runtime stats")));
			sliceIndex = LocallyExecutingSliceIndex(queryDesc->estate);
			info = new_queryStateInfo(sliceIndex, strInfo, params->reqid, queryDesc->plannedstmt->queryId, QS_RETURNED);
		}
		if (info != NULL)
		{
			msg_by_parts_result sendResult = send_msg_by_parts(mqh, info->length, info);
			pfree(info);
			if (sendResult != MSG_BY_PARTS_SUCCEEDED)
			{
				elog(DEBUG1, "pg_query_state: peer seems to have detached");
				success = false;
			}
		}
	}
	PG_CATCH();
	{
		elog_dismiss(WARNING);
		elog(WARNING, "failed to get QE query state");
		success = false;
		InterruptHoldoffCount = savedInterruptHoldoffCount;
	}
	PG_END_TRY();
	if (success)
		elog(DEBUG1, "Segment: %u slice: %d send query state successfully", GpIdentity.segindex, sliceIndex);
	return success;
}

/* copied from cbd_makeDispatchresults */
static CdbDispatchResults*
makeDispatchResults(SliceTable *table)
{
	CdbDispatchResults *results;
	int resultCapacity = 0;
	int nbytes;

	for(int i = 0; i < table->numSlices; i++)
	{
		resultCapacity += table->slices[i].planNumSegments;
	}

	nbytes = resultCapacity * sizeof(results->resultArray[0]);

	results = palloc0(sizeof(*results));
	results->resultArray = palloc0(nbytes);
	results->resultCapacity = resultCapacity;
	results->resultCount = 0;
	results->iFirstError = -1;
	results->errcode = 0;
	results->cancelOnError = true;

	results->sliceMap = NULL;
	results->sliceCapacity = table->numSlices;
	if (resultCapacity > 0)
	{
		nbytes = resultCapacity * sizeof(results->sliceMap[0]);
		results->sliceMap = palloc0(nbytes);
	}
	return results;
}

static bool
send_cdbComponents_pre_check(shm_mq_handle *mqh, int reqid, shm_mq_msg *msg)
{
	bool res = query_state_pre_check(mqh, reqid, msg);
	if (!res)
		return res;
	/* This function can only be called on QD */
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		res = false;
		if (msg != NULL)
			*msg = (shm_mq_msg){reqid, BASE_SIZEOF_SHM_MQ_MSG, MyProc, QUERY_NOT_RUNNING};
	}
	return res;

}

static  void
set_msg(shm_mq_msg *msg, int reqid, PG_QS_RequestResult res)
{
	if (msg != NULL)
		*msg = (shm_mq_msg){reqid, BASE_SIZEOF_SHM_MQ_MSG, MyProc, res};
}
static bool
query_state_pre_check(shm_mq_handle *mqh, int reqid, shm_mq_msg *msg)
{
	QueryDesc *queryDesc;
	/* check if module is enabled */
	if (!enable_qs_runtime())
	{
		set_msg(msg, reqid, STAT_DISABLED);
		return false;
	}
	/* On QE, check if there is a cached querystate info */
	if (Gp_role == GP_ROLE_EXECUTE && CachedQueryStateInfo != NULL )
	{
		return true;
	}

	/* no query running on QD/QE */
	if (list_length(QueryDescStack) <= 0)
	{
		set_msg(msg, reqid, QUERY_NOT_RUNNING);
		return false;
	}
	queryDesc = get_query();
	Assert(queryDesc);

	if (!filter_running_query(queryDesc))
	{
		set_msg(msg, reqid, QUERY_NOT_RUNNING);
		return false;
	}
	return true;
}

/* Receive and process query stats from QE
 *
 * Firstly get the num of results as numresults
 * Then transfrom the num of CdbExplain_StatHdr into pgresult
 * and construct the CdbDispatcherState which is need by
 * "ExplainPrintPlan"
 *
 * CdbExplain_StatHdr is saved in query_state_info.data
 */
static bool 
receive_QE_query_state(shm_mq_handle *mqh, List **query_state_info_list)
{
	shm_mq_result mq_receive_result;
	Size len;
	query_state_info *seg_query_state_info;
	int *numresults;
	mq_receive_result = shm_mq_receive_with_timeout(mqh,
													&len,
													(void **)&numresults,
													MAX_RCV_TIMEOUT);
	if (mq_receive_result != SHM_MQ_SUCCESS)
	{
		/* counterpart is dead, not considering it */
		elog(WARNING, "receive QE query state results failed through shm_mq");
		return false;
	}
	if (*numresults <= 0)
	{
		return true;
	}
	for (int i = 0; i < *numresults; i++)
	{
		mq_receive_result = shm_mq_receive_with_timeout(mqh,
														&len,
														(void **)&seg_query_state_info,
														MAX_RCV_TIMEOUT);
		if (mq_receive_result != SHM_MQ_SUCCESS)
		{
			elog(WARNING, "receive QE query state results failed through shm_mq");
			/* counterpart is dead, not considering it */
			return false;
		}
		*query_state_info_list = lappend(*query_state_info_list, seg_query_state_info);
		elog(DEBUG1, "receive QE query state slice %d, proc %d successfully", seg_query_state_info->sliceIndex, seg_query_state_info->proc->backendId);
	}
	return true;
}

static CdbDispatchResults*
process_qe_query_state(QueryDesc *queryDesc, List *query_state_info_list)
{
	EState *estate;
	CdbDispatchResults *results = NULL;
	uint64 queryId;
	/* The query maybe has been finished */
	if (queryDesc == NULL || queryDesc->estate == NULL)
	{
		return results;
	}
	estate = queryDesc->estate;
	queryId = queryDesc->plannedstmt->queryId;
	/* first constuct a CdbDispatchResults */
	results = makeDispatchResults(estate->es_sliceTable);
	if (results->resultCapacity < list_length(query_state_info_list))
	{
		/*
		explain analyze select test_auto_stats_in_function('delete from t_test_auto_stats_in_function',
                                   true, 't_test_auto_stats_in_function')*/
		return results;
	}
	/* the pgresult of the same slice should be put in continous memory */
	for(int i = 0 ; i < estate->es_sliceTable->numSlices; i++)
	{
		 ListCell   *c;
		 foreach(c, query_state_info_list)
		 {
			 query_state_info *info = (query_state_info *)lfirst(c);
			 /* if the query state's queryId not equal to current queryId, skip it */
			 if (info->queryId != queryId)
			 {
				continue;
			 }
			 pgCdbStatCell *statcell = (pgCdbStatCell *)palloc(sizeof(pgCdbStatCell));
			 PGresult *pgresult = palloc(sizeof(PGresult));
			 statcell->data = info->data;
			 statcell->len = info->length - sizeof(query_state_info);
			 statcell->next = NULL;
			 pgresult->cdbstats = statcell;
			 if (info->sliceIndex == i)
			 {
				 CdbDispatchResult *dispatchResult = cdbdisp_makeResult(results, NULL, info->sliceIndex);
				 cdbdisp_appendResult(dispatchResult, pgresult);
			}
		 }
	}
	return results;
}

static void
fill_segpid(CdbComponentDatabaseInfo *segInfo , backend_info *msg, int* index)
{

	ListCell *lc;
	foreach (lc, segInfo->activelist)
	{
		gp_segment_pid *segpid = &msg->pids[(*index)++];
		SegmentDatabaseDescriptor *dbdesc = (SegmentDatabaseDescriptor *)lfirst(lc);
		segpid->pid = dbdesc->backendPid;
		segpid->segid = dbdesc->segindex;
	}
}
