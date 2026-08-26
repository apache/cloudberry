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
 * signal_handler.c
 *		Custom signal handlers and plan-tree walker for pg_query_state.
 *
 * This module implements the two custom ProcSignal handlers registered by
 * pg_qs_init():
 *
 *   SendQueryState()    -- fired when QueryStatePollReason is received.
 *                          Walks the active plan tree, collects per-node stats,
 *                          logs them, then pushes the whole snapshot to the
 *                          yagpcc UDS sink (and, on the coordinator, the
 *                          deparsed plan document).
 *   SendCdbComponents() -- fired when BackendInfoPollReason is received (QD only).
 *                          Sends the list of active QE (segid, pid) pairs.
 *
 * Also contains:
 *   qs_planstate_walker()   -- recursive plan-tree traversal helper.
 *   qs_get_node_stats()     -- per-node stat collection callback.
 *   qs_debug_node_stats()   -- LOG-level dump of a collected stat list.
 *   qs_debug_node_sample()  -- LOG-level dump of a single GpscNodeSample.
 *   send_msg_by_parts()     -- chunked shm_mq send helper.
 *
 * Portions derived from pg_query_state
 * (https://github.com/postgrespro/pg_query_state), under the PostgreSQL
 * License:
 *   Portions Copyright (c) 2016-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/signal_handler.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "pg_query_state.h"
#include "PlanNodeEmitter.h"

#include "access/xact.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "cdb/cdbconn.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "pgstat.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "libpq/pqmq.h"

/*
 * Identity of the most recent coordinator plan-doc push, used to rate-limit
 * SetQueryPlanReq: SendQueryState() re-sends the deparsed plan only when the
 * query key changes or PLAN_DOC_RESEND_INTERVAL_MS has elapsed.
 */
static struct
{
	int32_t		tmid;
	int32_t		ssid;
	int32_t		ccnt;
	TimestampTz at;
} last_sent_query_key;

typedef struct NodeRollState
{
	int32_t plan_node_id;
	double prev_ntuples_sum;
	TimestampTz prev_executed_at;
	TimestampTz first_executed_at;
	int32_t relation_oid;
	char relation_name[MAX_RELNAME_LEN];
} NodeRollState;

static HTAB *node_roll_htab = NULL;

static void ensure_node_roll_htab(void)
{
	HASHCTL ctl;

	if (node_roll_htab)
	{
		return;
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(NodeRollState);
	ctl.hcxt = TopMemoryContext;
	node_roll_htab = hash_create("gpsc_per_node_roll_state",
								 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void gpsc_reset_node_roll_state(void)
{
	if (node_roll_htab)
	{
		hash_destroy(node_roll_htab);
		node_roll_htab = NULL;
	}
}

/*
 * qs_reporting_segid -- segid this backend stamps on the samples it emits.
 *
 * GpIdentity.segindex is -1 both here and on the QD; an executor role on the
 * coordinator host means the entry-db QE, which needs a segid of its own.
 */
static int32
qs_reporting_segid(void)
{
	if (Gp_role == GP_ROLE_EXECUTE && GpIdentity.segindex < 0)
		return GPSC_SEGID_ENTRY_DB;

	return GpIdentity.segindex;
}

/*
 * shm_mq_send_nonblocking -- attempt to send nbytes through mqh up to
 * `attempts` times, sleeping WRITING_DELAY µs between retries.
 *
 * Returns MSG_BY_PARTS_FAILED immediately on SHM_MQ_DETACHED; retries on
 * SHM_MQ_WOULD_BLOCK.
 */
static msg_by_parts_result
shm_mq_send_nonblocking(shm_mq_handle *mqh, Size nbytes,
						const void *data, Size attempts)
{
	int i;
	shm_mq_result res;

	for (i = 0; i < (int) attempts; i++)
	{
#if PG_VERSION_NUM < 150000
		res = shm_mq_send(mqh, nbytes, data, true);
#else
		res = shm_mq_send(mqh, nbytes, data, true, true);
#endif

		if (res == SHM_MQ_SUCCESS)
			break;
		else if (res == SHM_MQ_DETACHED)
			return MSG_BY_PARTS_FAILED;

		/* SHM_MQ_WOULD_BLOCK -- back off briefly and retry. */
		pg_usleep(WRITING_DELAY);
	}

	if (i == (int) attempts)
		return MSG_BY_PARTS_FAILED;

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * send_msg_by_parts -- transmit an arbitrarily large buffer through mqh.
 *
 * The wire protocol is: first send a Size value announcing the total payload
 * length, then send the payload itself in chunks of at most MSG_MAX_SIZE
 * bytes.  The receiver must use receive_msg_by_parts() (in pg_query_state.c)
 * to reassemble the chunks.
 *
 * Parameters:
 *   mqh    -- attached shm_mq handle (sender side)
 *   nbytes -- total payload size
 *   data   -- pointer to the payload
 *
 * Returns MSG_BY_PARTS_SUCCEEDED on success, MSG_BY_PARTS_FAILED otherwise.
 */
static msg_by_parts_result
send_msg_by_parts(shm_mq_handle *mqh, Size nbytes, const void *data)
{
	int offset;
	int bytes_left;
	int bytes_send;

	/* Announce total length. */
	if (shm_mq_send_nonblocking(mqh, sizeof(Size), &nbytes,
								NUM_OF_ATTEMPTS) == MSG_BY_PARTS_FAILED)
		return MSG_BY_PARTS_FAILED;

	/* Send payload in chunks. */
	for (offset = 0; offset < (int) nbytes; offset += bytes_send)
	{
		bytes_left = nbytes - offset;
		bytes_send = (bytes_left < MSG_MAX_SIZE) ? bytes_left : MSG_MAX_SIZE;
		if (shm_mq_send_nonblocking(mqh, bytes_send,
									&(((unsigned char *) data)[offset]),
									NUM_OF_ATTEMPTS) == MSG_BY_PARTS_FAILED)
			return MSG_BY_PARTS_FAILED;
	}

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * qs_planstate_walker -- depth-first traversal of a PlanState tree.
 *
 * Visits every node in the tree rooted at `planstate`, calling `executor`
 * on each node before recursing.  Handles all node types that have child
 * plan states (Append, MergeAppend, BitmapAnd/Or, SubqueryScan, CustomScan,
 * init-plans, and sub-plans).
 *
 * Parameters:
 *   planstate     -- root of the subtree to walk (NULL is a no-op)
 *   executor      -- callback invoked for each node
 *   qs_walker_ctx -- context threaded through all callbacks
 *   depth         -- current recursion depth (for stack-depth checks)
 */
static void
qs_planstate_walker(PlanState *planstate,
					qs_planstate_walker_callback executor,
					QsWalkerContext *qs_walker_ctx,
					int depth)
{
	int32     saved_parent_plan_node_id;
	int32     saved_slice_id;
	Plan     *plan;
	ListCell *lc;

	if (planstate == NULL)
		return;

	check_stack_depth();

	plan = planstate->plan;

	/*
	 * A Motion opens a new slice, and the node itself belongs to the sending
	 * side -- the same attribution ExplainNode() uses.  Switch before sampling
	 * so the Motion is reported under its own slice, not its parent's.
	 */
	saved_slice_id = qs_walker_ctx->slice_id;
	if (IsA(plan, Motion))
	{
		Motion     *motion = (Motion *) plan;
		SliceTable *sliceTable = planstate->state->es_sliceTable;

		if (sliceTable && motion->motionID >= 0 &&
			motion->motionID < sliceTable->numSlices)
			qs_walker_ctx->slice_id = sliceTable->slices[motion->motionID].sliceIndex;
	}

	executor(planstate, qs_walker_ctx);
	saved_parent_plan_node_id = qs_walker_ctx->parent_plan_node_id;
	qs_walker_ctx->parent_plan_node_id = plan->plan_node_id;

	/* initPlans */
	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		qs_planstate_walker(sps->planstate, executor, qs_walker_ctx, depth + 1);
	}

	/* Left and right children. */
	qs_planstate_walker(outerPlanState(planstate), executor, qs_walker_ctx,
						depth + 1);
	qs_planstate_walker(innerPlanState(planstate), executor, qs_walker_ctx,
						depth + 1);

	/* Type-specific child plans. */
	switch (nodeTag(plan))
	{
		case T_Append:
		{
			AppendState *as = (AppendState *) planstate;
			for (int i = 0; i < as->as_nplans; i++)
				qs_planstate_walker(as->appendplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_MergeAppend:
		{
			MergeAppendState *ms = (MergeAppendState *) planstate;
			for (int i = 0; i < ms->ms_nplans; i++)
				qs_planstate_walker(ms->mergeplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_BitmapAnd:
		{
			BitmapAndState *bas = (BitmapAndState *) planstate;
			for (int i = 0; i < bas->nplans; i++)
				qs_planstate_walker(bas->bitmapplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_BitmapOr:
		{
			BitmapOrState *bos = (BitmapOrState *) planstate;
			for (int i = 0; i < bos->nplans; i++)
				qs_planstate_walker(bos->bitmapplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_SubqueryScan:
			qs_planstate_walker(((SubqueryScanState *) planstate)->subplan,
								executor, qs_walker_ctx, depth + 1);
			break;
		case T_CustomScan:
			foreach(lc, ((CustomScanState *) planstate)->custom_ps)
				qs_planstate_walker((PlanState *) lfirst(lc), executor,
									qs_walker_ctx, depth + 1);
			break;
		default:
			break;
	}

	/* subPlans */
	foreach(lc, planstate->subPlan)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		qs_planstate_walker(sps->planstate, executor, qs_walker_ctx, depth + 1);
	}

	qs_walker_ctx->parent_plan_node_id = saved_parent_plan_node_id;
	qs_walker_ctx->slice_id = saved_slice_id;
}

/*
 * qs_get_node_stats -- walker callback that snapshots one plan node.
 *
 * Allocates a GpscNodeSample in the current memory context, fills it from
 * planstate->instrument (if available), and appends it to
 * qs_walker_ctx->per_node_stats.
 *
 * Parameters:
 *   planstate      -- the plan node being sampled
 *   qs_walker_ctx  -- walker context; per_node_stats is extended in-place
 */
static void
qs_get_node_stats(PlanState *planstate, QsWalkerContext *qs_walker_ctx)
{
	GpscNodeSample *nodestat =
		(GpscNodeSample *) palloc0(sizeof(GpscNodeSample));

	/* Identity fields. */
	nodestat->ssid = gp_session_id;
	nodestat->tmid = qs_walker_ctx->tmid;
	nodestat->ccnt = gp_command_count;

	/* Plan-tree position. */
	nodestat->plan_node_id        = planstate->plan->plan_node_id;
	nodestat->parent_plan_node_id = qs_walker_ctx->parent_plan_node_id;
	nodestat->node_tag            = nodeTag(planstate->plan);
	nodestat->slice_id            = qs_walker_ctx->slice_id;
	nodestat->segindex            = qs_reporting_segid();
	nodestat->dbid                = GpIdentity.dbid;
	nodestat->pid                 = MyProcPid;

	/* Planner estimate. */
	nodestat->plan_rows = planstate->plan->plan_rows;

	/* Runtime instrumentation (may be NULL for non-instrumented nodes). */
	if (planstate->instrument)
	{
		Instrumentation *instr = planstate->instrument;
		double			 eff_nloops;

		if (qs_walker_ctx->finalize)
		{
			InstrEndLoop(instr);
		}

		eff_nloops = instr->nloops;
		if (!qs_walker_ctx->finalize && instr->eof)
			eff_nloops += 1;

		nodestat->ntuples    = instr->ntuples + instr->tuplecount; /* include in-progress loop */
		nodestat->tuplecount = instr->tuplecount;
		nodestat->nloops     = eff_nloops;
		nodestat->startup    = instr->startup;
		nodestat->total      = instr->total;
		nodestat->firsttuple = instr->firsttuple;

		nodestat->shared_blks_hit  = instr->bufusage.shared_blks_hit;
		nodestat->shared_blks_read = instr->bufusage.shared_blks_read;

		/*
		 * eof lets a consumer tell a node that has finished producing (running
		 * but exhausted for this cycle) from one still actively pulling.
		 */
		nodestat->eof = instr->eof;

		if (instr->running && !instr->eof)
			nodestat->node_status = QS_NODE_STATUS_EXECUTING;
		else if (eff_nloops > 0)
			nodestat->node_status = QS_NODE_STATUS_FINISHED;
		else
			nodestat->node_status = QS_NODE_STATUS_INITIALIZED;

		nodestat->workfile_created = instr->workfileCreated;
		nodestat->workmem_used     = (int64_t) instr->workmemused;
		nodestat->workmem_wanted   = (int64_t) instr->workmemwanted;
	}
	else
	{
		nodestat->node_status = QS_NODE_STATUS_INITIALIZED;
	}

	qs_walker_ctx->per_node_stats =
		lappend(qs_walker_ctx->per_node_stats, nodestat);

	{
		TimestampTz ts_now = qs_walker_ctx->ts_now;
		double cur_sum = nodestat->ntuples;
		bool found;
		NodeRollState *rs;

		rs = (NodeRollState *) hash_search(node_roll_htab,
			&nodestat->plan_node_id, HASH_ENTER, &found);

		if (found)
		{
			double dt = (double) (ts_now - rs->prev_executed_at) / USECS_PER_SEC;
			nodestat->ntuples_delta = cur_sum - rs->prev_ntuples_sum;
			nodestat->tuples_per_sec = (dt > 0) ? nodestat->ntuples_delta / dt : 0;
			nodestat->time_since_init_sec = (double) (ts_now - rs->first_executed_at) / USECS_PER_SEC;

			/* Relation identity is invariant per plan node: reuse the cache. */
			nodestat->relation_oid = rs->relation_oid;
			strlcpy(nodestat->relation_name, rs->relation_name, MAX_RELNAME_LEN);
		}
		else
		{
			Index rti = 0;

			nodestat->ntuples_delta = cur_sum;
			nodestat->tuples_per_sec = 0;
			nodestat->time_since_init_sec = 0;
			rs->first_executed_at = ts_now;

			switch (nodeTag(planstate->plan))
			{
				case T_SeqScan:
				case T_DynamicSeqScan:
				case T_SampleScan:
				case T_IndexScan:
				case T_DynamicIndexScan:
				case T_DynamicIndexOnlyScan:
				case T_IndexOnlyScan:
				case T_BitmapHeapScan:
				case T_DynamicBitmapHeapScan:
				case T_TidScan:
				case T_TidRangeScan:
				case T_ForeignScan:
				case T_DynamicForeignScan:
				case T_CustomScan:
					rti = ((Scan *) planstate->plan)->scanrelid;
					break;
				case T_ModifyTable:
					rti = ((ModifyTable *) planstate->plan)->nominalRelation;
					break;
				default:
					break;
			}

			if (rti > 0 && planstate->state)
			{
				List *rtable = planstate->state->es_range_table;
				if (rti <= (Index) list_length(rtable))
				{
					RangeTblEntry *rte = rt_fetch(rti, rtable);
					if (rte->rtekind == RTE_RELATION)
					{
						char *relname;
						nodestat->relation_oid = (int32_t) rte->relid;
						relname = get_rel_name(rte->relid);
						if (relname)
						{
							strlcpy(nodestat->relation_name, relname, MAX_RELNAME_LEN);
							pfree(relname);
						}
					}
				}
			}

			/* Cache the resolved identity for subsequent polls. */
			rs->relation_oid = nodestat->relation_oid;
			strlcpy(rs->relation_name, nodestat->relation_name, MAX_RELNAME_LEN);
		}

		nodestat->stalled = (nodestat->ntuples_delta == 0
				&& nodestat->node_status == QS_NODE_STATUS_EXECUTING
				&& !nodestat->eof);
		rs->prev_ntuples_sum = cur_sum;
		rs->prev_executed_at = ts_now;
	}
}

/*
 * qs_debug_node_sample -- emit a single GpscNodeSample to the PostgreSQL LOG.
 *
 * Intended for development and integration testing.  In production deployments
 * this will produce a large number of log lines; suppress with log_min_messages.
 */
static void
qs_debug_node_sample(GpscNodeSample *s)
{
	elog(DEBUG1,
		 "GpscNodeSample: "
		 "plan_node_id=%d parent=%d node_tag=%d "
		 "slice_id=%d segindex=%d "
		 "tmid=%d ssid=%d ccnt=%d "
		 "plan_rows=%.0f "
		 "ntuples=%.0f tuplecount=%.0f nloops=%.0f "
		 "startup=%f total=%f firsttuple=%f "
		 "shared_blks_hit=%lu shared_blks_read=%lu "
		 "workfile_created=%d workmem_used=%ld workmem_wanted=%ld "
		 "node_status=%d",
		 s->plan_node_id, s->parent_plan_node_id, s->node_tag,
		 s->slice_id, s->segindex,
		 s->tmid, s->ssid, s->ccnt,
		 s->plan_rows,
		 s->ntuples, s->tuplecount, s->nloops,
		 s->startup, s->total, s->firsttuple,
		 s->shared_blks_hit, s->shared_blks_read,
		 (int) s->workfile_created, (long) s->workmem_used, (long) s->workmem_wanted,
		 (int) s->node_status);
}

/*
 * qs_debug_node_stats -- emit all nodes in per_node_stats to the PostgreSQL LOG.
 *
 * Logs a summary line followed by one line per node via qs_debug_node_sample().
 */
static void
qs_debug_node_stats(List *per_node_stats)
{
	ListCell *lc;
	int       i = 0;

	if (!message_level_is_interesting(DEBUG1))
		return;

	elog(DEBUG1, "GpscNodeSample list: %d nodes", list_length(per_node_stats));
	foreach(lc, per_node_stats)
	{
		GpscNodeSample *s = (GpscNodeSample *) lfirst(lc);
		elog(DEBUG1, "--- node[%d] ---", i++);
		qs_debug_node_sample(s);
	}
}

/*
 * runtime_explain -- snapshot the active query's plan tree.
 *
 * Retrieves the top-most QueryDesc from QueryDescStack, walks its planstate
 * tree with qs_get_node_stats(), and returns the resulting List of
 * GpscNodeSample pointers.
 *
 * Callers must ensure QueryDescStack is non-empty before calling this.
 */
static List *
runtime_explain(TimestampTz ts_now)
{
	QsWalkerContext *qs_walker_ctx =
		(QsWalkerContext *) palloc0(sizeof(QsWalkerContext));
	QueryDesc *queryDesc;

	Assert(list_length(QueryDescStack) > 0);
	queryDesc = get_toppest_query();
	qs_walker_ctx->ts_now = ts_now;
	qs_walker_ctx->parent_plan_node_id = GPSC_NO_PARENT_PLAN_NODE_ID;
	qs_walker_ctx->slice_id = queryDesc->estate
		? LocallyExecutingSliceIndex(queryDesc->estate)
		: currentSliceId;
	gp_gettmid(&qs_walker_ctx->tmid);
	ensure_node_roll_htab();
	qs_planstate_walker(queryDesc->planstate, qs_get_node_stats,
						qs_walker_ctx, 0);
	return qs_walker_ctx->per_node_stats;
}

/*
 * emit_node_batch -- push a whole plan-tree snapshot as one SetPerNodeBatchReq.
 *
 * Flattens the List<GpscNodeSample *> into a contiguous array and hands it to
 * the C++ emitter, which opens a single UDS connection for the whole backend
 * instead of one connection per node.  A NULL or empty list is a no-op.
 *
 * The caller is responsible for calling gpsc_qs_sync_config() beforehand.
 */
static void
emit_node_batch(List *per_node_stats, const char *trace_id)
{
	GpscNodeSample **arr;
	ListCell        *lc;
	int              n = list_length(per_node_stats);
	int              i = 0;

	if (n == 0)
		return;

	arr = (GpscNodeSample **) palloc(n * sizeof(GpscNodeSample *));
	foreach(lc, per_node_stats)
		arr[i++] = (GpscNodeSample *) lfirst(lc);

	gpsc_emit_node_batch(arr, n, trace_id);
}

/*
 * build_plan_doc -- render the active query's plan via ExplainPrintPlan.
 *
 * Produces the full deparsed plan document (expressions, costs, Settings) in
 * the requested ExplainFormat.  ExplainBeginOutput/ExplainEndOutput and the
 * enclosing "Query" group frame the output so JSON/XML/YAML come out
 * well-formed: ExplainPrintPlan on its own renders only the inner "Plan"
 * property, so without the group the non-text formats are an unwrapped
 * fragment no parser accepts.  The framing lives here, outside
 * ExplainPrintPlan, so that function is left untouched.
 *
 * Returns a palloc'd string in the current context, or NULL when queryDesc is
 * NULL.  Intended for the coordinator (QD) only: on a QE the plan subtree can
 * reach child PlanStates from other slices that are not instantiated here.
 */
static char *
build_plan_doc(QueryDesc *queryDesc, ExplainFormat format)
{
	ExplainState   *es;

	if (queryDesc == NULL)
		return NULL;

	HOLD_INTERRUPTS();
	{
		es = NewExplainState();
		es->format  = format;
		es->verbose = true;
		es->costs   = true;
		es->runtime = true;
		ExplainBeginOutput(es);
		ExplainOpenGroup("Query", NULL, true, es);
		ExplainPrintPlan(es, queryDesc);
		ExplainCloseGroup("Query", NULL, true, es);
		ExplainEndOutput(es);
	}
	RESUME_INTERRUPTS();

	return es->str->data;
}

/*
 * SendQueryState -- handler for QueryStatePollReason.
 *
 * Fired asynchronously when another backend (or the monitoring function)
 * sends QueryStatePollReason to this process.
 *
 * Collects a plan-tree snapshot via runtime_explain(), logs it via
 * qs_debug_node_stats(), then syncs the emitter config and pushes the whole
 * snapshot to the yagpcc UDS sink via emit_node_batch().  On the coordinator
 * it additionally pushes the deparsed plan document (SetQueryPlanReq), which
 * the compact per-node stats cannot reconstruct; that push is rate-limited to
 * once per PLAN_DOC_RESEND_INTERVAL_MS per query.
 *
 * The entire body runs inside a dedicated MemoryContext that is deleted on
 * exit, preventing any leaks into the backend's long-lived contexts.  Any
 * errors are swallowed with FlushErrorState() to avoid crashing the backend.
 */
void
SendQueryState(void)
{
	int                     saved_errno = errno;
	MemoryContext  volatile oldcontext = CurrentMemoryContext;
	MemoryContext  volatile qs_context = NULL;
	QueryDesc              *qd;

	if (!pg_qs_enable)
	{
		errno = saved_errno;
		return;
	}

	if (!list_length(QueryDescStack))
	{
		errno = saved_errno;
		return;
	}

	if (MyBackendId < 1 || MyBackendId > MaxBackends)
	{
		errno = saved_errno;
		return;
	}

	if (stack_is_too_deep())
	{
		elog(DEBUG1, "pg_query_state: skipping poll, call stack too deep");
		errno = saved_errno;
		return;
	}

	qd = get_toppest_query();
	if (qd == NULL || qd->planstate == NULL || qd->estate == NULL)
	{
		errno = saved_errno;
		return;
	}

	HOLD_INTERRUPTS();
	PG_TRY();
	{
		List       *qs_result;
		TimestampTz now = GetCurrentTimestamp();

		qs_context = AllocSetContextCreate(TopMemoryContext,
										   "pg_query_state signal context",
										   ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(qs_context);

		qs_result = runtime_explain(now);
		qs_debug_node_stats(qs_result);
		gpsc_qs_sync_config();
		emit_node_batch(qs_result, qs_trace_slots[MyBackendId]);

		if (Gp_role == GP_ROLE_DISPATCH &&
			IsTransactionState() && CurrentResourceOwner != NULL)
		{
			bool	is_same_query;
			bool	is_stale;
			int32_t tmid;

			gp_gettmid(&tmid);
			is_same_query = (tmid == last_sent_query_key.tmid &&
							 gp_session_id == last_sent_query_key.ssid &&
							 gp_command_count == last_sent_query_key.ccnt);

			is_stale = !is_same_query ||
				TimestampDifferenceExceeds(last_sent_query_key.at,
										   now,
										   PLAN_DOC_RESEND_INTERVAL_MS);

			if (is_stale)
			{
				char *plan_doc = build_plan_doc(qd, EXPLAIN_FORMAT_JSON);

				gpsc_emit_query_plan(tmid, gp_session_id, gp_command_count,
									 plan_doc, EXPLAIN_FORMAT_JSON);

				last_sent_query_key.tmid = tmid;
				last_sent_query_key.ssid = gp_session_id;
				last_sent_query_key.ccnt = gp_command_count;
				last_sent_query_key.at   = now;
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);

		if (!elog_dismiss(WARNING))
		{
			if (qs_context)
				MemoryContextDelete(qs_context);
			RESUME_INTERRUPTS();
			errno = saved_errno;
			PG_RE_THROW();
		}
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	if (qs_context)
		MemoryContextDelete(qs_context);
	RESUME_INTERRUPTS();
	errno = saved_errno;
}

/*
 * fill_segpid -- append (segid, pid) pairs from one CDB segment's activelist.
 *
 * msg->pids[] has room for exactly 'cap' entries in total (not 'cap' more).
 * *index is the running write position, shared across all calls for one
 * message; it is advanced past every entry actually written.
 *
 * Entries are skipped when the descriptor has no live backend pid yet, so the
 * final *index may be LESS than the capacity estimated by the caller. The
 * caller must derive both msg->number and msg->length from the final *index,
 * never from the estimate.
 *
 * `is_entry_db` selects which of cdbs->{segment_db_info,entry_db_info} the
 * caller is walking.  An entry-db descriptor carries segindex -1, the same
 * value the QD itself reports, so its entries go out as GPSC_SEGID_ENTRY_DB.
 *
 * Returns true if the capacity was hit and one or more writable entries were
 * dropped.
 */
static bool
fill_segpid(CdbComponentDatabaseInfo *segInfo, backend_info *msg, Size cap,
			Size *index, bool is_entry_db)
{
	ListCell *lc;
	gp_segment_pid *segpid;
	SegmentDatabaseDescriptor *dbdesc;

	foreach(lc, segInfo->activelist)
	{
		dbdesc 		  = (SegmentDatabaseDescriptor *) lfirst(lc);
		if (!dbdesc || dbdesc->backendPid <= 0)
			continue;

		if (!is_entry_db && dbdesc->segindex < 0)
			continue;

		if (*index >= cap)
			return true;

		segpid 		  = &msg->pids[(*index)++];
		segpid->pid   = dbdesc->backendPid;
		segpid->segid = is_entry_db ? GPSC_SEGID_ENTRY_DB : dbdesc->segindex;
	}

	return false;
}

static int
count_active(CdbComponentDatabaseInfo *dbs, Size n)
{
	int cnt = 0;
	for (Size i = 0; i < n; ++i)
	{
		cnt += list_length(dbs[i].activelist);
	}

	return cnt;
}

/*
 * SendCdbComponents -- handler for BackendInfoPollReason (QD only).
 *
 * Collects the list of active QE (segid, pid) pairs from the CDB component
 * database and sends them back to the requestor through shm_mq as a
 * backend_info message.
 *
 * Side effects:
 *   - Calls cdbcomponent_getCdbComponents(); the returned structure is owned
 *     and cached by the CDB component cache (CdbComponentsContext), NOT by
 *     the local context below, and must not be freed here.
 *   - Only the locally built backend_info message is allocated in the
 *     short-lived context, which is deleted on every exit path.
 */
void
SendCdbComponents(void)
{
	int 				   saved_errno = errno;
	shm_mq_handle         *volatile mqh = NULL;
	CdbComponentDatabases *cdbs;
	MemoryContext          volatile oldctx = CurrentMemoryContext;
	MemoryContext		   volatile ctx = NULL;
	Size                    index = 0;
	msg_by_parts_result    send_result = MSG_BY_PARTS_SUCCEEDED;

	if (!mq || shm_mq_get_sender(mq) != MyProc || !mq_req_id)
	{
		errno = saved_errno;
		return;
	}

	if (!params || params->reason != BackendInfoPollReason)
	{
		errno = saved_errno;
		return;
	}

	HOLD_INTERRUPTS();
	PG_TRY();
	{
		ctx = AllocSetContextCreate(TopMemoryContext,
			"pg_query_state SendCdbComponents", ALLOCSET_DEFAULT_SIZES);
		oldctx = MemoryContextSwitchTo(ctx);

		mqh = shm_mq_attach(mq, NULL, NULL);

		if (Gp_role != GP_ROLE_DISPATCH)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: running not on QD");
			shm_mq_msg error_msg = {*mq_req_id, BASE_SIZEOF_SHM_MQ_MSG,
										  MyProc, WRONG_ROLE};
			send_result = send_msg_by_parts(mqh, error_msg.length, &error_msg);
		}
		else if (!pg_qs_enable)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: module disabled");
			shm_mq_msg disabled_msg = {*mq_req_id, BASE_SIZEOF_SHM_MQ_MSG,
									   MyProc, STAT_DISABLED};
			send_result = send_msg_by_parts(mqh, disabled_msg.length, &disabled_msg);
		}
		else if (list_length(QueryDescStack) == 0)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: no active query");
			shm_mq_msg not_running_msg = {*mq_req_id, BASE_SIZEOF_SHM_MQ_MSG,
										  MyProc, QUERY_NOT_RUNNING};
			send_result = send_msg_by_parts(mqh, not_running_msg.length, &not_running_msg);
		}
		else
		{
			MemoryContextSwitchTo(oldctx);
			cdbs = cdbcomponent_getCdbComponents();
			MemoryContextSwitchTo(ctx);

			int qecount = count_active(cdbs->entry_db_info, cdbs->total_entry_dbs)
				+ count_active(cdbs->segment_db_info, cdbs->total_segment_dbs);

			size_t bufsz = BASE_SIZEOF_GP_BACKEND_INFO + sizeof(gp_segment_pid) * qecount;
			backend_info *msg = (backend_info *) palloc0(bufsz);

			bool truncated = false;

			for (int i = 0; i < cdbs->total_segment_dbs; ++i)
			{
				CdbComponentDatabaseInfo *segInfo = &cdbs->segment_db_info[i];
				truncated |= fill_segpid(segInfo, msg, qecount, &index, false);
			}

			for (int i = 0; i < cdbs->total_entry_dbs; ++i)
			{
				CdbComponentDatabaseInfo *segInfo = &cdbs->entry_db_info[i];
				truncated |= fill_segpid(segInfo, msg, qecount, &index, true);
			}

			if (truncated)
			{
				elog(WARNING, "pg_query_state: SendCdbComponents: backend list truncated at %d of %d entries",
					 (int) index, qecount);
			}

			msg->reqid       = *mq_req_id;
			msg->length      = BASE_SIZEOF_GP_BACKEND_INFO + sizeof(gp_segment_pid) * index;
			msg->result_code = QS_RETURNED;
			Assert(index <= qecount);
			msg->number = index;
			send_result = send_msg_by_parts(mqh, msg->length, msg);
		}

		if (send_result != MSG_BY_PARTS_SUCCEEDED)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: send failed (%d)", 
				(int) send_result);
		}

		shm_mq_detach(mqh);
		mqh = NULL;
	}
	PG_CATCH();
	{
		if (mqh)
		{
			shm_mq_detach(mqh);
			mqh = NULL;
		}
		MemoryContextSwitchTo(oldctx);

		if (!elog_dismiss(WARNING))
		{
			if (ctx)
				MemoryContextDelete(ctx);

			RESUME_INTERRUPTS();
			errno = saved_errno;
			PG_RE_THROW();
		}
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldctx);
	if (ctx)
		MemoryContextDelete(ctx);

	RESUME_INTERRUPTS();
	errno = saved_errno;
}
