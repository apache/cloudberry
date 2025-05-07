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
 * nodeRuntimeFilter.c
 *	  Routines to handle runtime filter.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeRuntimeFilter.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_namespace.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeRuntimeFilter.h"
#include "lib/bloomfilter.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"

static TupleTableSlot *RuntimeFilterTupleNext(RuntimeFilterState *node);
static void ExecRuntimeFilterExplainEnd(PlanState *planstate,
										struct StringInfoData *buf);
static void RFFillTupleValues(RuntimeFilterState *rfstate, List *values);

/* ----------------------------------------------------------------
 *		ExecRuntimeFilter
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRuntimeFilter(PlanState *pstate)
{
	RuntimeFilterState *node = castNode(RuntimeFilterState, pstate);
	PlanState  *outerPlan;

	outerPlan = outerPlanState(node);
	/* Check whether this filter is ready */
	if (!node->build_finish || node->build_suspend)
		return ExecProcNode(outerPlan);

	return RuntimeFilterTupleNext(node);
}

static TupleTableSlot *
RuntimeFilterTupleNext(RuntimeFilterState *node)
{
	ExprContext *econtext;
	HashJoinState *hjstate;
	PlanState  *outerPlan;
	HashJoinTable hashtable;
	List *hashkeys;
	FmgrInfo *hashfunctions;
	Oid *hashcollations;
	ListCell *hk;
	MemoryContext oldContext;
	TupleTableSlot *slot;

	CHECK_FOR_INTERRUPTS();

	outerPlan = outerPlanState(node);
	hjstate = node->hjstate;
	econtext = hjstate->js.ps.ps_ExprContext;
	hashkeys = hjstate->hj_OuterHashKeys;
	hashtable = hjstate->hj_HashTable;
	hashfunctions = hashtable->outer_hashfunctions;
	hashcollations = hashtable->collations;

	for (;;)
	{
		int idx = 0;
		bool hasnull = false;

		slot = ExecProcNode(outerPlan);
		if (TupIsNull(slot))
			return slot;

		ResetExprContext(econtext);
		oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		
		econtext->ecxt_outertuple = slot;

		foreach(hk, hashkeys)
		{
			ExprState  *keyexpr = (ExprState *) lfirst(hk);
			Datum		keyval;
			bool		isNull = false;
			
			/* Get the join attribute value of the tuple */
			keyval = ExecEvalExpr(keyexpr, econtext, &isNull);
			hasnull = hasnull || isNull;
			if (hasnull)
				break;

			if (node->raw_value[idx])
				node->value_buf[idx] = keyval;
			else
			{
				uint32 hkey;

				/* Compute the hash function */
				hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[idx], hashcollations[idx], keyval));
				node->value_buf[idx] = hkey;
			}

			idx++;
		}

		MemoryContextSwitchTo(oldContext);

		if (hasnull)
			return slot; /* We don't handle NULL here. */
		if (!bloom_lacks_element(node->bf, (unsigned char *) node->value_buf,
								hashkeys->length * sizeof(Datum)))
			return slot;
	}
	pg_unreachable();
}

/* ----------------------------------------------------------------
 *		ExecInitRuntimeFilter
 * ----------------------------------------------------------------
 */
RuntimeFilterState *
ExecInitRuntimeFilter(RuntimeFilter *node, EState *estate, int eflags)
{
	RuntimeFilterState	*rfstate;
	Plan				*outerPlan;

	/*
	 * create state structure
	 */
	rfstate = makeNode(RuntimeFilterState);
	rfstate->ps.plan = (Plan *) node;
	rfstate->ps.state = estate;
	rfstate->ps.ExecProcNode = ExecRuntimeFilter;

	rfstate->build_finish = false;
	rfstate->build_suspend = false;
	rfstate->inner_processed = 0;

	/* will setup later*/
	rfstate->inner_estimated = 0;
	rfstate->inner_threshold = 0;
	rfstate->value_buf = NULL;
	rfstate->raw_value = NULL;
	rfstate->bf = NULL;

	/* CDB: Offer extra info for EXPLAIN ANALYZE. */
	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
	{
		/* Allocate string buffer. */
		rfstate->ps.cdbexplainbuf = makeStringInfo();

		/* Request a callback at end of query. */
		rfstate->ps.cdbexplainfun = ExecRuntimeFilterExplainEnd;
	}

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rfstate->ps);

	/*
	 * initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(rfstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * Initialize result type
	 */
	ExecInitResultTypeTL(&rfstate->ps);

	rfstate->ps.resultopsset = true;
	rfstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(rfstate),
												 &rfstate->ps.resultopsfixed);

	/*
	 * RuntimeFilter nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	rfstate->ps.ps_ProjInfo = NULL;

	return rfstate;
}

static void
ExecRuntimeFilterExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	RuntimeFilterState *rfstate = (RuntimeFilterState *) planstate;

	if (rfstate->build_suspend)
	{
		appendStringInfoString(buf, "Suspend");
		return;
	}
	if (rfstate->bf == NULL)
		return;

	appendStringInfo(buf, "Inner Processed: %lu, ", rfstate->inner_processed);
	appendStringInfo(buf, "Flase Positive Rate: %f",
					 bloom_false_positive_rate(rfstate->bf));
}

void
ExecInitRuntimeFilterFinish(RuntimeFilterState *node, double inner_rows)
{
	List	   *hashops;
	ListCell   *lc;
	int			i = 0;

	hashops = node->hjstate->hj_HashOperators;

	node->value_buf = (Datum *) palloc(hashops->length * sizeof(Datum));
	node->raw_value = (bool *) palloc(hashops->length * sizeof(bool));
	node->inner_estimated = (uint64) inner_rows;

	node->bf = bloom_create_aggresive((int64) inner_rows, work_mem, random());
	if (node->bf == NULL)
		node->build_suspend = true;
	/* we can't handle inner table which is bigger than this threshold */
	node->inner_threshold = bloom_total_bits(node->bf) / 1.6;

	/* init hash function related fields */
	foreach(lc, hashops)
	{
		Oid outer_typ;
		Oid inner_typ;
		bool outer_raw;
		bool inner_raw;

		op_input_types(lfirst_oid(lc), &outer_typ, &inner_typ);
		outer_raw = IsRawInt8CmpType(outer_typ);
		inner_raw = IsRawInt8CmpType(inner_typ);

		/* can we directly compare the i-th value as int8? */
		node->raw_value[i++] = outer_raw && inner_raw;
	}
}

void
ExecEndRuntimeFilter(RuntimeFilterState *node)
{
	if (node->bf != NULL)
		bloom_free(node->bf);
	if (node->value_buf != NULL)
		pfree(node->value_buf);

	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanRuntimeFilter(RuntimeFilterState *node)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

void
RFBuildFinishCallback(RuntimeFilterState *rfstate, bool parallel)
{
	if (rfstate == NULL)
		return;

	rfstate->build_suspend = rfstate->build_suspend || parallel;
	rfstate->build_finish = true;	
}

void
RFAddTupleValues(RuntimeFilterState *rfstate, List *values)
{
	if (rfstate->build_suspend)
		return;

	RFFillTupleValues(rfstate, values);
	bloom_add_element(rfstate->bf, (unsigned char *) rfstate->value_buf,
					  sizeof(Datum) * values->length);
	list_free(values);

	rfstate->inner_processed += 1;
	/* check inner threshold */
	if (rfstate->inner_processed > rfstate->inner_threshold)
		rfstate->build_suspend = true;
}

static void
RFFillTupleValues(RuntimeFilterState *rfstate, List *values)
{
	int idx = 0;
	ListCell *lc;

	foreach(lc, values)
	{
		Datum *dp = (Datum *) lfirst(lc);

		rfstate->value_buf[idx] = *dp;
		idx++;
	}
}

void
ExecRFExplainEnd(HashState *hashState, struct StringInfoData *buf)
{
	ListCell	*lc;
	AttrFilter	*attr_filter;
	SeqScanState *sss;

	if (!hashState->filters)
		return;

	foreach (lc, hashState->filters)
	{
		attr_filter = lfirst(lc);
		if (attr_filter->empty || attr_filter->hasnulls)
			continue;

		sss = castNode(SeqScanState, attr_filter->target);
		appendStringInfo(buf, "RF: %s attrno: %d, range[%ld, %ld], n_distinct: %.2f\n",
						 RelationGetRelationName(sss->ss.ss_currentRelation),
						 attr_filter->lattno, (int64_t) attr_filter->min,
						 (int64_t) attr_filter->max, attr_filter->n_distinct);
	}
}
