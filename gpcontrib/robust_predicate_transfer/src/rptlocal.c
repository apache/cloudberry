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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * rptlocal.c
 *	  In-process bloom handoff for local reducer hops.
 *
 * A local hop joins co-located tables: every row of the destination that can
 * match a row of the source lives on the same segment as that row.  So the
 * source's filter on segment i is complete for the destination on segment i,
 * and the hop needs no coordinator.  Within the reducer both scans run in one
 * QE process, as children of one Sequence: the producer's child is exhausted
 * before the consumer's scan starts.  The producer leaves its filter in a
 * per-execution registry; the consumer picks it up there.
 *
 * Only hops consumed inside the reducer use this.  A filter the main plan
 * applies goes through Anser, because the main plan is a different dispatch.
 *
 * Failing open: a consumer that finds no completed filter passes every row.
 * The registry entry lives in the executor's query context and unregisters
 * itself when that context goes, so an aborted execution leaves nothing behind.
 *
 * Keys use Anser's width-normalized Datum representation. Unused upper
 * bytes can differ between heap and AOCO even for the same by-value column.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptlocal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anserplanutils.h"

#include "anser.h"
#include "anserbloom.h"
#include "anserfilter.h"
#include "anserplan.h"
#include "ansertlist.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "lib/bloomfilter.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/memutils.h"

#include "rptlocal.h"

/* custom_private layout */
#define RPT_LOCAL_PRIV_CONDITION	0
#define RPT_LOCAL_PRIV_KEY_ATTNO	1
#define RPT_LOCAL_PRIV_TOTAL_ELEMS	2

/* One filter of one execution. */
typedef struct RptLocalEntry
{
	dlist_node	node;
	EState	   *estate;
	uint32		condition_id;
	bloom_filter *filter;
	bool		complete;		/* the producer's child was exhausted */
	MemoryContextCallback unregister;
} RptLocalEntry;

static dlist_head rpt_local_entries = DLIST_STATIC_INIT(rpt_local_entries);

typedef struct RptLocalScanState
{
	CustomScanState csstate;
	uint32		condition_id;
	AttrNumber	key_attno;
	int16		key_length;
	int64		total_elems;
	bool		started;
	RptLocalEntry *entry;		/* producer: where it adds; consumer: what it probes */
} RptLocalScanState;

static Node *rpt_local_create_producer(CustomScan *cscan);
static Node *rpt_local_create_consumer(CustomScan *cscan);
static void rpt_local_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *rpt_local_produce_exec(CustomScanState *node);
static TupleTableSlot *rpt_local_consume_exec(CustomScanState *node);
static void rpt_local_end(CustomScanState *node);
static void rpt_local_rescan(CustomScanState *node);
static void rpt_local_explain(CustomScanState *node, List *ancestors,
							  ExplainState *es);

static const CustomScanMethods rpt_local_producer_methods = {
	.CustomName = "RPT Local Bloom Producer",
	.CreateCustomScanState = rpt_local_create_producer,
};

static const CustomScanMethods rpt_local_consumer_methods = {
	.CustomName = "RPT Local Bloom Consumer",
	.CreateCustomScanState = rpt_local_create_consumer,
};

static const CustomExecMethods rpt_local_producer_exec = {
	.CustomName = "RPT Local Bloom Producer",
	.BeginCustomScan = rpt_local_begin,
	.ExecCustomScan = rpt_local_produce_exec,
	.EndCustomScan = rpt_local_end,
	.ReScanCustomScan = rpt_local_rescan,
	.ExplainCustomScan = rpt_local_explain,
};

static const CustomExecMethods rpt_local_consumer_exec = {
	.CustomName = "RPT Local Bloom Consumer",
	.BeginCustomScan = rpt_local_begin,
	.ExecCustomScan = rpt_local_consume_exec,
	.EndCustomScan = rpt_local_end,
	.ReScanCustomScan = rpt_local_rescan,
	.ExplainCustomScan = rpt_local_explain,
};

void
RptRegisterLocalMethods(void)
{
	RegisterCustomScanMethods(&rpt_local_producer_methods);
	RegisterCustomScanMethods(&rpt_local_consumer_methods);
	/* Both are identity filters: in-place filters may look through them. */
	AnserRegisterFilterScanMethods(&rpt_local_producer_methods);
	AnserRegisterFilterScanMethods(&rpt_local_consumer_methods);
}

bool
RptIsLocalScan(const Plan *plan)
{
	if (plan == NULL || !IsA(plan, CustomScan))
		return false;
	return ((const CustomScan *) plan)->methods == &rpt_local_producer_methods ||
		((const CustomScan *) plan)->methods == &rpt_local_consumer_methods;
}

/* ---- plan construction ---- */

static CustomScan *
rpt_local_build(const CustomScanMethods *methods, Plan *child,
				AttrNumber key_attno, uint32 condition_id, int64 total_elems)
{
	CustomScan *cs = makeNode(CustomScan);

	cs->custom_private = list_make3(makeInteger((int) condition_id),
									makeInteger((int) key_attno),
									makeInteger((int) Min(total_elems, PG_INT32_MAX)));
	AnserInitFilterScan(cs, methods, child);
	cs->scan.plan.parallel_safe = false;

	return cs;
}

CustomScan *
RptBuildLocalProducerScan(Plan *child, AttrNumber key_attno,
						  uint32 condition_id, int64 total_elems)
{
	return rpt_local_build(&rpt_local_producer_methods, child, key_attno,
						   condition_id, total_elems);
}

CustomScan *
RptBuildLocalConsumerScan(Plan *child, AttrNumber key_attno,
						  uint32 condition_id, int64 total_elems)
{
	return rpt_local_build(&rpt_local_consumer_methods, child, key_attno,
						   condition_id, total_elems);
}

/* ---- registry ---- */

static void
rpt_local_unregister(void *arg)
{
	dlist_delete(&((RptLocalEntry *) arg)->node);
}

static RptLocalEntry *
rpt_local_find(EState *estate, uint32 condition_id)
{
	dlist_iter	iter;

	dlist_foreach(iter, &rpt_local_entries)
	{
		RptLocalEntry *e = dlist_container(RptLocalEntry, node, iter.cur);

		if (e->estate == estate && e->condition_id == condition_id)
			return e;
	}
	return NULL;
}

/* ---- execution ---- */

static Node *
rpt_local_create_state(CustomScan *cscan, const CustomExecMethods *exec)
{
	RptLocalScanState *st = (RptLocalScanState *)
		newNode(sizeof(RptLocalScanState), T_CustomScanState);

	st->csstate.methods = exec;
	return (Node *) st;
}

static Node *
rpt_local_create_producer(CustomScan *cscan)
{
	return rpt_local_create_state(cscan, &rpt_local_producer_exec);
}

static Node *
rpt_local_create_consumer(CustomScan *cscan)
{
	return rpt_local_create_state(cscan, &rpt_local_consumer_exec);
}

static void
rpt_local_begin(CustomScanState *node, EState *estate, int eflags)
{
	RptLocalScanState *st = (RptLocalScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	st->condition_id = (uint32) intVal(list_nth(cscan->custom_private, RPT_LOCAL_PRIV_CONDITION));
	st->key_attno = (AttrNumber) intVal(list_nth(cscan->custom_private, RPT_LOCAL_PRIV_KEY_ATTNO));
	st->total_elems = intVal(list_nth(cscan->custom_private, RPT_LOCAL_PRIV_TOTAL_ELEMS));
	st->started = false;
	st->entry = NULL;

	/* Nothing is allocated until the node runs: the coordinator initializes
	 * every slice, and EXPLAIN initializes without running. */
	node->custom_ps = list_make1(ExecInitNode((Plan *) linitial(cscan->custom_plans),
											  estate, eflags));
	AnserInitFilterChildSlot(node, gp_anser_zero_copy);
	st->key_length = AnserKeyLength((PlanState *) linitial(node->custom_ps), st->key_attno);
}

static TupleTableSlot *
rpt_local_produce_next(CustomScanState *node)
{
	RptLocalScanState *st = (RptLocalScanState *) node;
	PlanState  *child = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot *slot;

	if (!st->started)
	{
		EState	   *estate = node->ss.ps.state;

		st->started = true;
		st->entry = rpt_local_find(estate, st->condition_id);

		/*
		 * A rescan keeps the entry and adds to the same filter: the result
		 * is a superset of the keys of every pass, which is all a consumer
		 * needs.
		 */
		if (st->entry == NULL)
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
			RptLocalEntry *e = (RptLocalEntry *) palloc0(sizeof(RptLocalEntry));

			e->estate = estate;
			e->condition_id = st->condition_id;
			e->filter = bloom_create_with_minimum(Max(st->total_elems, 1), work_mem, 0,
													 ANSER_BLOOM_MIN_BYTES);
			e->unregister.func = rpt_local_unregister;
			e->unregister.arg = e;
			MemoryContextRegisterResetCallback(estate->es_query_cxt, &e->unregister);
			dlist_push_head(&rpt_local_entries, &e->node);
			MemoryContextSwitchTo(oldcxt);
			st->entry = e;
		}
	}

	slot = ExecProcNode(child);
	if (TupIsNull(slot))
	{
		st->entry->complete = true;
		return NULL;
	}

	{
		bool		isnull;
		Datum		value = slot_getattr(slot, st->key_attno, &isnull);

		if (!isnull)
		{
			AnserBloomAddKey(st->entry->filter, value, st->key_length);
		}
	}
	return AnserFilterChildSlot(node, slot);
}

static TupleTableSlot *
rpt_local_produce_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, (ExecScanAccessMtd) rpt_local_produce_next,
					(ExecScanRecheckMtd) AnserFilterRecheck);
}

static TupleTableSlot *
rpt_local_consume_next(CustomScanState *node)
{
	RptLocalScanState *st = (RptLocalScanState *) node;
	PlanState  *child = (PlanState *) linitial(node->custom_ps);

	if (!st->started)
	{
		RptLocalEntry *e = rpt_local_find(node->ss.ps.state, st->condition_id);

		st->started = true;
		st->entry = (e != NULL && e->complete) ? e : NULL;
	}

	for (;;)
	{
		TupleTableSlot *slot = ExecProcNode(child);
		bool		isnull;
		Datum		value;

		if (TupIsNull(slot))
			return NULL;
		if (st->entry == NULL || bloom_runtime_probe_skip(st->entry->filter))
			return AnserFilterChildSlot(node, slot);	/* fail open */

		/* NULLs never match an equijoin key; let the join handle them. */
		value = slot_getattr(slot, st->key_attno, &isnull);
		if (!isnull)
		{
			InstrCountFiltered1(node, 1);
			if (AnserBloomProbe(st->entry->filter, value, st->key_length))
			{
				InstrCountFiltered2(node, 1);
				continue;
			}
		}
		return AnserFilterChildSlot(node, slot);
	}
}

static TupleTableSlot *
rpt_local_consume_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, (ExecScanAccessMtd) rpt_local_consume_next,
					(ExecScanRecheckMtd) AnserFilterRecheck);
}

static void
rpt_local_end(CustomScanState *node)
{
	/* The entry belongs to the query context, not to this node. */
	((RptLocalScanState *) node)->entry = NULL;
	if (node->custom_ps != NIL)
		ExecEndNode((PlanState *) linitial(node->custom_ps));
}

static void
rpt_local_rescan(CustomScanState *node)
{
	RptLocalScanState *st = (RptLocalScanState *) node;

	st->started = false;
	st->entry = NULL;
	if (node->custom_ps != NIL)
		ExecReScan((PlanState *) linitial(node->custom_ps));
}

static void
rpt_local_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	RptLocalScanState *st = (RptLocalScanState *) node;

	ExplainPropertyInteger("Local Bloom Condition", NULL, st->condition_id, es);
	if (es->analyze && node->ss.ps.instrument != NULL &&
		node->methods == &rpt_local_consumer_exec &&
		node->ss.ps.instrument->nloops > 0)
		ExplainPropertyFloat("Rows Removed by Local Bloom Filter", NULL,
							 node->ss.ps.instrument->nfiltered2 /
							 node->ss.ps.instrument->nloops, 0, es);
}
