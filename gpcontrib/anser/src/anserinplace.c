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
 * anserinplace.c
 *	  In-place runtime filters: built on each segment from a hash join's
 *	  own Hash node and applied to the probe scan in the same process.
 *
 * Which joins get one is decided by AnserInplaceEligible (anserplan.c).  The
 * coordinator calls it to skip the exchange for those joins, and nothing
 * marks them in the plan: every segment calls it again here, at executor
 * start, on the plan it received.  For each eligible join in this process's
 * slice it
 *
 *	 - wraps the ExecProcNode of the Hash node's child, so that every row the
 *	   Hash node reads adds its key to a Bloom filter, and the end of the
 *	   input marks the filter ready;
 *	 - wraps the ExecProcNodeReal of the probe scans (the same scans
 *	   anser_inplace_has_target finds on the plan), so that they let rows
 *	   through until the filter is ready and then drop rows whose key it
 *	   lacks.  The rows are checked as the scan returns them, which works
 *	   for every table AM; a qual would not on AOCO, which evaluates the
 *	   qual before it loads columns the plan's own qual does not use.
 *
 * The filter never makes the scan wait: a join that fetches its first outer
 * tuple before building its hash table just sees that tuple unfiltered.
 * Keys are hashed in Anser's normalized form (AnserNormalizeBloomDatum) on
 * both sides, so every by-value key type AnserEqualityKeys accepts works, and
 * nothing here depends on core's gp_enable_runtime_filter_pushdown.
 *
 * A rescan of the build side adds its keys again; the filter only grows, so
 * it stays a superset of the keys.  A build side that a nested loop re-runs
 * with new parameter values would make it hold keys of earlier iterations
 * too -- still correct, but AnserInplaceEligible refuses it anyway, as the
 * exchange does.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserinplace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserbloom.h"
#include "anserfilter.h"
#include "anserplan.h"
#include "anserplanutils.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

/* One in-place filter: the Hash node's input it watches, and its keys. */
typedef struct AnserInplaceFilter
{
	PlanState  *build;			/* the Hash node's child */
	ExecProcNodeMtd build_next; /* the child's own ExecProcNode */
	AttrNumber	build_attno;	/* key position in the child's output */
	int16		key_length;		/* typlen of the key */
	bloom_filter *filter;
	bool		ready;			/* the build side has been read to its end */
} AnserInplaceFilter;

/* A probe scan we wrap, and the filters it applies to the rows it returns. */
typedef struct AnserInplaceProbe
{
	PlanState  *scan;
	ExecProcNodeMtd scan_next;	/* the scan's own ExecProcNodeReal */
	List	   *filters;		/* AnserInplaceFilter * */
	List	   *attnos;			/* each filter's key position in the scan's
								 * output */
} AnserInplaceProbe;

/*
 * The filters whose build side we wrap, found again by PlanState in the
 * wrapper.  A query has a handful, so a list is enough.  Kept in
 * TopMemoryContext; each entry is removed when its query's memory goes away,
 * on success and on error alike.
 */
static List *inplace_filters = NIL;
static List *inplace_probes = NIL;	/* AnserInplaceProbe *, likewise */

typedef struct AnserInplaceWalk
{
	PlannedStmt *stmt;
	EState	   *estate;
	int			local_slice;
	int			slice_index;
	Bitmapset  *nestloop_params;
} AnserInplaceWalk;

static AnserInplaceFilter *
anser_inplace_lookup(PlanState *build)
{
	ListCell   *lc;

	foreach(lc, inplace_filters)
	{
		AnserInplaceFilter *f = (AnserInplaceFilter *) lfirst(lc);

		if (f->build == build)
			return f;
	}
	return NULL;
}

static AnserInplaceProbe *
anser_inplace_probe_lookup(PlanState *scan)
{
	ListCell   *lc;

	foreach(lc, inplace_probes)
	{
		AnserInplaceProbe *p = (AnserInplaceProbe *) lfirst(lc);

		if (p->scan == scan)
			return p;
	}
	return NULL;
}

static void
anser_inplace_forget(void *arg)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	inplace_filters = list_delete_ptr(inplace_filters, arg);
	inplace_probes = list_delete_ptr(inplace_probes, arg);
	MemoryContextSwitchTo(oldcxt);
}

/* Drop the entry from its registry when its query's memory goes away. */
static void
anser_inplace_register(List **registry, void *entry, MemoryContext cxt)
{
	MemoryContextCallback *cb;
	MemoryContext oldcxt;

	cb = MemoryContextAlloc(cxt, sizeof(MemoryContextCallback));
	cb->func = anser_inplace_forget;
	cb->arg = entry;
	MemoryContextRegisterResetCallback(cxt, cb);

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	*registry = lappend(*registry, entry);
	MemoryContextSwitchTo(oldcxt);
}

/* The Hash node reads its input through this. */
static TupleTableSlot *
anser_inplace_build_next(PlanState *node)
{
	AnserInplaceFilter *f = anser_inplace_lookup(node);
	TupleTableSlot *slot;
	Datum		value;
	bool		isnull;

	if (f == NULL)
		elog(ERROR, "anser in-place filter lost its build side");

	slot = f->build_next(node);

	/*
	 * ExecProcNodeFirst replaces itself on the first call; take the function
	 * it installed and put ourselves back in front of it.
	 */
	if (node->ExecProcNode != anser_inplace_build_next)
	{
		f->build_next = node->ExecProcNode;
		node->ExecProcNode = anser_inplace_build_next;
	}

	if (TupIsNull(slot))
	{
		f->ready = true;
		return slot;
	}

	value = slot_getattr(slot, f->build_attno, &isnull);
	if (!isnull)
		AnserBloomAddKey(f->filter, value, f->key_length);
	return slot;
}

/* Does the row pass every ready filter of this scan? */
static bool
anser_inplace_keep(AnserInplaceProbe *p, TupleTableSlot *slot)
{
	ListCell   *lc1;
	ListCell   *lc2;

	forboth(lc1, p->filters, lc2, p->attnos)
	{
		AnserInplaceFilter *f = (AnserInplaceFilter *) lfirst(lc1);
		Datum		value;
		bool		isnull;

		/* Until the hash table is built, and while the filter has turned
		 * itself off (bloom_runtime_probe), rows pass. */
		if (!f->ready || bloom_runtime_probe_skip(f->filter))
			continue;
		value = slot_getattr(slot, lfirst_int(lc2), &isnull);
		if (!isnull && AnserBloomProbe(f->filter, value, f->key_length))
			return false;
	}
	return true;
}

/* The probe scan returns its rows through this. */
static TupleTableSlot *
anser_inplace_probe_next(PlanState *node)
{
	AnserInplaceProbe *p = anser_inplace_probe_lookup(node);

	if (p == NULL)
		elog(ERROR, "anser in-place filter lost its probe scan");

	for (;;)
	{
		TupleTableSlot *slot = p->scan_next(node);

		if (TupIsNull(slot) || anser_inplace_keep(p, slot))
			return slot;
		InstrCountFilteredPRF(node, 1);
	}
}

static void
anser_inplace_install_probe(PlanState *scan, AttrNumber output_attno,
							AnserInplaceFilter *f, MemoryContext cxt)
{
	AnserInplaceProbe *p = anser_inplace_probe_lookup(scan);
	MemoryContext oldcxt = MemoryContextSwitchTo(cxt);

	if (p == NULL)
	{
		p = palloc0(sizeof(AnserInplaceProbe));
		p->scan = scan;
		p->scan_next = scan->ExecProcNodeReal;
		scan->ExecProcNodeReal = anser_inplace_probe_next;
		anser_inplace_register(&inplace_probes, p, cxt);
	}
	p->filters = lappend(p->filters, f);
	p->attnos = lappend_int(p->attnos, output_attno);
	if (scan->instrument != NULL)
		scan->instrument->prf_work = true;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * The probe scans for this join: the PlanState twin of
 * anser_inplace_has_target (anserplan.c), which the coordinator and
 * AnserInplaceEligible already ran on the same plan.  Returns false, having
 * installed nothing, if the walk does not end where that one did.
 */
static bool
anser_inplace_find_scans(HashJoinState *hjs, AttrNumber attno, List **scans,
						 List **attnos)
{
	PlanState  *parent = &hjs->js.ps;
	PlanState  *child = outerPlanState(parent);

	*scans = NIL;
	*attnos = NIL;
	for (;;)
	{
		TargetEntry *te;
		Var		   *var;

		if (IsA(parent, AppendState) && child == NULL)
		{
			AppendState *as = (AppendState *) parent;

			if (as->as_nplans <= 0)
				return false;
			for (int i = 0; i < as->as_nplans; i++)
			{
				PlanState  *sub = as->appendplans[i];

				if (sub == NULL || !IsA(sub, SeqScanState) ||
					AnserScanOutputVar((Scan *) sub->plan, attno) == NULL)
					return false;
				*scans = lappend(*scans, sub);
				*attnos = lappend_int(*attnos, attno);
			}
			return true;
		}
		if (child == NULL)
			return false;

		if (IsA(child, SeqScanState))
		{
			if (AnserScanOutputVar((Scan *) child->plan, attno) == NULL)
				return false;
			*scans = lappend(*scans, child);
			*attnos = lappend_int(*attnos, attno);
			return true;
		}

		if (attno < 1 || attno > list_length(child->plan->targetlist))
			return false;
		te = (TargetEntry *) list_nth(child->plan->targetlist, attno - 1);
		if (te == NULL || !IsA(te->expr, Var))
			return false;
		var = (Var *) te->expr;

		if (IsA(child, CustomScanState) && AnserIsRuntimeFilterScan(child->plan) &&
			list_length(((CustomScanState *) child)->custom_ps) == 1)
		{
			if (var->varno != INDEX_VAR || var->varattno < 1)
				return false;
			attno = var->varattno;
			parent = child;
			child = (PlanState *) linitial(((CustomScanState *) child)->custom_ps);
			continue;
		}

		if (!IsA(child, HashJoinState) && !IsA(child, ResultState) &&
			!IsA(child, AppendState))
			return false;
		if (var->varno == INNER_VAR)
			return false;
		if (IsA(child, HashJoinState) &&
			!AnserJoinSidePushdownSafe(((HashJoin *) child->plan)->join.jointype,
									   false))
			return false;

		attno = var->varattno;
		parent = child;
		child = outerPlanState(child);
	}
}

static void
anser_inplace_setup(HashJoinState *hjs, AnserInplaceWalk *walk)
{
	HashJoin   *hj = (HashJoin *) hjs->js.ps.plan;
	PlanState  *hash = innerPlanState(hjs);
	PlanState  *build = hash != NULL ? outerPlanState(hash) : NULL;
	Var		   *outer_var;
	Var		   *inner_var;
	Oid			key_type;
	TargetEntry *te;
	AttrNumber	build_attno;
	Form_pg_attribute keyattr;
	int64		total_elems;
	int64		max_payload;
	int64		planned_bytes;
	List	   *scans;
	List	   *attnos;
	AnserInplaceFilter *f;
	MemoryContext oldcxt;
	ListCell   *lc1;
	ListCell   *lc2;

	if (build == NULL || !IsA(hash, HashState))
		return;
	if (anser_inplace_lookup(build) != NULL)
		return;					/* already set up */
	if (!AnserEqualityKeys((Node *) linitial(hj->hashclauses),
						   &outer_var, &inner_var, &key_type))
		return;

	/* The build key as the Hash node's child emits it. */
	if (inner_var->varattno < 1 ||
		inner_var->varattno > list_length(hash->plan->targetlist))
		return;
	te = (TargetEntry *) list_nth(hash->plan->targetlist, inner_var->varattno - 1);
	if (te == NULL || !IsA(te->expr, Var) || ((Var *) te->expr)->varattno < 1)
		return;
	build_attno = ((Var *) te->expr)->varattno;
	keyattr = TupleDescAttr(ExecGetResultType(build), build_attno - 1);
	if (!keyattr->attbyval)
		return;

	if (!anser_inplace_find_scans(hjs, outer_var->varattno, &scans, &attnos))
		return;
	if (!AnserRuntimeFilterSize(hash->plan->plan_rows, &total_elems,
								&max_payload, &planned_bytes))
		return;

	oldcxt = MemoryContextSwitchTo(walk->estate->es_query_cxt);

	f = palloc0(sizeof(AnserInplaceFilter));
	f->build = build;
	f->build_next = build->ExecProcNode;
	f->build_attno = build_attno;
	f->key_length = keyattr->attlen;
	f->filter = AnserBloomCreate(total_elems, max_payload, 0);
	f->ready = false;

	MemoryContextSwitchTo(oldcxt);

	forboth(lc1, scans, lc2, attnos)
		anser_inplace_install_probe((PlanState *) lfirst(lc1),
									(AttrNumber) lfirst_int(lc2), f,
									walk->estate->es_query_cxt);
	anser_inplace_register(&inplace_filters, f, walk->estate->es_query_cxt);

	build->ExecProcNode = anser_inplace_build_next;
}

/*
 * Walk the PlanState tree as anser_inject_walk (anserplan.c) walks the plan,
 * so that slices and nested-loop parameters are tracked the same way.
 */
static void
anser_inplace_walk(PlanState *ps, AnserInplaceWalk *walk)
{
	int			save_slice = walk->slice_index;

	if (ps == NULL)
		return;

	if (IsA(ps->plan, Motion))
		walk->slice_index = ((Motion *) ps->plan)->motionID;

	if (IsA(ps, HashJoinState) && walk->slice_index == walk->local_slice &&
		AnserInplaceEligible((HashJoin *) ps->plan, walk->stmt,
							 walk->slice_index, walk->nestloop_params))
		anser_inplace_setup((HashJoinState *) ps, walk);

	anser_inplace_walk(outerPlanState(ps), walk);

	if (IsA(ps->plan, NestLoop) && ((NestLoop *) ps->plan)->nestParams != NIL)
	{
		Bitmapset  *saved = walk->nestloop_params;
		Bitmapset  *inner = bms_copy(saved);
		ListCell   *lc;

		foreach(lc, ((NestLoop *) ps->plan)->nestParams)
			inner = bms_add_member(inner,
								   ((NestLoopParam *) lfirst(lc))->paramno);
		walk->nestloop_params = inner;
		anser_inplace_walk(innerPlanState(ps), walk);
		walk->nestloop_params = saved;
		bms_free(inner);
	}
	else
		anser_inplace_walk(innerPlanState(ps), walk);

	if (IsA(ps, CustomScanState))
	{
		ListCell   *lc;

		foreach(lc, ((CustomScanState *) ps)->custom_ps)
			anser_inplace_walk((PlanState *) lfirst(lc), walk);
	}

	walk->slice_index = save_slice;
}

/*
 * Called after standard executor start.  Only segments build in-place
 * filters: the coordinator's slice is never eligible.
 */
void
AnserInplaceExecutorStart(QueryDesc *queryDesc, int eflags)
{
	AnserInplaceWalk walk = {0};
	PlanState  *root = queryDesc->planstate;

	if (Gp_role != GP_ROLE_EXECUTE)
		return;
	if (!gp_anser_enable || !gp_anser_runtime_filter ||
		!gp_anser_runtime_filter_inplace)
		return;
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;
	if (queryDesc->plannedstmt == NULL ||
		queryDesc->plannedstmt->commandType != CMD_SELECT ||
		queryDesc->estate == NULL || root == NULL)
		return;

	walk.stmt = queryDesc->plannedstmt;
	walk.estate = queryDesc->estate;
	walk.local_slice = LocallyExecutingSliceIndex(queryDesc->estate);
	walk.slice_index = 0;		/* as the coordinator's walk starts */
	walk.nestloop_params = NULL;

	anser_inplace_walk(root, &walk);
}
