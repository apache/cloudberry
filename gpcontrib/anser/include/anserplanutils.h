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
 * anserplanutils.h
 *	  Shared filter-plan mechanics and explicit traversal policies.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserplanutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERPLANUTILS_H
#define ANSERPLANUTILS_H

#include "anserplan.h"
#include "ansertlist.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"

/* These header helpers are shared by C and C++ callers. Structural checks,
 * child iteration/replacement and key descent are allocation/ERROR-free;
 * callback errors remain the caller's responsibility. Filter construction and
 * slot copying can allocate/ERROR and must be inside RptPgCall in C++ code. */

/* Reduce one input using keys from the other input of the same join. */
static inline bool
AnserJoinSideReducible(JoinType type, bool inner_side)
{
	switch (type)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			return true;
		case JOIN_LEFT:
			return inner_side;
		case JOIN_RIGHT:
			return !inner_side;
		default:
			return false;
	}
}

/* Push a downstream filter THROUGH this join. Its nullable input is unsafe:
 * pruning there can create a NULL-extended row visible to the downstream
 * predicate. This is distinct from reducing an input by this join's keys.
 * A semi join's inner side is unsafe too: pruning it can change which inner
 * row an outer row matches, so it is refused even though no ordinary plan
 * outputs inner columns above a semi join. */
static inline bool
AnserJoinSidePushdownSafe(JoinType type, bool inner_side)
{
	switch (type)
	{
		case JOIN_INNER:
			return true;
		case JOIN_LEFT:
		case JOIN_SEMI:
			return !inner_side;
		case JOIN_RIGHT:
			return inner_side;
		default:
			return false;
	}
}

/* Policy chooses semantics, while the walker owns targetlist remapping and
 * child selection. A terminus must be a Scan over one range-table entry.
 * Identity-filter nodes must have exactly one custom_plans child. Callbacks
 * used here must not allocate or ERROR. */
typedef struct AnserKeyScanPolicy
{
	bool (*is_terminus)(const Plan *);
	bool (*may_descend)(const Plan *);
	bool (*is_identity_filter)(const Plan *);
	bool (*may_cross_edge)(const Plan *, const Plan *, bool inner_side, void *);
} AnserKeyScanPolicy;

typedef struct AnserKeyScanResult
{
	Plan *scan;
	Plan *parent;
	AttrNumber output_attno;
	AttrNumber table_attno;
} AnserKeyScanResult;

/* Resolve a scan output to a positive base-table attribute. Index-only scans
 * use INDEX_VAR positions into indextlist, not relation attribute numbers. */
static inline Var *
AnserScanOutputVar(const Scan *scan, AttrNumber position)
{
	Var *var = AnserPlanOutputVar(&scan->plan, position);

	if (var == NULL)
		return NULL;
	if (IsA(scan, IndexOnlyScan) && var->varno == INDEX_VAR)
	{
		List *index_tlist = ((const IndexOnlyScan *) scan)->indextlist;
		TargetEntry *tle;

		if (var->varattno < 1 || var->varattno > list_length(index_tlist))
			return NULL;
		tle = (TargetEntry *) list_nth(index_tlist, var->varattno - 1);
		if (tle == NULL || !IsA(tle->expr, Var))
			return NULL;
		var = (Var *) tle->expr;
	}
	if (var->varno <= 0 || (Index) var->varno != scan->scanrelid ||
		var->varattno < 1)
		return NULL;
	return var;
}

static inline bool
AnserFindKeyScan(Plan *top, AttrNumber position, const AnserKeyScanPolicy *policy,
				void *context, AnserKeyScanResult *result)
{
	Plan *node = top;
	Plan *parent = NULL;

	while (node != NULL)
	{
		Var *var;
		Plan *child;
		bool inner_side = false;
		bool terminus = policy->is_terminus(node);

		/* Check the starting node too, not just children: a Result, Limit or
		 * Agg must never become a transparent projection by accident. */
		if (!terminus && !policy->may_descend(node) &&
			!policy->is_identity_filter(node))
			return false;
		var = terminus ? AnserScanOutputVar((Scan *) node, position) :
			AnserPlanOutputVar(node, position);
		if (var == NULL)
			return false;
		if (terminus)
		{
			result->scan = node;
			result->parent = parent;
			result->output_attno = position;
			result->table_attno = var->varattno;
			return true;
		}
		if (var->varno == OUTER_VAR)
			child = outerPlan(node);
		else if (var->varno == INNER_VAR &&
				 (IsA(node, HashJoin) || IsA(node, MergeJoin) || IsA(node, NestLoop)))
		{
			child = innerPlan(node);
			inner_side = true;
		}
		else if (var->varno == INDEX_VAR && policy->is_identity_filter(node))
		{
			List *children = ((CustomScan *) node)->custom_plans;

			if (list_length(children) != 1)
				return false;
			child = (Plan *) linitial(children);
		}
		else
			return false;
		if (child == NULL || (policy->may_cross_edge != NULL &&
			!policy->may_cross_edge(node, child, inner_side, context)))
			return false;
		parent = node;
		node = child;
		position = var->varattno;
	}
	return false;
}

/* Direct plan children only; expressions and stmt->subplans are separate.
 * A callback is invoked once per non-NULL link, in spine-then-list order. */
static inline void
AnserForEachChildPlan(Plan *plan, void (*visit)(Plan *, void *), void *arg)
{
	Plan *left;
	Plan *right;
	List *children = NIL;
	ListCell *lc;

	if (plan == NULL)
		return;
	left = outerPlan(plan);
	right = innerPlan(plan);
	if (left != NULL)
		visit(left, arg);
	if (right != NULL)
		visit(right, arg);
	switch (nodeTag(plan))
	{
		case T_Append:
			children = ((Append *) plan)->appendplans;
			break;
		case T_MergeAppend:
			children = ((MergeAppend *) plan)->mergeplans;
			break;
		case T_Sequence:
			children = ((Sequence *) plan)->subplans;
			break;
		case T_BitmapAnd:
			children = ((BitmapAnd *) plan)->bitmapplans;
			break;
		case T_BitmapOr:
			children = ((BitmapOr *) plan)->bitmapplans;
			break;
		case T_CustomScan:
			children = ((CustomScan *) plan)->custom_plans;
			break;
		case T_SubqueryScan:
			if (((SubqueryScan *) plan)->subplan != NULL)
				visit(((SubqueryScan *) plan)->subplan, arg);
			break;
		default:
			break;
	}
	foreach(lc, children)
		if (lfirst(lc) != NULL)
			visit((Plan *) lfirst(lc), arg);
}

/* Only the links a filter injector can replace. Refuse absent or ambiguous
 * ownership without changing anything; no allocation or PostgreSQL calls. */
static inline bool
AnserReplaceChildPlan(Plan *parent, Plan *child, Plan *replacement)
{
	Plan **link = NULL;
	ListCell *lc;
	int matches = 0;

	if (parent == NULL || child == NULL || replacement == NULL)
		return false;
	if (parent->lefttree == child)
	{
		link = &parent->lefttree;
		++matches;
	}
	if (parent->righttree == child)
	{
		link = &parent->righttree;
		++matches;
	}
	if (IsA(parent, CustomScan))
		foreach(lc, ((CustomScan *) parent)->custom_plans)
			if (lfirst(lc) == child)
			{
				++matches;
				/* List cells hold void *, so assign this link separately. */
			}
	if (matches != 1)
		return false;
	if (link != NULL)
		*link = replacement;
	else
		foreach(lc, ((CustomScan *) parent)->custom_plans)
			if (lfirst(lc) == child)
			{
				lfirst(lc) = replacement;
				break;
			}
	return true;
}

/* Allocate a pass-through targetlist. May ERROR. */
static inline List *
AnserFilterIdentityTlist(Plan *child)
{
	List *tlist = NIL;
	ListCell *lc;
	AttrNumber attno = 0;

	foreach(lc, child->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Var *var = makeVar(INDEX_VAR, ++attno, exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr), exprCollation((Node *) tle->expr), 0);

		tlist = lappend(tlist, makeTargetEntry((Expr *) var, attno,
								 tle->resname ? pstrdup(tle->resname) : NULL, tle->resjunk));
	}
	return tlist;
}

/* Initialize a zeroed CustomScan; caller owns private data and node IDs.
 * Allocates/copies plan fields and can ERROR. Local-only filters must override
 * parallel_safe=false because an in-process registry cannot fold workers. */
static inline void
AnserInitFilterScan(CustomScan *cs, const CustomScanMethods *methods, Plan *child)
{
	cs->scan.plan.targetlist = AnserFilterIdentityTlist(child);
	cs->scan.plan.qual = NIL;
	cs->scan.plan.lefttree = cs->scan.plan.righttree = NULL;
	cs->scan.plan.startup_cost = child->startup_cost;
	cs->scan.plan.total_cost = child->total_cost;
	cs->scan.plan.plan_rows = child->plan_rows;
	cs->scan.plan.plan_width = child->plan_width;
	cs->scan.plan.parallel_aware = false;
	cs->scan.plan.parallel_safe = child->parallel_safe;
	cs->scan.plan.flow = (Flow *) copyObjectImpl(child->flow);
	cs->scan.scanrelid = 0;
	cs->flags = 0;
	cs->custom_plans = list_make1(child);
	cs->custom_exprs = NIL;
	cs->custom_scan_tlist = AnserFilterScanTlist(child);
	cs->methods = methods;
}

/* The parent must handle any slot implementation returned by the child.
 * Freeze the choice at executor initialization, before compiling expressions. */
static inline void
AnserInitFilterChildSlot(CustomScanState *node, bool zero_copy)
{
	PlanState *ps = &node->ss.ps;

	if (zero_copy && ps->ps_ProjInfo == NULL)
	{
		ps->resultopsset = true;
		ps->resultopsfixed = false;
		ps->resultops = NULL;
	}
}

/* Slot copying can allocate/ERROR through the tuple-slot implementation. */
static inline TupleTableSlot *
AnserFilterChildSlot(CustomScanState *node, TupleTableSlot *childslot)
{
	if (node->ss.ps.resultopsset && node->ss.ps.resultops == NULL)
		return childslot;
	ExecCopySlot(node->ss.ss_ScanTupleSlot, childslot);
	return node->ss.ss_ScanTupleSlot;
}

static inline bool
AnserFilterRecheck(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}

#endif /* ANSERPLANUTILS_H */
