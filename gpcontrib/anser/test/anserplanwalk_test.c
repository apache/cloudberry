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
 * anserplanwalk_test.c
 *    Linear maximum-ID traversal, including child lists and SubPlans.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/anserplanwalk_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/plannodes.h"
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) abort(); } while (0)
#define DEPTH 32

static int visits;
static bool count_visits = true;
static Plan **
counted_outer_plan(Plan *plan)
{
	/* Count actual production walker visits; fail fast on repeated recursion. */
	if (count_visits) CHECK(++visits <= DEPTH * 2);
	return &plan->lefttree;
}
#undef outerPlan
#define outerPlan(p) (*counted_outer_plan((Plan *) (p)))
#include "../src/anserplan.c"
#include "../src/anserplankeys.c"
#include "../src/anserinplace.c"

static Plan *identity_filter;
bool AnserIsRuntimeFilterScan(const Plan *plan) { return plan == identity_filter; }
bool AnserIsFilterScan(const Plan *plan) { return plan == identity_filter; }

static const Plan *test_consumer;
AttrNumber AnserBloomConsumerKey(const Plan *plan)
{
	return plan == test_consumer ? 1 : InvalidAttrNumber;
}

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }

typedef union TestNode
{
	Plan plan;
	Append append;
	MergeAppend merge;
	Sequence sequence;
	BitmapAnd bitmap_and;
	BitmapOr bitmap_or;
	SubqueryScan subquery;
	CustomScan custom;
} TestNode;

static void
one_shape(NodeTag tag, bool inner)
{
	TestNode nodes[DEPTH * 2] = {0};
	List children[DEPTH * 2] = {0};
	ListCell cells[DEPTH * 2] = {0};
	PlannedStmt stmt = {0};
	List subplans = {0};
	ListCell subplan_cell = {0};
	int i;

	for (i = 0; i < DEPTH * 2; ++i)
	{
		Plan *plan = &nodes[i].plan;

		plan->type = tag;
		/* Descendants have larger IDs: this triggers Max's double evaluation. */
		plan->plan_node_id = i + 1;
		if (i == DEPTH - 1 || i == DEPTH * 2 - 1)
			continue;
		cells[i].ptr_value = &nodes[i + 1].plan;
		children[i].type = T_List;
		children[i].length = children[i].max_length = 1;
		children[i].elements = &cells[i];
		switch (tag)
		{
			case T_Append: nodes[i].append.appendplans = &children[i]; break;
			case T_MergeAppend: nodes[i].merge.mergeplans = &children[i]; break;
			case T_Sequence: nodes[i].sequence.subplans = &children[i]; break;
			case T_BitmapAnd: nodes[i].bitmap_and.bitmapplans = &children[i]; break;
			case T_BitmapOr: nodes[i].bitmap_or.bitmapplans = &children[i]; break;
			case T_CustomScan: nodes[i].custom.custom_plans = &children[i]; break;
			case T_SubqueryScan: nodes[i].subquery.subplan = &nodes[i + 1].plan; break;
			default:
				if (inner) plan->righttree = &nodes[i + 1].plan;
				else plan->lefttree = &nodes[i + 1].plan;
				break;
		}
	}
	stmt.planTree = &nodes[0].plan;
	subplan_cell.ptr_value = &nodes[DEPTH].plan;
	subplans.type = T_List;
	subplans.length = subplans.max_length = 1;
	subplans.elements = &subplan_cell;
	stmt.subplans = &subplans;
	visits = 0;
	CHECK(AnserMaxPlanNodeId(&stmt) == DEPTH * 2);
	CHECK(visits == DEPTH * 2);
}

static void
key_descent(void)
{
	SeqScan scan = {0};
	Hash hash = {0};
	Motion motion = {0};
	HashJoin join = {0};
	CustomScan filter = {0};
	Var vars[4] = {0};
	TargetEntry entries[4] = {0};
	List tlists[4] = {0};
	ListCell cells[4] = {0};
	List child_list = {0};
	ListCell child_cell = {0};
	Plan *parent;
	Plan *resolved;
	AttrNumber output;
	int slice;
	int i;

	count_visits = false;
	for (i = 0; i < 4; ++i)
	{
		vars[i].xpr.type = T_Var;
		vars[i].varno = OUTER_VAR;
		vars[i].varattno = 1;
		entries[i].expr = (Expr *) &vars[i];
		cells[i].ptr_value = &entries[i];
		tlists[i].type = T_List;
		tlists[i].length = tlists[i].max_length = 1;
		tlists[i].elements = &cells[i];
	}
	scan.scan.plan.type = T_SeqScan;
	scan.scan.scanrelid = 9;
	scan.scan.plan.targetlist = &tlists[0];
	vars[0].varno = 9;
	vars[0].varattno = 3; /* table attno differs from output position */
	hash.plan.type = T_Hash;
	hash.plan.targetlist = &tlists[1];
	hash.plan.lefttree = &motion.plan;
	motion.plan.type = T_Motion;
	motion.plan.targetlist = &tlists[2];
	motion.plan.lefttree = &scan.scan.plan;
	motion.motionID = 7;
	CHECK(AnserResolveKeyScan(&hash.plan, 1, 2, true, &parent, &resolved, &output, &slice));
	CHECK(parent == &motion.plan && resolved == &scan.scan.plan && output == 1 && slice == 7);
	CHECK(!AnserResolveKeyScan(&hash.plan, 2, 2, true, &parent, &resolved, &output, &slice));
	/* Traversing a join's nullable side is unsafe for a consumer, whereas
	 * a producer's base-column superset remains valid. */
	join.join.plan.type = T_HashJoin;
	join.join.plan.targetlist = &tlists[3];
	join.join.plan.righttree = &hash.plan;
	join.join.jointype = JOIN_LEFT;
	vars[3].varno = INNER_VAR;
	CHECK(!AnserResolveKeyScan(&join.join.plan, 1, 2, true, &parent, &resolved, &output, &slice));
	CHECK(AnserResolveKeyScan(&join.join.plan, 1, 2, false, &parent, &resolved, &output, &slice));
	join.join.jointype = JOIN_RIGHT;
	CHECK(AnserResolveKeyScan(&join.join.plan, 1, 2, true, &parent, &resolved, &output, &slice));
	/* Pruning a semi join's inner side can change which inner row an outer
	 * row matches, so a consumer must not land there; a producer may. */
	join.join.jointype = JOIN_SEMI;
	CHECK(!AnserResolveKeyScan(&join.join.plan, 1, 2, true, &parent, &resolved, &output, &slice));
	CHECK(AnserResolveKeyScan(&join.join.plan, 1, 2, false, &parent, &resolved, &output, &slice));
	/* Identity-filter traversal requires exactly one child. */
	filter.scan.plan.type = T_CustomScan;
	filter.scan.plan.targetlist = &tlists[3];
	vars[3].varno = INDEX_VAR;
	identity_filter = &filter.scan.plan;
	CHECK(!AnserResolveKeyScan(identity_filter, 1, 2, true, &parent, &resolved, &output, &slice));
	child_list.type = T_List;
	child_list.length = child_list.max_length = 1;
	child_list.elements = &child_cell;
	child_cell.ptr_value = &scan.scan.plan;
	filter.custom_plans = &child_list;
	CHECK(AnserResolveKeyScan(identity_filter, 1, 2, true, &parent, &resolved, &output, &slice));
	CHECK(parent == identity_filter && output == 1);
	vars[0].varno = 8;
	CHECK(!AnserResolveKeyScan(identity_filter, 1, 2, true, &parent, &resolved, &output, &slice));
}

/* A one-entry targetlist holding "var", in caller storage. */
static List *
one_entry_tlist(Var *var, TargetEntry *entry, ListCell *cell, List *list)
{
	var->xpr.type = T_Var;
	entry->expr = (Expr *) var;
	entry->resno = 1;
	cell->ptr_value = entry;
	list->type = T_List;
	list->length = list->max_length = 1;
	list->elements = cell;
	return list;
}

/*
 * anser_inplace_has_target: down the outer side only, through HashJoin,
 * Result, Append and our own nodes, to a SeqScan -- never through a Motion.
 */
static void
inplace_target(void)
{
	SeqScan		scan = {0};
	SeqScan		scan2 = {0};
	HashJoin	join = {0};
	HashJoin	lower = {0};
	Motion		motion = {0};
	CustomScan	filter = {0};
	Append		append = {0};
	Var			vars[6] = {0};
	TargetEntry entries[6] = {0};
	ListCell	cells[6] = {{0}};
	List		tlists[6] = {{0}};
	List		children = {0};
	ListCell	child_cells[2] = {{0}};

	count_visits = false;
	scan.scan.plan.type = T_SeqScan;
	scan.scan.scanrelid = 1;
	scan.scan.plan.targetlist = one_entry_tlist(&vars[0], &entries[0], &cells[0], &tlists[0]);
	vars[0].varno = 1;
	vars[0].varattno = 3;
	scan2 = scan;
	join.join.plan.type = T_HashJoin;

	/* HashJoin over the scan. */
	join.join.plan.lefttree = &scan.scan.plan;
	CHECK(anser_inplace_has_target(&join, 1, false));
	CHECK(!anser_inplace_has_target(&join, 2, false));
	vars[0].varattno = 0;			/* a system column */
	CHECK(!anser_inplace_has_target(&join, 1, false));
	vars[0].varattno = 3;

	/* A Motion between the scan and the join: the filter must be exchanged. */
	motion.plan.type = T_Motion;
	motion.plan.targetlist = one_entry_tlist(&vars[1], &entries[1], &cells[1], &tlists[1]);
	vars[1].varno = OUTER_VAR;
	vars[1].varattno = 1;
	motion.plan.lefttree = &scan.scan.plan;
	join.join.plan.lefttree = &motion.plan;
	CHECK(!anser_inplace_has_target(&join, 1, false));

	/* Through a lower join's outer side, not its inner side. */
	lower.join.plan.type = T_HashJoin;
	lower.join.plan.targetlist = one_entry_tlist(&vars[2], &entries[2], &cells[2], &tlists[2]);
	lower.join.plan.lefttree = &scan.scan.plan;
	vars[2].varno = OUTER_VAR;
	vars[2].varattno = 1;
	join.join.plan.lefttree = &lower.join.plan;
	CHECK(anser_inplace_has_target(&join, 1, false));
	vars[2].varno = INNER_VAR;
	CHECK(!anser_inplace_has_target(&join, 1, false));
	vars[2].varno = OUTER_VAR;
	/* Not below the nullable outer side of a right or full join. */
	lower.join.jointype = JOIN_RIGHT;
	CHECK(!anser_inplace_has_target(&join, 1, false));
	lower.join.jointype = JOIN_FULL;
	CHECK(!anser_inplace_has_target(&join, 1, false));
	lower.join.jointype = JOIN_LEFT;
	CHECK(anser_inplace_has_target(&join, 1, false));
	lower.join.jointype = JOIN_INNER;

	/* Through one of our own nodes, and only when it says it only filters. */
	filter.scan.plan.type = T_CustomScan;
	filter.scan.plan.targetlist = one_entry_tlist(&vars[3], &entries[3], &cells[3], &tlists[3]);
	vars[3].varno = INDEX_VAR;
	vars[3].varattno = 1;
	child_cells[0].ptr_value = &scan.scan.plan;
	children.type = T_List;
	children.length = children.max_length = 1;
	children.elements = child_cells;
	filter.custom_plans = &children;
	lower.join.plan.lefttree = &filter.scan.plan;
	identity_filter = NULL;			/* some other CustomScan */
	CHECK(!anser_inplace_has_target(&join, 1, false));
	identity_filter = &filter.scan.plan;	/* an identity filter */
	CHECK(anser_inplace_has_target(&join, 1, false));
	test_consumer = &filter.scan.plan;	/* a consumer on the same key */
	CHECK(!anser_inplace_has_target(&join, 1, true));
	test_consumer = NULL;
	CHECK(anser_inplace_has_target(&join, 1, true));
	vars[3].varno = OUTER_VAR;		/* not the identity tlist */
	CHECK(!anser_inplace_has_target(&join, 1, false));
	vars[3].varno = INDEX_VAR;

	/* An Append whose children are all scans. */
	append.plan.type = T_Append;
	append.plan.targetlist = one_entry_tlist(&vars[4], &entries[4], &cells[4], &tlists[4]);
	vars[4].varno = OUTER_VAR;
	vars[4].varattno = 1;
	child_cells[0].ptr_value = &scan.scan.plan;
	child_cells[1].ptr_value = &scan2.scan.plan;
	children.length = children.max_length = 2;
	append.appendplans = &children;
	join.join.plan.lefttree = &append.plan;
	CHECK(anser_inplace_has_target(&join, 1, false));
	child_cells[1].ptr_value = &motion.plan;
	CHECK(!anser_inplace_has_target(&join, 1, false));
}

/* Fixed-capacity lists suffice for the two probe scans in this fixture. */
static List probe_scans;
static List probe_attnos;
static ListCell probe_scan_cells[2];
static ListCell probe_attno_cells[2];

List *
lappend(List *list, void *datum)
{
	if (list == NIL)
	{
		probe_scans = (List) {T_List, 0, 2, probe_scan_cells};
		list = &probe_scans;
	}
	CHECK(list == &probe_scans && list->length < list->max_length);
	list->elements[list->length++].ptr_value = datum;
	return list;
}

List *
lappend_int(List *list, int datum)
{
	if (list == NIL)
	{
		probe_attnos = (List) {T_IntList, 0, 2, probe_attno_cells};
		list = &probe_attnos;
	}
	CHECK(list == &probe_attnos && list->length < list->max_length);
	list->elements[list->length++].int_value = datum;
	return list;
}

static void
inplace_scan_states(void)
{
	HashJoinState join = {0};
	HashJoinState lower = {0};
	HashJoin lower_plan = {0};
	Append append = {0};
	AppendState append_state = {0};
	SeqScan scans[2] = {0};
	SeqScanState states[2] = {0};
	PlanState *children[] = {&states[0].ss.ps, &states[1].ss.ps};
	Var vars[4] = {0};
	TargetEntry entries[4] = {0};
	ListCell cells[4] = {{0}};
	List tlists[4] = {0};
	List *targets;
	List *attnos;
	int i;

	for (i = 0; i < 2; i++)
	{
		states[i].ss.ps.type = T_SeqScanState;
		states[i].ss.ps.plan = &scans[i].scan.plan;
		scans[i].scan.plan.type = T_SeqScan;
		scans[i].scan.scanrelid = i + 1;
		vars[i].varno = i + 1;
		vars[i].varattno = i + 2; /* output position is 1 in both children */
		scans[i].scan.plan.targetlist =
			one_entry_tlist(&vars[i], &entries[i], &cells[i], &tlists[i]);
	}
	append_state.ps.type = T_AppendState;
	append_state.ps.plan = &append.plan;
	append_state.appendplans = children;
	append_state.as_nplans = 2;
	vars[2].varno = OUTER_VAR;
	vars[2].varattno = 1;
	append.plan.targetlist = one_entry_tlist(&vars[2], &entries[2], &cells[2], &tlists[2]);
	outerPlanState(&join.js.ps) = &append_state.ps;

	CHECK(anser_inplace_find_scans(&join, 1, &targets, &attnos));
	CHECK(list_length(targets) == 2 && list_length(attnos) == 2);
	CHECK(linitial(targets) == children[0] && lsecond(targets) == children[1]);
	CHECK(linitial_int(attnos) == 1 && lsecond_int(attnos) == 1);
	children[1] = NULL;
	CHECK(!anser_inplace_find_scans(&join, 1, &targets, &attnos));
	children[1] = &states[1].ss.ps;

	lower.js.ps.type = T_HashJoinState;
	lower.js.ps.plan = &lower_plan.join.plan;
	outerPlanState(&lower.js.ps) = &append_state.ps;
	outerPlanState(&join.js.ps) = &lower.js.ps;
	vars[3].varno = OUTER_VAR;
	vars[3].varattno = 1;
	lower_plan.join.plan.targetlist =
		one_entry_tlist(&vars[3], &entries[3], &cells[3], &tlists[3]);
	lower_plan.join.jointype = JOIN_LEFT;
	CHECK(anser_inplace_find_scans(&join, 1, &targets, &attnos));
	lower_plan.join.jointype = JOIN_SEMI;
	CHECK(anser_inplace_find_scans(&join, 1, &targets, &attnos));
	vars[3].varno = INNER_VAR;
	CHECK(!anser_inplace_find_scans(&join, 1, &targets, &attnos));
	vars[3].varno = OUTER_VAR;
	lower_plan.join.jointype = JOIN_RIGHT;
	CHECK(!anser_inplace_find_scans(&join, 1, &targets, &attnos));

	CHECK(AnserJoinSideReducible(JOIN_RIGHT, false));
	CHECK(!AnserJoinSideReducible(JOIN_RIGHT, true));
	CHECK(AnserJoinSideReducible(JOIN_SEMI, false));
}

int main(void)
{
	NodeTag tags[] = {T_Result, T_Append, T_MergeAppend, T_Sequence,
		T_BitmapAnd, T_BitmapOr, T_SubqueryScan, T_CustomScan};
	PlannedStmt empty = {0};
	int i;

	CHECK(AnserMaxPlanNodeId(&empty) == 0);
	for (i = 0; i < lengthof(tags); ++i)
		one_shape(tags[i], false);
	one_shape(T_Result, true);
	key_descent();
	inplace_target();
	inplace_scan_states();
	return 0;
}
