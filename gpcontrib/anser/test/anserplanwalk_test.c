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

static Plan *identity_filter;
bool AnserIsRuntimeFilterScan(const Plan *plan) { return plan == identity_filter; }

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
	return 0;
}
