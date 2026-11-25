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
 * forward_walk_test.cpp
 *    SubPlan context tests using the real backend plan walker.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/test/forward_walk_test.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "../src/rptforward.cpp"
#undef fprintf
#define CHECK(c) do { if (!(c)) { fprintf(stderr, "check failed: %s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

static Plan *anser_node;
static Plan *local_node;
extern "C" {
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
int dispatch_nest_level = 0;
void ExceptionalCondition(const char *, const char *, int) { abort(); }
void check_stack_depth(void) {}
bool AnserIsRuntimeFilterScan(const Plan *p) { return p == anser_node; }
bool RptIsLocalScan(const Plan *p) { return p == local_node; }
bool is_plan_node(Node *p)
{
	return IsA(p, Result) || IsA(p, SeqScan) || IsA(p, CustomScan);
}
bool expression_tree_walker_impl(Node *node, tree_walker_callback walker, void *ctx)
{
	if (!node) return false;
	CHECK(IsA(node, List));
	ListCell *lc;
	foreach(lc, (List *) node)
		if (walker((Node *) lfirst(lc), ctx)) return true;
	return false;
}
bool query_tree_walker_impl(Query *, tree_walker_callback, void *, int) { abort(); }
}

int main()
{
	PlannedStmt stmt = {};
	stmt.type = T_PlannedStmt;
	Result root = {}, subroot = {};
	root.plan.type = subroot.plan.type = T_Result;
	stmt.planTree = &root.plan;
	ListCell plan_cell = {}, expression_cell = {};
	List plans = {}, expressions = {};
	plans.type = expressions.type = T_List;
	plans.length = plans.max_length = expressions.length = expressions.max_length = 1;
	plans.elements = &plan_cell; expressions.elements = &expression_cell;
	plan_cell.ptr_value = &subroot;
	stmt.subplans = &plans;
	SubPlan subplan = {};
	subplan.xpr.type = T_SubPlan;
	subplan.plan_id = 1;
	expression_cell.ptr_value = &subplan;

	/* Resolving an existing InitPlan must not assert or miss its children. */
	root.plan.initPlan = &expressions;
	CHECK(!rpt_has_runtime_filters(&stmt));
	CustomScan marker = {};
	marker.scan.plan.type = T_CustomScan;
	subroot.plan.lefttree = &marker.scan.plan;
	anser_node = &marker.scan.plan;
	CHECK(rpt_has_runtime_filters(&stmt));
	anser_node = NULL;
	local_node = &marker.scan.plan;
	CHECK(rpt_has_runtime_filters(&stmt));
	local_node = NULL;

	/* Correlated/expression SubPlans use the same lookup context. */
	root.plan.initPlan = NIL;
	root.plan.qual = &expressions;
	CHECK(!rpt_has_runtime_filters(&stmt));
	anser_node = &marker.scan.plan;
	CHECK(rpt_has_runtime_filters(&stmt));
	/* The parent walk shares the statement lookup context and preserves
	 * ambiguous placements as refusals rather than choosing the last one. */
	std::set<Plan *> targets = {&marker.scan.plan};
	std::map<Plan *, int> slices;
	auto parents = rpt_find_parents(&stmt, targets, &slices);
	CHECK(parents.empty()); /* SubPlan lookup does not enter its plan. */
	root.plan.lefttree = &marker.scan.plan;
	parents = rpt_find_parents(&stmt, targets, &slices);
	CHECK(parents.at(&marker.scan.plan) == &root.plan);
	root.plan.righttree = &marker.scan.plan;
	parents = rpt_find_parents(&stmt, targets, &slices);
	CHECK(parents.at(&marker.scan.plan) == NULL);

	/* A main-plan fan-out must have one producer's channel ID and seed. */
	std::vector<RptForwardComponent> comps(1);
	auto &comp = comps.front();
	comp.accepted = true;
	comp.root_rtindex = 1;
	RptForwardChannel root_channel;
	root_channel.source_rtindex = 3;
	root_channel.source_key = {3, 1};
	root_channel.target_rtindex = 1;
	RptForwardChannel subscriber = root_channel;
	subscriber.target_rtindex = 2;
	subscriber.main_only = true;
	comp.channels = {root_channel, subscriber};
	RptInjectionState injection;
	rpt_assign_forward_channels(comps, injection);
	CHECK(injection.next_condition_id == 1);
	CHECK(comp.channels[0].condition_id == comp.channels[1].condition_id);
	CHECK(comp.channels[0].condition_key == comp.channels[1].condition_key);
	CHECK(PG_exception_stack == NULL && error_context_stack == NULL);
	return 0;
}
