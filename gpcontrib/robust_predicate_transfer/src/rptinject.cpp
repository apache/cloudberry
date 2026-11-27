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
 * rptinject.cpp
 *    Finished-plan construction of the distributed forward InitPlan.
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"
#include "catalog/pg_type.h"
#include "cdb/cdbllize.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
}


static Result *
rpt_make_empty_result(Flow *flow, int plan_node_id)
{
	/* Result's constant qual is an implicit-AND list. */
	Result *empty = make_result(NIL, (Node *) list_make1(makeBoolConst(false, false)),
								NULL);

	empty->plan.plan_node_id = plan_node_id;
	empty->plan.flow = flow;
	return empty;
}

static Sequence *
rpt_make_reducer_sequence(List *children, Result *empty, int plan_node_id)
{
	Sequence *sequence = makeNode(Sequence);

	sequence->plan.plan_node_id = plan_node_id;
	sequence->plan.flow = empty->plan.flow;
	/* Sequence drains the producers and emits only the empty Result's output. */
	sequence->subplans = lappend(list_copy(children), empty);
	return sequence;
}

static Motion *
rpt_make_reducer_gather(Sequence *sequence, int sender_slice, int plan_node_id)
{
	Motion *gather = makeNode(Motion);
	Flow *flow = makeFlow(FLOW_SINGLETON, 1);

	flow->locustype = CdbLocusType_Entry;
	flow->segindex = -1;
	gather->plan.plan_node_id = plan_node_id;
	gather->plan.lefttree = &sequence->plan;
	gather->plan.flow = flow;
	gather->motionType = MOTIONTYPE_GATHER;
	gather->motionID = sender_slice;
	return gather;
}

static void
rpt_append_reducer_slices(PlannedStmt *stmt, int width)
{
	int root_slice = stmt->numSlices;
	int sender_slice = root_slice + 1;
	PlanSlice *slices = (PlanSlice *) palloc0(sizeof(PlanSlice) * (sender_slice + 1));

	memcpy(slices, stmt->slices, sizeof(PlanSlice) * stmt->numSlices);
	slices[root_slice].sliceIndex = root_slice;
	slices[root_slice].parentIndex = -1;
	slices[root_slice].gangType = GANGTYPE_UNALLOCATED;
	slices[root_slice].segindex = -1;
	slices[sender_slice].sliceIndex = sender_slice;
	slices[sender_slice].parentIndex = root_slice;
	slices[sender_slice].gangType = GANGTYPE_PRIMARY_READER;
	slices[sender_slice].segindex = 0;
	slices[sender_slice].numsegments = width;
	stmt->slices = slices;
	stmt->numSlices += 2;
}

static void
rpt_register_reducer_subplan(PlannedStmt *stmt, Motion *gather, int root_slice)
{
	int old_subplans = list_length(stmt->subplans);
	int param_id = list_length(stmt->paramExecTypes);
	int *slice_ids = (int *) palloc(sizeof(int) * (old_subplans + 1));
	SubPlan *subplan = makeNode(SubPlan);

	if (old_subplans)
		memcpy(slice_ids, stmt->subplan_sliceIds, sizeof(int) * old_subplans);
	slice_ids[old_subplans] = root_slice;

	/* EXISTS runs to EOF because the Gather receives no tuples. */
	subplan->subLinkType = EXISTS_SUBLINK;
	subplan->plan_id = old_subplans + 1;
	subplan->plan_name = psprintf("InitPlan %d", subplan->plan_id);
	subplan->firstColType = VOIDOID;
	subplan->firstColTypmod = -1;
	subplan->is_initplan = true;
	subplan->setParam = list_make1_int(param_id);
	stmt->subplans = lappend(stmt->subplans, gather);
	stmt->paramExecTypes = lappend_oid(stmt->paramExecTypes, BOOLOID);
	stmt->planTree->initPlan = lappend(stmt->planTree->initPlan, subplan);
	stmt->subplan_sliceIds = slice_ids;
}

/* The caller publishes its private statement only after injection succeeds.
 * Children are non-parallel reducer producers that return NULL at EOF. */
void
RptAttachReducerInitPlan(PlannedStmt *stmt, List *children, int width,
						int next_plan_node_id)
{
	RptPgCall([&]() {
		if (children == NIL || !stmt->planTree || width <= 0 ||
			stmt->numSlices <= 0 || !stmt->slices ||
			(stmt->subplans != NIL && !stmt->subplan_sliceIds))
			elog(ERROR, "invalid RPT reducer InitPlan input");

		int root_slice = stmt->numSlices;
		Flow *flow = makeFlow(FLOW_PARTITIONED, width);
		flow->locustype = CdbLocusType_Strewn;

		Result *empty = rpt_make_empty_result(flow, next_plan_node_id++);
		Sequence *sequence = rpt_make_reducer_sequence(children, empty, next_plan_node_id++);
		Motion *gather = rpt_make_reducer_gather(sequence, root_slice + 1, next_plan_node_id);

		rpt_append_reducer_slices(stmt, width);
		rpt_register_reducer_subplan(stmt, gather, root_slice);
	});
}
