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
 * rpteligible.cpp
 *    Backward eligibility proof shared by runtime injection and the survey.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rpteligible.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"
#include "optimizer/walkers.h"
#include "anserplan.h"
}


struct RptPlacement
{
	std::vector<Plan *> path;
	int slice_index = -1;
	bool ambiguous = false;
	bool in_subplan = false;
};

struct RptPlacementContext
{
	plan_tree_base_prefix base;
	std::set<const Plan *> scans;
	std::map<const Plan *, RptPlacement> placements;
	std::vector<Plan *> path;
	int slice_index = 0;
	bool in_subplan = false;
};

/* Expressions are visited too, but a scan's ancestor path contains only
 * plans and plan lists. Omit lists so the previous entry is its relink parent. */
static bool
rpt_placement_walker(Node *node, void *context)
{
	auto *ctx = static_cast<RptPlacementContext *>(context);
	if (node == NULL)
		return false;
	bool list = IsA(node, List) || IsA(node, IntList) || IsA(node, OidList);
	int saved_slice_index = ctx->slice_index;
	if (IsA(node, Motion))
		ctx->slice_index = ((Motion *) node)->motionID;
	if (!list)
		ctx->path.push_back((Plan *) node);
	if (ctx->scans.count((Plan *) node))
	{
		auto &placement = ctx->placements[(Plan *) node];
		placement.in_subplan = placement.in_subplan || ctx->in_subplan;
		if (!placement.path.empty())
			placement.ambiguous = true;
		else
		{
			placement.path = ctx->path;
			placement.slice_index = ctx->slice_index;
		}
	}
	bool result = RptPgCall([&]() {
		return plan_tree_walker(node, rpt_placement_walker, context, false);
	});
	if (!list)
		ctx->path.pop_back();
	ctx->slice_index = saved_slice_index;
	return result;
}

/* Map a base column upward through identity/projection Vars. Do not guess
 * that a relation's physical attno is also its output-tuple position. */
static AttrNumber
rpt_path_key_slot(const RptPlacement &placement, size_t top, const RptKey &key)
{
	Plan *scan = placement.path.back();
	AttrNumber slot = 0;
	ListCell *lc;

	if (!IsA(scan, SeqScan))
		return 0; /* Anser's builders/resolver support SeqScan endpoints. */
	foreach(lc, scan->targetlist)
	{
		auto *tle = (TargetEntry *) lfirst(lc);
		if (IsA(tle->expr, Var))
		{
			auto *var = (Var *) tle->expr;
			if (var->varno == key.rtindex && var->varattno == key.attno)
			{
				slot = tle->resno;
				break;
			}
		}
	}
	for (size_t i = placement.path.size() - 1; slot > 0 && i > top; --i)
	{
		Plan *parent = placement.path[i - 1];
		Plan *child = placement.path[i];
		int varno = 0;
		if (outerPlan(parent) == child)
			varno = OUTER_VAR;
		else if (innerPlan(parent) == child)
			varno = INNER_VAR;
		else if (AnserIsRuntimeFilterScan(parent))
			varno = INDEX_VAR;
		AttrNumber mapped = 0;
		foreach(lc, parent->targetlist)
		{
			auto *tle = (TargetEntry *) lfirst(lc);
			if (IsA(tle->expr, Var))
			{
				auto *var = (Var *) tle->expr;
				if (varno != 0 && var->varno == varno && var->varattno == slot)
				{
					mapped = tle->resno;
					break;
				}
			}
		}
		slot = mapped;
	}
	return slot;
}

static int
rpt_path_slice_index(const RptPlacement &placement, size_t top)
{
	int slice_index = 0;
	for (size_t i = 0; i <= top; ++i)
		if (IsA(placement.path[i], Motion))
			slice_index = ((Motion *) placement.path[i])->motionID;
	return slice_index;
}

static bool
rpt_resolve_placement(const RptPlacement &placement, size_t top,
					  const RptKey &key, bool consumer,
					  Plan **parent, AttrNumber *attno, int *slice_index)
{
	AttrNumber slot = rpt_path_key_slot(placement, top, key);
	if (slot <= 0 || placement.path.size() < 2)
		return false;
	Plan *scan = placement.path.back();
	Plan *resolved = NULL;
	if (top == placement.path.size() - 1)
	{
		*parent = placement.path[top - 1];
		*attno = slot;
		*slice_index = placement.slice_index;
		return outerPlan(*parent) == scan;
	}
	bool ok = RptPgCall([&]() {
		return AnserResolveKeyScan(placement.path[top], slot,
			rpt_path_slice_index(placement, top), consumer, parent, &resolved, attno, slice_index);
	});
	return ok && resolved == scan && *slice_index == placement.slice_index;
}

static const char *
rpt_consumer_path_reason(const RptPlacement &placement, size_t join,
						 bool builds_first)
{
	for (size_t i = join + 1; i + 1 < placement.path.size(); ++i)
	{
		Plan *node = placement.path[i];
		Plan *child = placement.path[i + 1];
		if (IsA(node, HashJoin) || IsA(node, MergeJoin) || IsA(node, NestLoop))
		{
			JoinType type = ((Join *) node)->jointype;
			bool inner = innerPlan(node) == child;
			if ((type == JOIN_LEFT && inner) || (type == JOIN_RIGHT && !inner) || type == JOIN_FULL)
				return "nullable side of outer join";
			if (type != JOIN_INNER && type != JOIN_LEFT && type != JOIN_RIGHT)
				return "unsupported join on consumer path";
		}
	}
	if (builds_first)
		return NULL;
	for (size_t i = join + 1; i + 1 < placement.path.size(); ++i)
	{
		switch (nodeTag(placement.path[i]))
		{
			case T_Sort: return "sort between consumer and join";
			case T_Agg: return "aggregate between consumer and join";
			case T_Material: return "material between consumer and join";
			case T_Unique: return "unique between consumer and join";
			case T_WindowAgg: return "window between consumer and join";
			case T_Hash: return "hash between consumer and join";
			case T_SetOp: return "set operation between consumer and join";
			case T_HashJoin:
			case T_MergeJoin:
			case T_NestLoop: return "join between consumer and join";
			default: break;
		}
	}
	return NULL;
}

/* Anser retains its publication across rescans. Any external parameter,
 * including SubPlan correlation and InitPlan parameters, is conservatively
 * refused until the runtime has a generation/rebuild protocol. */
static bool
rpt_parameterized_producer(Plan *build)
{
	return !RptPgCall([&]() { return bms_is_empty(build->extParam); });
}

static void
rpt_prove_consumer(RptBackwardConsumer *decision, const RptKey &producer_key,
				   const RptKey &consumer_key, const RptPlacement &producer,
				   const RptPlacement &consumer)
{
	decision->reason = "producer is not on a qualifying hash build side";
	if (producer.in_subplan || consumer.in_subplan)
	{
		decision->reason = "scan inside a SubPlan";
		return;
	}
	if (producer.ambiguous || consumer.ambiguous)
	{
		decision->reason = "scan has multiple plan placements";
		return;
	}
	if (producer.slice_index < 0 || consumer.slice_index < 0 || producer.slice_index == consumer.slice_index)
	{
		decision->reason = "same or unknown slice";
		return;
	}
	for (size_t i = 0; i + 1 < consumer.path.size(); ++i)
	{
		Plan *node = consumer.path[i];
		if (!IsA(node, HashJoin) || consumer.path[i + 1] != outerPlan(node))
			continue;
		auto *join = (HashJoin *) node;
		auto build = std::find(producer.path.begin(), producer.path.end(), innerPlan(node));
		if (build == producer.path.end())
			continue;
		decision->rule9 = true;
		if (join->join.jointype != JOIN_INNER && join->join.jointype != JOIN_RIGHT)
		{
			decision->reason = "unsupported qualifying hash join";
			continue;
		}
		bool builds_first = join->join.prefetch_inner || join->join.jointype == JOIN_RIGHT;
		const char *reason = rpt_consumer_path_reason(consumer, i, builds_first);
		if (reason != NULL)
		{
			decision->reason = reason;
			continue;
		}
		if (rpt_parameterized_producer(innerPlan(node)))
		{
			decision->reason = "parameterized producer can be rescanned";
			continue;
		}
		if (!rpt_resolve_placement(producer, build - producer.path.begin(), producer_key,
								 false, &decision->producer_parent,
								 &decision->producer_attno, &decision->producer_slice_index))
		{
			decision->reason = "unsupported producer key path";
			continue;
		}
		if (!rpt_resolve_placement(consumer, i + 1, consumer_key, true,
								 &decision->consumer_parent, &decision->consumer_attno,
								 &decision->consumer_slice_index))
		{
			decision->reason = "unsupported consumer key path";
			continue;
		}
		decision->defer_first = !builds_first;
		decision->strict_eligible = decision->eligible = true;
		decision->reason = "eligible";
		return;
	}
}

static void
rpt_cap_backward_consumers(RptTransferMgr *mgr)
{
	std::map<RptKey, std::vector<size_t> > classes;
	for (size_t i = 0; i < mgr->backward_consumers.size(); ++i)
		if (mgr->backward_consumers[i].strict_eligible)
			classes[mgr->backward_consumers[i].class_key].push_back(i);
	for (auto &group : classes)
	{
		auto &indices = group.second;
		std::stable_sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
			const auto &left = mgr->backward_consumers[a];
			const auto &right = mgr->backward_consumers[b];
			double l = left.estimated_rows_removed >= 0 ? left.estimated_rows_removed :
				mgr->vertices[mgr->vertex_index_by_rtindex.at(left.consumer_rtindex)].cardinality;
			double r = right.estimated_rows_removed >= 0 ? right.estimated_rows_removed :
				mgr->vertices[mgr->vertex_index_by_rtindex.at(right.consumer_rtindex)].cardinality;
			return l > r || (l == r && left.consumer_rtindex < right.consumer_rtindex);
		});
		for (size_t i = rpt_max_backward_consumers; i < indices.size(); ++i)
		{
			auto &decision = mgr->backward_consumers[indices[i]];
			decision.eligible = false;
			decision.reason = "backward consumer cap exceeded";
		}
	}
}

static void
rpt_consumer_verdicts(RptTransferMgr *mgr)
{
	for (const auto &vertex : mgr->vertices)
		mgr->consumer_ok_by_rtindex[vertex.rtindex] = false;
	std::map<int, bool> all_global;
	for (const auto &decision : mgr->backward_consumers)
	{
		if (decision.local)
			continue;
		auto found = all_global.find(decision.consumer_rtindex);
		if (found == all_global.end())
			all_global[decision.consumer_rtindex] = decision.eligible;
		else
			found->second = found->second && decision.eligible;
	}
	for (const auto &entry : all_global)
		mgr->consumer_ok_by_rtindex[entry.first] = entry.second;
}

void
RptCheckBackwardConsumers(PlannedStmt *stmt, RptTransferMgr *mgr)
{
	RptPlacementContext context;
	for (const auto &vertex : mgr->vertices)
	{
		mgr->consumer_ok_by_rtindex[vertex.rtindex] = false;
		context.scans.insert(vertex.scan);
	}
	mgr->backward_consumers.clear();
	if (stmt == NULL || stmt->planTree == NULL)
		return;
	RptPgCall([&]() { exec_init_plan_tree_base(&context.base, stmt); });
	rpt_placement_walker((Node *) stmt->planTree, &context);
	ListCell *lc;
	foreach(lc, stmt->subplans)
	{
		context.slice_index = -1;
		context.in_subplan = true; /* Motions must not clear the SubPlan refusal. */
		rpt_placement_walker((Node *) lfirst(lc), &context);
	}
	std::set<std::pair<int, RptKey> > seen;
	for (const auto &node : mgr->graph)
	{
		for (const auto &edge : node.second.backward_stage.out)
		{
			for (size_t key = 0; key < edge.left_keys.size(); ++key)
			{
				RptKey source = edge.right_keys[key];
				RptKey target = edge.left_keys[key];
				RptKey rep = RptFindKeyRoot(source, mgr->uf_parent);
				if (!seen.insert(std::make_pair(edge.other_rtindex, rep)).second)
					continue;
				RptBackwardConsumer decision;
				decision.class_key = rep;
				decision.producer_key = source;
				decision.consumer_key = target;
				decision.producer_rtindex = node.first;
				decision.consumer_rtindex = edge.other_rtindex;
				decision.local = edge.local;
				auto group = mgr->groups.find(rep);
				auto source_placement = context.placements.find(mgr->vertices[mgr->vertex_index_by_rtindex.at(node.first)].scan);
				auto target_placement = context.placements.find(mgr->vertices[mgr->vertex_index_by_rtindex.at(edge.other_rtindex)].scan);
				if ((source_placement != context.placements.end() && source_placement->second.in_subplan) ||
					(target_placement != context.placements.end() && target_placement->second.in_subplan))
					decision.reason = "scan inside a SubPlan";
				else if (edge.local)
					decision.reason = "local edge uses core runtime filtering";
				else if (group == mgr->groups.end() || group->second.leader_rtindex != node.first)
					decision.reason = "edge has no unprotected equivalence class";
				else if (!mgr->root_rtindexes.count(node.first))
					decision.reason = "bridge-led class is deferred";
				else if (!context.placements.count(mgr->vertices[mgr->vertex_index_by_rtindex.at(node.first)].scan) ||
						 !context.placements.count(mgr->vertices[mgr->vertex_index_by_rtindex.at(edge.other_rtindex)].scan))
					decision.reason = "scan placement not found";
				else
				{
					const auto &producer = mgr->vertices[mgr->vertex_index_by_rtindex.at(node.first)];
					const auto &consumer = mgr->vertices[mgr->vertex_index_by_rtindex.at(edge.other_rtindex)];
					rpt_prove_consumer(&decision, source, target,
						context.placements.at(producer.scan), context.placements.at(consumer.scan));
					if (decision.strict_eligible)
					{
						double root_ndv = RptEstimateDistinct(producer, source.attno);
						double member_ndv = RptEstimateDistinct(consumer, target.attno);
						if (root_ndv > 0 && member_ndv > 0)
						{
							double root_rows = producer.cardinality *
								(producer.replicated ? 1.0 : std::max(1, producer.numsegments));
							root_ndv = std::min(root_ndv, root_rows);
							decision.estimated_rows_removed = consumer.cardinality *
								(1.0 - std::min(1.0, root_ndv / member_ndv));
						}
					}
				}
				mgr->backward_consumers.push_back(decision);
			}
		}
	}
	rpt_cap_backward_consumers(mgr);
	rpt_consumer_verdicts(mgr);
}
