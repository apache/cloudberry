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
 * rptforward.cpp
 *	  Build the forward reducer as an InitPlan of the statement
 *	  and apply its filters to the root scans of the main plan, then add
 *	  backward filters through the shared injection entry point.
 *
 * A transfer is an Anser channel, except a local hop consumed only inside
 * the reducer, which is handed over in-process (rptlocal.c).  The reducer
 * InitPlan is dispatched by
 * preprocess_initplans() before the main plan, under the same
 * gp_command_count, and the coordinator keeps a command's channels until
 * ExecutorEnd -- so every forward channel is complete before a main-plan consumer
 * subscribes. Backward channels are produced in the main plan and use the
 * survey proof to place consumers that can safely wait.
 *
 * Reducer scans are copies of the main plan's scans (same scanrelid, so the
 * same RTE, locks and permission checks) with a key-only targetlist.  Their
 * row set must be the main scan's or a superset: the quals
 * are copied whole, in order, or dropped whole unless every one of them is
 * proven unable to fail.  The reducer runs eagerly, over rows the main plan
 * may never visit (LIMIT 0, an empty outer side), so a qual that can raise an
 * error there -- 100 / a with a = 0 -- would turn a successful query into a
 * failing one, and the planner's fail-open cannot catch an executor error.
 *
 * Everything runs on a private copy of the statement; the caller publishes
 * it only when the whole pass succeeds.  PostgreSQL calls are made through
 * RptPgCall, because C++ containers are alive throughout.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptforward.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"
#include "rptchannel.h"

extern "C"
{
#include "postgres.h"

#include "cdb/cdbllize.h"
#include "cdb/cdbutil.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/walkers.h"
#include "utils/lsyscache.h"

#include "catalog/pg_class.h"
#include "utils/syscache.h"

#include "anser.h"
#include "anserplan.h"
#include "anserplanutils.h"
#include "rptlocal.h"
}

#include "rptqual.h"


/* ---- finding where a main-plan scan hangs ---- */

typedef struct RptParentContext
{
	plan_tree_base_prefix base;		/* must be first */
	const std::set<Plan *> *targets;
	std::map<Plan *, Plan *> *parents;
	std::vector<Plan *> *stack;		/* enclosing plan nodes */
	std::map<Plan *, int> *scan_slices;
	int slice_index;
} RptParentContext;

static bool
rpt_parent_walker(Node *node, void *context)
{
	RptParentContext *ctx = (RptParentContext *) context;

	if (node == NULL)
		return false;
	if (!is_plan_node(node))
		return plan_tree_walker(node, rpt_parent_walker, context, false);

	int saved_slice_index = ctx->slice_index;
	if (IsA(node, Motion))
		ctx->slice_index = ((Motion *) node)->motionID;
	if (ctx->targets->count((Plan *) node) && !ctx->stack->empty())
	{
		auto inserted = ctx->parents->emplace((Plan *) node, ctx->stack->back());
		if (!inserted.second)
			inserted.first->second = NULL; /* ambiguous placement */
		(*ctx->scan_slices)[(Plan *) node] = ctx->slice_index;
	}

	ctx->stack->push_back((Plan *) node);
	bool		result = plan_tree_walker(node, rpt_parent_walker, context, false);

	ctx->stack->pop_back();
	ctx->slice_index = saved_slice_index;
	return result;
}

/* Preserve original parents and slice indexes in the main tree. SubPlan IDs
 * are resolved while visiting expressions, but their plans are not entered. */
static std::map<Plan *, Plan *>
rpt_find_parents(PlannedStmt *stmt, const std::set<Plan *> &targets,
				 std::map<Plan *, int> *scan_slices)
{
	std::map<Plan *, Plan *> parents;
	std::vector<Plan *> stack;
	RptParentContext ctx;

	ctx.targets = &targets;
	ctx.parents = &parents;
	ctx.stack = &stack;
	ctx.scan_slices = scan_slices;
	ctx.slice_index = 0;
	RptPgCall([&]() {
		exec_init_plan_tree_base(&ctx.base, stmt);
		(void) rpt_parent_walker((Node *) stmt->planTree, &ctx);
	});
	return parents;
}

/* Output position of a base column in a scan's targetlist, or 0. */
AttrNumber
RptScanKeyPosition(Plan *scan, const RptKey &key)
{
	if (!IsA(scan, SeqScan) || ((Scan *) scan)->scanrelid != (Index) key.rtindex)
		return 0;
	ListCell   *lc;

	foreach(lc, scan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (IsA(tle->expr, Var) &&
			((Var *) tle->expr)->varno == (int) ((Scan *) scan)->scanrelid &&
			((Var *) tle->expr)->varattno == key.attno)
			return tle->resno;
	}
	return 0;
}

/* Verify the original parent still holds the current top, exactly once. */
bool
RptStackMatches(const RptInjectionState &state, Plan *scan, Plan *expected_parent)
{
	auto parent = state.parents.find(scan);
	auto top = state.tops.find(scan);
	if (parent == state.parents.end() || parent->second == NULL ||
		parent->second != expected_parent || top == state.tops.end())
		return false;
	int matches = (parent->second->lefttree == top->second) +
				  (parent->second->righttree == top->second);
	if (IsA(parent->second, CustomScan))
	{
		ListCell *lc;
		foreach(lc, ((CustomScan *) parent->second)->custom_plans)
			matches += lfirst(lc) == top->second;
	}
	return matches == 1;
}

void
RptRelinkStack(RptInjectionState *state, Plan *scan, Plan *top)
{
	Plan *parent = state->parents.at(scan);
	if (!RptStackMatches(*state, scan, parent) ||
		!AnserReplaceChildPlan(parent, state->tops.at(scan), top))
		RptPgCall([]() { elog(ERROR, "RPT scan stack changed after preflight"); });
	state->tops.at(scan) = top;
}

/* ---- quals a reducer scan may evaluate ---- */

/* Only explicitly trusted built-in comparisons and their hash support may
 * run eagerly. LEAKPROOF prevents argument disclosure, not unconditional
 * errors. Every other call, coercion, Param or SubPlan drops the whole qual. */
/* Deterministic bpchar equality and hashing trim trailing spaces and
 * compare or hash bytes. Nondeterministic collations use sort keys. */
static bool
rpt_safe_deterministic_collation(Oid collation)
{
	return OidIsValid(collation) && get_collation_isdeterministic(collation);
}

static bool
rpt_safe_comparison(Oid function, Oid collation)
{
	return RptQualFunctionSafe(function) ||
		(function == F_BPCHAREQ && rpt_safe_deterministic_collation(collation)) ||
		(rpt_extended_safe_quals &&
		 (RptExtendedQualFunctionSafe(function) ||
		  ((function == F_TEXTEQ || function == F_TEXTNE || function == F_BPCHARNE) &&
		   rpt_safe_deterministic_collation(collation))));
}

static bool
rpt_unsafe_qual_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_List:
		case T_BoolExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_RelabelType:
			break;
		case T_FuncExpr:
			if (!rpt_safe_comparison(((FuncExpr *) node)->funcid,
									 ((FuncExpr *) node)->inputcollid))
				return true;
			break;
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
			set_opfuncid((OpExpr *) node);
			if (!rpt_safe_comparison(((OpExpr *) node)->opfuncid,
									 ((OpExpr *) node)->inputcollid))
				return true;
			break;
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *op = (ScalarArrayOpExpr *) node;

			set_sa_opfuncid(op);
			if (!rpt_safe_comparison(op->opfuncid, op->inputcollid) ||
				(OidIsValid(op->negfuncid) && !rpt_safe_comparison(op->negfuncid, op->inputcollid)) ||
				(OidIsValid(op->hashfuncid) && !RptQualHashFunctionSafe(op->hashfuncid) &&
				 !((op->hashfuncid == F_HASHBPCHAR ||
				   (rpt_extended_safe_quals && op->hashfuncid == F_HASHTEXT)) &&
				  rpt_safe_deterministic_collation(op->inputcollid))))
				return true;
			break;
		}
		default:
			return true;		/* Param, SubPlan, coercions, everything else */
	}
	return expression_tree_walker(node, rpt_unsafe_qual_walker, context);
}

/* Whether a reducer may evaluate these quals (all of them, or none). */
static bool
rpt_quals_safe(List *quals)
{
	return quals == NIL || RptPgCall([&]() {
		return !contain_volatile_functions((Node *) quals) &&
			!rpt_unsafe_qual_walker((Node *) quals, NULL);
	});
}

/* All of the main scan's quals, or none of them. */
static List *
rpt_reducer_quals(List *quals)
{
	if (quals == NIL || !rpt_quals_safe(quals))
		return NIL;
	return (List *) copyObjectImpl(quals);
}

/* Rows per segment before quals, from pg_class.reltuples; < 0 when unknown.
 * Partitioned tables spread them over numsegments, a replicated table keeps
 * them all on each segment (as costsize.c's adjust_reloptinfo). */
double
RptBaseRows(const RptVertexInfo &v)
{
	if (!OidIsValid(v.relid))
		return -1;
	return RptPgCall([&]() {
		HeapTuple	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(v.relid));
		double		rows;

		if (!HeapTupleIsValid(tuple))
			return -1.0;
		rows = ((Form_pg_class) GETSTRUCT(tuple))->reltuples;
		ReleaseSysCache(tuple);
		if (rows < 0)
			return -1.0;
		if (!v.replicated && v.numsegments > 0)
			rows /= v.numsegments;
		return rows;
	});
}

/* What each vertex's reducer scan is expected to keep. */
static std::map<int, RptForwardVertexEst>
rpt_vertex_estimates(const RptTransferMgr &forward)
{
	std::map<int, RptForwardVertexEst> est;

	for (const RptVertexInfo &v : forward.vertices)
	{
		RptForwardVertexEst e;
		double		base = RptBaseRows(v);

		e.quals_dropped = !rpt_quals_safe(v.scan->qual);
		e.rows = base > 0 ? base : (double) v.cardinality;
		if (v.scan->qual != NIL && !e.quals_dropped && base > 0)
			e.selectivity = std::min(1.0, std::max(0.0, v.scan->plan_rows / base));
		est[v.rtindex] = e;
	}
	return est;
}

/* ---- node construction ---- */

static void
rpt_reset_copied_plan(Plan *plan, int *next_plan_node_id)
{
	plan->plan_node_id = (*next_plan_node_id)++;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	plan->initPlan = NIL;
	plan->extParam = NULL;
	plan->allParam = NULL;
	plan->parallel_aware = false;
	plan->async_capable = false;
	plan->parallel = 0;
	plan->flow = NULL;
}

static Plan *
rpt_reducer_scan(Plan *main_scan, Oid relid, const std::vector<AttrNumber> &attnos,
				 bool *quals_dropped, int *next_plan_node_id)
{
	Plan	   *scan = (Plan *) copyObjectImpl(main_scan);
	List	   *tlist = NIL;
	Index		varno = ((Scan *) main_scan)->scanrelid;
	AttrNumber	resno = 0;

	for (AttrNumber attno : attnos)
	{
		Oid			typid;
		int32		typmod;
		Oid			collid;

		get_atttypetypmodcoll(relid, attno, &typid, &typmod, &collid);
		tlist = lappend(tlist,
						makeTargetEntry((Expr *) makeVar(varno, attno, typid, typmod, collid, 0),
										++resno, NULL, false));
	}
	scan->targetlist = tlist;
	scan->qual = rpt_reducer_quals(main_scan->qual);
	*quals_dropped = (main_scan->qual != NIL && scan->qual == NIL);
	rpt_reset_copied_plan(scan, next_plan_node_id);
	return scan;
}

static Plan *
rpt_wrap_consumer(Plan *child, AttrNumber position, const RptForwardChannel &ch,
				  int width, int *next_plan_node_id)
{
	CustomScan *cs = AnserBuildBloomConsumerScan(child, position, ch.condition_id,
												 ch.condition_key.c_str(),
												 ch.total_elems, (Size) ch.max_payload,
												 ch.planned_bytes, width,
												 false /* channels are complete */ );

	cs->scan.plan.plan_node_id = (*next_plan_node_id)++;
	return (Plan *) cs;
}

static Plan *
rpt_wrap_producer(Plan *child, AttrNumber position, const RptForwardChannel &ch,
				  int width, int *next_plan_node_id)
{
	CustomScan *cs = AnserBuildBloomProducerScan(child, position, ch.condition_id,
												 ch.condition_key.c_str(),
												 ch.total_elems, (Size) ch.max_payload,
												 ch.planned_bytes, width);

	cs->scan.plan.plan_node_id = (*next_plan_node_id)++;
	return (Plan *) cs;
}

static bool
rpt_anser_size(double est_rows, int64 *total_elems, int64 *max_payload,
			   int64 *planned_bytes)
{
	return RptPgCall([&]() {
		return AnserRuntimeFilterSize(est_rows, total_elems, max_payload,
									  planned_bytes);
	});
}

static bool
rpt_plan_has_anser_nodes_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (is_plan_node(node) &&
		(AnserIsRuntimeFilterScan((Plan *) node) || RptIsLocalScan((Plan *) node)))
		return true;
	return plan_tree_walker(node, rpt_plan_has_anser_nodes_walker, context, true);
}

/* The backend walker resolves SubPlan IDs even while walking expressions.
 * Always provide the statement-backed prefix, including for diagnostic runs. */
static bool
rpt_has_runtime_filters(PlannedStmt *stmt)
{
	plan_tree_base_prefix context;

	return RptPgCall([&]() {
		exec_init_plan_tree_base(&context, stmt);
		return rpt_plan_has_anser_nodes_walker((Node *) stmt->planTree, &context);
	});
}

/* Main-plan scans of `vertex`, wrapped in consumers of every channel into it.
 * False when the scan cannot be found or keyed; nothing is changed then. */
static bool
rpt_apply_in_main(const RptVertexInfo &vertex,
				  const std::vector<RptForwardChannel> &channels,
				  RptInjectionState *state, int width,
				  bool dry_run)
{
	auto		parent = state->parents.find(vertex.scan);

	if (!IsA(vertex.scan, SeqScan) || parent == state->parents.end() ||
		!RptStackMatches(*state, vertex.scan, parent->second))
		return false;
	for (const RptForwardChannel &ch : channels)
		if (ch.target_rtindex == vertex.rtindex && !ch.in_process &&
			RptScanKeyPosition(vertex.scan, ch.target_key) == 0)
			return false;
	if (dry_run)
		return true;

	Plan	   *node = state->tops.at(vertex.scan);

	for (const RptForwardChannel &ch : channels)
		if (ch.target_rtindex == vertex.rtindex && !ch.in_process)
			node = RptPgCall([&]() {
				return rpt_wrap_consumer(node, RptScanKeyPosition(vertex.scan, ch.target_key),
										 ch, width, &state->next_plan_node_id);
			});
	if (node == state->tops.at(vertex.scan))
		return false;
	RptRelinkStack(state, vertex.scan, node);
	return true;
}

/* Channel identities are assigned only after placement accepts a component. */
static void
rpt_assign_forward_channels(std::vector<RptForwardComponent> &comps,
							RptInjectionState &state)
{
	for (auto &comp : comps)
		if (comp.accepted)
		{
			std::map<RptKey, const RptForwardChannel *> root_channels;
			for (auto &ch : comp.channels)
			{
				if (ch.main_only)
				{
					const auto *producer = root_channels.at(ch.source_key);
					ch.condition_id = producer->condition_id;
					ch.condition_key = producer->condition_key;
				}
				else
				{
					RptAssignForwardChannel(state, ch);
					if (ch.target_rtindex == comp.root_rtindex)
						root_channels.emplace(ch.source_key, &ch);
				}
			}
		}
}

static void
rpt_dump_forward_injection(const std::vector<RptForwardComponent> &comps,
						   const std::map<int, RptForwardVertexEst> &est,
						   std::string *trace)
{
	if (!trace)
		return;

	RptDumpForward(comps, trace);
	for (const RptForwardComponent &comp : comps)
		for (int rtindex : comp.chain_rtindexes)
			if (est.at(rtindex).quals_dropped)
				*trace += "forward: quals of " + std::to_string(rtindex) +
					" dropped in the reducer (not provably error-free)\n";
}

/* Match local alternatives to exact scans in the forward graph. */
static void
rpt_find_inplace_transfers(PlannedStmt *stmt, const RptTransferMgr &forward,
                           RptForwardParams &params)
{
	struct InplaceContext
	{
		const RptTransferMgr &schedule;
		RptForwardParams &params;
	} context{forward, params};
	RptPgCall([&]() {
		AnserVisitInplaceCandidates(stmt,
			[](Plan *build, AttrNumber build_attno, Plan *probe, AttrNumber probe_attno,
			   void *arg) {
				auto &ctx = *static_cast<InplaceContext *>(arg);
				const auto &indexes = ctx.schedule.vertex_index_by_rtindex;
				int build_rti = ((Scan *) build)->scanrelid;
				int probe_rti = ((Scan *) probe)->scanrelid;
				auto b = indexes.find(build_rti);
				auto p = indexes.find(probe_rti);

				if (b != indexes.end() && p != indexes.end() &&
					ctx.schedule.vertices[b->second].scan == build &&
					ctx.schedule.vertices[p->second].scan == probe)
					ctx.params.inplace_transfers.emplace(
						RptKey{build_rti, build_attno}, RptKey{probe_rti, probe_attno});
			}, &context);
	});
}

/* Build the forward part using the statement's shared IDs and scan stacks.
 * The selected components also supply root selectivity to backward sizing. */
static List *
RptInjectForward(const RptTransferMgr &forward,
				 const RptForwardParams &params,
				 const std::map<int, RptForwardVertexEst> &est,
				 bool materialize, RptInjectionState &state,
				 std::vector<RptForwardComponent> &comps,
				 std::string *trace, bool *injected)
{
	RptSelectForward(forward, params, est, rpt_anser_size, RptEstimateDistinct, &comps);

	for (RptForwardComponent &comp : comps)
	{
		if (!comp.accepted)
			continue;
		for (int rtindex : comp.chain_rtindexes)
		{
			const RptVertexInfo &v = forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)];

			if (!IsA(v.scan, SeqScan) || !OidIsValid(v.relid))
			{
				comp.accepted = false;
				comp.reason = "chain table " + std::to_string(rtindex) + " is not a plain SeqScan";
				break;
			}
		}
		if (comp.accepted &&
			!rpt_apply_in_main(forward.vertices[forward.vertex_index_by_rtindex.at(comp.root_rtindex)],
							   comp.channels, &state, params.width, true))
		{
			comp.accepted = false;
			comp.reason = "root scan not found, not a SeqScan, or key not in its output";
		}
	}

	rpt_assign_forward_channels(comps, state);
	rpt_dump_forward_injection(comps, est, trace);

	for (const RptForwardComponent &comp : comps)
		*injected = *injected || comp.accepted;

	List	   *children = NIL;

	for (const RptForwardComponent &comp : comps)
	{
		if (!materialize || !comp.accepted)
			continue;

		for (int rtindex : comp.chain_rtindexes)
		{
			const RptVertexInfo &v = forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)];
			std::vector<AttrNumber> attnos;

			for (const RptForwardChannel &ch : comp.channels)
			{
				if (ch.main_only)
					continue;
				AttrNumber	a = 0;

				if (ch.target_rtindex == rtindex)
					a = ch.target_key.attno;
				else if (ch.source_rtindex == rtindex)
					a = ch.source_key.attno;
				if (a != 0 && std::find(attnos.begin(), attnos.end(), a) == attnos.end())
					attnos.push_back(a);
			}

			bool		dropped = false;	/* reported with the dump */
			Plan	   *node = RptPgCall([&]() {
				return rpt_reducer_scan(v.scan, v.relid, attnos, &dropped, &state.next_plan_node_id);
			});

			/* Consumers below producers: a filter is built from rows the
			 * inbound filters already reduced. */
			for (const RptForwardChannel &ch : comp.channels)
			{
				if (ch.main_only || ch.target_rtindex != rtindex)
					continue;
				AttrNumber	pos = (AttrNumber) (std::find(attnos.begin(), attnos.end(),
														   ch.target_key.attno) - attnos.begin()) + 1;

				node = RptPgCall([&]() {
					if (!ch.in_process)
						return rpt_wrap_consumer(node, pos, ch, params.width, &state.next_plan_node_id);
					CustomScan *cs = RptBuildLocalConsumerScan(node, pos, ch.condition_id,
															   ch.total_elems);

					cs->scan.plan.plan_node_id = state.next_plan_node_id++;
					return (Plan *) cs;
				});
			}
			for (const RptForwardChannel &ch : comp.channels)
			{
				if (ch.main_only || ch.source_rtindex != rtindex)
					continue;
				AttrNumber	pos = (AttrNumber) (std::find(attnos.begin(), attnos.end(),
														   ch.source_key.attno) - attnos.begin()) + 1;

				node = RptPgCall([&]() {
					if (!ch.in_process)
						return rpt_wrap_producer(node, pos, ch, params.width, &state.next_plan_node_id);
					CustomScan *cs = RptBuildLocalProducerScan(node, pos, ch.condition_id,
															   ch.total_elems);

					cs->scan.plan.plan_node_id = state.next_plan_node_id++;
					return (Plan *) cs;
				});
			}
			children = RptPgCall([&]() { return lappend(children, node); });
		}

		if (!rpt_apply_in_main(forward.vertices[forward.vertex_index_by_rtindex.at(comp.root_rtindex)],
							   comp.channels, &state, params.width, false))
			RptPgCall([]() { elog(ERROR, "RPT forward: root relink failed after preflight"); });
		std::set<int> subscribers;
		for (const auto &ch : comp.channels)
			if (ch.main_only)
				subscribers.insert(ch.target_rtindex);
		for (int rtindex : subscribers)
			if (!rpt_apply_in_main(forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)],
								   comp.channels, &state, params.width, false))
				RptPgCall([]() { elog(ERROR, "RPT forward: subscriber relink failed after preflight"); });
	}

	return children;
}

static void
rpt_trace_skip(std::string *trace, const char *reason)
{
	if (trace)
		trace->append(reason);
}

PlannedStmt *
RptInject(PlannedStmt *stmt, bool materialize, std::string *trace)
{
	if (!gp_anser_enable || gp_anser_runtime_filter)
	{
		rpt_trace_skip(trace, "forward: needs anser.enable=on and anser.runtime_filter=off\n");
		return stmt;
	}
	if (rpt_has_runtime_filters(stmt))
	{
		rpt_trace_skip(trace, "forward: plan already carries Anser nodes\n");
		return stmt;
	}

	PlannedStmt *work = materialize
		? RptPgCall([&]() { return (PlannedStmt *) copyObjectImpl(stmt); })
		: stmt;

	RptTransferMgr analysis;

	RptCollectGraph(work, &analysis);
	if (analysis.vertices.empty())
	{
		rpt_trace_skip(trace, "forward: no transfer regions\n");
		if (materialize && rpt_forward && rpt_backward &&
			RptPgCall([&]() { return AnserApplyRuntimeFilterFallback(work); }))
			return work;
		return stmt;
	}
	RptBuildClasses(&analysis);

	RptTransferMgr forward;

	RptBuildForwardPlan(&analysis, &forward);
	RptCheckBackwardConsumers(work, &forward, rpt_key_survival);

	/* Preflight against the plan: everything that could refuse a component
	 * is checked before any node is built, so a refusal changes nothing. */
	std::set<Plan *> targets;

	for (const RptVertexInfo &v : forward.vertices)
		targets.insert(v.scan);

	RptInjectionState state;
	std::map<Plan *, int> scan_slices;
	state.parents = rpt_find_parents(work, targets, &scan_slices);
	for (Plan *scan : targets)
		state.tops[scan] = scan;
	state.next_plan_node_id = materialize
		? RptPgCall([&]() { return AnserMaxPlanNodeId(work); }) + 1 : 0;

	RptForwardParams params;

	params.use_total_cost = rpt_forward_total_cost;
	params.key_survival = rpt_key_survival;
	if (params.key_survival)
	{
		params.reducer_scan_ms_per_row = rpt_cost_scan_row;
		params.reducer_scan_ms_per_byte = rpt_cost_scan_byte;
		params.bloom_build_ms_per_row = rpt_cost_build_row;
		params.bloom_probe_ms_per_row = rpt_cost_probe_row;
		params.downstream_ms_per_row = rpt_cost_downstream_row;
	}
	if (rpt_prefer_inplace && (rpt_forward || rpt_backward))
		RptPgCall([&]() {
			AnserVisitInplaceProbes(work,
				[](Plan *probe, AttrNumber key, HashJoin *, void *arg) {
					auto &keys = *static_cast<std::set<RptKey> *>(arg);
					keys.insert({(int) ((Scan *) probe)->scanrelid, key});
				}, &params.inplace_probe_keys);
		});
	params.width = RptPgCall([]() { return getgpsegmentCount(); });
	params.ms_per_mib_part = rpt_forward_ms_per_mib_part;
	params.delivery_ms_per_mib = rpt_delivery_ms_per_mib;
	params.max_exchange_ms = rpt_forward_max_exchange_ms;
	params.min_root_rows = rpt_forward_min_root_rows;
	params.max_selectivity = rpt_forward_max_selectivity;
	if (rpt_forward && rpt_backward && params.use_total_cost)
		rpt_find_inplace_transfers(work, forward, params);
	for (const auto &vertex : forward.vertices)
	{
		auto found = scan_slices.find(vertex.scan);
		if (found != scan_slices.end())
		{
			params.main_consumer_processes[vertex.rtindex] = RptPgCall([&]() {
				return AnserSliceProducers(work, found->second);
			});
			if (IsA(vertex.scan, SeqScan) && state.parents.count(vertex.scan) &&
				RptStackMatches(state, vertex.scan, state.parents.at(vertex.scan)))
				for (const auto &key : vertex.join_keys)
					if (RptScanKeyPosition(vertex.scan, key) != 0)
						params.main_consumer_keys.insert(key);
		}
	}

	std::map<int, RptForwardVertexEst> est = rpt_vertex_estimates(forward);
	std::vector<RptForwardComponent> comps;
	bool any = false;
	List *children = NIL;
	if (rpt_forward)
		children = RptInjectForward(forward, params, est, materialize,
									state, comps, trace, &any);
	else
		rpt_trace_skip(trace, "forward: disabled\n");

	if (rpt_backward)
		RptInjectBackward(work, forward, comps, params, rpt_anser_size,
						  materialize, &state, trace, &any);
	else
		rpt_trace_skip(trace, "backward: disabled\n");
	/* Without an RPT exchange, use ordinary Anser placement. Mixing
	 * independent exchanges requires a shared wait-dependency proof. */
	if (materialize && rpt_forward && rpt_backward && !any)
		return RptPgCall([&]() { return AnserApplyRuntimeFilterFallback(work); }) ?
			work : stmt;

	/* RPT places its exchanges first; add local hash filters on other keys. */
	if (materialize && (rpt_backward || (rpt_forward && rpt_prefer_inplace)) &&
		RptPgCall([&]() { return AnserApplyInplaceRuntimeFilters(work); }))
		any = true;
	if (materialize && children != NIL)
		RptAttachReducerInitPlan(work, children, params.width, state.next_plan_node_id);
	return materialize && any ? work : stmt;
}
