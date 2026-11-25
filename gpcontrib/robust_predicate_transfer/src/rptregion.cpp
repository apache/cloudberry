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
 * rptregion.cpp
 *	  Extract the RPT+ join graph -- base-scan vertices and equijoin
 *	  edges -- from a finished PlannedStmt.
 *
 * This pass replaces the RPT+ artifact's TableOperatorManager
 * (embryo-labs/dynamic-predicate-transfer): where DuckDB builds its transfer
 * graph from the logical operator tree before join ordering, we read the
 * finished plan after set_plan_references, so the join clauses are already
 * in OUTER_VAR/INNER_VAR form and the key columns must be traced back to
 * their base scans.  The traversal mirrors anser_inject_walk and the key
 * descent and equality validation mirror AnserResolveKeyScan and
 * anser_hashjoin_keys (all in gpcontrib/anser/src/anserplan.c), widened as
 * noted at each site.
 *
 * Graph extraction does not modify the plan tree.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptregion.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"

#include "catalog/gp_distribution_policy.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "utils/syscache.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "anserplan.h"
#include "anserplanutils.h"
#include "rptlocal.h"
}


static bool rpt_is_vertex_scan(const Plan *plan);
static bool rpt_scan_is_filtered(const Plan *plan);
static RptVertexInfo *rpt_vertex_for_scan(PlannedStmt *stmt,
										  RptTransferMgr *mgr, Plan *scan);
static bool rpt_resolve_key_scan(Plan *top, AttrNumber key_attno,
								 Plan **scan_out, AttrNumber *attno_out);
static void rpt_add_edge(PlannedStmt *stmt, RptTransferMgr *mgr,
						 JoinType jointype,
						 Plan *outer_plan, AttrNumber outer_attno,
						 Plan *inner_plan, AttrNumber inner_attno,
						 Oid key_type);
static void rpt_collect_clauses(PlannedStmt *stmt, RptTransferMgr *mgr,
								JoinType jointype, Plan *outer, Plan *inner,
								List *clauses);
static void rpt_walk(PlannedStmt *stmt, RptTransferMgr *mgr, Plan *plan);

/*
 * True for the scan nodes a reducer pass could replay, the only accepted
 * vertices of the join graph.
 */
static bool
rpt_is_vertex_scan(const Plan *plan)
{
	return IsA(plan, SeqScan) ||
		IsA(plan, IndexScan) ||
		IsA(plan, IndexOnlyScan) ||
		IsA(plan, BitmapHeapScan);
}

/*
 * Does the scan carry restriction quals?  Index and bitmap scans restrict
 * through their index quals as well as through plan.qual.
 */
static bool
rpt_scan_is_filtered(const Plan *plan)
{
	if (plan->qual != NIL)
		return true;

	switch (nodeTag(plan))
	{
		case T_IndexScan:
			return ((IndexScan *) plan)->indexqual != NIL;
		case T_IndexOnlyScan:
			return ((IndexOnlyScan *) plan)->indexqual != NIL;
		case T_BitmapHeapScan:
			return ((BitmapHeapScan *) plan)->bitmapqualorig != NIL;
		default:
			return false;
	}
}

/*
 * Find or create the vertex for a base scan.  One vertex per rangetable
 * index, deduplicated through mgr->by_rti; vertices are created lazily, only
 * when a resolved equijoin edge reaches the scan, so a scan no key resolves
 * to never becomes a vertex.  The classification fields (intermediate,
 * unfiltered) are rptgraph.cpp's; join_keys accumulates here as edges are
 * recorded.
 */
static RptVertexInfo *
rpt_vertex_for_scan(PlannedStmt *stmt, RptTransferMgr *mgr, Plan *scan)
{
	int			rtindex = ((Scan *) scan)->scanrelid;
	std::map<int, int>::iterator it;
	RptVertexInfo v;

	it = mgr->vertex_index_by_rtindex.find(rtindex);
	if (it != mgr->vertex_index_by_rtindex.end())
		return &mgr->vertices[it->second];

	v.rtindex = rtindex;
	v.scan = scan;
	v.cardinality = (int64) scan->plan_rows;
	v.filtered = rpt_scan_is_filtered(scan);
	v.intermediate = false;
	v.unfiltered = false;

	if (rtindex >= 1 && rtindex <= list_length(stmt->rtable))
	{
		RangeTblEntry *rte = rt_fetch(rtindex, stmt->rtable);

		if (rte->rtekind == RTE_RELATION)
		{
			v.relid = rte->relid;
			GpPolicy *policy = RptPgCall([&]() { return GpPolicyFetch(rte->relid); });
			v.replicated = policy->ptype == POLICYTYPE_REPLICATED;
			v.numsegments = policy->numsegments;
			if (policy->ptype == POLICYTYPE_PARTITIONED && policy->nattrs > 0)
			{
				v.distkeys.assign(policy->attrs, policy->attrs + policy->nattrs);
				v.distopclasses.assign(policy->opclasses, policy->opclasses + policy->nattrs);
			}
			RptPgCall([&]() { pfree(policy); });
		}
	}

	mgr->vertex_index_by_rtindex[rtindex] = (int) mgr->vertices.size();
	mgr->vertices.push_back(v);
	return &mgr->vertices.back();
}

/* RPT's policy accepts index/bitmap scan termini and additional transparent
 * joins and unary nodes. The shared walker handles output positions, base
 * attributes and identity-filter children; RPT needs the table coordinate. */
static bool
rpt_identity_filter(const Plan *plan)
{
	return AnserIsRuntimeFilterScan(plan) || RptIsLocalScan(plan);
}

static bool
rpt_key_passthrough(const Plan *plan)
{
	return AnserKeyPassthroughNode(plan) || IsA(plan, MergeJoin) ||
		IsA(plan, NestLoop) || IsA(plan, Sort) || IsA(plan, Material);
}

static bool
rpt_resolve_key_scan(Plan *top, AttrNumber key_attno,
					 Plan **scan_out, AttrNumber *attno_out)
{
	const AnserKeyScanPolicy policy = {rpt_is_vertex_scan,
		rpt_key_passthrough, rpt_identity_filter, NULL};
	AnserKeyScanResult result;

	if (!AnserFindKeyScan(top, key_attno, &policy, NULL, &result))
		return false;
	*scan_out = result.scan;
	*attno_out = result.table_attno;
	return true;
}

/*
 * Resolve one validated equijoin clause to its two base scans and record the
 * edge between their vertices.
 *
 * left_rtindex/right_rtindex are canonicalized (lower rtindex on the left), not the
 * join's outer/inner: every clause of a vertex pair merges into one
 * RptEdgeInfo and the neighbor matrix is symmetric, so a second join over
 * the same pair with the opposite orientation must land on the same edge
 * rather than colliding with its mirror image.  The clause's key pair is
 * stored in the same canonical order, and the protect flags name the
 * protected *vertex*: JOIN_LEFT protects the outer side's vertex (its rows
 * survive whether or not they match, so it must never be reduced by the
 * other side's keys), JOIN_RIGHT the inner side's, INNER and SEMI neither,
 * and every other join type (ANTI, FULL, LASJ_NOTIN, ...) makes no edge at
 * all.  protect_left therefore means left must NOT be reduced by right's
 * keys.
 *
 * key_type is the validated equality input type of the first clause recorded
 * for the edge; the data model carries one type per edge even though
 * different clauses of a pair could equate different types.
 */
static void
rpt_add_edge(PlannedStmt *stmt, RptTransferMgr *mgr, JoinType jointype,
			 Plan *outer_plan, AttrNumber outer_attno,
			 Plan *inner_plan, AttrNumber inner_attno, Oid key_type)
{
	Plan	   *outer_scan;
	Plan	   *inner_scan;
	AttrNumber	outer_key;
	AttrNumber	inner_key;
	int			outer_rtindex;
	int			inner_rtindex;
	RptVertexInfo *outer_v;
	RptVertexInfo *inner_v;
	int			left_rtindex;
	int			right_rtindex;
	RptKey		left_key;
	RptKey		right_key;
	int			protected_rtindex = 0;
	RptEdgeInfo *edge;

	/* ANTI, FULL and the GP extras make no usable edge at all. */
	if (!AnserJoinSideReducible(jointype, false) &&
		!AnserJoinSideReducible(jointype, true))
		return;

	if (!rpt_resolve_key_scan(outer_plan, outer_attno,
							  &outer_scan, &outer_key))
		return;
	if (!rpt_resolve_key_scan(inner_plan, inner_attno,
							  &inner_scan, &inner_key))
		return;

	outer_rtindex = ((Scan *) outer_scan)->scanrelid;
	inner_rtindex = ((Scan *) inner_scan)->scanrelid;

	/*
	 * Create both vertices before taking pointers into mgr.vertices: creating
	 * the second can reallocate the vector the first pointer refers into.
	 */
	rpt_vertex_for_scan(stmt, mgr, outer_scan);
	rpt_vertex_for_scan(stmt, mgr, inner_scan);
	outer_v = &mgr->vertices[mgr->vertex_index_by_rtindex[outer_rtindex]];
	inner_v = &mgr->vertices[mgr->vertex_index_by_rtindex[inner_rtindex]];

	/* The preserved side of an outer join is protected from reduction. */
	if (!AnserJoinSideReducible(jointype, false))
		protected_rtindex = outer_rtindex;
	else if (!AnserJoinSideReducible(jointype, true))
		protected_rtindex = inner_rtindex;

	if (outer_rtindex <= inner_rtindex)
	{
		left_rtindex = outer_rtindex;
		left_key.rtindex = outer_rtindex;
		left_key.attno = outer_key;
		right_rtindex = inner_rtindex;
		right_key.rtindex = inner_rtindex;
		right_key.attno = inner_key;
	}
	else
	{
		left_rtindex = inner_rtindex;
		left_key.rtindex = inner_rtindex;
		left_key.attno = inner_key;
		right_rtindex = outer_rtindex;
		right_key.rtindex = outer_rtindex;
		right_key.attno = outer_key;
	}

	edge = &mgr->neighbor_matrix[left_rtindex][right_rtindex];
	if (edge->clauses.empty())
	{
		edge->left_rtindex = left_rtindex;
		edge->right_rtindex = right_rtindex;
		edge->key_type = key_type;
		edge->protect_left = false;
		edge->protect_right = false;
	}
	edge->clauses.push_back(std::make_pair(left_key, right_key));
	if (protected_rtindex == left_rtindex)
		edge->protect_left = true;
	else if (protected_rtindex == right_rtindex)
		edge->protect_right = true;

	outer_v->join_keys.insert(outer_rtindex == left_rtindex ? left_key : right_key);
	inner_v->join_keys.insert(inner_rtindex == left_rtindex ? left_key : right_key);

	/* The matrix is symmetric: both directions name the same edge value. */
	mgr->neighbor_matrix[right_rtindex][left_rtindex] = *edge;
}

/*
 * Run one join's clause list through validation, resolution and edge
 * recording.  Clauses that fail validation or resolution are skipped
 * individually; a join contributes an edge only for the clauses that
 * survive.
 */
static void
rpt_collect_clauses(PlannedStmt *stmt, RptTransferMgr *mgr, JoinType jointype,
					Plan *outer, Plan *inner, List *clauses)
{
	ListCell   *lc;

	foreach(lc, clauses)
	{
		Var		   *outer_var;
		Var		   *inner_var;
		Oid			key_type;

		if (!RptPgCall([&]() {
			return AnserEqualityKeys((Node *) lfirst(lc),
									 &outer_var, &inner_var, &key_type);
		}))
			continue;

		rpt_add_edge(stmt, mgr, jointype,
					 outer, outer_var->varattno,
					 inner, inner_var->varattno, key_type);
	}
}

/*
 * Manual recursive walk, after anser_inject_walk
 * (gpcontrib/anser/src/anserplan.c): the outerPlan/innerPlan spine plus the
 * child lists anser's walk omits (Append, MergeAppend, Sequence, BitmapAnd,
 * BitmapOr, SubqueryScan, CustomScan custom_plans), widened the same way
 * anser_max_plan_node_id widens its own.  plan->initPlan is skipped on
 * purpose: the SubPlan's Plan tree sits in stmt->subplans, which
 * RptCollectGraph walks separately.  Descending these lists finds joins
 * behind region boundaries; the resolver still refuses to cross them, so a
 * join inside a UNION arm can link scans of that arm but never out of it.
 */
static void
rpt_walk(PlannedStmt *stmt, RptTransferMgr *mgr, Plan *plan)
{
	struct WalkContext { PlannedStmt *stmt; RptTransferMgr *mgr; } context = {stmt, mgr};

	if (plan == NULL)
		return;

	switch (nodeTag(plan))
	{
		case T_HashJoin:
			rpt_collect_clauses(stmt, mgr, ((HashJoin *) plan)->join.jointype,
								outerPlan(plan), innerPlan(plan),
								((HashJoin *) plan)->hashclauses);
			break;
		case T_MergeJoin:
			rpt_collect_clauses(stmt, mgr, ((MergeJoin *) plan)->join.jointype,
								outerPlan(plan), innerPlan(plan),
								((MergeJoin *) plan)->mergeclauses);
			break;
		case T_NestLoop:
			rpt_collect_clauses(stmt, mgr, ((NestLoop *) plan)->join.jointype,
								outerPlan(plan), innerPlan(plan),
								((NestLoop *) plan)->join.joinqual);
			break;
		default:
			break;
	}

	AnserForEachChildPlan(plan, [](Plan *child, void *arg) {
		WalkContext *context = (WalkContext *) arg;
		rpt_walk(context->stmt, context->mgr, child);
	}, &context);
}

/*
 * Extract the join graph of a finished statement: walk the main plan tree
 * and every subplan, create a vertex for each base scan a resolved equijoin
 * edge reaches, and record the edges in the (symmetric) neighbor matrix.
 * Only vertices, by_rti and neighbor_matrix are filled here; the
 * classification, equivalence classes and the transfer plan itself are
 * rptgraph.cpp's and rptplan.cpp's passes over the same manager.
 */
void
RptCollectGraph(PlannedStmt *stmt, RptTransferMgr *mgr)
{
	ListCell   *lc;

	if (stmt == NULL || stmt->planTree == NULL)
		return;

	rpt_walk(stmt, mgr, stmt->planTree);
	foreach(lc, stmt->subplans)
		rpt_walk(stmt, mgr, (Plan *) lfirst(lc));
}

/* Catalog NDV is global. A negative stadistinct is a fraction of reltuples.
 * This is a survey estimate, never a predicate-transfer correctness input. */
double
RptEstimateDistinct(const RptVertexInfo &vertex, AttrNumber attno)
{
	if (!OidIsValid(vertex.relid) || attno <= 0)
		return -1;
	HeapTuple stats = RptPgCall([&]() { return SearchSysCache3(STATRELATTINH,
		ObjectIdGetDatum(vertex.relid), Int16GetDatum(attno), BoolGetDatum(false)); });
	if (!HeapTupleIsValid(stats))
		return -1;
	double distinct = ((Form_pg_statistic) GETSTRUCT(stats))->stadistinct;
	RptPgCall([&]() { ReleaseSysCache(stats); });
	if (distinct < 0)
	{
		HeapTuple relation = RptPgCall([&]() { return SearchSysCache1(RELOID,
			ObjectIdGetDatum(vertex.relid)); });
		if (!HeapTupleIsValid(relation))
			return -1;
		double rows = ((Form_pg_class) GETSTRUCT(relation))->reltuples;
		RptPgCall([&]() { ReleaseSysCache(relation); });
		if (rows < 0)
			return -1;
		distinct *= -rows;
	}
	return distinct > 0 ? distinct : -1;
}
