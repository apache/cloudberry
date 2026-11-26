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
 * rpt.h
 *	  Shared data model for the robust_predicate_transfer extension.
 *
 * The structures here are a port of the RPT+ artifact's transfer graph
 * (embryo-labs/dynamic-predicate-transfer, MIT license, Copyright 2018-2025
 * Stichting DuckDB Foundation; see licenses/LICENSE-mit-duckdb-rpt.txt):
 * DuckDB's ColumnBinding becomes RptKey (rtindex, attno), EdgeInfo becomes
 * RptEdgeInfo, JoinKeyTableGroup becomes RptKeyGroup, and dag.hpp's
 * GraphNode/GraphEdge become RptGraphNode/RptGraphEdge.  What does not exist
 * there is ours: the extraction of the join graph from a finished
 * PlannedStmt (rptregion.cpp) replaces DuckDB's TableOperatorManager, and
 * the consumer-eligibility check answers an MPP question (Anser's deadlock
 * rule) the single-node original never had.
 *
 * STL containers own malloc-backed storage. PostgreSQL errors must be
 * translated to C++ exceptions before unwinding those containers, then
 * rethrown at the C boundary.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/include/rpt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPT_H
#define RPT_H

/* libstdc++ locale headers may include libintl.h. Parse them before
 * postgres.h defines gettext macros in builds without NLS. */
#include <algorithm>
#include <cmath>
#include <cstdarg>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <locale>
#include <map>
#include <new>
#include <set>
#include <sstream>
#include <string>
#include <vector>

extern "C"
{
#include "postgres.h"

#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "lib/stringinfo.h"
}

/*
 * Like gpopt's GP_WRAP: never longjmp over a live C++ container. Only call
 * PostgreSQL through this boundary; callbacks must do the same for their
 * own PostgreSQL calls. The guard also restores PG's stacks when a callback
 * throws a C++ exception through a C walker.
 */
struct RptPgError {};

struct RptPgStackGuard
{
	sigjmp_buf *exception_stack;
	ErrorContextCallback *context_stack;

	RptPgStackGuard() : exception_stack(PG_exception_stack),
		context_stack(error_context_stack) {}
	~RptPgStackGuard()
	{
		PG_exception_stack = exception_stack;
		error_context_stack = context_stack;
	}
};

template <typename F>
auto RptPgCall(const F &call) -> decltype(call())
{
	RptPgStackGuard guard;
	sigjmp_buf local;
	int saved_dispatch_nest_level = dispatch_nest_level;

	if (sigsetjmp(local, 0) != 0)
	{
		dispatch_nest_level = saved_dispatch_nest_level;
		throw RptPgError();
	}
	PG_exception_stack = &local;
	return call();
}

/* GUCs (defined in rptinit.cpp). */
extern bool rpt_enable;
extern bool rpt_debug;
extern bool rpt_backward;
extern int rpt_max_backward_consumers;
extern int	rpt_max_chain_depth;

/*
 * One join key: a column of a base relation.  Ports DuckDB's ColumnBinding;
 * the union-find element for equivalence classes.
 */
struct RptKey
{
	int			rtindex;		/* rangetable index of the base relation */
	AttrNumber	attno;			/* column in that relation */
};

inline bool
operator<(const RptKey &a, const RptKey &b)
{
	return a.rtindex < b.rtindex ||
		(a.rtindex == b.rtindex && a.attno < b.attno);
}

inline bool
operator==(const RptKey &a, const RptKey &b)
{
	return a.rtindex == b.rtindex && a.attno == b.attno;
}

/*
 * A base scan taking part in at least one resolved equijoin edge; the join
 * graph's vertex.  Accepted scan types are the ones a reducer pass could
 * replay: SeqScan (heap and AO/AOCO), IndexScan, IndexOnlyScan,
 * BitmapHeapScan.  Everything else never becomes a vertex, because no key
 * resolution reaches it.
 */
struct RptVertexInfo
{
	int			rtindex;		/* range-table index of the scan */
	Plan	   *scan;			/* the base scan node */
	Oid relid = InvalidOid;
	int64		cardinality;	/* scan->plan_rows */
	bool		filtered;		/* the scan carries restriction quals */
	bool		intermediate;	/* touches >1 equivalence class */
	bool		unfiltered;		/* neither filtered nor intermediate */
	bool replicated = false;
	int numsegments = 0;
	std::vector<AttrNumber> distkeys;
	std::vector<Oid> distopclasses;
	std::set<RptKey> join_keys; /* this table's keys over all its edges */
};

/*
 * One equijoin edge between two vertices.  Ports EdgeInfo.  All equality
 * clauses of one join over the same vertex pair merge into one edge, so
 * clauses.size() is the shared-attribute count the paper uses as the edge
 * weight; each clause is one (left key, right key) pair.
 *
 * protect_left/protect_right name the protected destination: the preserved side of an
 * outer join keeps its rows whether or not they match, so it must never be
 * reduced by the other side's keys.  LEFT protects left and RIGHT
 * protects right; unsupported join kinds, ANTI and FULL create no edge.
 */
struct RptEdgeInfo
{
	int			left_rtindex;
	int			right_rtindex;		/* range-table indexes (RptVertexInfo.rtindex) */
	Oid			key_type;		/* the equality operator's input type */
	std::vector<std::pair<RptKey, RptKey> > clauses;
	bool		protect_left;
	bool		protect_right;
};

/*
 * One equivalence class of join keys.  Ports JoinKeyTableGroup: members are
 * the vertices touching the class, and the leader is the vertex the backward
 * broadcast builds its single filter on (registered in Prim insertion
 * order, so it is the class's earliest-placed vertex).
 */
struct RptKeyGroup
{
	std::set<int> member_rtindexes;
	int			leader_rtindex;		/* -1 until registered */
	RptKey		leader_key;
	Oid			key_type;
};

/* Ports dag.hpp's GraphEdge: one transfer edge of the built plan. */
struct RptGraphEdge
{
	int			other_rtindex;	/* producer for in-edges, consumer for out-edges */
	std::vector<RptKey> left_keys;	/* keys on the smaller table's side */
	std::vector<RptKey> right_keys; /* keys on the larger table's side */
	std::vector<Oid>	key_types;
	bool local = false; /* policy permits local merge; never use QD transport */
};

/* Ports dag.hpp's GraphNode: one vertex's full transfer plan. */
struct RptGraphNode
{
	int			rtindex;
	int32_t		cardinality_order;	/* Prim insertion order (0 = root) */

	struct
	{
		std::vector<RptGraphEdge> in;
		std::vector<RptGraphEdge> out;
	}			forward_stage,
				backward_stage;
};

/* One class/member decision. Relink points and tuple-output positions are
 * recorded only after the complete consumer and producer paths are proven. */
struct RptBackwardConsumer
{
	RptKey class_key = {0, 0};
	RptKey producer_key = {0, 0};
	RptKey consumer_key = {0, 0};
	int producer_rtindex = 0;
	int consumer_rtindex = 0;
	Plan *producer_parent = NULL;
	Plan *consumer_parent = NULL;
	AttrNumber producer_attno = 0;
	AttrNumber consumer_attno = 0;
	int producer_slice_index = -1;
	int consumer_slice_index = -1;
	bool local = false;
	bool rule9 = false;
	bool strict_eligible = false;
	bool eligible = false;
	bool defer_first = true;
	double estimated_rows_removed = -1; /* unknown without column statistics */
	double producer_base_rows = -1;
	double producer_ndv = -1;
	double consumer_ndv = -1;
	std::string reason;
};

/*
 * The whole analysis.  Ports TransferGraphManager; the neighbor matrix is
 * symmetric (both [a][b] and [b][a] name the same edge) as in the original.
 */
struct RptTransferMgr
{
	/* rptregion.cpp fills these from the PlannedStmt. */
	std::vector<RptVertexInfo> vertices;
	/* rtindex -> vector index in vertices */
	std::map<int, int> vertex_index_by_rtindex;
	/* rtindex -> (rtindex -> edge); rewritten/pruned for forward transfers */
	std::map<int, std::map<int, RptEdgeInfo> > neighbor_matrix;
	/* Logical edges before forward-only SkipUnfilteredTable pruning. */
	std::map<int, std::map<int, RptEdgeInfo> > logical_neighbor_matrix;

	/* rptgraph.cpp: classes and classification. */
	std::map<RptKey, RptKeyGroup> groups;	/* by class representative key */
	std::map<RptKey, RptKey> uf_parent;		/* union-find over keys */

	/* rptplan.cpp: the transfer plan. */
	std::map<int, RptGraphNode> graph;
	std::set<int> root_rtindexes;
	std::set<int> bridge_rtindexes;
	std::vector<int> reducer_order_rtindexes;
	std::vector<int> transfer_order_rtindexes;	/* Prim insertion order of range-table indexes */

	/* Eligibility for blocking global backward consumers only. Every global
	 * producer must be on a hash join's build side in a different slice from
	 * the probe-side consumer. Local edges never wait and do not participate;
	 * a vertex with no global in-edges has consumer_ok_by_rtindex=false. */
	std::map<int, bool> consumer_ok_by_rtindex;
	std::vector<RptBackwardConsumer> backward_consumers;
};

/*
 * Forward reducer.  A channel is one forward transfer edge: the
 * reducer scan of source_rtindex builds a bloom filter on source_key, and
 * every scan of target_rtindex (in the reducer or main plan) applies it on
 * target_key.  Only the first clause of a multi-key edge is transferred;
 * one key of a conjunctive equality still over-approximates, so this loses
 * selectivity, never rows.
 */
struct RptForwardChannel
{
	int			source_rtindex = 0;
	int			target_rtindex = 0;
	RptKey		source_key = {0, 0};
	RptKey		target_key = {0, 0};
	bool		local = false;		/* policy-local (rptplan.cpp, rpt_local_hop) */
	bool		in_process = false; /* local and consumed only inside the reducer:
									 * handed over in-process, no Anser channel */
	bool		main_only = false; /* extra subscriber to an existing root channel */
	uint32		condition_id = 0;
	std::string condition_key;
	int64		total_elems = 0;
	int64		max_payload = 0;
	int64		planned_bytes = 0;
	double		est_cost_ms = 0;
	double		est_keep = 1;
};

/* One root's forward chain and the decision about it. */
struct RptForwardComponent
{
	int			root_rtindex = 0;
	std::vector<int> chain_rtindexes;			/* reducer scans, producers before consumers */
	std::vector<RptForwardChannel> channels;
	std::set<RptKey> inplace_refusals;
	double		est_cost_ms = 0;	/* sum of hop estimates */
	double		est_work_ms = 0;	/* reducer, probes and exchange */
	double		est_benefit_ms = 0;	/* downstream work avoided */
	double		est_selectivity = 1;	/* estimated fraction of root rows kept */
	bool		accepted = false;
	std::string reason;				/* "inject", or why not */
};

struct RptForwardParams
{
	bool		use_total_cost = false;
	bool		key_survival = false;
	double		reducer_scan_ms_per_row = 0.00015;
	double		reducer_scan_ms_per_byte = 0.000002;
	double		bloom_build_ms_per_row = 0.00010;
	double		bloom_probe_ms_per_row = 0.00008;
	double		downstream_ms_per_row = 0.00060;
	int			width = 0;			/* reducer gang size: producers per channel */
	double		delivery_ms_per_mib = 0;
	double		ms_per_mib_part = 0;	/* c in c * n_producers * filter_MiB */
	double		max_exchange_ms = 0;	/* component budget; < 0 disables the limit */
	double		min_root_rows = 0;	/* root scan plan_rows below this: no reducer */
	double		max_selectivity = 1;	/* lambda; >= 1 disables the check */
	std::map<int, int> main_consumer_processes; /* rtindex -> main scan width */
	std::set<RptKey> main_consumer_keys;
	std::set<std::pair<RptKey, RptKey>> inplace_transfers;
	std::set<RptKey> inplace_probe_keys;
};

/*
 * What the reducer scan of one vertex is expected to keep, from the plan and
 * the catalog (rptforward.cpp).  `rows` is per segment, before quals; it sizes
 * the vertex's filters together with `selectivity`, the fraction its own
 * reducer quals keep -- 1 when they were dropped.  Vertices with no
 * entry use their plan_rows and selectivity 1.
 */
struct RptForwardVertexEst
{
	double		rows = -1;
	double		selectivity = 1;
	bool		quals_dropped = false;
};

/* Expected retained keys under independent sampling of rows within a key. */
inline double
RptRetainedKeys(double rows, double ndv, double row_keep)
{
	if (row_keep <= 0)
		return 0;
	if (row_keep >= 1)
		return ndv;
	return ndv * -std::expm1(std::max(1.0, rows / ndv) * std::log1p(-row_keep));
}

extern double RptBaseRows(const RptVertexInfo &vertex);
extern bool rpt_key_survival;
extern bool rpt_extended_safe_quals;
extern double rpt_cost_scan_row, rpt_cost_scan_byte, rpt_cost_build_row,
	rpt_cost_probe_row, rpt_cost_downstream_row;

/* Distinct values of a column over the whole table, or < 0 when unknown. */
typedef double (*RptNdvFn) (const RptVertexInfo &vertex, AttrNumber attno);

/* Anser's bloom sizing (AnserRuntimeFilterSize), injectable for unit tests. */
typedef bool (*RptFilterSizeFn) (double est_rows, int64 *total_elems,
								  int64 *max_payload, int64 *planned_bytes);

/* Backward selection preserves the survey's per-edge proof and refusal. */
struct RptBackwardTarget
{
	RptBackwardConsumer proof;
	int processes = 0;
	double estimated_rows_removed = -1;
	bool accepted = false;
	std::string reason;
};

struct RptBackwardClass
{
	RptKey class_key = {0, 0};
	int producer_rtindex = 0;
	RptKey producer_key = {0, 0};
	int producer_processes = 0;
	int64 total_elems = 0;
	int64 max_payload = 0;
	int64 planned_bytes = 0;
	uint32 condition_id = 0;
	std::string condition_key;
	double estimated_producer_rows = 0;
	double est_cost_ms = 0;
	bool accepted = false;
	std::string reason;
	std::vector<RptBackwardTarget> consumers;
};

extern void RptSelectBackward(const RptTransferMgr &schedule,
							 const std::vector<RptForwardComponent> &forward,
							 const RptForwardParams &params,
							 const std::map<int, int> &slice_processes,
							 RptFilterSizeFn size_fn,
							 std::vector<RptBackwardClass> *out);
extern void RptDumpBackward(const RptTransferMgr &schedule,
							const std::vector<RptBackwardClass> &classes,
						   std::string *out);

/* rptforwardplan.cpp: pure selection over a forward schedule. */
extern void RptSelectForward(const RptTransferMgr &forward,
							 const RptForwardParams &params,
							 const std::map<int, RptForwardVertexEst> &est,
							 RptFilterSizeFn size_fn, RptNdvFn ndv_fn,
							 std::vector<RptForwardComponent> *out);
extern void RptDumpForward(const std::vector<RptForwardComponent> &components,
						   std::string *out);

/* rptforward.cpp: the planner-hook pass.  Returns a new statement, or stmt
 * itself when nothing is injected; *trace receives the decision dump.  May
 * throw; the caller fails open. */
extern PlannedStmt *RptInject(PlannedStmt *stmt, bool materialize,
									 std::string *trace);

/* Forward reducer GUCs (rptinit.cpp). */
extern bool rpt_forward;
extern double rpt_forward_ms_per_mib_part;
extern double rpt_delivery_ms_per_mib;
extern double rpt_forward_max_exchange_ms;
extern bool rpt_forward_total_cost;
extern bool rpt_prefer_inplace;
extern double rpt_forward_min_root_rows;
extern double rpt_forward_max_selectivity;

/* rptregion.cpp: extract the join graph from the finished statement. */
extern void RptCollectGraph(PlannedStmt *stmt, RptTransferMgr *mgr);

extern RptKey RptFindKeyRoot(const RptKey &key,
							   std::map<RptKey, RptKey> &uf_parent);

/* rptgraph.cpp: union-find classes, classification, SkipUnfilteredTable. */
extern void RptBuildClasses(RptTransferMgr *mgr);

/* rptplan.cpp: the Prim forest (LargestRootUpdated), forward/backward stage
 * assignment (CreateTransferPlanUpdated), consumer eligibility, and dumps. */
extern void RptBuildTransferPlan(PlannedStmt *stmt, RptTransferMgr *mgr);
extern void RptBuildForwardPlan(const RptTransferMgr *analysis,
								RptTransferMgr *forward);
extern void RptAttachReducerInitPlan(PlannedStmt *stmt, List *children, int width,
									int next_plan_node_id);
extern void RptCheckBackwardConsumers(PlannedStmt *stmt, RptTransferMgr *mgr,
									 bool key_survival = false);
extern double RptEstimateDistinct(const RptVertexInfo &vertex, AttrNumber attno);
extern void RptDumpTransferPlan(const PlannedStmt *stmt,
								const RptTransferMgr *mgr, StringInfo out);

#endif							/* RPT_H */
