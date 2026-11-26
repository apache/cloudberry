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
 * graph_test.cpp
 *    Deterministic tests of the production graph, planner, resolver and
 *    PostgreSQL/C++ error boundary, using synthetic plans and C-call stubs.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/test/graph_test.cpp
 *
 *-------------------------------------------------------------------------
 */
/* Standalone tests of the production graph/plan code. PostgreSQL calls are
 * stubbed below; no server or optimizer choices affect these graph fixtures. */
#include "../src/rptgraph.cpp"
#include "../src/rptplan.cpp"
#include "../src/rptregion.cpp"
#include "../src/rpteligible.cpp"
#include "../include/rptqual.h"
#include "../src/rptforwardplan.cpp"
#include "../src/rptbackwardplan.cpp"

#include <cstdlib>
#include <new>
#include <cstdarg>
#include <deque>
#include <initializer_list>
#undef vsnprintf

/* Unlike assert(), checks and their side effects remain active with NDEBUG. */
#define CHECK(condition) \
	do { \
		if (!(condition)) { \
			fputs("check failed: " #condition "\n", stderr); \
			abort(); \
		} \
	} while (0)

struct TestResolution
{
	Plan *parent;
	Plan *scan;
	AttrNumber attno;
	int slice_index;
	bool for_consumer;
};
static std::map<Plan *, TestResolution> test_resolutions;
static const Plan *test_local_identity;
static const Plan *test_anser_filter;

extern "C" {
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
int dispatch_nest_level = 0;
HeapTuple SearchSysCache1(int, Datum) { abort(); }
HeapTuple SearchSysCache3(int, Datum, Datum, Datum) { abort(); }
void ReleaseSysCache(HeapTuple) { abort(); }
bool AnserResolveKeyScan(Plan *top, AttrNumber slot, int, bool consumer,
						Plan **parent, Plan **scan, AttrNumber *attno, int *slice_index)
{
	CHECK(slot > 0);
	auto found = test_resolutions.find(top);
	if (found == test_resolutions.end())
		return false;
	CHECK(found->second.for_consumer == consumer);
	*parent = found->second.parent;
	*scan = found->second.scan;
	*attno = found->second.attno;
	*slice_index = found->second.slice_index;
	return true;
}
Oid getBaseType(Oid type) { return type; }
bool get_typbyval(Oid) { return true; }
TypeCacheEntry *lookup_type_cache(Oid type, int flags)
{
	CHECK(type == INT4OID && flags == TYPECACHE_EQ_OPR);
	static TypeCacheEntry entry = {};
	entry.eq_opr = 96; /* int4eq */
	return &entry;
}
GpPolicy *GpPolicyFetch(Oid) { abort(); }
void pfree(void *) { abort(); }
bool AnserIsRuntimeFilterScan(const Plan *plan) { return plan != NULL && plan == test_anser_filter; }
bool RptIsLocalScan(const Plan *plan) { return plan == test_local_identity; }

void ExceptionalCondition(const char *, const char *, int) { abort(); }
void exec_init_plan_tree_base(plan_tree_base_prefix *, PlannedStmt *) {}
bool plan_tree_walker(Node *node, bool (*walker)(Node *, void *), void *ctx, bool)
{
	Plan *plan = (Plan *) node;
	return walker((Node *) outerPlan(plan), ctx) ||
		walker((Node *) innerPlan(plan), ctx);
}
void appendStringInfo(StringInfo str, const char *format, ...)
{
	va_list args;
	va_start(args, format);
	int count = vsnprintf(str->data + str->len, str->maxlen - str->len, format, args);
	va_end(args);
	CHECK(count >= 0 && count < str->maxlen - str->len);
	str->len += count;
}
void appendStringInfoString(StringInfo str, const char *text)
{
	appendStringInfo(str, "%s", text);
}
void appendStringInfoChar(StringInfo str, char ch)
{
	appendStringInfo(str, "%c", ch);
}
}
double RptBaseRows(const RptVertexInfo &) { return -1; }

bool rpt_enable = false;
bool rpt_debug = false;
bool rpt_backward = true;
int rpt_max_backward_consumers = 4;
int rpt_max_chain_depth = 4;

static void vertex(RptTransferMgr &mgr, int rtindex, int rows, bool filtered)
{
	RptVertexInfo v = {};
	v.rtindex = rtindex;
	v.cardinality = rows;
	v.filtered = filtered;
	mgr.vertex_index_by_rtindex[rtindex] = mgr.vertices.size();
	mgr.vertices.push_back(v);
}

static RptEdgeInfo edge(int a, int b, bool protect_a = false,
						bool protect_b = false, int col_a = 1, int col_b = 1)
{
	RptEdgeInfo e = {};
	e.left_rtindex = a;
	e.right_rtindex = b;
	e.key_type = 23;
	e.protect_left = protect_a;
	e.protect_right = protect_b;
	e.clauses.push_back(std::make_pair(RptKey{a, (AttrNumber) col_a},
									 RptKey{b, (AttrNumber) col_b}));
	return e;
}

static void connect(RptTransferMgr &mgr, const RptEdgeInfo &e)
{
	rpt_upsert_edge(&mgr, e);
	mgr.vertices[mgr.vertex_index_by_rtindex[e.left_rtindex]].join_keys.insert(e.clauses[0].first);
	mgr.vertices[mgr.vertex_index_by_rtindex[e.right_rtindex]].join_keys.insert(e.clauses[0].second);
}

static void bypass_direction(bool reverse)
{
	RptTransferMgr mgr;
	vertex(mgr, 1, 100, true);
	vertex(mgr, 2, 10, true);
	vertex(mgr, 3, 50, false);
	/* 1 LEFT JOIN (2 JOIN 3): only 1 -> 3 -> 2 is legal. */
	connect(mgr, reverse ? edge(3, 1, false, true) : edge(1, 3, true));
	connect(mgr, reverse ? edge(3, 2) : edge(2, 3));
	mgr.vertices[2].unfiltered = true;
	rpt_skip_unfiltered(&mgr);
	const auto &e = mgr.neighbor_matrix.at(1).at(2);
	CHECK(rpt_can_flow(&e, 1, 2));
	CHECK(!rpt_can_flow(&e, 2, 1));
	CHECK(!rpt_can_flow(&mgr.neighbor_matrix.at(2).at(1), 2, 1));
}

static void protection_and_idempotence()
{
	RptTransferMgr mgr;
	CHECK(rpt_upsert_edge(&mgr, edge(1, 2, true)));
	CHECK(!rpt_upsert_edge(&mgr, edge(1, 2, true)));
	CHECK(!rpt_upsert_edge(&mgr, edge(2, 1)));
	CHECK(mgr.neighbor_matrix[1][2].protect_left);
	CHECK(rpt_upsert_edge(&mgr, edge(2, 1, true)));
	CHECK(mgr.neighbor_matrix[1][2].protect_right);
}

static RptTransferMgr star(int cap)
{
	RptTransferMgr mgr;
	rpt_max_chain_depth = cap;
	for (int rtindex = 1; rtindex <= 4; ++rtindex)
		vertex(mgr, rtindex, 100 / rtindex, true);
	for (int rtindex = 2; rtindex <= 4; ++rtindex)
		connect(mgr, edge(1, rtindex));
	RptBuildClasses(&mgr);
	RptBuildTransferPlan(NULL, &mgr);
	return mgr;
}

static void asymmetric_shape()
{
	auto mgr = star(4);
	CHECK(mgr.groups.size() == 1);
	CHECK(mgr.transfer_order_rtindexes == std::vector<int>({1, 2, 3, 4}));
	CHECK(mgr.reducer_order_rtindexes == std::vector<int>({4, 3, 2}));
	for (int rtindex = 2; rtindex <= 4; ++rtindex)
	{
		CHECK(mgr.graph[rtindex].forward_stage.out.size() == 1);
		CHECK(mgr.graph[rtindex].forward_stage.out[0].other_rtindex == rtindex - 1);
		CHECK(mgr.graph[rtindex].backward_stage.in.size() == 1);
		CHECK(mgr.graph[rtindex].backward_stage.in[0].other_rtindex == 1);
	}
	CHECK(mgr.graph[1].forward_stage.out.empty());
	CHECK(mgr.graph[1].backward_stage.out.size() == 3);
	mgr = star(1);
	CHECK(mgr.graph[4].forward_stage.out.size() == 1);
	CHECK(mgr.graph[3].forward_stage.out.empty());
	CHECK(mgr.graph[2].forward_stage.in.empty());
}

static void unfiltered_cycle()
{
	RptTransferMgr mgr;
	for (int rtindex = 1; rtindex <= 4; ++rtindex)
		vertex(mgr, rtindex, 10, rtindex == 1);
	connect(mgr, edge(1, 2));
	connect(mgr, edge(2, 3));
	connect(mgr, edge(3, 4));
	connect(mgr, edge(4, 2));
	RptBuildClasses(&mgr); /* Must terminate, with no unfiltered producers. */
	for (const auto &row : mgr.neighbor_matrix)
		for (const auto &entry : row.second)
			if (row.first != 1)
				CHECK(!rpt_can_flow(&entry.second, row.first, entry.first));
}

/* A single-column identity targetlist, allocated without PostgreSQL memory. */
struct TestTargetList
{
	Var var = {};
	TargetEntry entry = {};
	ListCell cell = {};
	List list = {};

	void bind(Plan *plan, int varno, int attno)
	{
		NodeSetTag(&var, T_Var);
		var.varno = varno;
		var.varattno = attno;
		entry.expr = (Expr *) &var;
		entry.resno = 1;
		cell.ptr_value = &entry;
		list.type = T_List;
		list.length = list.max_length = 1;
		list.elements = &cell;
		plan->targetlist = &list;
	}
};

struct TestBackwardPlan
{
	SeqScan consumer = {};
	SeqScan producer = {};
	Motion motion = {};
	Hash hash = {};
	HashJoin join = {};
	PlannedStmt stmt = {};
	TestTargetList lists[4];
	RptTransferMgr mgr;

	TestBackwardPlan()
	{
		test_resolutions.clear();
		NodeSetTag(&consumer, T_SeqScan);
		NodeSetTag(&producer, T_SeqScan);
		consumer.scan.scanrelid = 1;
		producer.scan.scanrelid = 2;
		lists[0].bind((Plan *) &consumer, 1, 3);
		lists[1].bind((Plan *) &producer, 2, 3);
		NodeSetTag(&motion, T_Motion);
		motion.motionID = 1;
		outerPlan(&motion) = (Plan *) &producer;
		lists[2].bind((Plan *) &motion, OUTER_VAR, 1);
		NodeSetTag(&hash, T_Hash);
		outerPlan(&hash) = (Plan *) &motion;
		lists[3].bind((Plan *) &hash, OUTER_VAR, 1);
		NodeSetTag(&join, T_HashJoin);
		join.join.jointype = JOIN_INNER;
		outerPlan(&join) = (Plan *) &consumer;
		innerPlan(&join) = (Plan *) &hash;
		stmt.planTree = (Plan *) &join;
		vertex(mgr, 1, 1000, false);
		vertex(mgr, 2, 100, true);
		mgr.vertices[0].scan = (Plan *) &consumer;
		mgr.vertices[1].scan = (Plan *) &producer;
		connect(mgr, edge(1, 2, false, false, 3, 3));
		RptBuildClasses(&mgr);
		test_resolutions[(Plan *) &hash] = {(Plan *) &motion, (Plan *) &producer, 1, 1, false};
		RptBuildTransferPlan(&stmt, &mgr);
	}
};

static void strict_backward_eligibility()
{
	TestBackwardPlan plan;
	CHECK(plan.mgr.backward_consumers.size() == 1);
	auto decision = plan.mgr.backward_consumers[0];
	CHECK(decision.eligible && decision.rule9 && decision.defer_first);
	CHECK(decision.producer_rtindex == 2 && decision.consumer_rtindex == 1);
	CHECK(decision.consumer_attno == 1 && decision.producer_attno == 1);
	CHECK(decision.consumer_parent == (Plan *) &plan.join);
	CHECK(decision.producer_parent == (Plan *) &plan.motion);
	CHECK(decision.producer_slice_index == 1 && decision.consumer_slice_index == 0);
	CHECK(plan.mgr.consumer_ok_by_rtindex[1]);

	for (NodeTag tag : {T_Sort, T_Agg, T_Material, T_Unique, T_WindowAgg, T_Hash, T_SetOp})
	{
		Plan blocker = {};
		blocker.type = tag;
		outerPlan(&blocker) = (Plan *) &plan.consumer;
		outerPlan(&plan.join) = &blocker;
		RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
		CHECK(!plan.mgr.backward_consumers[0].eligible);
		CHECK(plan.mgr.backward_consumers[0].rule9);
		CHECK(plan.mgr.backward_consumers[0].reason.find("between consumer and join") != std::string::npos);
	}
	outerPlan(&plan.join) = (Plan *) &plan.consumer;
	plan.join.join.prefetch_inner = true;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].eligible);
	CHECK(!plan.mgr.backward_consumers[0].defer_first);
	plan.join.join.prefetch_inner = false;
	plan.join.join.jointype = JOIN_RIGHT;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].eligible);
	CHECK(!plan.mgr.backward_consumers[0].defer_first);
	plan.join.join.jointype = JOIN_INNER;

	HashJoin nested = {};
	Hash nested_hash = {};
	Plan other = {};
	NodeSetTag(&nested, T_HashJoin);
	NodeSetTag(&nested_hash, T_Hash);
	nested.join.jointype = JOIN_LEFT;
	outerPlan(&nested) = &other;
	innerPlan(&nested) = (Plan *) &nested_hash;
	outerPlan(&nested_hash) = (Plan *) &plan.consumer;
	outerPlan(&plan.join) = (Plan *) &nested;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "nullable side of outer join");

	TestTargetList nested_list;
	nested.join.jointype = JOIN_INNER;
	outerPlan(&nested) = (Plan *) &plan.consumer;
	innerPlan(&nested) = &other;
	nested_list.bind((Plan *) &nested, OUTER_VAR, 1);
	test_resolutions[(Plan *) &nested] = {(Plan *) &nested, (Plan *) &plan.consumer, 1, 0, true};
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "join between consumer and join");
	plan.join.join.prefetch_inner = true;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].eligible);
	CHECK(!plan.mgr.backward_consumers[0].defer_first);
	plan.join.join.prefetch_inner = false;

	outerPlan(&plan.join) = (Plan *) &plan.hash;
	innerPlan(&plan.join) = (Plan *) &plan.consumer;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(!plan.mgr.backward_consumers[0].rule9);
	CHECK(plan.mgr.backward_consumers[0].reason == "producer is not on a qualifying hash build side");
	outerPlan(&plan.join) = (Plan *) &plan.consumer;
	innerPlan(&plan.join) = (Plan *) &plan.hash;
	plan.motion.motionID = 0;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "same or unknown slice");
	plan.motion.motionID = 1;
	plan.mgr.root_rtindexes.clear();
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "bridge-led class is deferred");
	plan.mgr.graph[2].backward_stage.out[0].local = true;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "local edge uses core runtime filtering");
}

static void subplans_and_external_parameters_are_refused()
{
	TestBackwardPlan plan;
	Bitmapset *parameters = (Bitmapset *) calloc(1, offsetof(Bitmapset, words) + sizeof(bitmapword));
	parameters->nwords = 1;
	parameters->words[0] = 1;
	plan.hash.plan.extParam = parameters;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(!plan.mgr.backward_consumers[0].eligible);
	CHECK(plan.mgr.backward_consumers[0].reason == "parameterized producer can be rescanned");
	plan.hash.plan.extParam = NULL;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].eligible);
	plan.hash.plan.extParam = NULL;
	free(parameters);

	/* The Motion still gives valid slices inside the SubPlan. A separate
	 * placement flag must refuse it regardless of slice-number changes. */
	ListCell cell = {};
	cell.ptr_value = &plan.join;
	List subplans = {};
	subplans.type = T_List;
	subplans.length = subplans.max_length = 1;
	subplans.elements = &cell;
	Plan main = {};
	NodeSetTag(&main, T_Result);
	plan.stmt.planTree = &main;
	plan.stmt.subplans = &subplans;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(!plan.mgr.backward_consumers[0].eligible);
	CHECK(plan.mgr.backward_consumers[0].reason == "scan inside a SubPlan");
	plan.mgr.graph[2].backward_stage.out[0].local = true;
	RptCheckBackwardConsumers(&plan.stmt, &plan.mgr);
	CHECK(plan.mgr.backward_consumers[0].reason == "scan inside a SubPlan");
}

static void every_global_producer_is_required()
{
	RptTransferMgr mgr;
	vertex(mgr, 1, 100, false);
	RptBackwardConsumer first;
	first.consumer_rtindex = 1;
	first.producer_rtindex = 2;
	first.eligible = true;
	RptBackwardConsumer second = first;
	second.producer_rtindex = 3;
	second.eligible = false;
	mgr.backward_consumers = {first, second};
	rpt_consumer_verdicts(&mgr);
	CHECK(!mgr.consumer_ok_by_rtindex[1]);
	mgr.backward_consumers[1].local = true;
	rpt_consumer_verdicts(&mgr);
	CHECK(mgr.consumer_ok_by_rtindex[1]);
	mgr.backward_consumers[0].local = true;
	rpt_consumer_verdicts(&mgr);
	CHECK(!mgr.consumer_ok_by_rtindex[1]);
}

static void backward_consumer_cap()
{
	RptTransferMgr mgr;
	for (int i = 1; i <= 3; ++i)
	{
		vertex(mgr, i, 100 * i, false);
		RptBackwardConsumer decision;
		decision.class_key = {9, 1};
		decision.consumer_rtindex = i;
		decision.strict_eligible = decision.eligible = true;
		decision.estimated_rows_removed = 100 * i;
		mgr.backward_consumers.push_back(decision);
	}
	rpt_max_backward_consumers = 1;
	rpt_cap_backward_consumers(&mgr);
	CHECK(!mgr.backward_consumers[0].eligible);
	CHECK(!mgr.backward_consumers[1].eligible);
	CHECK(mgr.backward_consumers[2].eligible);
	CHECK(mgr.backward_consumers[0].reason == "backward consumer cap exceeded");
	rpt_max_backward_consumers = 4;
}

static void bridges_and_locality()
{
	RptTransferMgr mgr;
	vertex(mgr, 1, 100, true);
	vertex(mgr, 2, 50, false);
	vertex(mgr, 3, 20, true);
	connect(mgr, edge(1, 2));
	connect(mgr, edge(2, 3, false, false, 2));
	RptBuildClasses(&mgr);
	rpt_max_chain_depth = 4;
	RptBuildTransferPlan(NULL, &mgr);
	CHECK(mgr.groups.size() == 2);
	CHECK(mgr.bridge_rtindexes == std::set<int>({2}));
	CHECK(mgr.reducer_order_rtindexes == std::vector<int>({3, 2}));
	RptTransferMgr runtime;
	RptBuildForwardPlan(&mgr, &runtime);
	CHECK(runtime.root_rtindexes.count(1));
	CHECK(runtime.bridge_rtindexes.count(2));
	CHECK(!runtime.graph.at(2).backward_stage.out.empty());
	for (auto &v : mgr.vertices)
	{
		v.numsegments = 3;
		v.distkeys = {1};
		v.distopclasses = {1978};
	}
	RptGraphEdge hop = {};
	hop.other_rtindex = 2;
	hop.left_keys = {{1, 1}};
	hop.right_keys = {{2, 1}};
	CHECK(rpt_local_hop(&mgr, 1, hop));
	mgr.vertices[1].numsegments = 2;
	CHECK(!rpt_local_hop(&mgr, 1, hop));
	mgr.vertices[1].numsegments = 3;
	mgr.vertices[1].distopclasses = {1};
	CHECK(!rpt_local_hop(&mgr, 1, hop));
	mgr.vertices[1].distopclasses = {1978};
	mgr.vertices[1].distkeys = {2};
	CHECK(!rpt_local_hop(&mgr, 1, hop));
}

static void hash_policy(RptVertexInfo &v, int segments = 3)
{
	v.numsegments = segments;
	v.distkeys = {1};
	v.distopclasses = {1978};
}

static void local_star_and_backward()
{
	auto mgr = star(4);
	for (auto &v : mgr.vertices)
		hash_policy(v);
	rpt_classify_hops(&mgr);
	int forward = 0;
	int backward = 0;
	for (const auto &node : mgr.graph)
	{
		for (const auto &hop : node.second.forward_stage.out)
		{
			++forward;
			CHECK(hop.local); /* Includes synthetic 4->3 and 3->2. */
			CHECK(mgr.graph.at(hop.other_rtindex).forward_stage.in[0].local);
		}
		for (const auto &hop : node.second.backward_stage.out)
		{
			++backward;
			CHECK(hop.local);
			CHECK(mgr.graph.at(hop.other_rtindex).backward_stage.in[0].local);
		}
	}
	CHECK(forward == 3 && backward == 3);
	char buffer[16384] = {};
	StringInfoData dump = {};
	dump.data = buffer;
	dump.maxlen = sizeof(buffer);
	PlannedStmt stmt = {};
	stmt.type = T_PlannedStmt;
	RangeTblEntry entries[4] = {};
	Alias aliases[4] = {};
	ListCell cells[4] = {};
	List rtable = {};
	rtable.type = T_List;
	rtable.length = rtable.max_length = 4;
	rtable.elements = cells;
	stmt.rtable = &rtable;
	char employee[] = "employee", manager[] = "manager", other[] = "other";
	for (int i = 0; i < 4; ++i)
	{
		entries[i].type = T_RangeTblEntry;
		entries[i].relid = 99; /* Same relation, distinct query references. */
		entries[i].eref = &aliases[i];
		aliases[i].type = T_Alias;
		aliases[i].aliasname = i == 0 ? employee : i == 1 ? manager : other;
		cells[i].ptr_value = &entries[i];
	}
	RptDumpTransferPlan(&stmt, &mgr, &dump);
	std::string text(buffer);
	CHECK(text.find("vertex 1 employee rows=") != std::string::npos);
	CHECK(text.find("vertex 2 manager rows=") != std::string::npos);
	/* Changing the RTE after graph construction is reflected immediately. */
	aliases[0].aliasname = manager;
	dump.len = 0;
	buffer[0] = '\0';
	RptDumpTransferPlan(&stmt, &mgr, &dump);
	CHECK(std::string(buffer).find("vertex 1 manager rows=") != std::string::npos);
	CHECK(text.find("merge=global") == std::string::npos);
	CHECK(text.find("forward hop 3->2 merge=local\n") != std::string::npos);
	CHECK(text.find("forward hop 4->3 merge=local\n") != std::string::npos);
	CHECK(text.find("backward hop 1->4 merge=local\n") != std::string::npos);

	/* A class can mix local and global broadcast recipients. */
	mgr.vertices[mgr.vertex_index_by_rtindex[4]].numsegments = 2;
	rpt_classify_hops(&mgr);
	for (const auto &hop : mgr.graph[1].backward_stage.out)
		CHECK(hop.local == (hop.other_rtindex != 4));

	/* Bypassing an unfiltered table preserves policy-local transfer too. */
	RptTransferMgr bypass;
	vertex(bypass, 1, 100, true);
	vertex(bypass, 2, 10, true);
	vertex(bypass, 3, 50, false);
	for (auto &v : bypass.vertices)
		hash_policy(v);
	connect(bypass, edge(1, 3, true));
	connect(bypass, edge(2, 3));
	bypass.vertices[2].unfiltered = true;
	rpt_skip_unfiltered(&bypass);
	const auto &e = bypass.neighbor_matrix.at(1).at(2);
	RptGraphEdge hop = {};
	hop.other_rtindex = 2;
	hop.left_keys = {e.clauses[0].first};
	hop.right_keys = {e.clauses[0].second};
	CHECK(rpt_local_hop(&bypass, 1, hop));
}

static void replicated_and_compound_policies()
{
	RptTransferMgr mgr;
	vertex(mgr, 1, 100, true);
	vertex(mgr, 2, 10, true);
	for (auto &v : mgr.vertices)
		hash_policy(v);
	RptGraphEdge hop = {};
	hop.other_rtindex = 2;
	hop.left_keys = {{1, 1}};
	hop.right_keys = {{2, 1}};
	/* No neighbor-matrix edge is needed to prove policy locality. */
	CHECK(rpt_local_hop(&mgr, 1, hop));
	auto &src = mgr.vertices[0];
	auto &dst = mgr.vertices[1];
	src.distkeys = {1, 2};
	src.distopclasses = {1978, 1978};
	dst.distkeys = {1, 2};
	dst.distopclasses = {1978, 1978};
	CHECK(!rpt_local_hop(&mgr, 1, hop)); /* Missing a distribution column. */
	hop.left_keys.push_back({1, 2});
	hop.right_keys.push_back({2, 2});
	CHECK(rpt_local_hop(&mgr, 1, hop));
	std::swap(dst.distkeys[0], dst.distkeys[1]);
	CHECK(!rpt_local_hop(&mgr, 1, hop)); /* Hash order matters. */

	src.replicated = true;
	src.distkeys.clear();
	src.distopclasses.clear();
	CHECK(rpt_local_hop(&mgr, 1, hop));
	src.numsegments = 4;
	CHECK(rpt_local_hop(&mgr, 1, hop));
	src.numsegments = 2;
	CHECK(!rpt_local_hop(&mgr, 1, hop));
	src.numsegments = 3;
	dst.replicated = true;
	CHECK(!rpt_local_hop(&mgr, 1, hop)); /* Into replicated always global. */
	src.replicated = false;
	CHECK(!rpt_local_hop(&mgr, 1, hop));
	dst.replicated = false;
	CHECK(!rpt_local_hop(&mgr, 1, hop)); /* Random/unknown source. */
}

static void index_only_endpoint()
{
	IndexOnlyScan scan = {};
	NodeSetTag(&scan, T_IndexOnlyScan);
	scan.scan.scanrelid = 7;
	TargetEntry output = {};
	Var index_var = {};
	NodeSetTag(&index_var, T_Var);
	index_var.varno = INDEX_VAR;
	index_var.varattno = 1;
	output.expr = (Expr *) &index_var;
	Var base_var = {};
	NodeSetTag(&base_var, T_Var);
	base_var.varno = 7;
	base_var.varattno = 3;
	TargetEntry index_column = {};
	index_column.expr = (Expr *) &base_var;
	/* A one-cell PostgreSQL list, without requiring palloc in the driver. */
	ListCell out_cell = {};
	out_cell.ptr_value = &output;
	List out_list = {};
	out_list.type = T_List;
	out_list.length = out_list.max_length = 1;
	out_list.elements = &out_cell;
	ListCell index_cell = {};
	index_cell.ptr_value = &index_column;
	List index_list = out_list;
	index_list.elements = &index_cell;
	scan.scan.plan.targetlist = &out_list;
	scan.indextlist = &index_list;
	Plan *resolved = NULL;
	AttrNumber attno = 0;
	CHECK(rpt_resolve_key_scan((Plan *) &scan, 1, &resolved, &attno));
	CHECK(resolved == (Plan *) &scan && attno == 3);
}

/*
 * Interface tests for the helpers RPT shares with Anser.  Their correctness
 * is Anser's (gpcontrib/anser/test/anserapi_test.c); these check only what
 * RPT relies on: the policy it passes, the coordinate it reads back and how
 * it maps the answers onto its graph.
 */
namespace {
struct Fixture
{
	std::deque<std::vector<ListCell> > cells;
	std::deque<List> lists;

	List *list(const std::vector<void *> &items)
	{
		cells.emplace_back(items.size());
		for (size_t i = 0; i < items.size(); ++i)
			cells.back()[i].ptr_value = items[i];
		lists.emplace_back();
		List &l = lists.back();
		l.type = T_List;
		l.length = l.max_length = (int) items.size();
		l.elements = cells.back().data();
		return &l;
	}
	Var *var(int varno, AttrNumber attno, Oid type = INT4OID)
	{
		Var *v = new Var();
		NodeSetTag(v, T_Var);
		v->varno = varno;
		v->varattno = attno;
		v->vartype = type;
		return v;
	}
	List *tlist(std::initializer_list<Var *> vars)
	{
		std::vector<void *> entries;
		for (Var *v : vars)
		{
			TargetEntry *t = new TargetEntry();
			NodeSetTag(t, T_TargetEntry);
			t->expr = (Expr *) v;
			t->resno = (AttrNumber) entries.size() + 1;
			entries.push_back(t);
		}
		return list(entries);
	}
	template <typename T> T *plan(NodeTag tag, List *tlist,
								  Plan *left = NULL, Plan *right = NULL)
	{
		T *node = new T();
		NodeSetTag(node, tag);
		((Plan *) node)->targetlist = tlist;
		((Plan *) node)->lefttree = left;
		((Plan *) node)->righttree = right;
		return node;
	}
	template <typename T> Plan *scan(NodeTag tag, Index rti, List *tlist)
	{
		T *node = plan<T>(tag, tlist);
		((Scan *) node)->scanrelid = rti;
		return (Plan *) node;
	}
};
}

/*
 * rpt_resolve_key_scan over AnserFindKeyScan.  RPT expects: the TABLE attno
 * back, not the scan's output position; MergeJoin, NestLoop, Sort and
 * Material crossed in addition to Anser's set; index and bitmap scans as
 * vertices; Anser's and RPT's own identity filters seen through; and a
 * boundary (Result, Agg) refused at the top as well as below.
 */
static void rpt_key_descent_policy()
{
	Fixture f;
	Plan *resolved = NULL;
	AttrNumber attno = 0;
	Plan *scan = f.scan<SeqScan>(T_SeqScan, 3, f.tlist({f.var(3, 5), f.var(3, 9)}));
	Plan *other = f.scan<SeqScan>(T_SeqScan, 4, f.tlist({f.var(4, 1)}));
	Plan *sort = (Plan *) f.plan<Sort>(T_Sort, f.tlist({f.var(OUTER_VAR, 2)}), scan);
	Plan *mat = (Plan *) f.plan<Material>(T_Material, f.tlist({f.var(OUTER_VAR, 1)}), sort);
	Plan *nl = (Plan *) f.plan<NestLoop>(T_NestLoop,
		f.tlist({f.var(OUTER_VAR, 1), f.var(INNER_VAR, 1)}), other, mat);
	Plan *mj = (Plan *) f.plan<MergeJoin>(T_MergeJoin, f.tlist({f.var(OUTER_VAR, 2)}),
		nl, f.scan<SeqScan>(T_SeqScan, 5, f.tlist({f.var(5, 1)})));

	/* MergeJoin -> NestLoop inner -> Material -> Sort -> output 2 = table attno 9. */
	CHECK(rpt_resolve_key_scan(mj, 1, &resolved, &attno));
	CHECK(resolved == scan && attno == 9);

	CHECK(rpt_resolve_key_scan(f.scan<IndexScan>(T_IndexScan, 6, f.tlist({f.var(6, 3)})),
							   1, &resolved, &attno) && attno == 3);
	CHECK(rpt_resolve_key_scan(f.scan<BitmapHeapScan>(T_BitmapHeapScan, 7, f.tlist({f.var(7, 4)})),
							   1, &resolved, &attno) && attno == 4);

	/* Identity filters, Anser's and RPT's own. */
	CustomScan *filter = f.plan<CustomScan>(T_CustomScan, f.tlist({f.var(INDEX_VAR, 1)}));
	filter->custom_plans = f.list({scan});
	CHECK(!rpt_resolve_key_scan(&filter->scan.plan, 1, &resolved, &attno));
	test_anser_filter = &filter->scan.plan;
	CHECK(rpt_resolve_key_scan(&filter->scan.plan, 1, &resolved, &attno));
	CHECK(resolved == scan && attno == 5);
	test_anser_filter = NULL;
	test_local_identity = &filter->scan.plan;
	CHECK(rpt_resolve_key_scan(&filter->scan.plan, 1, &resolved, &attno));
	CHECK(resolved == scan && attno == 5);
	test_local_identity = NULL;

	/* Boundaries: a Result on top, an Agg below a Hash. */
	CHECK(!rpt_resolve_key_scan((Plan *) f.plan<Result>(T_Result,
		f.tlist({f.var(OUTER_VAR, 1)}), scan), 1, &resolved, &attno));
	Plan *agg = (Plan *) f.plan<Agg>(T_Agg, f.tlist({f.var(OUTER_VAR, 1)}), scan);
	CHECK(!rpt_resolve_key_scan((Plan *) f.plan<Hash>(T_Hash,
		f.tlist({f.var(OUTER_VAR, 1)}), agg), 1, &resolved, &attno));
}

/*
 * RptCollectGraph over AnserForEachChildPlan and AnserEqualityKeys.  RPT
 * expects a join nested under any child list (CustomScan, SubqueryScan,
 * Append) to be found, the equality's sides -- not its operand order --
 * mapped onto the edge's vertices, the validated key type recorded, and an
 * equality Anser refuses to produce no edge.
 */
static void rpt_graph_uses_shared_walk_and_keys()
{
	for (Oid type : {INT4OID, FLOAT8OID})
	{
		Fixture f;
		Plan *probe = f.scan<SeqScan>(T_SeqScan, 1, f.tlist({f.var(1, 4, type)}));
		Plan *build = f.scan<SeqScan>(T_SeqScan, 2, f.tlist({f.var(2, 6, type)}));
		Plan *hash = (Plan *) f.plan<Hash>(T_Hash, f.tlist({f.var(OUTER_VAR, 1, type)}), build);
		HashJoin *join = f.plan<HashJoin>(T_HashJoin, f.tlist({f.var(OUTER_VAR, 1, type)}),
										  probe, hash);
		OpExpr *op = new OpExpr();
		NodeSetTag(op, T_OpExpr);
		op->opno = 96;			/* int4eq; the stub type cache knows only int4 */
		op->args = f.list({f.var(INNER_VAR, 1, type), f.var(OUTER_VAR, 1, type)});
		join->hashclauses = f.list({op});
		Append *append = f.plan<Append>(T_Append, NIL);
		append->appendplans = f.list({&join->join.plan});
		SubqueryScan *subquery = f.plan<SubqueryScan>(T_SubqueryScan, NIL);
		subquery->subplan = &append->plan;
		CustomScan *top = f.plan<CustomScan>(T_CustomScan, NIL);
		top->custom_plans = f.list({&subquery->scan.plan});
		PlannedStmt stmt = {};
		stmt.planTree = &top->scan.plan;
		RptTransferMgr mgr;

		RptCollectGraph(&stmt, &mgr);
		if (type != INT4OID)
		{
			CHECK(mgr.neighbor_matrix.empty());
			continue;
		}
		const RptEdgeInfo &e = mgr.neighbor_matrix.at(1).at(2);
		CHECK(e.key_type == INT4OID && e.clauses.size() == 1);
		CHECK(e.clauses[0].first.rtindex == 1 && e.clauses[0].first.attno == 4);
		CHECK(e.clauses[0].second.rtindex == 2 && e.clauses[0].second.attno == 6);
	}
}

static void join_protection()
{
	for (JoinType type : {JOIN_INNER, JOIN_SEMI, JOIN_LEFT, JOIN_RIGHT,
						  JOIN_FULL, JOIN_ANTI, JOIN_LASJ_NOTIN})
	{
		RptTransferMgr mgr;
		SeqScan scans[2] = {};
		Var vars[2] = {};
		TargetEntry entries[2] = {};
		ListCell cells[2] = {};
		List lists[2] = {};
		for (int i = 0; i < 2; ++i)
		{
			NodeSetTag(&scans[i], T_SeqScan);
			scans[i].scan.scanrelid = i + 1;
			scans[i].scan.plan.plan_rows = 100 - i;
			NodeSetTag(&vars[i], T_Var);
			vars[i].varno = i + 1;
			vars[i].varattno = 1;
			entries[i].expr = (Expr *) &vars[i];
			cells[i].ptr_value = &entries[i];
			lists[i].type = T_List;
			lists[i].length = lists[i].max_length = 1;
			lists[i].elements = &cells[i];
			scans[i].scan.plan.targetlist = &lists[i];
			scans[i].scan.plan.qual = &lists[i]; /* mark both scans filtered */
		}
		PlannedStmt stmt = {};
		rpt_add_edge(&stmt, &mgr, type, (Plan *) &scans[0], 1,
					 (Plan *) &scans[1], 1, 23);
		if (type == JOIN_FULL || type == JOIN_ANTI || type == JOIN_LASJ_NOTIN)
		{
			CHECK(mgr.vertices.empty() && mgr.neighbor_matrix.empty());
			continue;
		}
		const auto &e = mgr.neighbor_matrix.at(1).at(2);
		CHECK(e.protect_left == (type == JOIN_LEFT));
		CHECK(e.protect_right == (type == JOIN_RIGHT));
		RptBuildClasses(&mgr);
		RptBuildTransferPlan(NULL, &mgr);
		if (type == JOIN_LEFT || type == JOIN_RIGHT)
		{
			int preserved = type == JOIN_LEFT ? 1 : 2;
			CHECK(mgr.graph[preserved].forward_stage.in.empty());
			CHECK(mgr.graph[preserved].backward_stage.in.empty());
			RptTransferMgr runtime;
			RptBuildForwardPlan(&mgr, &runtime);
			CHECK(runtime.graph[preserved].forward_stage.in.empty());
			CHECK(runtime.graph[preserved].backward_stage.in.empty());
		}
	}
}

static void directed_parent_choice()
{
	RptTransferMgr mgr;
	for (int rtindex = 1; rtindex <= 3; ++rtindex)
		vertex(mgr, rtindex, 100 / rtindex, true);
	std::set<int> constructed = {1, 2};
	std::set<int> remaining = {3};
	std::map<int, int> forward_depth = {{1, 0}, {2, 3}};
	std::map<int, int> backward_depth = forward_depth;
	int forward_parent_rtindex, backward_parent_rtindex, target_rtindex;

	connect(mgr, edge(1, 3));
	connect(mgr, edge(2, 3, true)); /* deepest parent blocks 3 -> 2 */
	CHECK(rpt_find_edge(&mgr, constructed, remaining, forward_depth, backward_depth,
						&forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex));
	CHECK(target_rtindex == 3 && forward_parent_rtindex == 1 && backward_parent_rtindex == 1);

	mgr.neighbor_matrix.clear();
	connect(mgr, edge(1, 3, false, true)); /* shallowest blocks 1 -> 3 */
	connect(mgr, edge(2, 3));
	CHECK(rpt_find_edge(&mgr, constructed, remaining, forward_depth, backward_depth,
						&forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex));
	CHECK(forward_parent_rtindex == 2 && backward_parent_rtindex == 2);

	mgr.neighbor_matrix.clear();
	RptEdgeInfo heavy = edge(1, 3, true);
	heavy.clauses.push_back(std::make_pair(RptKey{1, 2}, RptKey{3, 2}));
	connect(mgr, heavy);
	connect(mgr, edge(2, 3));
	CHECK(rpt_find_edge(&mgr, constructed, remaining, forward_depth, backward_depth,
						&forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex));
	CHECK(forward_parent_rtindex == 2 && backward_parent_rtindex == 1); /* lower-weight open edge */

	mgr.neighbor_matrix.clear();
	connect(mgr, edge(1, 3, true));
	connect(mgr, edge(2, 3, true));
	CHECK(rpt_find_edge(&mgr, constructed, remaining, forward_depth, backward_depth,
						&forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex));
	CHECK(forward_parent_rtindex == -1 && backward_parent_rtindex == 1);

	mgr.neighbor_matrix.clear();
	connect(mgr, edge(1, 3, false, true));
	connect(mgr, edge(2, 3, false, true));
	CHECK(rpt_find_edge(&mgr, constructed, remaining, forward_depth, backward_depth,
						&forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex));
	CHECK(forward_parent_rtindex == 2 && backward_parent_rtindex == -1);
}

static void forward_root_includes_unfiltered_fact()
{
	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false);
	vertex(analysis, 2, 100, true);
	vertex(analysis, 3, 50, true);
	connect(analysis, edge(1, 2));
	connect(analysis, edge(1, 3));
	RptBuildClasses(&analysis);
	RptBuildTransferPlan(NULL, &analysis);
	CHECK(analysis.root_rtindexes.count(2));
	RptTransferMgr forward;
	RptBuildForwardPlan(&analysis, &forward);
	CHECK(forward.root_rtindexes.size() == 1 && forward.root_rtindexes.count(1));
	CHECK(!forward.graph.at(1).forward_stage.in.empty());
	CHECK(forward.graph.at(1).forward_stage.out.empty());
	CHECK(forward.reducer_order_rtindexes.size() == 2);
	CHECK(analysis.root_rtindexes.count(2));
	CHECK(analysis.groups.begin()->second.leader_rtindex == 2);
	CHECK(forward.backward_consumers.empty());
	CHECK(!forward.graph.at(1).backward_stage.out.empty());
	for (const auto &group : forward.groups)
		CHECK(group.second.leader_rtindex == 1);
	/* Rebuilding must discard the previous schedule. */
	RptBuildForwardPlan(&analysis, &forward);
	CHECK(forward.reducer_order_rtindexes.size() == 2);
}

static void error_unwinds()
{
	int destroyed = 0;
	struct Owner {
		int &count;
		~Owner() { ++count; }
	};
	try
	{
		Owner owner{destroyed};
		RptPgCall([]() { siglongjmp(*PG_exception_stack, 1); });
		abort();
	}
	catch (const RptPgError &) {}
	CHECK(destroyed == 1);
	CHECK(PG_exception_stack == NULL);
	/* A C++ callback exception must restore PG's exception stack too. */
	try
	{
		RptPgCall([]() { throw std::bad_alloc(); });
		abort();
	}
	catch (const std::bad_alloc &) {}
	CHECK(PG_exception_stack == NULL);
}

static bool size_one_mib(double, int64 *elems, int64 *payload, int64 *planned)
{
	*elems = 1000; *payload = 1 << 20; *planned = 1 << 20;
	return true;
}

static bool size_declined(double, int64 *, int64 *, int64 *)
{
	return false;
}

static RptTransferMgr forward_star()
{
	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false);
	vertex(analysis, 2, 100, true);
	vertex(analysis, 3, 50, true);
	connect(analysis, edge(1, 2));
	connect(analysis, edge(1, 3));
	RptBuildClasses(&analysis);
	rpt_max_chain_depth = 4;
	RptBuildTransferPlan(NULL, &analysis);
	RptTransferMgr forward;
	RptBuildForwardPlan(&analysis, &forward);
	return forward;
}

static RptForwardParams forward_params(double budget, double min_rows)
{
	RptForwardParams p;
	p.width = 3; p.ms_per_mib_part = 3.5;
	p.max_exchange_ms = budget; p.min_root_rows = min_rows;
	return p;
}

static void forward_selection_star()
{
	RptTransferMgr forward = forward_star();
	std::vector<RptForwardComponent> comps;
	RptSelectForward(forward, forward_params(-1, 0), {}, size_one_mib, nullptr, &comps);
	CHECK(comps.size() == 1);
	const RptForwardComponent &c = comps[0];
	CHECK(c.root_rtindex == 1 && c.accepted && c.reason == "inject");
	CHECK(!c.chain_rtindexes.empty() && !c.channels.empty());
	CHECK(std::find(c.chain_rtindexes.begin(), c.chain_rtindexes.end(), 1) == c.chain_rtindexes.end());
	double cost = 0;
	bool reaches_root = false;
	for (const RptForwardChannel &ch : c.channels)
	{
		/* producers before consumers: a channel's source precedes its target */
		auto source_position = std::find(c.chain_rtindexes.begin(), c.chain_rtindexes.end(), ch.source_rtindex);
		CHECK(source_position != c.chain_rtindexes.end());
		if (ch.target_rtindex == c.root_rtindex)
			reaches_root = true;
		else
		{
			auto target_position = std::find(c.chain_rtindexes.begin(), c.chain_rtindexes.end(), ch.target_rtindex);
			CHECK(target_position != c.chain_rtindexes.end() && source_position < target_position);
		}
		CHECK(ch.condition_key.empty()); /* assigned by the shared injector */
		CHECK(ch.source_key.rtindex == ch.source_rtindex && ch.target_key.rtindex == ch.target_rtindex);
		CHECK(ch.est_cost_ms > 10.49 && ch.est_cost_ms < 10.51);	/* 3.5 * 3 * 1 MiB */
		cost += ch.est_cost_ms;
	}
	CHECK(reaches_root);
	CHECK(c.est_cost_ms > cost - 1e-9 && c.est_cost_ms < cost + 1e-9);
	std::string dump;
	RptDumpForward(comps, &dump);
	CHECK(dump.find("forward component root=1") == 0);
	CHECK(dump.find("decision=inject") != std::string::npos);
}

static void forward_total_cost_and_weak_channels()
{
	RptTransferMgr forward = forward_star();
	RptForwardParams params = forward_params(-1, 0);
	std::vector<RptForwardComponent> comps;
	std::map<int, RptForwardVertexEst> estimates;
	params.use_total_cost = true;

	/* A selective sibling cannot justify building a full-domain filter. */
	estimates[2].rows = 400000;
	estimates[2].quals_dropped = true;
	estimates[3].rows = 500;
	estimates[3].selectivity = 0.1;
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(comps.size() == 1 && comps[0].accepted);
	CHECK(comps[0].channels.size() == 1 && comps[0].channels[0].source_rtindex == 3);
	CHECK(comps[0].chain_rtindexes == std::vector<int>{3});
	CHECK(comps[0].est_work_ms > comps[0].est_cost_ms);

	/* A matching in-place filter makes the only useful exchange redundant. */
	const auto &channel = comps[0].channels.front();
	params.inplace_transfers.emplace(channel.source_key, channel.target_key);
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted && comps[0].channels.empty());
	params.inplace_transfers.clear();

	/* A selective sibling whose filter cannot be sized is also optional. */
	estimates[2].quals_dropped = false;
	estimates[2].selectivity = 0.5;
	RptSelectForward(forward, params, estimates,
		[](double rows, int64 *elems, int64 *payload, int64 *planned) {
			return rows < 100000 && size_one_mib(rows, elems, payload, planned);
		}, nullptr, &comps);
	CHECK(comps[0].accepted);
	CHECK(comps[0].channels.size() == 1 && comps[0].channels[0].source_rtindex == 3);
	CHECK(comps[0].chain_rtindexes == std::vector<int>{3});

	/* The same transfer stops paying when the producer needs a large scan. */
	estimates[2].selectivity = 1.0;
	estimates[3].rows = 10000000;
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted);
	CHECK(comps[0].est_work_ms > comps[0].est_benefit_ms);
	CHECK(comps[0].reason == "estimated reducer and probing work exceeds downstream savings");
}

static void forward_filter_reaches_root_and_returns()
{
	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false); // sales
	vertex(analysis, 2, 100000, false);  // returns
	vertex(analysis, 3, 10, true);       // filtered item
	connect(analysis, edge(1, 2));
	connect(analysis, edge(1, 2, false, false, 2, 2)); // ticket equality
	connect(analysis, edge(2, 3));
	RptBuildClasses(&analysis);

	RptTransferMgr forward;
	RptBuildForwardPlan(&analysis, &forward);
	CHECK(forward.root_rtindexes.count(1));
	CHECK(forward.graph.at(2).forward_stage.out.empty());

	RptForwardParams params = forward_params(-1, 0);
	params.use_total_cost = true;
	params.delivery_ms_per_mib = 0.8;
	params.main_consumer_processes[2] = 3;
	params.main_consumer_keys.insert({2, 1});
	std::map<int, RptForwardVertexEst> estimates;
	estimates[3].rows = 1000;
	estimates[3].selectivity = 0.01;
	std::vector<RptForwardComponent> comps;
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(comps.size() == 1 && comps[0].accepted);
	CHECK(comps[0].chain_rtindexes == std::vector<int>{3});
	CHECK(comps[0].channels.size() == 2);
	const auto &root = comps[0].channels[0];
	const auto &subscriber = comps[0].channels[1];
	CHECK(root.source_rtindex == 3 && root.target_rtindex == 1 && !root.main_only);
	CHECK(subscriber.source_key == root.source_key);
	CHECK(subscriber.target_rtindex == 2 && subscriber.main_only);
	CHECK(std::abs(subscriber.est_cost_ms - 2.4) < 1e-9);
}

static void forward_key_survival_and_selection()
{
	CHECK(std::abs(RptRetainedKeys(100, 100, 0.4) - 40) < 1e-9);
	CHECK(std::abs(RptRetainedKeys(100000, 100, 0.4) - 100) < 1e-9);

	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false);
	vertex(analysis, 2, 40000, true);
	connect(analysis, edge(1, 2));
	connect(analysis, edge(1, 2, false, false, 2, 2));
	RptBuildClasses(&analysis);
	RptTransferMgr forward;
	RptBuildForwardPlan(&analysis, &forward);

	RptForwardParams params = forward_params(-1, 0);
	params.use_total_cost = true;
	std::map<int, RptForwardVertexEst> estimates;
	estimates[2].rows = 100000;
	estimates[2].selectivity = 0.4;
	std::vector<RptForwardComponent> components;
	auto ndv = [](const RptVertexInfo &, AttrNumber key) -> double {
		return key == 1 ? 100 : 100000;
	};

	RptSelectForward(forward, params, estimates, size_one_mib, ndv, &components);
	CHECK(components[0].channels[0].source_key.attno == 1);
	params.key_survival = true;
	RptSelectForward(forward, params, estimates, size_one_mib, ndv, &components);
	CHECK(components[0].channels.size() == 1);
	CHECK(components[0].channels[0].source_key.attno == 2);
	CHECK(std::abs(components[0].channels[0].est_keep - 0.4) < 1e-9);

	RptSelectForward(forward, params, estimates, size_one_mib,
		[](const RptVertexInfo &, AttrNumber) -> double { return 100; }, &components);
	CHECK(!components[0].accepted && components[0].channels.empty());
}

static void forward_selection_gates()
{
	RptTransferMgr forward = forward_star();
	std::vector<RptForwardComponent> comps;

	RptSelectForward(forward, forward_params(-1, 2000000), {}, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted && comps[0].reason == "root rows below rpt.forward_min_root_rows");
	for (const RptForwardChannel &ch : comps[0].channels)
		CHECK(ch.condition_key.empty());

	RptSelectForward(forward, forward_params(1.0, 0), {}, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted &&
		  comps[0].reason == "estimated exchange cost above rpt.forward_max_exchange_ms");

	RptSelectForward(forward, forward_params(-1, 0), {}, size_declined, nullptr, &comps);
	CHECK(!comps[0].accepted && comps[0].reason.find("filter sizing declined") == 0);

	/* nothing filtered: no table can produce, so there is no chain */
	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false);
	vertex(analysis, 2, 100, false);
	connect(analysis, edge(1, 2));
	RptBuildClasses(&analysis);
	RptBuildTransferPlan(NULL, &analysis);
	RptTransferMgr bare;
	RptBuildForwardPlan(&analysis, &bare);
	RptSelectForward(bare, forward_params(-1, 0), {}, size_one_mib, nullptr, &comps);
	for (const RptForwardComponent &c : comps)
		CHECK(!c.accepted && c.reason == "no forward edges");

	/* ids restart per call and only accepted components consume them */
	RptSelectForward(forward, forward_params(-1, 0), {}, size_one_mib, nullptr, &comps);
	CHECK(comps[0].channels[0].condition_id == 0);
}

/* Snowflake: fact 1 joins dimension 2 on 1.1 = 2.1, dimension 2 joins
 * subdimension 3 on 2.2 = 3.1 -- a genuine chain 3 -> 2 -> 1.  Colocated
 * means 2 and 3 are both hash-distributed on their join columns, so 3 -> 2
 * is a local hop; 2 -> 1 never is. */
static RptTransferMgr forward_chain(bool colocated)
{
	RptTransferMgr analysis;
	vertex(analysis, 1, 1000000, false);
	vertex(analysis, 2, 100, true);
	vertex(analysis, 3, 50, true);
	for (auto &v : analysis.vertices)
		hash_policy(v);
	analysis.vertices[analysis.vertex_index_by_rtindex[2]].distkeys = {(AttrNumber) (colocated ? 2 : 3)};
	connect(analysis, edge(1, 2));
	connect(analysis, edge(2, 3, false, false, 2, 1));
	RptBuildClasses(&analysis);
	rpt_max_chain_depth = 4;
	RptBuildTransferPlan(NULL, &analysis);
	RptTransferMgr forward;
	RptBuildForwardPlan(&analysis, &forward);
	return forward;
}

static std::vector<double> sized_rows;

static bool size_recording(double rows, int64 *elems, int64 *payload, int64 *planned)
{
	sized_rows.push_back(rows);
	return size_one_mib(rows, elems, payload, planned);
}

static double ndv_small_dims(const RptVertexInfo &v, AttrNumber)
{
	return v.rtindex == 1 ? 1000 : 100;
}

static double ndv_large_dims(const RptVertexInfo &v, AttrNumber)
{
	return v.rtindex == 1 ? 100 : 1000;
}

static void forward_selection_chain_and_selectivity()
{
	RptTransferMgr forward = forward_chain(false);
	std::vector<RptForwardComponent> comps;
	RptForwardParams params = forward_params(-1, 0);
	std::map<int, RptForwardVertexEst> est;

	params.max_selectivity = 0.35;
	est[2].rows = 100; est[2].selectivity = 0.1;
	est[3].rows = 50; est[3].selectivity = 0.5;
	sized_rows.clear();
	RptSelectForward(forward, params, est, size_recording, nullptr, &comps);
	CHECK(comps.size() == 1 && comps[0].accepted);
	CHECK(comps[0].chain_rtindexes == std::vector<int>({3, 2}));
	CHECK(comps[0].channels.size() == 2);
	CHECK(comps[0].channels[0].source_rtindex == 3 && comps[0].channels[0].target_rtindex == 2);
	CHECK(comps[0].channels[1].source_rtindex == 2 && comps[0].channels[1].target_rtindex == 1);
	/* 2 keeps 0.1 of itself times 0.5 from 3; the root keeps what 2 keeps */
	CHECK(comps[0].est_selectivity > 0.0499 && comps[0].est_selectivity < 0.0501);
	/* filters are sized for rows after the vertex's own reducer quals, on
	 * all 3 segments: both hops are exchanged and fold every segment's keys */
	CHECK(sized_rows == std::vector<double>({75, 30}));

	/* NDV: a smaller source domain keeps proportionally less of the target */
	RptSelectForward(forward, params, est, size_one_mib, ndv_small_dims, &comps);
	CHECK(comps[0].est_selectivity > 0.00499 && comps[0].est_selectivity < 0.00501);
	/* a larger source domain cannot keep more than its own fraction */
	RptSelectForward(forward, params, est, size_one_mib, ndv_large_dims, &comps);
	CHECK(comps[0].est_selectivity > 0.0499 && comps[0].est_selectivity < 0.0501);

	/* Quals dropped in the reducer: nothing selective is left, the
	 * filters grow to the whole table, and lambda refuses the chain. */
	est[2].selectivity = 1; est[2].quals_dropped = true;
	est[3].selectivity = 1; est[3].quals_dropped = true;
	sized_rows.clear();
	RptSelectForward(forward, params, est, size_recording, nullptr, &comps);
	CHECK(!comps[0].accepted &&
		  comps[0].reason == "estimated root selectivity not below rpt.forward_max_selectivity");
	CHECK(sized_rows == std::vector<double>({150, 300}));
	for (const RptForwardChannel &ch : comps[0].channels)
		CHECK(ch.condition_key.empty());

	/* Just below lambda passes, exactly lambda does not; 1 disables it. */
	est[2].selectivity = 0.34;
	RptSelectForward(forward, params, est, size_one_mib, nullptr, &comps);
	CHECK(comps[0].accepted);
	est[2].selectivity = 0.35;
	RptSelectForward(forward, params, est, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted);
	params.max_selectivity = 1;
	RptSelectForward(forward, params, est, size_one_mib, nullptr, &comps);
	CHECK(comps[0].accepted);
}

/* A local hop consumed only inside the reducer is in-process and free; the
 * hop into the root stays on Anser. */
static void forward_selection_in_process()
{
	RptTransferMgr forward = forward_chain(true);
	std::vector<RptForwardComponent> comps;
	RptForwardParams params = forward_params(-1, 0);

	RptSelectForward(forward, params, {}, size_one_mib, nullptr, &comps);
	CHECK(comps[0].accepted && comps[0].channels.size() == 2);
	const RptForwardChannel &inner = comps[0].channels[0];
	const RptForwardChannel &into_root = comps[0].channels[1];
	CHECK(inner.target_rtindex == 2 && inner.local && inner.in_process && inner.est_cost_ms == 0);
	CHECK(into_root.target_rtindex == 1 && !into_root.local && !into_root.in_process);
	CHECK(comps[0].est_cost_ms > 10.49 && comps[0].est_cost_ms < 10.51);
	std::string dump;
	RptDumpForward(comps, &dump);
	CHECK(dump.find(" local in-process\n") != std::string::npos);
	/* The in-process hop holds one segment's keys, the exchange all three. */
	sized_rows.clear();
	RptSelectForward(forward, params, {}, size_recording, nullptr, &comps);
	CHECK(sized_rows == std::vector<double>({50, 300}));

	/* Keep the root exchange if its build gets a reducer-only restriction. */
	params.use_total_cost = true;
	params.inplace_transfers.emplace(into_root.source_key, into_root.target_key);
	std::map<int, RptForwardVertexEst> estimates;
	estimates[3].selectivity = 0.1;
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(comps[0].accepted && comps[0].channels.size() == 2);
	params.inplace_probe_keys.insert(comps[0].channels.back().target_key);
	RptSelectForward(forward, params, estimates, size_one_mib, nullptr, &comps);
	CHECK(!comps[0].accepted && comps[0].channels.empty());
	CHECK(comps[0].chain_rtindexes.empty());
	CHECK(comps[0].reason == "in-place filter covers 1.1");

	params.inplace_probe_keys.clear();
	params.use_total_cost = false;
	params.inplace_transfers.clear();
	forward = forward_chain(false);
	RptSelectForward(forward, params, {}, size_one_mib, nullptr, &comps);
	for (const RptForwardChannel &ch : comps[0].channels)
		CHECK(!ch.local && !ch.in_process);
}

static void forward_delivery_process_counts()
{
	auto schedule = forward_chain(false);
	auto params = forward_params(-1, 0);
	params.delivery_ms_per_mib = 0.8;
	for (const auto &vertex : schedule.vertices)
		params.main_consumer_processes[vertex.rtindex] = 6;
	std::vector<RptForwardComponent> components;
	RptSelectForward(schedule, params, {}, size_one_mib, nullptr, &components);
	for (const auto &channel : components[0].channels)
	{
		double expected = 10.5 + 0.8 *
			(channel.target_rtindex == components[0].root_rtindex ? 6 : 3);
		CHECK(std::abs(channel.est_cost_ms - expected) < 1e-6);
	}
}

static void qual_function_allowlist()
{
	CHECK(RptQualFunctionSafe(F_INT4LT));
	CHECK(RptQualFunctionSafe(F_INT8EQ));
	CHECK(RptQualFunctionSafe(F_INT24NE));
	CHECK(RptQualFunctionSafe(F_OIDGE));
	CHECK(RptQualFunctionSafe(F_NUMERIC_GE));
	CHECK(RptQualFunctionSafe(F_NUMERIC_LE));
	CHECK(RptExtendedQualFunctionSafe(F_DATE_LE_TIMESTAMP));
	CHECK(RptExtendedQualFunctionSafe(F_FLOAT84GE));
	CHECK(RptExtendedQualFunctionSafe(F_NUMERIC_NE));
	CHECK(!RptExtendedQualFunctionSafe(F_TEXT_LT));
	CHECK(!RptExtendedQualFunctionSafe(F_NUMERIC_DIV));
	CHECK(!RptQualFunctionSafe(F_INT4DIV));
	CHECK(!RptQualFunctionSafe(F_INT4PL));
	CHECK(!RptQualFunctionSafe(1000000)); /* user function, even if leakproof */
	CHECK(!RptQualFunctionSafe(InvalidOid));
	CHECK(RptQualHashFunctionSafe(F_HASHINT4));
	CHECK(!RptQualHashFunctionSafe(F_INT4EQ));
	CHECK(!RptQualHashFunctionSafe(1000000));
}

static void backward_selection()
{
	RptTransferMgr schedule;
	vertex(schedule, 1, 1000, true);
	vertex(schedule, 2, 10000, false);
	vertex(schedule, 3, 10000, false);
	schedule.root_rtindexes.insert(1);
	RptBackwardConsumer decision;
	decision.class_key = decision.producer_key = {1, 1};
	decision.consumer_key = {2, 1};
	decision.producer_rtindex = 1;
	decision.consumer_rtindex = 2;
	decision.producer_slice_index = 1;
	decision.consumer_slice_index = 2;
	decision.eligible = decision.strict_eligible = true;
	decision.estimated_rows_removed = 9000;
	decision.reason = "eligible";
	schedule.backward_consumers.push_back(decision);
	RptForwardParams params = forward_params(-1, 0);
	params.max_selectivity = 0.35;
	params.delivery_ms_per_mib = 0.8;
	std::vector<RptBackwardClass> classes;
	std::map<int, int> widths = {{1, 3}, {2, 6}}; /* parallel consumer */
	auto select = [&](RptFilterSizeFn size_fn = size_one_mib,
					  const std::vector<RptForwardComponent> &forward = {}) {
		RptSelectBackward(schedule, forward, params, widths, size_fn, &classes);
	};
	select();
	CHECK(classes.size() == 1 && classes[0].accepted);
	CHECK(classes[0].consumers[0].accepted);
	params.inplace_probe_keys.insert(decision.consumer_key);
	select();
	CHECK(!classes[0].accepted && !classes[0].consumers[0].accepted);
	CHECK(classes[0].consumers[0].reason == "in-place filter covers 2.1");
	params.inplace_probe_keys.clear();
	select();
	CHECK(classes[0].est_cost_ms > 15.29 && classes[0].est_cost_ms < 15.31);
	/* 1000 rows in each of the producer slice's 3 processes */
	CHECK(classes[0].estimated_producer_rows == 3000);
	params.max_exchange_ms = 15;
	select();
	CHECK(!classes[0].accepted && classes[0].reason.find("exchange cost") != std::string::npos);
	CHECK(!classes[0].consumers[0].accepted);
	params.max_exchange_ms = -1;
	schedule.backward_consumers[0].estimated_rows_removed = 0;
	select();
	CHECK(!classes[0].accepted);
	CHECK(classes[0].consumers[0].reason.find("member selectivity") != std::string::npos);
	RptForwardComponent forward;
	forward.root_rtindex = 1; forward.accepted = true; forward.est_selectivity = 0.1;
	select(size_one_mib, {forward});
	CHECK(classes[0].accepted && classes[0].estimated_producer_rows == 300);
	CHECK(classes[0].consumers[0].estimated_rows_removed == 9000);
	select([](double rows, int64 *elems, int64 *payload, int64 *bytes) {
		*elems = rows; *payload = *bytes = rows * 2; return true;
	}, {forward});
	CHECK(classes[0].total_elems == 300 && classes[0].planned_bytes == 600);
	forward.accepted = false;
	select(size_one_mib, {forward});
	CHECK(!classes[0].accepted && classes[0].estimated_producer_rows == 3000);
	schedule.backward_consumers[0].estimated_rows_removed = -1;
	select();
	CHECK(!classes[0].accepted); /* unknown NDV */
	params.max_selectivity = 1;
	select();
	CHECK(classes[0].accepted);
	params.max_exchange_ms = 0;
	select(); CHECK(!classes[0].accepted); /* unknown NDV still obeys budget */
	params.max_exchange_ms = -1;
	select([](double, int64 *, int64 *, int64 *) { return false; });
	CHECK(!classes[0].accepted && classes[0].reason == "backward filter sizing declined");
	widths[1] = 0;
	select(); CHECK(!classes[0].accepted);
	widths[1] = 3;
	widths[2] = 0;
	select(); CHECK(!classes[0].accepted);
	widths[2] = 3;
	schedule.root_rtindexes.clear();
	select(); CHECK(classes[0].reason == "bridge-led class is deferred");
	schedule.root_rtindexes.insert(1);
	/* A refused first member must not hide a later member's producer width. */
	auto refused = schedule.backward_consumers[0];
	refused.eligible = false; refused.reason = "backward consumer cap exceeded";
	refused.producer_slice_index = -1;
	schedule.backward_consumers.insert(schedule.backward_consumers.begin(), refused);
	select();
	CHECK(classes[0].accepted && !classes[0].consumers[0].accepted);
	CHECK(classes[0].consumers[1].accepted);
	/* A replicated producer's every process reads all of it. */
	schedule.vertices[schedule.vertex_index_by_rtindex.at(1)].replicated = true;
	select();
	CHECK(classes[0].estimated_producer_rows == 1000);
	schedule.vertices[schedule.vertex_index_by_rtindex.at(1)].replicated = false;
}

static void unfiltered_backward_edges()
{
	for (bool filtered : {false, true})
	{
		RptTransferMgr analysis;
		vertex(analysis, 1, 10000, filtered);
		vertex(analysis, 2, 1000, filtered);
		connect(analysis, edge(1, 2));
		RptBuildClasses(&analysis);
		CHECK(!analysis.logical_neighbor_matrix.at(1).empty());
		if (!filtered) CHECK(analysis.neighbor_matrix.at(1).empty());
		RptTransferMgr runtime;
		RptBuildForwardPlan(&analysis, &runtime);
		CHECK(runtime.root_rtindexes == std::set<int>({1}));
		CHECK(runtime.graph.at(1).backward_stage.out.size() == 1);
		CHECK(runtime.graph.at(1).backward_stage.out[0].other_rtindex == 2);
		CHECK(runtime.graph.at(2).backward_stage.in.size() == 1);
		if (!filtered) CHECK(runtime.graph.at(1).forward_stage.in.empty());
	}
}

int main()
{
	rpt_key_descent_policy();
	rpt_graph_uses_shared_walk_and_keys();
	unfiltered_backward_edges();
	forward_delivery_process_counts();
	backward_selection();
	qual_function_allowlist();
	forward_selection_chain_and_selectivity();
	forward_selection_in_process();
	forward_selection_star();
	forward_selection_gates();
	forward_key_survival_and_selection();
	forward_total_cost_and_weak_channels();
	forward_filter_reaches_root_and_returns();
	bypass_direction(false);
	bypass_direction(true);
	protection_and_idempotence();
	asymmetric_shape();
	unfiltered_cycle();
	strict_backward_eligibility();
	subplans_and_external_parameters_are_refused();
	every_global_producer_is_required();
	backward_consumer_cap();
	bridges_and_locality();
	local_star_and_backward();
	replicated_and_compound_policies();
	index_only_endpoint();
	join_protection();
	directed_parent_choice();
	forward_root_includes_unfiltered_fact();
	error_unwinds();
	return 0;
}
