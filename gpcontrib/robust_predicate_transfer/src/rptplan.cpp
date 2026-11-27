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
 * rptplan.cpp
 *	  The asymmetric transfer plan: a Prim spanning forest
 *	  (LargestRootUpdated), forward/backward stage assignment
 *	  (CreateTransferPlanUpdated), backward-consumer eligibility, and
 *	  the deterministic text dump that feeds the regression tests.
 *
 * Ported from the RPT+ artifact (embryo-labs/dynamic-predicate-transfer),
 * src/optimizer/predicate_transfer/transfer_graph_manager.cpp and dag.cpp --
 * MIT license, Copyright 2018-2025 Stichting DuckDB Foundation (see
 * licenses/LICENSE-mit-duckdb-rpt.txt):
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptplan.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"

#include "optimizer/walkers.h"
#include "parser/parsetree.h"
}


#include <sstream>

/*
 * GraphNode::Add (dag.cpp:9-19): find-or-create the edge to `other` in the
 * given stage and direction.
 */
static RptGraphEdge *
rpt_node_add(RptGraphNode *node, int other_rtindex, bool is_forward, bool is_in_edge)
{
	std::vector<RptGraphEdge> &edges = is_forward
		? (is_in_edge ? node->forward_stage.in : node->forward_stage.out)
		: (is_in_edge ? node->backward_stage.in : node->backward_stage.out);
	size_t		i;

	for (i = 0; i < edges.size(); i++)
	{
		if (edges[i].other_rtindex == other_rtindex)
			return &edges[i];
	}
	edges.push_back(RptGraphEdge());
	edges.back().other_rtindex = other_rtindex;
	return &edges.back();
}

/* GraphNode::Add with keys (dag.cpp:21-28). */
static RptGraphEdge *
rpt_node_add_keys(RptGraphNode *node, int other_rtindex,
				  const std::vector<RptKey> &left_keys,
				  const std::vector<RptKey> &right_keys,
				  const std::vector<Oid> &key_types,
				  bool is_forward, bool is_in_edge)
{
	RptGraphEdge *edge = rpt_node_add(node, other_rtindex, is_forward, is_in_edge);

	edge->left_keys = left_keys;
	edge->right_keys = right_keys;
	edge->key_types = key_types;
	return edge;
}

/*
 * RegisterLeader (transfer_graph_manager.hpp:50-55): the first vertex of a
 * class to enter the transfer order becomes the leader the backward
 * broadcast builds its single filter on.
 */
static void
rpt_register_leader(RptTransferMgr *mgr, int rtindex)
{
	const RptVertexInfo &v = mgr->vertices[mgr->vertex_index_by_rtindex[rtindex]];
	std::set<RptKey>::const_iterator kit;

	for (kit = v.join_keys.begin(); kit != v.join_keys.end(); ++kit)
	{
		RptKey		rep = RptFindKeyRoot(*kit, mgr->uf_parent);
		std::map<RptKey, RptKeyGroup>::iterator git = mgr->groups.find(rep);

		if (git != mgr->groups.end() && git->second.leader_rtindex == -1)
		{
			git->second.leader_rtindex = rtindex;
			git->second.leader_key = *kit;
		}
	}
}

/*
 * Algorithm 1: choose the maximum-weight frontier vertex, break ties by
 * cardinality, then select deepest forward / shallowest backward parents.
 * Fully protected edges never participate.
 */
static bool
rpt_find_edge(const RptTransferMgr *mgr, const std::set<int> &constructed,
			  const std::set<int> &unconstructed,
			  const std::map<int, int> &forward_depth,
			  const std::map<int, int> &backward_depth,
			  int *forward_parent_rtindex, int *backward_parent_rtindex, int *target_rtindex)
{
	size_t best_weight = 0;
	int64 best_rows = -1;
	bool found = false;
	auto edge_at = [&](int parent_rtindex, int candidate_rtindex, bool forward)
		-> const RptEdgeInfo * {
		const auto &matrix = forward || mgr->logical_neighbor_matrix.empty()
			? mgr->neighbor_matrix : mgr->logical_neighbor_matrix;
		auto row = matrix.find(parent_rtindex);
		if (row == matrix.end())
			return NULL;
		auto edge = row->second.find(candidate_rtindex);
		if (edge == row->second.end() ||
			(edge->second.protect_left && edge->second.protect_right))
			return NULL;
		return &edge->second;
	};

	/* Grow one forest over both views. Forward pruning must not disconnect
	 * tables that can still receive a useful backward filter. */
	for (int candidate_rtindex : unconstructed)
		for (int parent_rtindex : constructed)
			for (bool forward : {true, false})
			{
				const RptEdgeInfo *edge = edge_at(parent_rtindex, candidate_rtindex, forward);
				if (edge == NULL)
					continue;
				size_t weight = edge->clauses.size();
				int64 rows = mgr->vertices[mgr->vertex_index_by_rtindex.at(candidate_rtindex)].cardinality;
				if (!found || weight > best_weight ||
					(weight == best_weight && rows > best_rows))
				{
					found = true;
					best_weight = weight;
					best_rows = rows;
					*target_rtindex = candidate_rtindex;
				}
			}
	if (!found)
		return false;

	*forward_parent_rtindex = *backward_parent_rtindex = -1;
	for (bool forward : {true, false})
	{
		int *selected = forward ? forward_parent_rtindex : backward_parent_rtindex;
		const auto &depth = forward ? forward_depth : backward_depth;
		size_t best_parent_weight = 0;
		for (int parent_rtindex : constructed)
		{
			const RptEdgeInfo *edge = edge_at(parent_rtindex, *target_rtindex, forward);
			if (edge == NULL)
				continue;
			int source_rtindex = forward ? *target_rtindex : parent_rtindex;
			bool open = edge->left_rtindex == source_rtindex
				? !edge->protect_right : !edge->protect_left;
			size_t weight = edge->clauses.size();
			if (open && (*selected == -1 || weight > best_parent_weight ||
				(weight == best_parent_weight &&
				 (forward ? depth.at(parent_rtindex) > depth.at(*selected)
						  : depth.at(parent_rtindex) < depth.at(*selected)))))
			{
				*selected = parent_rtindex;
				best_parent_weight = weight;
			}
		}
	}
	return true;
}

/*
 * LargestRootUpdated (transfer_graph_manager.cpp:557-626): grow one
 * component of the spanning forest by Prim's algorithm, rooted at the
 * largest filtered-or-intermediate vertex, falling back to the largest
 * vertex.  sorted_rtindexes lists every vertex by ascending cardinality; only
 * those still in `remaining` are candidates, and placed vertices are removed
 * from it (the artifact instead erases from its operator map and can pick a
 * stale root from its sorted list on the next component; keeping `remaining`
 * explicit avoids that).
 */
static void
rpt_largest_root_updated(RptTransferMgr *mgr, std::set<int> &remaining,
						 const std::vector<int> &sorted_rtindexes,
						 std::vector<std::pair<int, int> > &forward_edges,
						 std::vector<std::pair<int, int> > &backward_edges,
						 int32 &next_order, bool largest_relation)
{
	std::map<int, int> forward_depth;
	std::map<int, int> backward_depth;
	std::set<int> constructed_set;
	std::set<int> unconstructed_set;
	std::vector<int>::const_reverse_iterator rit;
	std::set<int>::iterator sit;
	int			root_rtindex = -1;

	/* Try to choose the largest filtered or intermediate table as the root */
	for (rit = sorted_rtindexes.rbegin(); rit != sorted_rtindexes.rend(); ++rit)
	{
		const RptVertexInfo &v = mgr->vertices[mgr->vertex_index_by_rtindex[*rit]];

		if (remaining.count(*rit) &&
			(largest_relation || v.filtered || v.intermediate))
		{
			root_rtindex = *rit;
			break;
		}
	}

	/* If we cannot find it, all tables have no filter */
	if (root_rtindex == -1)
	{
		for (rit = sorted_rtindexes.rbegin(); rit != sorted_rtindexes.rend(); ++rit)
		{
			if (remaining.count(*rit))
			{
				root_rtindex = *rit;
				break;
			}
		}
	}
	Assert(root_rtindex != -1);

	/* Initialize nodes */
	for (sit = remaining.begin(); sit != remaining.end(); ++sit)
	{
		if (*sit == root_rtindex)
			constructed_set.insert(*sit);
		else
			unconstructed_set.insert(*sit);

		mgr->graph[*sit].rtindex = *sit;
		mgr->graph[*sit].cardinality_order = -1;
	}

	/* Add root */
	mgr->root_rtindexes.insert(root_rtindex);
	forward_depth[root_rtindex] = backward_depth[root_rtindex] = 0;
	mgr->graph[root_rtindex].cardinality_order = next_order++;
	mgr->transfer_order_rtindexes.push_back(root_rtindex);
	rpt_register_leader(mgr, root_rtindex);
	remaining.erase(root_rtindex);

	/* Build graph */
	while (!unconstructed_set.empty())
	{
		int			forward_parent_rtindex;
		int			backward_parent_rtindex;
		int			target_rtindex;

		if (!rpt_find_edge(mgr, constructed_set, unconstructed_set,
						   forward_depth, backward_depth,
						   &forward_parent_rtindex, &backward_parent_rtindex, &target_rtindex))
			break;

		if (forward_parent_rtindex != -1)
			forward_edges.push_back(std::make_pair(forward_parent_rtindex, target_rtindex));
		if (backward_parent_rtindex != -1)
			backward_edges.push_back(std::make_pair(backward_parent_rtindex, target_rtindex));
		forward_depth[target_rtindex] = forward_parent_rtindex == -1 ? 0 : forward_depth[forward_parent_rtindex] + 1;
		backward_depth[target_rtindex] = backward_parent_rtindex == -1 ? 0 : backward_depth[backward_parent_rtindex] + 1;
		mgr->graph[target_rtindex].cardinality_order = next_order++;
		mgr->transfer_order_rtindexes.push_back(target_rtindex);
		rpt_register_leader(mgr, target_rtindex);

		unconstructed_set.erase(target_rtindex);
		constructed_set.insert(target_rtindex);
		remaining.erase(target_rtindex);
	}

	/*
	 * Whatever is still unconstructed is disconnected from this root; it
	 * stays in `remaining` and the caller wraps it in another component.
	 */
}

/*
 * CreateTransferPlanUpdated (transfer_graph_manager.cpp:677-734): assign
 * the selected edges to the forward and backward stages.  The smaller table
 * -- the one that entered the Prim order later -- is on the left; forward
 * transfers flow from the smaller to the larger, backward transfers from
 * the larger to the smaller, with backward edges redirected at the
 * equivalence-class leader.
 */
static void
rpt_create_transfer_plan(RptTransferMgr *mgr,
						 const std::vector<std::pair<int, int> > &selected_edges,
						 bool forward)
{
	size_t		i;

	for (i = 0; i < selected_edges.size(); i++)
	{
		std::map<int, RptEdgeInfo>::iterator eit;
		RptEdgeInfo *edge;
		RptGraphNode *left_node;
		RptGraphNode *right_node;
		std::vector<RptKey> left_keys;
		std::vector<RptKey> right_keys;
		std::vector<Oid> key_types;
		bool		protect_left;
		bool		protect_right;
		size_t		c;

		auto &matrix = forward || mgr->logical_neighbor_matrix.empty()
			? mgr->neighbor_matrix : mgr->logical_neighbor_matrix;
		eit = matrix[selected_edges[i].first].find(selected_edges[i].second);
		if (eit == matrix[selected_edges[i].first].end())
			continue;
		edge = &eit->second;

		left_node = &mgr->graph[edge->left_rtindex];
		right_node = &mgr->graph[edge->right_rtindex];
		protect_left = edge->protect_left;
		protect_right = edge->protect_right;

		for (c = 0; c < edge->clauses.size(); c++)
		{
			left_keys.push_back(edge->clauses[c].first);
			right_keys.push_back(edge->clauses[c].second);
			key_types.push_back(edge->key_type);
		}

		/* smaller table is in the left */
		if (left_node->cardinality_order < right_node->cardinality_order)
		{
			std::swap(left_node, right_node);
			std::swap(left_keys, right_keys);
			std::swap(protect_left, protect_right);
		}

		/* forward: from the smaller to the larger */
		if (forward && !protect_right && !mgr->root_rtindexes.count(left_node->rtindex) &&
			(mgr->vertices[mgr->vertex_index_by_rtindex[left_node->rtindex]].filtered ||
			 mgr->bridge_rtindexes.count(left_node->rtindex)))
		{
			rpt_node_add_keys(left_node, right_node->rtindex, left_keys, right_keys,
							  key_types, true, false);
			rpt_node_add_keys(right_node, left_node->rtindex, left_keys, right_keys,
							  key_types, true, true);
		}

		/* backward: from the larger to the smaller */
		if (!forward && !protect_left)
		{
			/*
			 * Group-leader redirection (transfer_graph_manager.cpp:719-732):
			 * the backward edge is built on the class leader, which produces
			 * the single broadcast filter for the whole class.  The
			 * artifact's edges are single-key; ours bucket the clauses by
			 * destination so a multi-key edge can fan out to one leader per
			 * clause class.
			 */
			std::map<int, RptGraphEdge> by_dest;
			std::map<int, RptGraphEdge>::iterator dit;

			for (c = 0; c < left_keys.size(); c++)
			{
				RptKey		rep = RptFindKeyRoot(right_keys[c], mgr->uf_parent);
				std::map<RptKey, RptKeyGroup>::iterator git = mgr->groups.find(rep);
				int			target_rtindex;
				RptKey		dest_key;
				RptGraphEdge *bucket;

				if (git != mgr->groups.end() && git->second.leader_rtindex != -1)
				{
					target_rtindex = git->second.leader_rtindex;
					dest_key = git->second.leader_key;
				}
				else
				{
					target_rtindex = right_node->rtindex;
					dest_key = right_keys[c];
				}

				bucket = &by_dest[target_rtindex];
				bucket->other_rtindex = target_rtindex;
				bucket->left_keys.push_back(left_keys[c]);
				bucket->right_keys.push_back(dest_key);
				bucket->key_types.push_back(key_types[c]);
			}

			for (dit = by_dest.begin(); dit != by_dest.end(); ++dit)
			{
				const RptGraphEdge &bucket = dit->second;

				rpt_node_add_keys(left_node, bucket.other_rtindex,
								  bucket.left_keys, bucket.right_keys,
								  bucket.key_types, false, true);
				rpt_node_add_keys(&mgr->graph[bucket.other_rtindex], left_node->rtindex,
								  bucket.left_keys, bucket.right_keys,
								  bucket.key_types, false, false);
			}
		}
	}
}

/* Locality is a property of the base scans' distribution policies, including
 * for implied and bypass edges. A Motion elsewhere in the main plan does not
 * change where the reducer reads a key. Parallel workers still need a
 * segment-local fold (or a non-parallel reducer). */
static bool
rpt_local_hop(const RptTransferMgr *mgr, int source_rtindex, const RptGraphEdge &hop)
{
	const auto &src = mgr->vertices[mgr->vertex_index_by_rtindex.at(source_rtindex)];
	const auto &dst = mgr->vertices[mgr->vertex_index_by_rtindex.at(hop.other_rtindex)];

	/* A replicated destination needs the complete key set on every segment. */
	if (dst.replicated || src.numsegments <= 0 || dst.numsegments <= 0)
		return false;
	if (src.replicated)
		return src.numsegments >= dst.numsegments;
	if (src.numsegments != dst.numsegments || src.distkeys.empty() ||
		src.distkeys.size() != dst.distkeys.size() ||
		src.distopclasses.size() != src.distkeys.size() ||
		src.distopclasses != dst.distopclasses)
		return false;
	for (size_t col = 0; col < src.distkeys.size(); ++col)
	{
		bool matched = false;
		for (size_t key = 0; key < hop.left_keys.size(); ++key)
		{
			const RptKey &a = hop.left_keys[key];
			const RptKey &b = hop.right_keys[key];
			if ((a == RptKey{source_rtindex, src.distkeys[col]} &&
				 b == RptKey{hop.other_rtindex, dst.distkeys[col]}) ||
				(b == RptKey{source_rtindex, src.distkeys[col]} &&
				 a == RptKey{hop.other_rtindex, dst.distkeys[col]}))
				matched = true;
		}
		if (!matched)
			return false;
	}
	return true;
}

/* Persist the decision on both copies of every stage edge. In/out edges
 * carry the same key pairs; other_rtindex is the producer on an in-edge. */
static void
rpt_classify_hops(RptTransferMgr *mgr)
{
	for (auto &node : mgr->graph)
	{
		for (bool forward : {true, false})
		{
			auto &outs = forward ? node.second.forward_stage.out : node.second.backward_stage.out;
			for (auto &edge : outs)
			{
				edge.local = rpt_local_hop(mgr, node.first, edge);
				auto &target_rtindex = mgr->graph.at(edge.other_rtindex);
				auto &ins = forward ? target_rtindex.forward_stage.in : target_rtindex.backward_stage.in;
				for (auto &in : ins)
					if (in.other_rtindex == node.first)
						in.local = edge.local;
			}
		}
	}
}

/*
 * Build the transfer plan for the join graph: the Prim forest, the
 * forward/backward stage assignment, and the consumer-eligibility check.
 */
static void
rpt_build_transfer_plan(PlannedStmt *stmt, RptTransferMgr *mgr,
						bool largest_relation)
{
	std::vector<std::pair<int, int> > forward_edges;
	std::vector<std::pair<int, int> > backward_edges;
	std::vector<int> sorted_rtindexes;
	std::set<int> remaining;
	int32		next_order = 0;
	size_t		i;

	/* vertices by ascending cardinality, ties by rtindex, for a stable plan */
	for (i = 0; i < mgr->vertices.size(); i++)
	{
		sorted_rtindexes.push_back(mgr->vertices[i].rtindex);
		remaining.insert(mgr->vertices[i].rtindex);
	}
	std::sort(sorted_rtindexes.begin(), sorted_rtindexes.end(),
			  [&mgr] (int a, int b)
	{
		const RptVertexInfo &va = mgr->vertices[mgr->vertex_index_by_rtindex[a]];
		const RptVertexInfo &vb = mgr->vertices[mgr->vertex_index_by_rtindex[b]];

		if (va.cardinality != vb.cardinality)
			return va.cardinality < vb.cardinality;
		return a < b;
	});

	/*
	 * The while-loop forest wrapping (transfer_graph_manager.cpp:677-683):
	 * repeat the Prim pass until every vertex is placed, giving a spanning
	 * forest -- one tree per connected component of the join graph.
	 */
	while (!remaining.empty())
		rpt_largest_root_updated(mgr, remaining, sorted_rtindexes, forward_edges, backward_edges,
								 next_order, largest_relation);

	for (const auto &group : mgr->groups)
		if (group.second.leader_rtindex != -1 && !mgr->root_rtindexes.count(group.second.leader_rtindex))
			mgr->bridge_rtindexes.insert(group.second.leader_rtindex);

	rpt_create_transfer_plan(mgr, forward_edges, true);
	rpt_create_transfer_plan(mgr, backward_edges, false);

	/* Transfers run in reverse insertion order. Cap actual dependency hops,
	 * keeping every inbound edge mirrored in its producer's out list. */
	std::map<int, int> depth;
	for (auto it = mgr->transfer_order_rtindexes.rbegin(); it != mgr->transfer_order_rtindexes.rend(); ++it)
	{
		auto &outs = mgr->graph[*it].forward_stage.out;
		for (auto edge = outs.begin(); edge != outs.end();)
		{
			int target_rtindex = edge->other_rtindex;
			if (depth[*it] >= rpt_max_chain_depth)
			{
				auto &ins = mgr->graph[target_rtindex].forward_stage.in;
				ins.erase(std::remove_if(ins.begin(), ins.end(),
					[&](const RptGraphEdge &in) { return in.other_rtindex == *it; }), ins.end());
				edge = outs.erase(edge);
			}
			else
			{
				depth[target_rtindex] = std::max(depth[target_rtindex], depth[*it] + 1);
				++edge;
			}
		}
		if (!outs.empty())
			mgr->reducer_order_rtindexes.push_back(*it);
	}

	rpt_classify_hops(mgr);
	if (!largest_relation)
		RptCheckBackwardConsumers(stmt, mgr);
}

/* Backward analysis retains the artifact's filtered-root heuristic. */
void
RptBuildTransferPlan(PlannedStmt *stmt, RptTransferMgr *mgr)
{
	rpt_build_transfer_plan(stmt, mgr, false);
}

/* Runtime transfers use one largest-relation forest for both passes.
 * Keep the filtered-root survey schedule separate for diagnostic continuity.
 * Eligibility is evaluated by the caller against this forest before surgery. */
void
RptBuildForwardPlan(const RptTransferMgr *analysis, RptTransferMgr *forward)
{
	*forward = *analysis;
	forward->graph.clear();
	forward->root_rtindexes.clear();
	forward->bridge_rtindexes.clear();
	forward->reducer_order_rtindexes.clear();
	forward->transfer_order_rtindexes.clear();
	forward->consumer_ok_by_rtindex.clear();
	forward->backward_consumers.clear();
	for (auto &group : forward->groups)
		group.second.leader_rtindex = -1;
	rpt_build_transfer_plan(NULL, forward, true);

	/* A filtered leaf can be attached to a sink that does not produce
	 * forward filters. Give that key a direct route to the root when
	 * the unprotected equality graph already proves the route. */
	for (int source : forward->reducer_order_rtindexes)
	{
		const auto &vertex = forward->vertices[forward->vertex_index_by_rtindex.at(source)];
		if (!vertex.filtered)
			continue;
		auto &node = forward->graph.at(source);
		auto adjacent = forward->neighbor_matrix.find(source);
		if (adjacent == forward->neighbor_matrix.end())
			continue;

		for (int root : forward->root_rtindexes)
		{
			auto entry = adjacent->second.find(root);
			if (entry == adjacent->second.end())
				continue;
			const auto &edge = entry->second;
			if (edge.protect_left || edge.protect_right ||
				std::any_of(node.forward_stage.out.begin(), node.forward_stage.out.end(),
					[&](const auto &hop) { return hop.other_rtindex == root; }))
				continue;

			for (const auto &clause : edge.clauses)
			{
				const auto &from = clause.first.rtindex == source ? clause.first : clause.second;
				const auto &to = clause.first.rtindex == source ? clause.second : clause.first;
				bool dead_end = std::any_of(node.forward_stage.out.begin(), node.forward_stage.out.end(),
					[&](const auto &hop) {
						return !hop.left_keys.empty() && hop.left_keys.front() == from &&
							!forward->root_rtindexes.count(hop.other_rtindex) &&
							forward->graph.at(hop.other_rtindex).forward_stage.out.empty();
					});
				if (!dead_end)
					continue;

				auto *out = rpt_node_add_keys(&node, root, {from}, {to}, {edge.key_type}, true, false);
				auto *in = rpt_node_add_keys(&forward->graph.at(root), source,
										 {from}, {to}, {edge.key_type}, true, true);
				out->local = in->local = rpt_local_hop(forward, source, *out);
				break;
			}
		}
	}
}

/*
 * Dump support.  Everything below iterates std::map / sorted copies only,
 * so the output is deterministic and can feed expected-output regression
 * tests.
 */

static bool
rpt_edge_less(const RptGraphEdge &a, const RptGraphEdge &b)
{
	if (a.other_rtindex != b.other_rtindex)
		return a.other_rtindex < b.other_rtindex;
	if (a.left_keys != b.left_keys)
		return a.left_keys < b.left_keys;
	return a.right_keys < b.right_keys;
}

static void
rpt_dump_key_pairs(std::ostream &out, const RptGraphEdge &edge)
{
	for (size_t i = 0; i < edge.left_keys.size(); i++)
	{
		if (i > 0)
			out << ',';
		out << edge.left_keys[i].rtindex << '.' << edge.left_keys[i].attno
			<< "->" << edge.right_keys[i].rtindex << '.' << edge.right_keys[i].attno;
	}
}

/* A stage's sorted build/apply entries, preserving the diagnostic format. */
static void
rpt_dump_stage(std::ostream &out, int rtindex, const char *stage,
			   const std::vector<RptGraphEdge> &in_edges,
			   const std::vector<RptGraphEdge> &out_edges)
{
	std::vector<RptGraphEdge> outs(out_edges);
	std::vector<RptGraphEdge> ins(in_edges);
	bool any = false;

	std::sort(outs.begin(), outs.end(), rpt_edge_less);
	std::sort(ins.begin(), ins.end(), rpt_edge_less);
	out << "vertex " << rtindex << ' ' << stage << ':';
	for (const auto &edge : outs)
	{
		out << (any ? "; " : " ") << "build for " << edge.other_rtindex << " on (";
		rpt_dump_key_pairs(out, edge);
		out << ')';
		any = true;
	}
	for (const auto &edge : ins)
	{
		out << (any ? "; " : " ") << "apply from " << edge.other_rtindex << " on (";
		rpt_dump_key_pairs(out, edge);
		out << ')';
		any = true;
	}
	if (!any)
		out << " none";
	out << '\n';
}

/* Build in C++ storage, then cross the PostgreSQL allocation boundary once.
 * RptPgCall ensures a failed append unwinds the stream and string owners. */
void
RptDumpTransferPlan(const PlannedStmt *stmt, const RptTransferMgr *mgr, StringInfo output)
{
	std::ostringstream out;
	std::vector<int> rtindexes;

	out.exceptions(std::ios::badbit | std::ios::failbit);
	out.imbue(std::locale::classic());
	for (const auto &vertex : mgr->vertices)
		rtindexes.push_back(vertex.rtindex);
	std::sort(rtindexes.begin(), rtindexes.end());

	out << "transfer order:";
	for (int rtindex : mgr->transfer_order_rtindexes)
		out << ' ' << rtindex;
	out << "\nforward depth limit: " << rpt_max_chain_depth << "\nreducer order:";
	for (int rtindex : mgr->reducer_order_rtindexes)
		out << ' ' << rtindex;
	out << "\nbridge tables:";
	for (int rtindex : mgr->bridge_rtindexes)
		out << ' ' << rtindex;
	out << '\n';

	for (int rtindex : rtindexes)
	{
		const auto &v = mgr->vertices[mgr->vertex_index_by_rtindex.at(rtindex)];
		const char *alias = "";

		/* Use the query alias, so self-join references remain distinct. No
		 * name is copied or looked up until a diagnostic dump is requested. */
		if (stmt != NULL && rtindex > 0 && rtindex <= list_length(stmt->rtable))
		{
			RangeTblEntry *rte = rt_fetch(rtindex, stmt->rtable);

			if (rte->eref != NULL && rte->eref->aliasname != NULL)
				alias = rte->eref->aliasname;
		}
		out << "vertex " << v.rtindex << ' ' << alias << " rows=" << v.cardinality
			<< ' ' << (v.filtered ? "filtered" : v.intermediate ? "intermediate" : "unfiltered")
			<< '\n';
	}
	for (const auto &group : mgr->groups)
	{
		bool first = true;

		out << "class " << group.first.rtindex << '.' << group.first.attno << ": members=";
		for (int member_rtindex : group.second.member_rtindexes)
		{
			if (!first)
				out << ',';
			out << member_rtindex;
			first = false;
		}
		out << " leader=" << group.second.leader_rtindex << '\n';
	}
	for (int rtindex : rtindexes)
	{
		const auto &node = mgr->graph.find(rtindex);

		if (node == mgr->graph.end())
		{
			out << "vertex " << rtindex << " forward: none\n"
				<< "vertex " << rtindex << " backward: none\n";
			continue;
		}
		rpt_dump_stage(out, rtindex, "forward", node->second.forward_stage.in,
					   node->second.forward_stage.out);
		rpt_dump_stage(out, rtindex, "backward", node->second.backward_stage.in,
					   node->second.backward_stage.out);
	}
	for (const auto &node : mgr->graph)
	{
		for (const auto &edge : node.second.forward_stage.out)
			out << "forward hop " << node.first << "->" << edge.other_rtindex
				<< " merge=" << (edge.local ? "local" : "global") << '\n';
		for (const auto &edge : node.second.backward_stage.out)
			out << "backward hop " << node.first << "->" << edge.other_rtindex
				<< " merge=" << (edge.local ? "local" : "global") << '\n';
	}
	int global_edges = 0, rule9 = 0, strict = 0, eligible = 0, root_classes = 0;
	int unknown_estimates = 0;
	double rows_removed = 0;
	for (const auto &group : mgr->groups)
		if (mgr->root_rtindexes.count(group.second.leader_rtindex))
			++root_classes;
	for (const auto &decision : mgr->backward_consumers)
	{
		global_edges += !decision.local;
		rule9 += decision.rule9;
		strict += decision.strict_eligible;
		eligible += decision.eligible;
		if (decision.strict_eligible)
		{
			if (decision.estimated_rows_removed < 0)
				++unknown_estimates;
			else
				rows_removed += decision.estimated_rows_removed;
		}
		out << "backward consumer " << decision.consumer_rtindex << " producer " << decision.producer_rtindex
			<< " class " << decision.class_key.rtindex << '.' << decision.class_key.attno
			<< " merge=" << (decision.local ? "local" : "global")
			<< " rule9=" << (decision.rule9 ? "yes" : "no")
			<< " strict=" << (decision.strict_eligible ? "yes" : "no")
			<< " eligible=" << (decision.eligible ? "yes" : "no")
			<< " defer_first=" << (decision.defer_first ? "yes" : "no")
			<< " producer_slice=" << decision.producer_slice_index
			<< " consumer_slice=" << decision.consumer_slice_index
			<< " producer_attno=" << decision.producer_attno
			<< " consumer_attno=" << decision.consumer_attno
			<< " rows_removed=";
		if (decision.estimated_rows_removed < 0)
			out << "unknown";
		else
			out << decision.estimated_rows_removed;
		out << " reason=" << decision.reason << '\n';
	}
	out << "survey classes=" << mgr->groups.size() << " root_classes=" << root_classes
		<< " global_edges=" << global_edges << " rule9=" << rule9 << " strict=" << strict
		<< " eligible=" << eligible << " known_rows_removed=" << rows_removed
		<< " unknown_estimates=" << unknown_estimates << '\n';
	for (int rtindex : rtindexes)
	{
		const auto &consumer = mgr->consumer_ok_by_rtindex.find(rtindex);
		bool ok = consumer != mgr->consumer_ok_by_rtindex.end() && consumer->second;
		out << "consumer_ok " << rtindex << ' ' << (ok ? "yes" : "no");
		if (!ok)
		{
			std::string reason = "no backward edge";
			for (const auto &decision : mgr->backward_consumers)
				if (decision.consumer_rtindex == rtindex && !decision.eligible)
				{
					reason = decision.reason;
					if (!decision.local)
						break;
				}
			out << ": " << reason;
		}
		out << '\n';
	}

	std::string text = out.str();
	RptPgCall([&]() { appendStringInfoString(output, text.c_str()); });
}
