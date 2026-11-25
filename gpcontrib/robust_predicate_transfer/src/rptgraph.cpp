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
 * rptgraph.cpp
 *	  Union-find equivalence classes over join keys, vertex
 *	  classification, and the SkipUnfilteredTable rewiring of the
 *	  join graph.
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
 *	  gpcontrib/robust_predicate_transfer/src/rptgraph.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"
}



/*
 * One (edge, clause) pair.  The artifact's EdgeInfo carries a single join
 * condition; ours merges every equality clause of a vertex pair into one
 * edge, so the SkipUnfilteredTable helpers track the clause alongside the
 * edge wherever the original tracked the edge alone.
 */
struct RptEdgeClause
{
	RptEdgeInfo *edge;
	size_t		clause;
};

/*
 * Find the representative key of a key's equivalence class.  Ports
 * FindBindingRoot (transfer_graph_manager.cpp:12-23), with path
 * compression.
 */
RptKey
RptFindKeyRoot(const RptKey &key, std::map<RptKey, RptKey> &uf_parent)
{
	std::map<RptKey, RptKey>::iterator it = uf_parent.find(key);
	RptKey		representative_key;

	if (it == uf_parent.end())
		return key;

	representative_key = it->second;
	if (!(representative_key == key))
	{
		representative_key = RptFindKeyRoot(representative_key, uf_parent);
		uf_parent[key] = representative_key;	/* path compression */
	}
	return representative_key;
}

/*
 * Merge the equivalence classes of two clause keys.  Ports UnionBindings
 * (transfer_graph_manager.cpp:25-48), union by attaching b to a.  The
 * artifact keeps a group pointer per stale root and resolves the root again
 * at lookup time; mgr->groups is keyed by the representative key, so the
 * loser's entry is merged into the winner's and erased here instead.
 */
static void
rpt_union_keys(const RptKey &a, const RptKey &b, Oid key_type,
			   RptTransferMgr *mgr)
{
	RptKey		root_a = RptFindKeyRoot(a, mgr->uf_parent);
	RptKey		root_b = RptFindKeyRoot(b, mgr->uf_parent);
	std::map<RptKey, RptKeyGroup>::iterator ga;
	std::map<RptKey, RptKeyGroup>::iterator gb;

	if (root_a == root_b)
		return;

	/* Union by attaching b to a */
	mgr->uf_parent[root_b] = root_a;

	ga = mgr->groups.find(root_a);
	if (ga == mgr->groups.end())
	{
		RptKeyGroup	group;

		group.leader_rtindex = -1;
		group.key_type = key_type;
		group.member_rtindexes.insert(a.rtindex);
		ga = mgr->groups.insert(std::make_pair(root_a, group)).first;
	}

	gb = mgr->groups.find(root_b);
	if (gb == mgr->groups.end())
	{
		RptKeyGroup	group;

		group.leader_rtindex = -1;
		group.key_type = key_type;
		group.member_rtindexes.insert(b.rtindex);
		gb = mgr->groups.insert(std::make_pair(root_b, group)).first;
	}

	ga->second.member_rtindexes.insert(gb->second.member_rtindexes.begin(),
							  gb->second.member_rtindexes.end());
	mgr->groups.erase(root_b);
}

/*
 * ClassifyTables (transfer_graph_manager.cpp:265-309): classify all tables
 * into three categories: intermediate table, unfiltered table, and filtered
 * table.
 *
 * The artifact tests LOGICAL_FILTER / LogicalGet.table_filters; our region
 * pass has already set RptVertexInfo.filtered from the scan's restriction
 * quals, which is the same test seen one plan layer down.  The artifact
 * counts the group of each incident edge's left binding; we count the group
 * of each of the vertex's own join keys.  The two differ only on protected
 * edges, whose keys were never unioned and so count as groups of their own
 * here (the artifact conflates all of them into one null group).
 */
static void
rpt_classify_tables(RptTransferMgr *mgr)
{
	size_t		i;

	for (i = 0; i < mgr->vertices.size(); i++)
	{
		RptVertexInfo *v = &mgr->vertices[i];
		std::set<RptKey> belong_groups;
		std::set<RptKey>::const_iterator kit;

		v->intermediate = false;
		v->unfiltered = false;

		/* 1. Filtered table: the scan carries restriction quals */
		if (v->filtered)
			continue;

		/* 2. Find belong groups */
		for (kit = v->join_keys.begin(); kit != v->join_keys.end(); ++kit)
		{
			if (mgr->uf_parent.find(*kit) != mgr->uf_parent.end() ||
				mgr->groups.find(*kit) != mgr->groups.end())
				belong_groups.insert(RptFindKeyRoot(*kit, mgr->uf_parent));
			else
				belong_groups.insert(*kit); /* never unioned: a group of its own */
		}

		/* 3. Intermediate table: touches more than one key group */
		if (belong_groups.size() > 1)
		{
			v->intermediate = true;
			continue;
		}

		/* 4. Last, it is an unfiltered table */
		v->unfiltered = true;
	}
}

/* Helper: check whether data can flow from one table to another */
static bool
rpt_can_flow(const RptEdgeInfo *e, int source_rtindex, int target_rtindex)
{
	if (e->left_rtindex == source_rtindex && e->right_rtindex == target_rtindex)
		return !e->protect_right;	/* Left → Right allowed if not protected */
	if (e->right_rtindex == source_rtindex && e->left_rtindex == target_rtindex)
		return !e->protect_left;	/* Right → Left allowed if not protected */
	return false;
}

/* Helper: get the key on vertex t's side of one clause of edge e */
static const RptKey &
rpt_key_at(const RptEdgeInfo *e, size_t clause, int rtindex)
{
	return (e->left_rtindex == rtindex) ? e->clauses[clause].first : e->clauses[clause].second;
}

/* Helper: get the key on the side opposite vertex t in one clause of edge e */
static const RptKey &
rpt_key_opposite(const RptEdgeInfo *e, size_t clause, int rtindex)
{
	return (e->left_rtindex == rtindex) ? e->clauses[clause].second : e->clauses[clause].first;
}

/*
 * Helper: append the clauses of `other` that `old` does not have yet, in
 * old's orientation (the artifact's edges are single-clause, so it has no
 * need for this).
 */
static void
rpt_merge_clauses(RptEdgeInfo *old, const RptEdgeInfo &other, bool reversed)
{
	size_t		i;

	for (i = 0; i < other.clauses.size(); i++)
	{
		std::pair<RptKey, RptKey> clause = other.clauses[i];
		bool		have = false;
		size_t		j;

		if (reversed)
			std::swap(clause.first, clause.second);
		for (j = 0; j < old->clauses.size(); j++)
		{
			if (old->clauses[j].first == clause.first &&
				old->clauses[j].second == clause.second)
			{
				have = true;
				break;
			}
		}
		if (!have)
			old->clauses.push_back(clause);
	}
}

/*
 * Helper: insert new edge into neighbor_matrix, merging if already exists.
 * The matrix holds each edge twice ([a][b] and [b][a]) by value where the
 * artifact shares one EdgeInfo between both directions, so every write here
 * is mirrored explicitly.
 */
static bool
rpt_upsert_edge(RptTransferMgr *mgr, const RptEdgeInfo &ne)
{
	int			i = ne.left_rtindex;
	int			j = ne.right_rtindex;
	std::map<int, RptEdgeInfo>::iterator ij;
	bool		exists = false;
	bool		changed = false;

	ij = mgr->neighbor_matrix[i].find(j);
	if (ij != mgr->neighbor_matrix[i].end())
	{
		/*
		 * The artifact compares the single bindings and clobbers a parallel
		 * edge on different columns; our edges already hold every clause of
		 * the vertex pair, so the same vertex pair in either orientation is
		 * the same edge and the clauses merge instead.
		 */
		RptEdgeInfo before = ij->second;

		bool		same = (ij->second.left_rtindex == ne.left_rtindex &&
							ij->second.right_rtindex == ne.right_rtindex);
		bool		rev = (ij->second.left_rtindex == ne.right_rtindex &&
						   ij->second.right_rtindex == ne.left_rtindex);

		if (same)
		{
			ij->second.protect_left |= ne.protect_left;
			ij->second.protect_right |= ne.protect_right;
			rpt_merge_clauses(&ij->second, ne, false);
			exists = true;
		}
		else if (rev)
		{
			ij->second.protect_left |= ne.protect_right;
			ij->second.protect_right |= ne.protect_left;
			rpt_merge_clauses(&ij->second, ne, true);
			exists = true;
		}
		if (exists)
		{
			changed = before.protect_left != ij->second.protect_left ||
				before.protect_right != ij->second.protect_right ||
				before.clauses.size() != ij->second.clauses.size();
			mgr->neighbor_matrix[j][i] = ij->second;
		}
	}
	if (!exists)
	{
		mgr->neighbor_matrix[i][j] = ne;
		mgr->neighbor_matrix[j][i] = ne;
		changed = true;
	}
	return changed;
}

/*
 * SkipUnfilteredTable (transfer_graph_manager.cpp:311-511).
 *
 * Unfiltered tables only receive Bloom filters, they do not generate them:
 * splice a direct bypass edge between the carriers and the destinations on
 * the same join key, block the unfiltered table's outgoing edges, and drop
 * fully blocked edges.  Then, for each intermediate table with no incoming
 * edge left, block its outgoing edges as well.
 */
static void
rpt_skip_unfiltered(RptTransferMgr *mgr)
{
	std::vector<int> unfiltered_rtindexes;
	std::vector<int> intermediate_rtindexes;
	size_t		i;
	bool		changed;

	/* Sorted range-table indexes keep iteration and dumps deterministic. */
	for (i = 0; i < mgr->vertices.size(); i++)
	{
		if (mgr->vertices[i].unfiltered)
			unfiltered_rtindexes.push_back(mgr->vertices[i].rtindex);
		else if (mgr->vertices[i].intermediate)
			intermediate_rtindexes.push_back(mgr->vertices[i].rtindex);
	}
	std::sort(unfiltered_rtindexes.begin(), unfiltered_rtindexes.end());
	std::sort(intermediate_rtindexes.begin(), intermediate_rtindexes.end());

	/* 1) Iteratively skip unfiltered tables until no new edges are added. */
	do
	{
		changed = false;

		for (i = 0; i < unfiltered_rtindexes.size(); i++)
		{
			int			rtindex = unfiltered_rtindexes[i];
			std::map<int, RptEdgeInfo> &adj = mgr->neighbor_matrix[rtindex];
			std::map<RptKey, std::vector<RptEdgeClause> > incoming_by_col;
			std::vector<RptEdgeInfo *> outgoing;
			std::map<int, RptEdgeInfo>::iterator pit;
			std::map<RptKey, std::vector<RptEdgeClause> >::iterator git;
			size_t		j;

			/* ---- Step 1: collect all incoming edges from carrier tables → t ---- */
			for (pit = adj.begin(); pit != adj.end(); ++pit)
			{
				RptEdgeInfo *e = &pit->second;
				size_t		c;

				if (mgr->vertices[mgr->vertex_index_by_rtindex[pit->first]].unfiltered ||
					!rpt_can_flow(e, pit->first, rtindex))
					continue;
				for (c = 0; c < e->clauses.size(); c++)
				{
					RptEdgeClause ref;

					ref.edge = e;
					ref.clause = c;
					incoming_by_col[rpt_key_at(e, c, rtindex)].push_back(ref);
				}
			}

			/* ---- Step 2: collect all outgoing edges t → neighbor ---- */
			for (pit = adj.begin(); pit != adj.end(); ++pit)
			{
				if (rpt_can_flow(&pit->second, rtindex, pit->first))
					outgoing.push_back(&pit->second);
			}

			/*
			 * ---- Step 3: bypass t ----
			 * For every (carrier → t) and (t → dst) pair with the same join
			 * column, create a direct edge (carrier → dst).
			 */
			for (git = incoming_by_col.begin(); git != incoming_by_col.end(); ++git)
			{
				for (j = 0; j < outgoing.size(); j++)
				{
					RptEdgeInfo *out_e = outgoing[j];
					size_t		oc;

					for (oc = 0; oc < out_e->clauses.size(); oc++)
					{
						RptEdgeClause rep = git->second[0]; /* representative incoming edge */
						RptEdgeInfo *in_e = rep.edge;
						RptEdgeInfo ne;
						int			src_v;
						int			dst_v;

						if (!(rpt_key_at(out_e, oc, rtindex) == git->first))
							continue;	/* must be same join key */

						/* src is the carrier: in_e's side opposite t */
						src_v = (in_e->left_rtindex == rtindex) ? in_e->right_rtindex : in_e->left_rtindex;

						ne.left_rtindex = src_v;
						ne.key_type = out_e->key_type;
						ne.protect_left = false;
						ne.protect_right = false;

						if (out_e->left_rtindex == rtindex)
						{
							/* t is left side of out_e */
							dst_v = out_e->right_rtindex;
							ne.right_rtindex = dst_v;
							ne.clauses.push_back(std::make_pair(rpt_key_opposite(in_e, rep.clause, rtindex),
																out_e->clauses[oc].second));
							ne.protect_left = true; /* disable reverse flow (R → L) */
						}
						else
						{
							/* t is right side of out_e */
							dst_v = out_e->left_rtindex;
							ne.right_rtindex = dst_v;
							ne.clauses.push_back(std::make_pair(rpt_key_opposite(in_e, rep.clause, rtindex),
																out_e->clauses[oc].first));

							ne.protect_left = true; /* disable dst -> src */
						}

						if (src_v == dst_v)
							continue;	/* a self-edge no later pass can observe */

						changed = rpt_upsert_edge(mgr, ne) || changed;
					}
				}
			}

			/* ---- Step 4: disable all outgoing edges from this unfiltered table ---- */
			for (j = 0; j < outgoing.size(); j++)
			{
				RptEdgeInfo *e = outgoing[j];

				if (e->left_rtindex == rtindex)
				{
					if (!e->protect_right)
					{
						e->protect_right = true;	/* block Left→Right */
						changed = true;
					}
				}
				else
				{
					if (!e->protect_left)
					{
						e->protect_left = true; /* block Right→Left */
						changed = true;
					}
				}
			}

			/* ---- Step 5: remove fully blocked edges (no direction left) ---- */
			for (pit = adj.begin(); pit != adj.end();)
			{
				RptEdgeInfo *e = &pit->second;
				int			nbr = pit->first;

				if (e->protect_left && e->protect_right)
				{
					++pit;
					mgr->neighbor_matrix[rtindex].erase(nbr);
					mgr->neighbor_matrix[nbr].erase(rtindex);
					changed = true;
				}
				else
				{
					/* keep the reverse matrix entry in sync */
					mgr->neighbor_matrix[nbr][rtindex] = *e;
					++pit;
				}
			}
		}
	} while (changed);

	/*
	 * 2) For each intermediate table, if it has no filter and any in-edge, it
	 * should not have out-edge.  (The artifact's comment says "any in-edge"
	 * but its code -- and ours -- does this when no in-edge is left.)
	 */
	for (i = 0; i < intermediate_rtindexes.size(); i++)
	{
		int			rtindex = intermediate_rtindexes[i];
		std::map<int, RptEdgeInfo> &adj = mgr->neighbor_matrix[rtindex];
		std::map<RptKey, std::vector<RptEdgeClause> > incoming_by_col;
		std::map<int, RptEdgeInfo>::iterator pit;
		size_t		c;

		/* ---- collect all incoming edges from carrier tables → t ---- */
		for (pit = adj.begin(); pit != adj.end(); ++pit)
		{
			RptEdgeInfo *e = &pit->second;

			if (!rpt_can_flow(e, pit->first, rtindex))
				continue;
			for (c = 0; c < e->clauses.size(); c++)
			{
				RptEdgeClause ref;

				ref.edge = e;
				ref.clause = c;
				incoming_by_col[rpt_key_at(e, c, rtindex)].push_back(ref);
			}
		}

		if (incoming_by_col.empty())
		{
			for (pit = adj.begin(); pit != adj.end(); ++pit)
			{
				RptEdgeInfo *e = &pit->second;

				if (!rpt_can_flow(e, rtindex, pit->first))
					continue;
				if (e->left_rtindex == rtindex)
					e->protect_right = true;	/* block Left→Right */
				else
					e->protect_left = true; /* block Right→Left */
			}

			for (pit = adj.begin(); pit != adj.end();)
			{
				RptEdgeInfo *e = &pit->second;
				int			nbr = pit->first;

				if (e->protect_left && e->protect_right)
				{
					++pit;
					mgr->neighbor_matrix[rtindex].erase(nbr);
					mgr->neighbor_matrix[nbr].erase(rtindex);
				}
				else
				{
					/* keep the reverse matrix entry in sync */
					mgr->neighbor_matrix[nbr][rtindex] = *e;
					++pit;
				}
			}
		}
	}
}

/*
 * Build the join-key equivalence classes and classify the vertices, then
 * rewire the graph around tables that carry no information of their own.
 * This is the rptgraph half of the artifact's Build()
 * (transfer_graph_manager.cpp:50-70): the union pass of ExtractEdgesInfo
 * over edges the region pass already extracted, ClassifyTables, and
 * SkipUnfilteredTable.
 */
void
RptBuildClasses(RptTransferMgr *mgr)
{
	std::map<int, std::map<int, RptEdgeInfo> >::iterator row;

	/*
	 * Union the clause keys of every edge that is unprotected both ways
	 * (transfer_graph_manager.cpp:249-251): the preserved side of an outer
	 * join keeps its rows whether or not they match, so a protected edge
	 * never merges classes.  The matrix holds each edge in both directions;
	 * unioning twice is idempotent.
	 */
	for (row = mgr->neighbor_matrix.begin(); row != mgr->neighbor_matrix.end(); ++row)
	{
		std::map<int, RptEdgeInfo>::iterator eit;

		for (eit = row->second.begin(); eit != row->second.end(); ++eit)
		{
			RptEdgeInfo *edge = &eit->second;
			size_t		c;

			if (edge->protect_left || edge->protect_right)
				continue;
			for (c = 0; c < edge->clauses.size(); c++)
				rpt_union_keys(edge->clauses[c].first, edge->clauses[c].second,
							   edge->key_type, mgr);
		}
	}

	/* Equality is transitive within an unprotected class. Make its implicit
	 * edges explicit so Algorithm 1 can choose deepest/shallowest parents
	 * even when the SQL spelled a star rather than a clique. Never merge
	 * through a protected outer-join edge. */
	for (const auto &group : mgr->groups)
	{
		std::vector<RptKey> keys;
		for (const auto &vertex : mgr->vertices)
			for (const auto &key : vertex.join_keys)
				if (RptFindKeyRoot(key, mgr->uf_parent) == group.first)
					keys.push_back(key);
		for (size_t i = 0; i < keys.size(); ++i)
			for (size_t j = i + 1; j < keys.size(); ++j)
			{
				if (keys[i].rtindex == keys[j].rtindex)
					continue;
				RptEdgeInfo edge;
				edge.left_rtindex = keys[i].rtindex;
				edge.right_rtindex = keys[j].rtindex;
				edge.key_type = group.second.key_type;
				edge.protect_left = edge.protect_right = false;
				edge.clauses.push_back(std::make_pair(keys[i], keys[j]));
				const auto &existing_row = mgr->neighbor_matrix.find(edge.left_rtindex);
				if (existing_row != mgr->neighbor_matrix.end())
				{
					const auto &existing = existing_row->second.find(edge.right_rtindex);
					if (existing != existing_row->second.end())
					{
						bool reversed = existing->second.left_rtindex != edge.left_rtindex;
						auto clause = reversed ? std::make_pair(keys[j], keys[i]) :
							std::make_pair(keys[i], keys[j]);
						if (std::find(existing->second.clauses.begin(),
									  existing->second.clauses.end(), clause) !=
							existing->second.clauses.end())
							continue;
					}
				}
				rpt_upsert_edge(mgr, edge);
			}
	}

	rpt_classify_tables(mgr);
    mgr->logical_neighbor_matrix = mgr->neighbor_matrix;
	rpt_skip_unfiltered(mgr);
}
