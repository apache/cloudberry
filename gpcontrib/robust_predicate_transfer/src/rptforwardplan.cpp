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
 * rptforwardplan.cpp
 *	  Forward reducer selection: which forward chains to build, their channels,
 *	  and the cost gate.  No PostgreSQL calls, so it is unit-tested directly.
 *
 * The cost model was measured on one host with three primaries: every
 * global hop pays about c milliseconds per MiB of bitset per producer, because
 * the coordinator receives and folds the parts one at a time.  Hops of one
 * chain run one after another inside the reducer, so a component's cost is
 * the sum over its channels.  A local hop consumed only inside the reducer is
 * handed over in-process (rptlocal.c) and costs no exchange.
 *
 * The benefit side is the paper's selectivity test: inject only when the
 * chain is expected to keep less than lambda of the root's rows.  A vertex
 * keeps the fraction its own reducer quals keep, times what its inbound
 * filters keep; a filter from u keeps s(u) * min(1, ndv(u.key) / ndv(v.key))
 * of v, assuming the smaller key domain is contained in the larger one.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptforwardplan.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"
}


#include <sstream>

/* Every vertex whose filters can reach `root` through forward edges. */
static std::set<int>
rpt_forward_member_rtindexes(const RptTransferMgr &forward, int root_rtindex)
{
	std::set<int> member_rtindexes;
	std::vector<int> pending_rtindexes(1, root_rtindex);

	member_rtindexes.insert(root_rtindex);
	while (!pending_rtindexes.empty())
	{
		int			rtindex = pending_rtindexes.back();
		auto		it = forward.graph.find(rtindex);

		pending_rtindexes.pop_back();
		if (it == forward.graph.end())
			continue;
		/* An in-edge's other_rtindex names the producing side. */
		for (const RptGraphEdge &in : it->second.forward_stage.in)
			if (member_rtindexes.insert(in.other_rtindex).second)
				pending_rtindexes.push_back(in.other_rtindex);
	}
	return member_rtindexes;
}

static double
rpt_estimated_scan_rows(const RptTransferMgr &forward,
						const std::map<int, RptForwardVertexEst> &est, int rtindex)
{
	auto entry = est.find(rtindex);
	if (entry != est.end() && entry->second.rows >= 0)
		return entry->second.rows;
	return forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)].cardinality;
}

/* The fraction of `to` rows a filter from `from` keeps. */
static double
rpt_transfer_keep(const RptTransferMgr &forward, const RptForwardChannel &ch,
				  double from_keep, RptNdvFn ndv_fn,
				  const RptForwardParams &params,
				  const std::map<int, RptForwardVertexEst> &est)
{
	double		d = ndv_fn ? ndv_fn(forward.vertices[forward.vertex_index_by_rtindex.at(ch.source_rtindex)],
									ch.source_key.attno) : -1;
	double		n = ndv_fn ? ndv_fn(forward.vertices[forward.vertex_index_by_rtindex.at(ch.target_rtindex)],
									ch.target_key.attno) : -1;

	if (params.key_survival && d > 0 && n > 0)
	{
		const auto &source = forward.vertices[forward.vertex_index_by_rtindex.at(ch.source_rtindex)];
		double rows = rpt_estimated_scan_rows(forward, est, ch.source_rtindex) *
			(source.replicated ? 1 : std::max(1, source.numsegments));
		/* A catalog NDV larger than the source row count is stale. */
		if (rows > 0)
			return std::min(1.0, RptRetainedKeys(rows, std::min(d, rows), from_keep) / n);
	}
	if (d > 0 && n > 0 && d < n)
		return from_keep * d / n;
	return from_keep;
}

/* First-pass millisecond estimates per segment. The exchange estimate
 * already sums serial hops. The avoided work is downstream of the scan:
 * Bloom probing does not imply that the scan can skip I/O. */
static constexpr double reducer_startup_ms = 1.0;
static constexpr double minimum_hop_rejection = 0.05;

static void
rpt_add_forward_subscribers(const RptTransferMgr &forward,
							const RptForwardParams &params,
							const std::map<int, RptForwardVertexEst> &est,
							const std::map<int, double> &keep, RptNdvFn ndv_fn,
							RptForwardComponent &comp)
{
	std::vector<RptForwardChannel> subscribers;
	std::set<RptKey> covered;
	for (const auto &ch : comp.channels)
		covered.insert(ch.target_key);

	for (const auto &ch : comp.channels)
	{
		if (ch.target_rtindex != comp.root_rtindex)
			continue;
		auto adjacency = forward.logical_neighbor_matrix.find(ch.source_rtindex);
		if (adjacency == forward.logical_neighbor_matrix.end())
			continue;
		for (const auto &neighbor : adjacency->second)
		{
			const auto &edge = neighbor.second;
			auto deliveries = params.main_consumer_processes.find(neighbor.first);
			if (edge.protect_left || edge.protect_right ||
				forward.root_rtindexes.count(neighbor.first) ||
				deliveries == params.main_consumer_processes.end() || deliveries->second <= 0)
				continue;

			for (const auto &clause : edge.clauses)
			{
				const auto &from = clause.first.rtindex == ch.source_rtindex ? clause.first : clause.second;
				const auto &to = clause.first.rtindex == ch.source_rtindex ? clause.second : clause.first;
				if (!(from == ch.source_key) || covered.count(to) ||
					!params.main_consumer_keys.count(to) ||
					params.inplace_transfers.count({from, to}))
					continue;
				if (params.inplace_probe_keys.count(to))
				{
					comp.inplace_refusals.insert(to);
					continue;
				}
				RptForwardChannel extra = ch;
				extra.target_rtindex = to.rtindex;
				extra.target_key = to;
				extra.local = false;
				extra.main_only = true;
				extra.est_cost_ms = params.delivery_ms_per_mib * deliveries->second *
					((double) ch.planned_bytes / (1024.0 * 1024.0));
				extra.est_keep = rpt_transfer_keep(forward, extra, keep.at(ch.source_rtindex),
												   ndv_fn, params, est);
				double rejected = 1 - extra.est_keep;
				double work = extra.est_cost_ms +
					rpt_estimated_scan_rows(forward, est, to.rtindex) * params.bloom_probe_ms_per_row;
				double benefit = forward.vertices[forward.vertex_index_by_rtindex.at(to.rtindex)].cardinality *
					rejected * params.downstream_ms_per_row;
				if (rejected < minimum_hop_rejection || benefit <= work ||
					(params.max_exchange_ms >= 0 &&
					 comp.est_cost_ms + extra.est_cost_ms > params.max_exchange_ms))
					continue;
				comp.est_cost_ms += extra.est_cost_ms;
				comp.est_work_ms += work;
				comp.est_benefit_ms += benefit;
				covered.insert(to);
				subscribers.push_back(extra);
			}
		}
	}
	comp.channels.insert(comp.channels.end(), subscribers.begin(), subscribers.end());
}

static void
rpt_prune_disconnected(RptForwardComponent &comp)
{
	/* After pruning, remove producer subtrees with no path to the root. */
	std::set<int> reachable = {comp.root_rtindex};
	for (auto it = comp.channels.rbegin(); it != comp.channels.rend(); ++it)
		if (reachable.count(it->target_rtindex))
			reachable.insert(it->source_rtindex);

	comp.channels.erase(std::remove_if(comp.channels.begin(), comp.channels.end(),
		[&](const auto &ch) {
			return !reachable.count(ch.target_rtindex);
		}), comp.channels.end());
	comp.chain_rtindexes.erase(std::remove_if(comp.chain_rtindexes.begin(),
		comp.chain_rtindexes.end(), [&](int rtindex) {
			return !reachable.count(rtindex);
		}), comp.chain_rtindexes.end());
}

static void
rpt_cost_forward_component(const RptTransferMgr &forward,
						   const RptForwardParams &params,
						   const std::map<int, RptForwardVertexEst> &est,
						   const std::map<int, double> &keep,
						   RptForwardComponent &comp)
{
	for (int rtindex : comp.chain_rtindexes)
	{
		const auto &vertex = forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)];
		int width = vertex.scan ? vertex.scan->plan_width : 0;
		double rows = rpt_estimated_scan_rows(forward, est, rtindex);

		/* Charge decoded rows and retained-qual evaluation even when an
		 * incoming filter rejects most of the tuples. */
		comp.est_work_ms += rows * (params.reducer_scan_ms_per_row +
									width * params.reducer_scan_ms_per_byte);
	}
	for (const auto &ch : comp.channels)
	{
		comp.est_work_ms += ch.est_cost_ms +
			rpt_estimated_scan_rows(forward, est, ch.source_rtindex) *
			keep.at(ch.source_rtindex) * params.bloom_build_ms_per_row +
			rpt_estimated_scan_rows(forward, est, ch.target_rtindex) *
			params.bloom_probe_ms_per_row;
	}
	const auto &root = forward.vertices[forward.vertex_index_by_rtindex.at(comp.root_rtindex)];
	comp.est_benefit_ms = root.cardinality * (1 - comp.est_selectivity) *
		params.downstream_ms_per_row;
}

void
RptSelectForward(const RptTransferMgr &forward, const RptForwardParams &params,
				 const std::map<int, RptForwardVertexEst> &est,
				 RptFilterSizeFn size_fn, RptNdvFn ndv_fn,
				 std::vector<RptForwardComponent> *out)
{

	out->clear();
	double combined_savings_ms = 0;
	for (int root_rtindex : forward.root_rtindexes)
	{
		RptForwardComponent comp;
		std::set<int> member_rtindexes = rpt_forward_member_rtindexes(forward, root_rtindex);
		std::map<int, double> keep;	/* chain vertex -> fraction it keeps */
		bool		sized = true;

		comp.root_rtindex = root_rtindex;

		/* reducer_order is already topological: producers before consumers. */
		for (int rtindex : forward.reducer_order_rtindexes)
			if (rtindex != root_rtindex && member_rtindexes.count(rtindex))
				comp.chain_rtindexes.push_back(rtindex);

		for (int rtindex : comp.chain_rtindexes)
		{
			const RptGraphNode &node = forward.graph.at(rtindex);
			const RptVertexInfo &source = forward.vertices[forward.vertex_index_by_rtindex.at(rtindex)];
			auto		e = est.find(rtindex);
			double		rows = (e != est.end() && e->second.rows >= 0)
				? e->second.rows : (double) source.cardinality;
			double		own = e != est.end() ? e->second.selectivity : 1.0;
			double		k = own;
			bool		has_incoming = false;

			/* In-channels of this rtindex were made by earlier chain vertices. */
			for (const RptForwardChannel &in : comp.channels)
				if (in.target_rtindex == rtindex)
				{
					has_incoming = true;
					k *= in.est_keep;
				}
			keep[rtindex] = k;

			for (const RptGraphEdge &edge : node.forward_stage.out)
			{
				RptForwardChannel ch;

				if (edge.left_keys.empty() || edge.right_keys.empty())
					continue;
				ch.source_rtindex = rtindex;
				ch.target_rtindex = edge.other_rtindex;
				ch.source_key = edge.left_keys.front(); /* out-edge: left = this side */
				ch.target_key = edge.right_keys.front();
				ch.est_keep = rpt_transfer_keep(forward, ch, k, ndv_fn, params, est);
				if (params.key_survival)
					for (size_t key = 1; key < edge.left_keys.size(); ++key)
					{
						RptForwardChannel candidate = ch;
						candidate.source_key = edge.left_keys[key];
						candidate.target_key = edge.right_keys[key];
						candidate.est_keep = rpt_transfer_keep(forward, candidate, k, ndv_fn, params, est);
						if (candidate.est_keep < ch.est_keep)
							ch = candidate;
					}
				ch.local = edge.local;
				ch.in_process = ch.local && ch.target_rtindex != root_rtindex;
				/* The Hash input preserves the base predicates. Without a
				 * reducer-only incoming filter, the exchange cannot improve
				 * on that key set and requires an extra scan. */
				if (ch.target_rtindex == root_rtindex &&
					params.inplace_probe_keys.count(ch.target_key))
				{
					comp.inplace_refusals.insert(ch.target_key);
					comp.reason = "in-place filter covers " + std::to_string(ch.target_key.rtindex) +
						"." + std::to_string(ch.target_key.attno);
					continue;
				}
				if (!has_incoming && ch.target_rtindex == root_rtindex &&
					params.inplace_transfers.count({ch.source_key, ch.target_key}))
					continue;
				if (params.use_total_cost &&
					ch.est_keep > 1 - minimum_hop_rejection)
					continue;

				/* Sized for what the reducer scan inserts: its rows after its
				 * own quals -- all of them when the quals were dropped.  An
				 * exchanged filter folds every segment's keys; rows is what
				 * one segment of a partitioned table holds. */
				double		inserted = rows * own;

				if (!ch.in_process && !source.replicated)
					inserted *= std::max(1, source.numsegments);
				if (!size_fn(inserted, &ch.total_elems,
							 &ch.max_payload, &ch.planned_bytes))
				{
					if (params.use_total_cost)
						continue;
					comp.reason = "filter sizing declined for " +
						std::to_string(ch.source_rtindex) + "->" + std::to_string(ch.target_rtindex);
					sized = false;
					break;
				}
				if (!ch.in_process)
				{
					/* Each hop delivers either to a reducer scan or to the
					 * root scan in the main plan. */
					int deliveries = params.width;
					if (ch.target_rtindex == root_rtindex)
					{
						auto processes = params.main_consumer_processes.find(root_rtindex);
						if (processes != params.main_consumer_processes.end())
							deliveries = processes->second;
					}
					ch.est_cost_ms =
						(params.ms_per_mib_part * params.width +
						 params.delivery_ms_per_mib * deliveries) *
						((double) ch.planned_bytes / (1024.0 * 1024.0));
				}
				comp.est_cost_ms += ch.est_cost_ms;
				comp.channels.push_back(ch);
			}
			if (!sized)
				break;
		}

		for (const RptForwardChannel &ch : comp.channels)
			if (ch.target_rtindex == root_rtindex)
				comp.est_selectivity *= ch.est_keep;

		if (sized && (params.use_total_cost || !params.inplace_probe_keys.empty()))
		{
			rpt_prune_disconnected(comp);
			comp.est_cost_ms = 0;
			for (const auto &ch : comp.channels)
				comp.est_cost_ms += ch.est_cost_ms;
		}
		if (params.use_total_cost && sized)
			rpt_cost_forward_component(forward, params, est, keep, comp);

		const RptVertexInfo &rootv = forward.vertices[forward.vertex_index_by_rtindex.at(root_rtindex)];

		if (!sized)
			;					/* reason already set */
		else if (comp.channels.empty())
		{
			if (comp.reason.empty())
				comp.reason = "no forward edges";
		}
		else if ((double) rootv.cardinality < params.min_root_rows)
			comp.reason = "root rows below rpt.forward_min_root_rows";
		else if (params.max_selectivity < 1 &&
				 comp.est_selectivity >= params.max_selectivity)
			comp.reason = "estimated root selectivity not below rpt.forward_max_selectivity";
		else if (params.max_exchange_ms >= 0 &&
				 comp.est_cost_ms > params.max_exchange_ms)
			comp.reason = "estimated exchange cost above rpt.forward_max_exchange_ms";
		else if (params.use_total_cost && comp.est_benefit_ms <= comp.est_work_ms)
			comp.reason = "estimated reducer and probing work exceeds downstream savings";
		else
		{
			comp.accepted = true;
			comp.reason = "inject";
			combined_savings_ms += comp.est_benefit_ms - comp.est_work_ms;
			if (params.use_total_cost)
				rpt_add_forward_subscribers(forward, params, est, keep, ndv_fn, comp);
		}
		out->push_back(comp);
	}

	/* One reducer InitPlan serves all accepted components. */
	if (params.use_total_cost && combined_savings_ms <= reducer_startup_ms)
		for (auto &comp : *out)
			if (comp.accepted)
			{
				comp.accepted = false;
				comp.reason = "estimated downstream savings do not repay reducer startup";
			}
}

void
RptDumpForward(const std::vector<RptForwardComponent> &components, std::string *out)
{
	std::ostringstream text;

	text << std::fixed << std::setprecision(3);
	for (const RptForwardComponent &comp : components)
	{
		text << "forward component root=" << comp.root_rtindex << " chain=[";
		for (size_t i = 0; i < comp.chain_rtindexes.size(); ++i)
			text << (i ? " " : "") << comp.chain_rtindexes[i];
		text << "] channels=" << comp.channels.size()
			 << " est_cost_ms=" << comp.est_cost_ms
			 << " est_work_ms=" << comp.est_work_ms
				 << " est_benefit_ms=" << comp.est_benefit_ms
				 << " est_sel=" << comp.est_selectivity
			 << " decision=" << comp.reason << '\n';
		for (const auto &key : comp.inplace_refusals)
			text << "forward: in-place filter covers " << key.rtindex << '.' << key.attno << '\n';
		for (const RptForwardChannel &ch : comp.channels)
			text << "forward channel " << ch.source_key.rtindex << '.' << ch.source_key.attno
				 << "->" << ch.target_key.rtindex << '.' << ch.target_key.attno
				 << " key=" << (ch.condition_key.empty() ? "-" : ch.condition_key)
				 << " bytes=" << ch.planned_bytes
				 << " est_ms=" << ch.est_cost_ms
				 << (ch.local ? " local" : "")
				 << (ch.in_process ? " in-process" : "")
				 << (ch.main_only ? " shared main subscriber" : "") << '\n';
	}
	*out += text.str();
}
