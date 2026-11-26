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
 * rptbackwardplan.cpp
 *    Pure backward selection over the shared schedule and eligibility proof.
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"
#include <sstream>

static int
rpt_slice_processes(const std::map<int, int> &widths, int slice_index)
{
	auto found = widths.find(slice_index);
	return found == widths.end() ? 0 : found->second;
}

void
RptSelectBackward(const RptTransferMgr &schedule,
				  const std::vector<RptForwardComponent> &forward,
				  const RptForwardParams &params,
				  const std::map<int, int> &slice_processes,
				  RptFilterSizeFn size_fn, std::vector<RptBackwardClass> *out)
{
	out->clear();
	std::map<RptKey, std::vector<RptBackwardConsumer> > decisions;
	for (const auto &decision : schedule.backward_consumers)
		decisions[decision.class_key].push_back(decision);
	for (const auto &entry : decisions)
	{
		RptBackwardClass cls;
		cls.class_key = entry.first;
		auto chosen = std::find_if(entry.second.begin(), entry.second.end(),
			[](const RptBackwardConsumer &d) { return d.eligible; });
		const auto &first = chosen == entry.second.end() ? entry.second.front() : *chosen;
		cls.producer_rtindex = first.producer_rtindex;
		cls.producer_key = first.producer_key;
		cls.producer_processes = rpt_slice_processes(slice_processes,
													first.producer_slice_index);
		const auto &producer = schedule.vertices.at(
			schedule.vertex_index_by_rtindex.at(cls.producer_rtindex));
		double root_keep = 1;
		for (const auto &comp : forward)
			if (comp.accepted && comp.root_rtindex == cls.producer_rtindex)
				root_keep = comp.est_selectivity;
		/* The folded filter holds every producer's keys, and cardinality is
		 * what one process of a partitioned scan reads. */
		cls.estimated_producer_rows = producer.cardinality * root_keep *
			(producer.replicated ? 1 : std::max(1, cls.producer_processes));
		bool any = false;
		for (const auto &decision : entry.second)
		{
			RptBackwardTarget target;
			target.proof = decision;
			target.reason = decision.reason;
			target.processes = rpt_slice_processes(slice_processes,
												   decision.consumer_slice_index);
			double rows = schedule.vertices.at(
				schedule.vertex_index_by_rtindex.at(decision.consumer_rtindex)).cardinality;
			double kept = 1;
			if (decision.estimated_rows_removed >= 0 && rows > 0)
			{
				kept = std::max(0.0, std::min(1.0,
					1 - decision.estimated_rows_removed / rows)) * root_keep;
				if (params.key_survival && decision.producer_base_rows > 0)
				{
					double surviving = std::min(1.0, cls.estimated_producer_rows / decision.producer_base_rows);
					kept = std::min(1.0,
						RptRetainedKeys(decision.producer_base_rows, decision.producer_ndv, surviving) /
						decision.consumer_ndv);
				}
				target.estimated_rows_removed = rows * (1 - kept);
			}
			if (decision.eligible)
			{
				if (params.inplace_probe_keys.count(decision.consumer_key))
					target.reason = "in-place filter covers " + std::to_string(decision.consumer_key.rtindex) +
						"." + std::to_string(decision.consumer_key.attno);
				else if (target.processes <= 0)
					target.reason = "unknown consumer process count";
				else if (params.max_selectivity < 1 && kept >= params.max_selectivity)
					target.reason = "estimated member selectivity not below rpt.forward_max_selectivity";
				else
				{
					target.accepted = true;
					target.reason = "inject";
					any = true;
				}
			}
			cls.consumers.push_back(target);
		}
		if (!schedule.root_rtindexes.count(cls.producer_rtindex))
			cls.reason = "bridge-led class is deferred";
		else if (!any)
			cls.reason = "no eligible consumers";
		else if (cls.producer_processes <= 0)
			cls.reason = "unknown producer process count";
		else if (!size_fn(cls.estimated_producer_rows, &cls.total_elems,
						  &cls.max_payload, &cls.planned_bytes))
			cls.reason = "backward filter sizing declined";
		else
		{
			int64 consumer_processes = 0;
			for (const auto &target : cls.consumers)
				if (target.accepted)
					consumer_processes += target.processes;
			double mib = cls.planned_bytes / (1024.0 * 1024.0);
			cls.est_cost_ms = mib *
				(params.ms_per_mib_part * cls.producer_processes +
				 params.delivery_ms_per_mib * consumer_processes);
			if (params.max_exchange_ms >= 0 && cls.est_cost_ms > params.max_exchange_ms)
				cls.reason = "estimated exchange cost above rpt.forward_max_exchange_ms";
			else
			{
				cls.accepted = true;
				cls.reason = "inject";
			}
		}
		if (!cls.accepted)
			for (auto &target : cls.consumers)
				if (target.accepted)
				{
					target.accepted = false;
					target.reason = cls.reason;
				}
		out->push_back(cls);
	}
}

void
RptDumpBackward(const RptTransferMgr &schedule,
				const std::vector<RptBackwardClass> &classes, std::string *out)
{
	size_t root_classes = 0, strict = 0, eligible = 0;
	for (const auto &group : schedule.groups)
		root_classes += schedule.root_rtindexes.count(group.second.leader_rtindex);
	for (const auto &proof : schedule.backward_consumers)
	{
		strict += proof.strict_eligible;
		eligible += proof.eligible;
	}
	*out += "runtime survey classes=" + std::to_string(schedule.groups.size()) +
		" root_classes=" + std::to_string(root_classes) +
		" strict=" + std::to_string(strict) + " eligible=" + std::to_string(eligible) + "\n";


	std::ostringstream text;
	text << std::fixed << std::setprecision(3);
	for (const auto &cls : classes)
	{
		size_t accepted = 0;
		for (const auto &target : cls.consumers)
			accepted += target.accepted;
		text << "backward class root=" << cls.producer_rtindex
			 << " rep=" << cls.class_key.rtindex << '.' << cls.class_key.attno
			 << " consumers=" << accepted << " bytes=" << cls.planned_bytes
			 << " est_cost_ms=" << cls.est_cost_ms
			 << " decision=" << cls.reason << '\n';
		for (const auto &target : cls.consumers)
			text << "backward consumer " << target.proof.consumer_rtindex
				 << " root=" << cls.producer_rtindex
				 << " rep=" << cls.class_key.rtindex << '.' << cls.class_key.attno
				 << " key=" << (cls.condition_key.empty() ? "-" : cls.condition_key)
				 << " defer_first=" << (target.proof.defer_first ? "yes" : "no")
				 << " rows_removed=" << target.estimated_rows_removed
				 << " decision=" << target.reason << '\n';
	}
	*out += text.str();
}
