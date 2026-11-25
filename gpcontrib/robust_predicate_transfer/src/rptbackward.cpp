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
 * rptbackward.cpp
 *    Backward placement on the scan stacks built by the forward pass.
 *
 *-------------------------------------------------------------------------
 */
#include "rptchannel.h"

extern "C"
{
#include "anserplan.h"
}

/* The proof names the original parent, not a wrapper added by forward. */
static bool
rpt_backward_endpoint(const RptTransferMgr &schedule,
					  const RptInjectionState &state, int rtindex,
					  const RptKey &key, Plan *parent, AttrNumber attno)
{
	const auto &vertex = schedule.vertices.at(schedule.vertex_index_by_rtindex.at(rtindex));
	return vertex.scan != NULL && IsA(vertex.scan, SeqScan) &&
		RptStackMatches(state, vertex.scan, parent) &&
		attno > 0 && RptScanKeyPosition(vertex.scan, key) == attno;
}

/* Refuse the whole class before changing any scan stack. */
static bool
rpt_preflight_backward_class(const RptTransferMgr &schedule,
							 const RptInjectionState &state,
							 const std::map<int, int> &widths,
							 RptBackwardClass &cls)
{
	for (const auto &target : cls.consumers)
	{
		if (!target.accepted)
			continue;
		const auto &proof = target.proof;
		auto width = widths.find(proof.producer_slice_index);
		if (proof.producer_key == cls.producer_key &&
			width != widths.end() && width->second == cls.producer_processes &&
			rpt_backward_endpoint(schedule, state, cls.producer_rtindex,
								  cls.producer_key, proof.producer_parent, proof.producer_attno) &&
			rpt_backward_endpoint(schedule, state, proof.consumer_rtindex,
								  proof.consumer_key, proof.consumer_parent, proof.consumer_attno))
			continue;

		cls.accepted = false;
		cls.reason = "backward placement changed or key is unavailable";
		for (auto &consumer : cls.consumers)
			if (consumer.accepted)
			{
				consumer.accepted = false;
				consumer.reason = cls.reason;
			}
		return false;
	}
	return true;
}

/* Wrap the current stack tops so the producer sees forward-filtered rows. */
static void
rpt_install_backward_class(const RptTransferMgr &schedule,
						   const RptBackwardClass &cls, RptInjectionState *state)
{
	Plan *scan = schedule.vertices.at(
		schedule.vertex_index_by_rtindex.at(cls.producer_rtindex)).scan;
	Plan *producer = RptPgCall([&]() {
		CustomScan *node = AnserBuildBloomProducerScan(state->tops.at(scan),
			RptScanKeyPosition(scan, cls.producer_key), cls.condition_id,
			cls.condition_key.c_str(), cls.total_elems, (Size) cls.max_payload,
			cls.planned_bytes, cls.producer_processes);
		node->scan.plan.plan_node_id = state->next_plan_node_id++;
		return (Plan *) node;
	});
	RptRelinkStack(state, scan, producer);
	for (const auto &target : cls.consumers)
	{
		if (!target.accepted)
			continue;
		const auto &proof = target.proof;
		scan = schedule.vertices.at(
			schedule.vertex_index_by_rtindex.at(proof.consumer_rtindex)).scan;
		Plan *consumer = RptPgCall([&]() {
			CustomScan *node = AnserBuildBloomConsumerScan(state->tops.at(scan),
				proof.consumer_attno, cls.condition_id, cls.condition_key.c_str(),
				cls.total_elems, (Size) cls.max_payload, cls.planned_bytes,
				cls.producer_processes, proof.defer_first);
			node->scan.plan.plan_node_id = state->next_plan_node_id++;
			return (Plan *) node;
		});
		RptRelinkStack(state, scan, consumer);
	}
}

void
RptInjectBackward(PlannedStmt *stmt, const RptTransferMgr &schedule,
				  const std::vector<RptForwardComponent> &forward,
				  const RptForwardParams &params, RptFilterSizeFn size_fn,
				  bool materialize, RptInjectionState *state,
				  std::string *trace, bool *injected)
{
	std::map<int, int> widths;
	for (const auto &decision : schedule.backward_consumers)
		if (decision.eligible)
		{
			for (int slice_index : {decision.producer_slice_index, decision.consumer_slice_index})
				if (!widths.count(slice_index))
					widths[slice_index] = RptPgCall([&]() {
						return AnserSliceProducers(stmt, slice_index);
					});
		}
	std::vector<RptBackwardClass> classes;
	RptSelectBackward(schedule, forward, params, widths, size_fn, &classes);
	for (auto &cls : classes)
	{
		if (!cls.accepted || !rpt_preflight_backward_class(schedule, *state, widths, cls))
			continue;
		RptAssignBackwardChannel(*state, cls);
		if (!materialize)
			continue;
		rpt_install_backward_class(schedule, cls, state);
		*injected = true;
	}
	if (trace)
		RptDumpBackward(schedule, classes, trace);
}
