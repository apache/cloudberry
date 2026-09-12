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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * PlanNodeEmitter.cpp
 *		Build and send per-node protobuf messages to the yagpcc UDS sink.
 *
 * This file is the bridge between the C pg_query_state layer and the C++
 * protobuf / UDS connector infrastructure.  It implements the functions
 * declared in PlanNodeEmitter.h and callable from plain C:
 *
 *   gpsc_qs_sync_config()   -- reload the Config singleton
 *   gpsc_emit_node_batch()  -- serialize a plan-tree snapshot and send it
 *   gpsc_emit_query_plan()  -- serialize a plan document and send it
 *
 * The outgoing message types are yagpcc::SetPerNodeBatchReq and
 * yagpcc::SetQueryPlanReq (generated from protos/yagpcc_set_per_node.proto).
 * Transmission is handled by UDSConnector, which prepends the 8-byte extended
 * protocol header before writing to the socket.
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/PlanNodeEmitter.cpp
 */

#include "PlanNodeEmitter.h"
#include "protos/yagpcc_set_per_node.pb.h"
#include "UDSConnector.h"
#include "Config.h"
#include "ProtoUtils.h"

/* Module-private Config instance shared across all emit calls in a session. */
static Config pne_config;

/*
 * gpsc_qs_sync_config -- reload the Config singleton.
 *
 * Must be called before a gpsc_emit_node_batch() call so that the UDS path
 * and other settings are up to date.  It is a no-op when the config has not
 * changed since the last call.
 */
extern "C" void
gpsc_qs_sync_config()
{
	pne_config.sync();
}

/*
 * map_node_status -- convert a QsNodeStatus enum to yagpcc::PlanNodeStatus.
 *
 * Returns PLAN_NODE_STATUS_UNSPECIFIED for any value not recognised by the
 * switch, which is safe because the receiver ignores unknown status codes.
 */
static yagpcc::PlanNodeStatus
map_node_status(QsNodeStatus status)
{
	switch (status)
	{
		case QS_NODE_STATUS_INITIALIZED:
			return yagpcc::PLAN_NODE_STATUS_INITIALIZED;
		case QS_NODE_STATUS_EXECUTING:
			return yagpcc::PLAN_NODE_STATUS_EXECUTING;
		case QS_NODE_STATUS_FINISHED:
			return yagpcc::PLAN_NODE_STATUS_FINISHED;
		default:
			return yagpcc::PLAN_NODE_STATUS_UNSPECIFIED;
	}
}

extern "C" void
gpsc_emit_node_batch(GpscNodeSample **nodes, int count, const char *trace_id)
{
	if (count <= 0)
		return;

	yagpcc::SetPerNodeBatchReq request;

	/* Timestamp */
	*request.mutable_datetime() = current_ts();
	request.set_trace_id(trace_id, GPSC_TRACE_ID_LEN);

	auto *sk = request.mutable_segment_key();
	sk->set_dbid(nodes[0]->dbid);
	sk->set_segindex(nodes[0]->segindex);

	for (int i = 0; i < count; i++)
	{
		GpscNodeSample   *node = nodes[i];
		yagpcc::BatchNode *bn  = request.add_nodes();

		bn->set_pid(node->pid);
		bn->set_plan_node_id(node->plan_node_id);
		bn->set_parent_plan_node_id(node->parent_plan_node_id);
		bn->set_node_type(node->node_tag);
		bn->set_slice_id(node->slice_id);
		bn->set_plan_rows(node->plan_rows);
		bn->set_relation_oid(node->relation_oid);
		bn->set_ntuples(node->ntuples);
		bn->set_tuplecount(node->tuplecount);
		bn->set_nloops(node->nloops);
		bn->set_startup(node->startup);
		bn->set_total(node->total);
		bn->set_firsttuple(node->firsttuple);
		bn->set_shared_blks_hit(node->shared_blks_hit);
		bn->set_shared_blks_read(node->shared_blks_read);
		bn->set_node_status(map_node_status(node->node_status));
		bn->set_eof(node->eof);
		bn->set_relation_name(node->relation_name);
		bn->set_ccnt(node->ccnt);
		/*
		 * executed_at is the snapshot instant, shared by every node in this
		 * pass. It is stamped per node (not at message level) because the
		 * receiver aggregates nodes across segments and loses the batch
		 * grouping; each node needs its own compute time to derive a per-node
		 * rate. Same value as datetime here since one walk = one instant.
		 */
		*bn->mutable_executed_at() = request.datetime();
		bn->set_workfile_created(node->workfile_created);
		bn->set_workmem_used(node->workmem_used);
		bn->set_workmem_wanted(node->workmem_wanted);
		/*
		 * Derived rate fields, computed in signal_handler from the per-node
		 * rolling state (prev ntuples + prev executed_at). They MUST be
		 * serialized here too: the receiver keys per invocation trace_id and
		 * sees each node once, so it cannot re-derive a rate on its side.
		 */
		bn->set_ntuples_delta(node->ntuples_delta);
		bn->set_tuples_per_sec(node->tuples_per_sec);
		bn->set_time_since_init_sec(node->time_since_init_sec);
		bn->set_stalled(node->stalled);
	}

	UDSConnector::report_per_node_batch(request, pne_config);
}

extern "C" void
gpsc_emit_query_plan(int32_t tmid, int32_t ssid, int32_t ccnt,
					 const char *plan_doc, int32_t format)
{
	if (plan_doc == nullptr || plan_doc[0] == '\0')
		return;

	yagpcc::SetQueryPlanReq request;

	*request.mutable_datetime() = current_ts();

	auto *qk = request.mutable_query_key();
	qk->set_tmid(tmid);
	qk->set_ssid(ssid);
	qk->set_ccnt(ccnt);

	request.set_plan_doc(plan_doc);
	request.set_format(format);

	UDSConnector::report_query_plan(request, pne_config);
}
