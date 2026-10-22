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
 * PlanNodeEmitter.h
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/PlanNodeEmitter.h
 */

#ifndef PLAN_NODE_EMITTER_H
#define PLAN_NODE_EMITTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "pg_query_state/qs_types.h"

extern void gpsc_emit_node_batch(GpscNodeSample **nodes, int count,
								 const char *trace_id);
extern void gpsc_emit_query_plan(int32_t tmid, int32_t ssid, int32_t ccnt,
								 const char *plan_doc, int32_t format);
extern void gpsc_qs_sync_config();

#ifdef __cplusplus
}
#endif

#endif /* PLAN_NODE_EMITTER_H */
