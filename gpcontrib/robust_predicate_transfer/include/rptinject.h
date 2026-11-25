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
 * rptinject.h
 *    Shared scan-stack state and backward injection interface.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPTINJECT_H
#define RPTINJECT_H

#include "rpt.h"

/* One allocator and one relink point per original scan, shared by both passes. */
struct RptInjectionState
{
	uint32 next_condition_id = 0;
	int next_plan_node_id = 0;
	std::map<Plan *, Plan *> parents;
	std::map<Plan *, Plan *> tops;
};

extern AttrNumber RptScanKeyPosition(Plan *scan, const RptKey &key);
extern bool RptStackMatches(const RptInjectionState &state, Plan *scan,
						   Plan *expected_parent);
extern void RptRelinkStack(RptInjectionState *state, Plan *scan, Plan *top);
extern void RptInjectBackward(PlannedStmt *stmt, const RptTransferMgr &schedule,
							 const std::vector<RptForwardComponent> &forward,
							 const RptForwardParams &params,
							 RptFilterSizeFn size_fn, bool materialize,
							 RptInjectionState *state, std::string *trace,
							 bool *injected);

#endif /* RPTINJECT_H */
