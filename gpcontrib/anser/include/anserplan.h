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
 * anserplan.h
 *	  Post-planning transformation that injects Anser runtime bloom-filter
 *	  producer/consumer nodes into a finished plan tree.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserplan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERPLAN_H
#define ANSERPLAN_H

#include "nodes/plannodes.h"

/*
 * Post-plan pass: recognize the supported join shape in a finished PlannedStmt
 * and inject an Anser bloom-filter producer (on the hash build side) and a
 * consumer (above the probe scan).  Called once from planner(), so it covers
 * both the Postgres planner and ORCA.  A no-op unless the Anser runtime-filter
 * GUCs are on and this is a coordinator SELECT.
 */
extern void AnserApplyRuntimeFilters(PlannedStmt *stmt);

/*
 * Register the two CustomScan providers (producer, consumer) so their methods
 * resolve by name when a dispatched plan is deserialized.  Must run once per
 * backend (QD and every QE) before any plan execution.
 */
extern void AnserRegisterRuntimeFilterMethods(void);

/*
 * Node builders (implemented in anserplanexec.c, where the CustomScan method
 * tables live).  Each wraps `child` in a pass-through CustomScan carrying the
 * runtime-filter parameters in custom_private; the caller assigns plan_node_id.
 * `key_attno` is the build (producer) / probe (consumer) join-key attno in the
 * child's output tuple.
 */
extern CustomScan *AnserBuildBloomProducerScan(Plan *child, AttrNumber key_attno,
											   uint32 condition_id,
											   const char *condition_key,
											   int64 total_elems,
											   Size max_payload_bytes,
											   int64 planned_bytes);
extern CustomScan *AnserBuildBloomConsumerScan(Plan *child, AttrNumber key_attno,
											   uint32 condition_id,
											   const char *condition_key,
											   int64 total_elems,
											   Size max_payload_bytes,
											   int64 planned_bytes);

#endif							/* ANSERPLAN_H */
