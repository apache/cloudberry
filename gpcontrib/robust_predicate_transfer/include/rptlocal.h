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
 * rptlocal.h
 *	  In-process bloom handoff for local reducer hops.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/include/rptlocal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPTLOCAL_H
#define RPTLOCAL_H

#include "nodes/plannodes.h"

extern void RptRegisterLocalMethods(void);
extern CustomScan *RptBuildLocalProducerScan(Plan *child, AttrNumber key_attno,
											 uint32 condition_id, int64 total_elems);
extern CustomScan *RptBuildLocalConsumerScan(Plan *child, AttrNumber key_attno,
											 uint32 condition_id, int64 total_elems);
extern bool RptIsLocalScan(const Plan *plan);

#endif							/* RPTLOCAL_H */
