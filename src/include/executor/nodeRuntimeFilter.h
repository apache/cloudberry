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
 * nodeRuntimeFilter.h
 *
 * src/include/executor/nodeRuntimeFilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODERUNTIMEFILTER_H
#define NODERUNTIMEFILTER_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RuntimeFilterState *ExecInitRuntimeFilter(RuntimeFilter *node,
                                                 EState *estate, int eflags);
extern void ExecEndRuntimeFilter(RuntimeFilterState *node);
extern void ExecReScanRuntimeFilter(RuntimeFilterState *node);
extern void RFBuildFinishCallback(RuntimeFilterState *rfstate, bool parallel);
extern void RFAddTupleValues(RuntimeFilterState *rfstate, List *vals);

extern void ExecInitRuntimeFilterFinish(RuntimeFilterState *node,
                                        double inner_rows);

#endif							/* NODERUNTIMEFILTER_H */
