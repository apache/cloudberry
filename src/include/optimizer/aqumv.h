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
 * aqumv.h
 *	  prototypes for optimizer/plan/aqumv.c.
 *
 * src/include/optimizer/aqumv.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AQUMV_H
#define AQUMV_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planmain.h"

typedef struct AqumvContextData {
    RelOptInfo      *current_rel;
    List            *raw_processed_tlist;
    Node            *raw_havingQual;
    void            *qp_extra;
    query_pathkeys_callback qp_callback;
} AqumvContextData;

typedef AqumvContextData *AqumvContext;

extern RelOptInfo* answer_query_using_materialized_views(PlannerInfo *root, AqumvContextData *aqumv_context);

#endif   /* AQUMV_H */
