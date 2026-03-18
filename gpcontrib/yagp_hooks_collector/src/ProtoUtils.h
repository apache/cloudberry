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
 * ProtoUtils.h
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/ProtoUtils.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "protos/yagpcc_set_service.pb.h"

struct QueryDesc;
struct ICStatistics;
class Config;

google::protobuf::Timestamp current_ts();
void set_query_plan(yagpcc::SetQueryReq *req, QueryDesc *query_desc,
                    const Config &config);
void set_query_text(yagpcc::SetQueryReq *req, QueryDesc *query_desc,
                    const Config &config);
void clear_big_fields(yagpcc::SetQueryReq *req);
void set_query_info(yagpcc::SetQueryReq *req);
void set_qi_nesting_level(yagpcc::SetQueryReq *req, int nesting_level);
void set_qi_slice_id(yagpcc::SetQueryReq *req);
void set_qi_error_message(yagpcc::SetQueryReq *req, const char *err_msg,
                          const Config &config);
void set_gp_metrics(yagpcc::GPMetrics *metrics, QueryDesc *query_desc,
                    int nested_calls, double nested_time);
void set_ic_stats(yagpcc::MetricInstrumentation *metrics,
                  const ICStatistics *ic_statistics);
yagpcc::SetQueryReq create_query_req(yagpcc::QueryStatus status);
double protots_to_double(const google::protobuf::Timestamp &ts);
void set_analyze_plan_text(QueryDesc *query_desc, yagpcc::SetQueryReq *message,
                           const Config &config);
