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
 *	  gpcontrib/gp_stats_collector/src/ProtoUtils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROTOUTILS_H
#define PROTOUTILS_H

#include "protos/gpsc_set_service.pb.h"

struct QueryDesc;
struct ICStatistics;
class Config;

google::protobuf::Timestamp current_ts();
void set_query_plan(gpsc::SetQueryReq *req, QueryDesc *query_desc,
					const Config &config);
void set_query_text(gpsc::SetQueryReq *req, QueryDesc *query_desc,
					const Config &config);
void clear_big_fields(gpsc::SetQueryReq *req);
void set_query_info(gpsc::SetQueryReq *req);
void set_qi_nesting_level(gpsc::SetQueryReq *req, int nesting_level);
void set_qi_slice_id(gpsc::SetQueryReq *req);
void set_qi_error_message(gpsc::SetQueryReq *req, const char *err_msg,
						  const Config &config);
void set_gp_metrics(gpsc::GPMetrics *metrics, QueryDesc *query_desc,
					int nested_calls, double nested_time);
void set_ic_stats(gpsc::MetricInstrumentation *metrics,
				  const ICStatistics *ic_statistics);
gpsc::SetQueryReq create_query_req(gpsc::QueryStatus status);
double protots_to_double(const google::protobuf::Timestamp &ts);
void set_analyze_plan_text(QueryDesc *query_desc, gpsc::SetQueryReq *message,
						   const Config &config);

#endif /* PROTOUTILS_H */
