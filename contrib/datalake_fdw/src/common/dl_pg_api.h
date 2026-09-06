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
 * dl_pg_api.h
 *	  The PostgreSQL headers, safe to include from C++.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_pg_api.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_PG_API_H
#define DL_PG_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "access/xact.h"
#include "utils/elog.h"

#ifdef __cplusplus
}
#endif

#endif							/* DL_PG_API_H */
