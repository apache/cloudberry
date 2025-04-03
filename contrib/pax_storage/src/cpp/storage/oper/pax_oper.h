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
 * pax_oper.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/oper/pax_oper.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <functional>
#include <map>

namespace pax {

using OperMinMaxFunc = std::function<bool(const void *, const void *, Oid)>;
using OperMinMaxKey = std::tuple<Oid, Oid, StrategyNumber>;

extern std::map<OperMinMaxKey, OperMinMaxFunc> min_max_opers;

}  // namespace pax