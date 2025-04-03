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
 * pax_stats.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/oper/pax_stats.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include "storage/oper/pax_oper.h"

#define LessStrategyStr "<"
#define LessEqualStrategyStr "<="
#define EqualStrategyStr "="
#define GreaterEqualStrategyStr ">="
#define GreaterStrategyStr ">"

#define ArithmeticAddStr "+"
#define ArithmeticSubStr "-"
#define ArithmeticMulStr "*"
#define ArithmeticDivStr "/"

extern StrategyNumber OpernameToStrategy(const char *data);
extern StrategyNumber InvertStrategy(StrategyNumber strategy);
extern const char *StrategyToOpername(StrategyNumber number);
extern bool SupportedArithmeticOpername(const char *opername);
namespace pax {

// Get the min/max oper from pax
bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum);
// Get the min/max oper from pg
bool MinMaxGetPgStrategyProcinfo(Oid atttypid, Oid subtype, FmgrInfo *finfos,
                                 StrategyNumber strategynum);
// Get the operator from pax
bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs);
// Get the operator from pg
bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<FmgrInfo, FmgrInfo> &finfos);

bool CollateIsSupport(Oid collid);
}  // namespace pax