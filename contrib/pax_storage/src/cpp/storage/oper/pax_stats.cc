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
 * pax_stats.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/oper/pax_stats.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/oper/pax_stats.h"

#include "comm/cbdb_wrappers.h"

typedef struct {
  const char *opername;
  const StrategyNumber strategy;
} opername_strategy_mapping;

static const opername_strategy_mapping strategy_map[] = {
    {LessStrategyStr, BTLessStrategyNumber},
    {LessEqualStrategyStr, BTLessEqualStrategyNumber},
    {EqualStrategyStr, BTEqualStrategyNumber},
    {GreaterEqualStrategyStr, BTGreaterEqualStrategyNumber},
    {GreaterStrategyStr, BTGreaterStrategyNumber},
};

StrategyNumber InvertStrategy(StrategyNumber strategy) {
  if (strategy == InvalidStrategy) {
    return InvalidStrategy;
  }

  // Note that this is not the handling logic for the `not expression`
  // Instead, it is to flip the variables in the `opexpr` from the right side to
  // the left side For example:
  //    (where 100 < v1) == (where v1 > 100)
  //    (where 100 < v1) != (where v1 >= 100)
  //
  //    (where 100 <= v1) == (where v1 >= 100)
  //    (where 100 <= v1) != (where v1 > 100)
  switch (strategy) {
    case BTLessStrategyNumber:
      return BTGreaterStrategyNumber;
    case BTLessEqualStrategyNumber:
      return BTGreaterEqualStrategyNumber;
    case BTEqualStrategyNumber:
      return BTEqualStrategyNumber;
    case BTGreaterEqualStrategyNumber:
      return BTLessEqualStrategyNumber;
    case BTGreaterStrategyNumber:
      return BTLessStrategyNumber;
    default:
      Assert(false);
  }
  Assert(false);
  return InvalidStrategy;
}

static const char *supported_arithmetic_opname[] = {
    ArithmeticAddStr,
    ArithmeticSubStr,
    ArithmeticMulStr,
};

bool SupportedArithmeticOpername(const char *opername) {
  Assert(opername);
  if (!opername) {
    return false;
  }

  for (size_t i = 0; i < lengthof(supported_arithmetic_opname); i++) {
    if (strcmp(opername, supported_arithmetic_opname[i]) == 0) {
      return true;
    }
  }

  return false;
}

StrategyNumber OpernameToStrategy(const char *opername) {
  Assert(opername);
  if (!opername) {
    return InvalidStrategy;
  }

  for (size_t i = 0; i < lengthof(strategy_map); i++) {
    if (strcmp(opername, strategy_map[i].opername) == 0) {
      return strategy_map[i].strategy;
    }
  }

  return InvalidStrategy;
}

const char *StrategyToOpername(StrategyNumber number) {
  for (size_t i = 0; i < lengthof(strategy_map); i++) {
    if (strategy_map[i].strategy == number) {
      return strategy_map[i].opername;
    }
  }

  return nullptr;
}

namespace pax {

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum) {
  OperMinMaxKey key;

  if (!CollateIsSupport(collid)) {
    return false;
  }

  key = {atttypid, subtype, strategynum};
  auto it = min_max_opers.find(key);
  if (it != min_max_opers.end()) {
    func = it->second;
    return true;
  }
  return false;
}

bool MinMaxGetPgStrategyProcinfo(Oid atttypid, Oid subtype, FmgrInfo *finfos,
                                 StrategyNumber strategynum) {
  [[maybe_unused]] Oid opno;

  return cbdb::PGGetOperator(StrategyToOpername(strategynum),
                             PG_CATALOG_NAMESPACE, atttypid, subtype, &opno,
                             finfos);
}

bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<FmgrInfo, FmgrInfo> &finfos) {
  Oid opno_less = InvalidOid;
  Oid opno_great = InvalidOid;

  auto ok = cbdb::PGGetOperator(LessStrategyStr, PG_CATALOG_NAMESPACE, typid,
                                subtype, &opno_less, &finfos.first) &&
            cbdb::PGGetOperator(GreaterStrategyStr, PG_CATALOG_NAMESPACE, typid,
                                subtype, &opno_great, &finfos.second);
  AssertImply(ok, OidIsValid(opno_less) && OidIsValid(opno_great));
  return ok;
}

bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs) {
  return MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.first,
                                   BTLessStrategyNumber) &&
         MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.second,
                                   BTGreaterStrategyNumber);
}

bool CollateIsSupport(Oid collid) {
  return collid == InvalidOid ||  // Allow no setting collate
         collid == DEFAULT_COLLATION_OID || collid == C_COLLATION_OID ||
         collid == POSIX_COLLATION_OID;
}

}  // namespace pax
