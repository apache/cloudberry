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
 * pax_row_filter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_row_filter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/pax_defined.h"

namespace pax {

struct ExecutionFilterContext {
  ExprContext *econtext;
  ExprState *estate_final = nullptr;
  ExprState **estates;
  AttrNumber *attnos;
  PlanState *ps;
  int size = 0;
  inline bool HasExecutionFilter() const { return size > 0 || estate_final; }

  // runtime bloom filters pushed down via SeqScanState->filters
  // (SK_BLOOM_FILTER)
  std::vector<ScanKeyData> runtime_bloom_keys;

  // unified filter nodes (expr + bloom) for execution ordering
  enum class FilterKind { kExpr, kBloom };
  struct FilterNode {
    FilterKind kind;
    int index;  // index in estates (for kExpr) or in runtime_bloom_keys (for
                // kBloom)
    uint64 tested = 0;   // number of rows tested during sampling
    uint64 passed = 0;   // number of rows passed during sampling
    double score = 1.0;  // pass rate used for ordering (lower is better)
  };
  std::vector<FilterNode> filter_nodes;

  // sampling control to determine filter order
  bool sampling = true;
  uint64 sample_target = 65536;  // number of rows for sampling phase
  uint64 sample_rows = 0;        // rows seen in sampling
};

class PaxRowFilter final {
 public:
  PaxRowFilter();

  bool Initialize(Relation rel, PlanState *ps,
                  const std::vector<bool> &projection, ScanKey key, int nkeys);

  inline const ExecutionFilterContext *GetExecutionFilterContext() const {
    return &efctx_;
  }

  inline const std::vector<AttrNumber> &GetRemainingColumns() const {
    return remaining_attnos_;
  }

 private:
  void FillRemainingColumns(Relation rel, const std::vector<bool> &projection);

 private:
  ExecutionFilterContext efctx_;
  // all selected columns - single row filting columns
  // before running final cross columns expression filtering, the remaining
  // columns should be filled.
  std::vector<AttrNumber> remaining_attnos_;
};

};  // namespace pax