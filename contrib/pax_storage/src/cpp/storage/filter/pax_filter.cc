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
 * pax_filter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_filter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_filter.h"

#include "comm/cbdb_api.h"

#include "comm/bloomfilter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "comm/paxc_wrappers.h"
#include "storage/filter/pax_row_filter.h"
#include "storage/filter/pax_sparse_filter.h"

namespace pax {

PaxFilter::PaxFilter() : sparse_filter_(nullptr), row_filter_(nullptr) {}

void PaxFilter::InitSparseFilter(Relation relation, List *quals,
                                 bool allow_fallback_to_pg) {
  Assert(!sparse_filter_);
  sparse_filter_ =
      std::make_shared<PaxSparseFilter>(relation, allow_fallback_to_pg);
  sparse_filter_->Initialize(quals);
}

#ifdef VEC_BUILD
void PaxFilter::InitSparseFilter(
    Relation relation, const arrow::compute::Expression &expr,
    const std::vector<std::pair<const char *, size_t>> &table_names) {
  Assert(!sparse_filter_);
  sparse_filter_ = std::make_shared<PaxSparseFilter>(relation, false);
  sparse_filter_->Initialize(expr, table_names);
}
#endif

bool PaxFilter::SparseFilterEnabled() {
  return sparse_filter_ && sparse_filter_->ExistsFilterPath();
}

bool PaxFilter::ExecSparseFilter(const ColumnStatsProvider &provider,
                                 const TupleDesc desc, int kind) {
  if (!SparseFilterEnabled()) {
    return true;
  }
  return sparse_filter_->ExecFilter(provider, desc, kind);
}

static bool ProjShouldReadAll(const std::vector<bool> &cols) {
  if (cols.empty()) return true;

  for (size_t i = 0; i < cols.size(); i++) {
    if (!cols[i]) {
      return false;
    }
  }
  return true;
}

void PaxFilter::SetColumnProjection(std::vector<bool> &&cols) {
  proj_column_index_.clear();
  if (ProjShouldReadAll(cols)) {
    proj_.clear();
  } else {
    Assert(proj_.empty());
    proj_ = std::move(cols);
    for (size_t i = 0; i < proj_.size(); i++) {
      if (proj_[i]) proj_column_index_.emplace_back(i);
    }
  }
}

void PaxFilter::SetColumnProjection(const std::vector<int> &cols, int natts) {
  bool all_proj = (cols.size() == (size_t)natts);

#ifdef ENABLE_DEBUG
  Assert(cols.size() <= (size_t)natts);
  for (int i = 0; (size_t)i < cols.size(); i++) {
    Assert(cols[i] >= i);
    Assert(cols[i] < natts);
    AssertImply(i > 0, cols[i] > cols[i - 1]);
  }
#endif

  if (all_proj) {
    proj_.clear();
    proj_column_index_.clear();
  } else {
    proj_ = std::vector<bool>(natts, false);
    for (int i = 0; (size_t)i < cols.size(); i++) {
      proj_[cols[i]] = true;
      proj_column_index_.emplace_back(cols[i]);
    }
  }
}

void PaxFilter::InitRowFilter(Relation relation, PlanState *ps,
                              const std::vector<bool> &projection) {
  Assert(!row_filter_);
  row_filter_ = std::make_shared<PaxRowFilter>();
  if (!row_filter_->Initialize(relation, ps, projection)) {
    row_filter_ = nullptr;
  }
}

std::shared_ptr<PaxRowFilter> PaxFilter::GetRowFilter() { return row_filter_; }

void PaxFilter::LogStatistics() const {
  if (sparse_filter_) {
    sparse_filter_->LogStatistics();
  } else {
    PAX_LOG_IF(pax_enable_debug, "%s", "No sparse filter");
  }
}

}  // namespace pax