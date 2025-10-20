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
 * micro_partition_row_filter_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_row_filter_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_row_filter_reader.h"

#include <algorithm>

#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/filter/pax_filter.h"
#include "storage/filter/pax_row_filter.h"
#include "storage/filter/pax_sparse_filter.h"
#include "storage/pax_defined.h"
#include "storage/pax_itemptr.h"

namespace pax {
static inline bool TestExecQual(ExprState *estate, ExprContext *econtext) {
  CBDB_WRAP_START;
  { return ExecQual(estate, econtext); }
  CBDB_WRAP_END;
}

std::unique_ptr<MicroPartitionReader> MicroPartitionRowFilterReader::New(
    std::unique_ptr<MicroPartitionReader> &&reader,
    std::shared_ptr<PaxFilter> filter,
    std::shared_ptr<Bitmap8> visibility_bitmap) {
  Assert(reader);
  Assert(filter && filter->GetRowFilter());

  auto r = std::make_unique<MicroPartitionRowFilterReader>();
  r->SetReader(std::move(reader));
  r->SetVisibilityBitmap(visibility_bitmap);
  r->filter_ = filter;
  return r;
}

std::shared_ptr<MicroPartitionReader::Group>
MicroPartitionRowFilterReader::GetNextGroup(TupleDesc desc) {
  auto ngroups = reader_->GetGroupNums();
retry_next_group:
  if (group_index_ >= ngroups) return nullptr;
  auto info = reader_->GetGroupStatsInfo(group_index_);
  ++group_index_;
  if (!filter_->ExecSparseFilter(*info, desc,
                                 PaxSparseFilter::StatisticsKind::kGroup)) {
    goto retry_next_group;
  }
  group_ = reader_->ReadGroup(group_index_ - 1);
  current_group_row_index_ = 0;
  return group_;
}

void MicroPartitionRowFilterReader::LoadExprFilterColumns(
    MicroPartitionReader::Group *group, TupleDesc desc,
    const ExecutionFilterContext *ctx, size_t row_index, TupleTableSlot *slot) {
  // There will not be duplicate attnos here because the attnos in ctx come from
  // qual expressions. For each column index, there is at most one corresponding
  // attno in ctx->attnos, so no attno appears more than once. As a result, this
  // loop does not load the same column multiple times.
  for (int i = 0; i < ctx->size; i++) {
    auto attno = ctx->attnos[i];
    Assert(attno > 0);
    std::tie(slot->tts_values[attno - 1], slot->tts_isnull[attno - 1]) =
        group->GetColumnValue(desc, attno - 1, row_index);
  }
}

bool MicroPartitionRowFilterReader::EvalBloomNode(
    const ExecutionFilterContext *ctx, MicroPartitionReader::Group *group,
    TupleDesc desc, size_t row_index, int bloom_index) {
  Assert(bloom_index >= 0 &&
         (size_t)bloom_index < ctx->runtime_bloom_keys.size());
  const auto &skd = ctx->runtime_bloom_keys[bloom_index];
  const ScanKey sk = const_cast<ScanKeyData *>(&skd);
  bool isnull = false;
  Datum val;
  std::tie(val, isnull) =
      group->GetColumnValue(desc, sk->sk_attno - 1, row_index);
  if (isnull) return true;
  bloom_filter *bf = (bloom_filter *)DatumGetPointer(sk->sk_argument);
  return !bloom_lacks_element(bf, (unsigned char *)&val, sizeof(Datum));
}

bool MicroPartitionRowFilterReader::EvalExprNode(
    const ExecutionFilterContext *ctx, TupleTableSlot *slot, int expr_index) {
  return TestRowScanInternal(slot, ctx->estates[expr_index],
                             ctx->attnos[expr_index]);
}

// Execute a filter node.
// During the sampling phase, updates the filter's pass rate statistics.
bool MicroPartitionRowFilterReader::EvalFilterNode(
    ExecutionFilterContext *ctx, MicroPartitionReader::Group *group,
    TupleDesc desc, size_t row_index, TupleTableSlot *slot,
    ExecutionFilterContext::FilterNode &node, bool update_stats) {
  bool pass = true;
  if (node.kind == ExecutionFilterContext::FilterKind::kBloom) {
    pass = EvalBloomNode(ctx, group, desc, row_index, node.index);
    if (ctx->ps->instrument && !pass) ctx->ps->instrument->nfilteredPRF += 1;
  } else {
    pass = EvalExprNode(ctx, slot, node.index);
  }
  if (update_stats) {
    node.tested++;
    node.passed += pass ? 1 : 0;
  }
  return pass;
}

// Applies the row filter nodes to the current tuple in two phases: Sampling and
// Filtering.
// In the sampling phase, pass rates for each filter expression are collected on
// the first 64k rows, then the filters are sorted by effectiveness (lower pass
// rate first) for optimal filtering order.
// In the filtering phase, filters are applied in the determined order with
// short-circuit evaluation; a failure in any filter causes immediate rejection
// of the tuple.
bool MicroPartitionRowFilterReader::ApplyFiltersWithSampling(
    ExecutionFilterContext *ctx, MicroPartitionReader::Group *group,
    TupleDesc desc, size_t row_index, TupleTableSlot *slot) {
  if (!ctx->sampling) {
    for (auto &node : ctx->filter_nodes) {
      if (!EvalFilterNode(ctx, group, desc, row_index, slot, node, false)) {
        return false;
      }
    }
    return true;
  }

  ctx->sample_rows++;
  bool all_pass = true;
  // in the sampling phase, we need to evaluate all filter nodes, if any node
  // fails, the tuple is rejected
  for (auto &node : ctx->filter_nodes) {
    if (!EvalFilterNode(ctx, group, desc, row_index, slot, node, true)) {
      all_pass = false;
    }
  }

  if (ctx->sample_rows >= ctx->sample_target) {
    for (auto &node : ctx->filter_nodes) {
      node.score =
          (node.tested == 0) ? 1.0 : (double)node.passed / (double)node.tested;
    }
    std::stable_sort(ctx->filter_nodes.begin(), ctx->filter_nodes.end(),
                     [](const auto &a, const auto &b) {
                       // Lower pass rate first (better selectivity)
                       if (a.score != b.score) return a.score < b.score;
                       return (int)a.kind < (int)b.kind;
                     });
    ctx->sampling = false;
  }
  return all_pass;
}

bool MicroPartitionRowFilterReader::ReadTuple(TupleTableSlot *slot) {
  auto g = group_;
  Assert(filter_->GetRowFilter());
  auto ctx = filter_->GetRowFilter()->GetExecutionFilterContext();
  const auto &remaining_columns =
      filter_->GetRowFilter()->GetRemainingColumns();
  size_t nrows;
  TupleDesc desc;

  desc = slot->tts_tupleDescriptor;
  slot->tts_nvalid = desc->natts;

retry_next_group:
  if (group_ == nullptr) {
    g = GetNextGroup(desc);
    if (!g) return false;
  }
  nrows = g->GetRows();
retry_next:
  if (current_group_row_index_ >= nrows) {
    group_ = nullptr;
    goto retry_next_group;
  }

  if (micro_partition_visibility_bitmap_) {
    while (micro_partition_visibility_bitmap_->Test(current_group_row_index_ +
                                                    g->GetRowOffset())) {
      current_group_row_index_++;
      if (current_group_row_index_ >= nrows) {
        group_ = nullptr;
        goto retry_next_group;
      }
    }
  }

  LoadExprFilterColumns(g.get(), desc, ctx, current_group_row_index_, slot);
  if (!ApplyFiltersWithSampling(const_cast<ExecutionFilterContext *>(ctx),
                                g.get(), desc, current_group_row_index_,
                                slot)) {
    current_group_row_index_++;
    goto retry_next;
  }

  for (auto attno : remaining_columns) {
    std::tie(slot->tts_values[attno - 1], slot->tts_isnull[attno - 1]) =
        g->GetColumnValue(desc, attno - 1, current_group_row_index_);
  }
  current_group_row_index_++;
  SetTupleOffset(&slot->tts_tid,
                 g->GetRowOffset() + current_group_row_index_ - 1);
  if (ctx->estate_final && !TestRowScanInternal(slot, ctx->estate_final, 0))
    goto retry_next;

  return true;
}

bool MicroPartitionRowFilterReader::TestRowScanInternal(TupleTableSlot *slot,
                                                        ExprState *estate,
                                                        AttrNumber attno) {
  Assert(filter_);
  Assert(estate);
  Assert(slot);
  Assert(attno >= 0);
  Assert(filter_->GetRowFilter());

  auto ctx = filter_->GetRowFilter()->GetExecutionFilterContext();
  auto econtext = ctx->econtext;
  econtext->ecxt_scantuple = slot;

  ResetExprContext(econtext);
  return TestExecQual(estate, econtext);
}
}  // namespace pax
