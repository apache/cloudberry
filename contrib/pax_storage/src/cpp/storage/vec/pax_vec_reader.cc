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
 * pax_vec_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_reader.h"

#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "storage/pax_itemptr.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/filter/pax_sparse_filter.h"
#ifdef VEC_BUILD

namespace pax {

PaxVecReader::PaxVecReader(std::unique_ptr<MicroPartitionReader> &&reader,
                           std::shared_ptr<VecAdapter> adapter,
                           std::shared_ptr<PaxFilter> filter)
    : adapter_(std::move(adapter)),
      current_group_index_(0),
      ctid_offset_(0),
      filter_(std::move(filter)) {
  Assert(reader && adapter_);
  SetReader(std::move(reader));
}

void PaxVecReader::Open(const ReaderOptions &options) {
  auto visimap = options.visibility_bitmap;
  reader_->Open(options);
  if (visimap) {
    adapter_->SetVisibitilyMapInfo(visimap);
  }
}

void PaxVecReader::Close() { reader_->Close(); }

PaxVecReader::~PaxVecReader() {}

std::shared_ptr<arrow::RecordBatch> PaxVecReader::ReadBatch(
    PaxFragmentInterface *frag) {
  auto desc = adapter_->GetRelationTupleDesc();
  std::shared_ptr<arrow::RecordBatch> result;
  size_t flush_nums_of_rows = 0;

retry_next_group:
  if (!working_group_) {
    if (current_group_index_ >= reader_->GetGroupNums()) {
      return nullptr;
    }
    auto group_index = current_group_index_++;
    auto info = reader_->GetGroupStatsInfo(group_index);
    if (filter_ && !filter_->ExecSparseFilter(
                       *info, desc, PaxSparseFilter::StatisticsKind::kGroup)) {
      goto retry_next_group;
    }

    working_group_ = reader_->ReadGroup(group_index);
    adapter_->SetDataSource(working_group_->GetAllColumns(),
                            working_group_->GetRowOffset());
  }

  if (!adapter_->AppendToVecBuffer()) {
    working_group_ = nullptr;
    goto retry_next_group;
  }

  result = adapter_->FlushVecBuffer(ctid_offset_, frag, flush_nums_of_rows);
  ctid_offset_ += flush_nums_of_rows;
  if (!result) {
    working_group_ = nullptr;
    goto retry_next_group;
  }

  Assert(flush_nums_of_rows > 0);
  return result;
}

bool PaxVecReader::ReadTuple(TupleTableSlot *slot) {
  auto desc = adapter_->GetRelationTupleDesc();
retry_read_group:
  if (!working_group_) {
    if (current_group_index_ >= reader_->GetGroupNums()) {
      return false;
    }
    auto group_index = current_group_index_++;
    auto info = reader_->GetGroupStatsInfo(group_index);
    if (filter_ && !filter_->ExecSparseFilter(
                       *info, desc, PaxSparseFilter::StatisticsKind::kGroup)) {
      goto retry_read_group;
    }

    working_group_ = reader_->ReadGroup(group_index);

    adapter_->SetDataSource(working_group_->GetAllColumns(),
                            working_group_->GetRowOffset());
  }

  auto flush_nums_of_rows = adapter_->AppendToVecBuffer();
  if (flush_nums_of_rows == -1) {
    working_group_ = nullptr;
    goto retry_read_group;
  }

  if (flush_nums_of_rows == 0) {
    goto retry_read_group;
  }

  adapter_->FlushVecBuffer(slot);

  return true;
}

bool PaxVecReader::GetTuple(TupleTableSlot *slot, size_t row_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxVecReader::GetGroupNums() {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

size_t PaxVecReader::GetTupleCountsInGroup(size_t group_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

std::unique_ptr<ColumnStatsProvider> PaxVecReader::GetGroupStatsInfo(
    size_t group_index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

std::unique_ptr<MicroPartitionReader::Group> PaxVecReader::ReadGroup(
    size_t index) {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

}  // namespace pax

#endif  // VEC_BUILD
