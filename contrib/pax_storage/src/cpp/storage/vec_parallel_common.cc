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
 * vec_parallel_common.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec_parallel_common.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec_parallel_common.h"

#include <algorithm>
#include <stdexcept>

#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "comm/paxc_wrappers.h"
#include "comm/vec_numeric.h"
#include "storage/file_system.h"
#include "storage/filter/pax_sparse_filter.h"
#ifdef VEC_BUILD

namespace pax {

class PaxRecordBatchGenerator {
 public:
  PaxRecordBatchGenerator(const std::shared_ptr<PaxFragmentInterface> &desc)
      : desc_(desc) {}
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    return desc_->Next();
  }

 private:
  std::shared_ptr<PaxFragmentInterface> desc_;
};

template <typename T>
class ParallelIteratorImpl : public ParallelIterator<T> {
 public:
  ParallelIteratorImpl(std::vector<T> &&v) : v_(std::move(v)), index_(0) {}
  virtual ~ParallelIteratorImpl() = default;
  std::optional<T> Next() override;

 private:
  std::vector<T> v_;
  std::atomic_int32_t index_;
};

template <typename T>
std::optional<T> ParallelIteratorImpl<T>::Next() {
  int32_t index;
  int32_t next;
  auto size = static_cast<int32_t>(v_.size());

  index = index_.load(std::memory_order_relaxed);
  do {
    next = index + 1;
  } while (index < size && !index_.compare_exchange_weak(
                               index, next, std::memory_order_relaxed));

  if (index >= size) return std::optional<T>();
  return std::optional<T>(v_[index]);
}

PaxFragmentInterface::PaxFragmentInterface(
    const std::shared_ptr<ParallelScanDesc> &scan_desc)
    : scan_desc_(scan_desc), block_no_(-1) {
  Assert(scan_desc && scan_desc->GetRelation());

  relation_ = scan_desc->GetRelation();
}

void PaxFragmentInterface::Release() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

void PaxFragmentInterface::InitAdapter() {
  if (adapter_)
    adapter_->Reset();
  else
    adapter_ = std::make_shared<VecAdapter>(RelationGetDescr(relation_),
                                            scan_desc_->ShouldBuildCtid());
}

// return true if open micro partition successfully, false if no more micro
// partition left
bool PaxFragmentInterface::OpenFile() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
    visimap_ = nullptr;
  }

  auto desc = scan_desc_.get();

  auto op = desc->Iterator()->Next();
  if (!op) return false;

  auto file_system = desc->GetFileSystem();
  auto m = std::move(op).value();
  auto filter = desc->GetPaxFilter();

  MicroPartitionReader::ReaderOptions options;
  block_no_ = m->GetBlockId();

  // TODO: convert arrow filter to pax filter
  options.filter = filter;
  options.reused_buffer = nullptr;
  std::tie(visimap_, options.visibility_bitmap) = m->GetVisibilityBitmap(file_system);

  InitAdapter();

  auto data_file = file_system->Open(m->GetFileName(), fs::kReadMode, desc->GetFileSystemOptions());
  std::shared_ptr<File> toast_file;
  if (auto name = m->GetToastName(); !name.empty()) {
    toast_file = file_system->Open(name, fs::kReadMode, desc->GetFileSystemOptions());
  }
  auto reader = std::make_unique<OrcReader>(std::move(data_file), 
    std::move(toast_file));


  reader_ = std::make_unique<PaxVecReader>(std::move(reader), adapter_, filter);
  reader_->Open(options);
  return true;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
PaxFragmentInterface::Next() {
  std::shared_ptr<arrow::RecordBatch> result;

  if (reader_) result = reader_->ReadBatch(this);

  while (!result) {
    // If no more tuples in the current reader, try next micro partition
    if (!OpenFile())
      // No micro partitions left
      return arrow::IterationTraits<std::shared_ptr<arrow::RecordBatch>>::End();

    // If the current micro partition has no tuple, try next one
    result = reader_->ReadBatch(this);
  }
  return result;
}

arrow::Result<arrow::RecordBatchIterator>
PaxFragmentInterface::ScanBatchesAsyncImpl(
    const std::shared_ptr<arrow::dataset::ScanOptions> &options) {
  PaxRecordBatchGenerator g(
      std::dynamic_pointer_cast<PaxFragmentInterface>(shared_from_this()));

  Assert(scan_desc_->ScanSchema()->Equals(options->projected_schema));

  return arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>(std::move(g));
}

void ParallelScanDesc::CalculateScanColumns(
    const std::vector<std::pair<const char *, size_t>> &table_names) {
  Assert(scan_columns_.empty());

  auto natts = RelationGetNumberOfAttributes(relation_);
  std::vector<bool> proj_bits(natts, false);

  build_ctid_bitmap_ = false;
  for (int i = 0, n = scan_schema_->num_fields(); i < n; i++) {
    const auto &name = scan_schema_->field(i)->name();
    auto pname = arrow::ExtractFieldName(name);
    auto index = arrow::FindFieldIndex(table_names, pname);

    if (index >= 0) {
      Assert(!proj_bits[index]);  // only once
      scan_columns_.push_back(index);
      proj_bits[index] = true;
    } else {
      Assert(index == SelfItemPointerAttributeNumber);
      Assert(!build_ctid_bitmap_);  // only once

      build_ctid_bitmap_ = true;
      scan_columns_.push_back(SelfItemPointerAttributeNumber);
    }
  }

  Assert(pax_filter_);
  pax_filter_->SetColumnProjection(std::move(proj_bits));
}


arrow::Status ParallelScanDesc::Initialize(Relation relation,
  const std::shared_ptr<arrow::Schema> &table_schema,
  const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options,
  FileSystem *file_system, std::shared_ptr<FileSystemOptions> fs_options,
  void *context,
  pax::IteratorBase<std::shared_ptr<MicroPartitionInfoProvider>> &&it) {
  relation_ = relation;
  file_system_ = file_system;
  fs_options_ = std::move(fs_options);
  Assert(relation && file_system);

  auto tupdesc = RelationGetDescr(relation);
  PlanState *ps = (PlanState *)context;

  std::vector<std::shared_ptr<MicroPartitionInfoProvider>> result;

  table_schema_ = table_schema;
  scan_schema_ = scan_options->dataset_schema;
  pax_filter_ = std::make_shared<PaxFilter>();

  std::vector<std::pair<const char *, size_t>> table_names;
  for (int i = 0, n = table_schema_->num_fields(); i < n; i++)
    table_names.push_back(
        arrow::ExtractFieldName(table_schema_->field(i)->name()));

  CalculateScanColumns(table_names);
  if (pax_enable_sparse_filter)
    pax_filter_->InitSparseFilter(relation, ps->plan->qual);

  while (it.HasNext()) {
    auto meta = it.Next();
    bool ok = true;
    if (pax_enable_sparse_filter && pax_filter_->SparseFilterEnabled()) {
      MicroPartitionStatsProvider provider(meta->GetStats());
      ok = pax_filter_->ExecSparseFilter(provider, tupdesc,
                                         PaxSparseFilter::StatisticsKind::kFile);
    }
    if (ok) result.emplace_back(std::move(meta));
  }
  it.Release();

  num_micro_partitions_ = static_cast<int>(result.size());
  iterator_ = std::make_unique<ParallelIteratorImpl<std::shared_ptr<MicroPartitionInfoProvider>>>(std::move(result));

  return arrow::Status::OK();
}

void ParallelScanDesc::Release() {
  Assert(relation_);

  if (pax_enable_debug && pax_filter_) {
    pax_filter_->LogStatistics();
  }
  relation_ = nullptr;
  iterator_ = nullptr;
}

ParallelScanDesc::FragmentIteratorInternal::FragmentIteratorInternal(
    const std::shared_ptr<ParallelScanDesc> &desc)
    : desc_(desc), fragment_counter_(0) {}

arrow::Result<std::shared_ptr<arrow::dataset::Fragment>>
ParallelScanDesc::FragmentIteratorInternal::Next() {
  // limit the number of fragment threads.
  if (fragment_counter_ >= desc_->num_micro_partitions_)
    return arrow::IterationTraits<
        std::shared_ptr<arrow::dataset::Fragment>>::End();

  fragment_counter_++;
  return std::static_pointer_cast<arrow::dataset::Fragment>(
      std::make_shared<PaxFragmentInterface>(desc_));
}


}  // namespace pax

#endif
