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
 * vec_parallel_common.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec_parallel_common.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <shared_mutex>
#include <thread>
#include <unordered_map>

#ifdef VEC_BUILD
#include "comm/cbdb_api.h"

#include "storage/filter/pax_filter.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"
#include "storage/pax.h"
#include "storage/pax_itemptr.h"
#include "storage/vec/arrow_wrapper.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace pax {

template <typename T>
class ParallelIterator {
 public:
  virtual ~ParallelIterator() = default;
  virtual std::optional<T> Next() = 0;
};

class MicroPartitionInfoProvider {
 public:
  virtual ~MicroPartitionInfoProvider() = default;
  virtual int GetBlockId() const = 0;
  virtual std::string GetFileName() const = 0;
  virtual std::string GetToastName() const = 0;
  virtual const ::pax::stats::MicroPartitionStatisticsInfo &GetStats() const = 0;
  virtual std::pair<std::shared_ptr<std::vector<uint8_t>>,
                   std::unique_ptr<Bitmap8>> GetVisibilityBitmap(FileSystem *file_system) = 0;
};

class PaxFragmentInterface;

class ParallelScanDesc final : public std::enable_shared_from_this<ParallelScanDesc> {
 public:
  inline bool ShouldBuildCtid() const { return build_ctid_bitmap_; }
  inline ParallelIterator<std::shared_ptr<MicroPartitionInfoProvider>> *Iterator() { return iterator_.get(); }
  ~ParallelScanDesc() = default;

  arrow::Status Initialize(Relation relation,
    const std::shared_ptr<arrow::Schema> &table_schema,
    const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options,
    FileSystem *file_system, std::shared_ptr<FileSystemOptions> options,
    void *context /* pg context about the scan */,
    pax::IteratorBase<std::shared_ptr<MicroPartitionInfoProvider>> &&it);
  void Release();
  const std::shared_ptr<arrow::Schema> &TableSchema() const {
    return table_schema_;
  }
  std::shared_ptr<arrow::Schema> &TableSchema() { return table_schema_; }
  const std::shared_ptr<arrow::Schema> &ScanSchema() const {
    return scan_schema_;
  }
  std::shared_ptr<arrow::Schema> &ScanSchema() { return scan_schema_; }
  const std::vector<int> &ScanColumns() const { return scan_columns_; }
  const std::shared_ptr<PaxFilter> &GetPaxFilter() const { return pax_filter_; }
  Relation GetRelation() const { return relation_; }
  FileSystem *GetFileSystem() const { return file_system_; }
  const std::shared_ptr<FileSystemOptions> &GetFileSystemOptions() const { return fs_options_; }

  struct FragmentIteratorInternal {
    FragmentIteratorInternal(const std::shared_ptr<ParallelScanDesc> &desc);
    FragmentIteratorInternal(const FragmentIteratorInternal &copy) = default;
    arrow::Result<std::shared_ptr<arrow::dataset::Fragment>> Next();

    std::shared_ptr<ParallelScanDesc> desc_;
    int fragment_counter_;
  };

 private:
  void CalculateScanColumns(const std::vector<std::pair<const char *, size_t>> &table_names);

  Relation relation_ = nullptr;
  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> fs_options_;
  std::shared_ptr<PaxFilter> pax_filter_;
  std::vector<int> scan_columns_;
  std::unique_ptr<ParallelIterator<std::shared_ptr<MicroPartitionInfoProvider>>> iterator_;
  std::shared_ptr<arrow::Schema> table_schema_;
  std::shared_ptr<arrow::Schema> scan_schema_;
  int num_micro_partitions_ = 0;
  bool build_ctid_bitmap_ = false;
};

class PaxFragmentInterface final : public arrow::dataset::FragmentInterface {
 public:
  PaxFragmentInterface(const std::shared_ptr<ParallelScanDesc> &scan_desc);
  virtual ~PaxFragmentInterface() = default;
  arrow::Result<arrow::RecordBatchIterator> ScanBatchesAsyncImpl(
      const std::shared_ptr<arrow::dataset::ScanOptions> &options) override;
  std::string type_name() const override { return "pax-fragment"; }
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next();

  int BlockNum() const { return block_no_; }
  const std::shared_ptr<arrow::Schema> &ScanSchema() const {
    return scan_desc_->ScanSchema();
  }
  const std::vector<int> &ScanColumns() const {
    return scan_desc_->ScanColumns();
  }
  void Release() override;

 private:
  bool OpenFile();
  void InitAdapter();
  void CalculateScanColumns();

  // readonly reference
  Relation relation_ = nullptr;

  // owned adapter
  std::shared_ptr<VecAdapter> adapter_;

  // owned reader
  std::unique_ptr<PaxVecReader> reader_;

  std::shared_ptr<std::vector<uint8_t>> visimap_;
  // readonly reference
  std::shared_ptr<ParallelScanDesc> scan_desc_;
  int block_no_ = -1;
};

}  // namespace pax

#endif
