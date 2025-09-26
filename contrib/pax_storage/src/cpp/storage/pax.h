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
 * pax.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "catalog/pax_catalog.h"
#include "comm/bitmap.h"
#include "comm/iterator.h"
#include "storage/file_system.h"
#include "storage/filter/pax_filter.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"
#include "storage/orc/porc.h"
#include "storage/pax_itemptr.h"
#include "storage/strategy.h"

#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace pax {

class TableWriter {
 public:
  using WriteSummaryCallback = MicroPartitionWriter::WriteSummaryCallback;

  explicit TableWriter(Relation relation);

  virtual ~TableWriter();

  virtual const FileSplitStrategy *GetFileSplitStrategy() const;

  virtual void WriteTuple(TupleTableSlot *slot);

  virtual void Open();

  virtual void Close();

  TableWriter *SetWriteSummaryCallback(WriteSummaryCallback callback);

  TableWriter *SetFileSplitStrategy(
      std::unique_ptr<FileSplitStrategy> &&strategy);

  BlockNumber GetBlockNumber() const { return current_blockno_; }

 protected:
  virtual std::string GenFilePath(const std::string &block_id);

  virtual std::string GenToastFilePath(const std::string &file_path);

#ifdef RUN_GTEST
  virtual std::vector<std::tuple<ColumnEncoding_Kind, int>>
  GetRelEncodingOptions() {
    Assert(options_cached_);
    return encoding_opts_;
  }
#else

  inline std::vector<std::tuple<ColumnEncoding_Kind, int>>
  GetRelEncodingOptions() {
    Assert(options_cached_);
    return encoding_opts_;
  }

#endif
  inline std::vector<int> GetMinMaxColumnIndexes() {
    Assert(options_cached_);
    return min_max_col_idx_;
  }

  inline std::vector<int> GetBloomFilterColumnIndexes() {
    Assert(options_cached_);
    return bf_col_idx_;
  }

  inline PaxStorageFormat GetStorageFormat() { return storage_format_; }

  void InitOptionsCaches();

  std::unique_ptr<MicroPartitionWriter> CreateMicroPartitionWriter(
      std::shared_ptr<MicroPartitionStats> mp_stats, bool write_only = true);

 protected:
  std::string rel_path_;
  const Relation relation_ = nullptr;
  std::unique_ptr<MicroPartitionWriter> writer_;
  std::unique_ptr<FileSplitStrategy> strategy_;
  std::shared_ptr<MicroPartitionStats> mp_stats_;
  WriteSummaryCallback summary_callback_;
  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_;

  size_t num_tuples_ = 0;
  BlockNumber current_blockno_ = 0;

  bool options_cached_;
  PaxStorageFormat storage_format_ = PaxStorageFormat::kTypeStoragePorcNonVec;
  std::vector<int> min_max_col_idx_;
  std::vector<int> bf_col_idx_;
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts_;

  bool is_dfs_table_space_;
  size_t cur_physical_size_ = 0;
};

class TableReader final {
 public:
  struct ReaderOptions {
    Oid table_space_id = 0;

    std::shared_ptr<DataBuffer<char>> reused_buffer;

    // Will not used in TableReader
    // But pass into micro partition reader
    std::shared_ptr<PaxFilter> filter;

#ifdef VEC_BUILD
    bool is_vec = false;
    TupleDesc tuple_desc = nullptr;
    bool vec_build_ctid = false;
#endif
  };

  TableReader(std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
              ReaderOptions options);
  virtual ~TableReader();

  void Open();

  void ReOpen();

  void Close();

  bool ReadTuple(TupleTableSlot *slot);

  bool GetTuple(TupleTableSlot *slot, ScanDirection direction,
                const size_t offset);

  // deprecate:
  // DON'T USE, this function will be removed
  int GetCurrentMicroPartitionId() const { return micro_partition_id_; }

 private:
  void OpenFile();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iterator_;
  std::unique_ptr<MicroPartitionReader> reader_ = nullptr;
  bool is_empty_ = false;
  ReaderOptions reader_options_;
  uint32 current_block_number_ = 0;

  int micro_partition_id_;

  // only for analyze scan
  MicroPartitionMetadata current_block_metadata_;
  // only for analyze scan
  size_t current_block_row_index_ = 0;
  FileSystem *file_system_ = nullptr;
};

class TableDeleter final {
 public:
  TableDeleter(Relation rel,
               std::map<int, std::shared_ptr<Bitmap8>> delete_bitmap,
               Snapshot snapshot);

  ~TableDeleter() = default;

  void Delete(std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);

  void DeleteWithVisibilityMap(
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
      TransactionId delete_xid);

 private:
  void UpdateStatsInAuxTable(pax::PaxCatalogUpdater &catalog_update,
                             const pax::MicroPartitionMetadata &meta,
                             std::shared_ptr<Bitmap8> visi_bitmap,
                             const std::vector<int> &min_max_col_idxs,
                             const std::vector<int> &bf_col_idxs,
                             std::shared_ptr<PaxFilter> filter);

  std::unique_ptr<TableWriter> OpenWriter();

  std::unique_ptr<TableReader> OpenReader(
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);

 private:
  Relation rel_;
  Snapshot snapshot_;

  std::map<int, std::shared_ptr<Bitmap8>> delete_bitmap_;

  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_;
  bool need_wal_;
};

}  // namespace pax
