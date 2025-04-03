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
 * micro_partition.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <stddef.h>

#include <functional>
#include <stdexcept>
#include <string>

#include "comm/guc.h"
#include "storage/columns/pax_columns.h"
#include "storage/filter/pax_filter.h"
#include "storage/micro_partition_metadata.h"
#include "storage/pax_defined.h"

namespace pax {

struct WriteSummary;
class FileSystem;
class MicroPartitionStats;
class PaxFilter;

class MicroPartitionWriter {
 public:
  struct WriterOptions {
    std::string file_name;
    std::string block_id;
    TupleDesc rel_tuple_desc = nullptr;
    Oid rel_oid = InvalidOid;
    RelFileNode node;
    bool need_wal = false;
    std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;
    std::pair<ColumnEncoding_Kind, int> offsets_encoding_opts;
    std::vector<int> enable_min_max_col_idxs;
    std::vector<int> enable_bf_col_idxs;

    size_t group_limit = pax_max_tuples_per_group;
    PaxStorageFormat storage_format = PaxStorageFormat::kTypeStoragePorcNonVec;

    WriterOptions() = default;
    WriterOptions(const WriterOptions &other) = default;
    WriterOptions(WriterOptions &&wo)
        : file_name(std::move(wo.file_name)),
          block_id(std::move(wo.block_id)),
          rel_tuple_desc(wo.rel_tuple_desc),
          rel_oid(wo.rel_oid),
          node(wo.node),
          encoding_opts(std::move(wo.encoding_opts)),
          enable_min_max_col_idxs(std::move(wo.enable_min_max_col_idxs)),
          enable_bf_col_idxs(std::move(wo.enable_bf_col_idxs)),
          group_limit(wo.group_limit) {}

    WriterOptions &operator=(WriterOptions &&wo) {
      file_name = std::move(wo.file_name);
      block_id = std::move(wo.block_id);
      rel_tuple_desc = wo.rel_tuple_desc;
      rel_oid = wo.rel_oid;
      node = wo.node;
      encoding_opts = std::move(wo.encoding_opts);
      enable_min_max_col_idxs = std::move(wo.enable_min_max_col_idxs);
      enable_bf_col_idxs = std::move(wo.enable_bf_col_idxs);
      group_limit = wo.group_limit;
      return *this;
    }
  };

  explicit MicroPartitionWriter(const WriterOptions &writer_options);

  virtual ~MicroPartitionWriter() = default;

  // close the current write file. Create may be called after Close
  // to write a new micro partition.
  virtual void Close() = 0;

  // immediately, flush memory data into file system
  virtual void Flush() = 0;

  // estimated size of the writing size, used to determine
  // whether to switch to another micro partition.
  virtual size_t PhysicalSize() const = 0;

  // append tuple to the current micro partition file
  // return the number of tuples the current micro partition has written
  virtual void WriteTuple(TupleTableSlot *slot) = 0;

  // The current writer merges with another open `MicroPartitionWriter`
  // two of `MicroPartitionWriter` must be the same sub-class.
  // Notice that: not support different format writer call `Merge`
  //
  // - Combine the group in memory
  // - Merge the group from disk and remove the unstate file in disk
  // - Merge the summary
  virtual void MergeTo(MicroPartitionWriter *writer) = 0;

  using WriteSummaryCallback = std::function<void(const WriteSummary &summary)>;

  // summary callback is invoked after the file is closed.
  // returns MicroPartitionWriter to enable chain call.
  virtual MicroPartitionWriter *SetWriteSummaryCallback(
      WriteSummaryCallback callback);

  virtual MicroPartitionWriter *SetStatsCollector(
      std::shared_ptr<MicroPartitionStats> mpstats);

 protected:
  WriteSummaryCallback summary_callback_;
  WriterOptions writer_options_;
  FileSystem *file_system_ = nullptr;
  // only reference the mpstats, not the owner
  std::shared_ptr<MicroPartitionStats> mp_stats_;
};

template <typename T>
class DataBuffer;

class MicroPartitionReader {
 public:
  class Group {
   public:
    virtual ~Group() = default;

    virtual size_t GetRows() const = 0;

    virtual size_t GetRowOffset() const = 0;

    // `ReadTuple` is the same interface in the `MicroPartitionReader`
    // this interface at the group level, if no rows remain in current
    // group, then the first value in return std::pair will be `false`.
    //
    // the secord value in return std::pair is the row offset of current
    // group.
    virtual std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) = 0;

    // ------------------------------------------
    // The below interfaces is used to directly access
    // the pax columns in the group.
    // Other `MicroPartitionReader` can quickly perform
    // some operations, like filter, convert format...
    // ------------------------------------------
    virtual bool GetTuple(TupleTableSlot *slot, size_t row_index) = 0;

    // Direct get datum from columns by column index + row index
    virtual std::pair<Datum, bool> GetColumnValue(TupleDesc desc,
                                                  size_t column_index,
                                                  size_t row_index) = 0;

    // Allow different MicroPartitionReader shared columns
    // but should not let export columns out of micro partition
    //
    // In MicroPartition writer/reader implementation, all in-memory data should
    // be accessed by pax column This is because most of the common logic of
    // column operation is done in pax column, such as type mapping, bitwise
    // fetch, compression/encoding. At the same time, pax column can also be
    // used as a general interface for internal using, because it's zero copy
    // from buffer. more details in `storage/columns`
    virtual const std::shared_ptr<PaxColumns> &GetAllColumns() const = 0;

    virtual void SetVisibilityMap(
        std::shared_ptr<Bitmap8> visibility_bitmap) = 0;

    // Used in `OpenApi`. Once user call the `GetAllColumns`,
    // then it still need use the visible map to filter.
    virtual std::shared_ptr<Bitmap8> GetVisibilityMap() const = 0;

    // Used to get the no missing column
    virtual std::pair<Datum, bool> GetColumnValueNoMissing(
        size_t column_index, size_t row_index) = 0;
  };

  struct ReaderOptions {
    // additioinal info to initialize a reader.

    // Optional, when reused buffer is not set, new memory will be generated for
    // ReadTuple
    std::shared_ptr<DataBuffer<char>> reused_buffer;

    std::shared_ptr<PaxFilter> filter;

#ifdef VEC_BUILD
    TupleDesc tuple_desc = nullptr;
#endif

    // should only reference
    std::shared_ptr<Bitmap8> visibility_bitmap = nullptr;
  };
  MicroPartitionReader() = default;

  virtual ~MicroPartitionReader() = default;

  virtual void Open(const ReaderOptions &options) = 0;

  // Close the current reader. It may be re-Open.
  virtual void Close() = 0;

  // read tuple from the micro partition with a filter.
  // the default behavior doesn't push the predicate down to
  // the low-level storage code.
  // returns the offset of the tuple in the micro partition
  // NOTE: the ctid is stored in slot, mapping from block_id to micro partition
  // is also created during this stage, no matter the map relation is needed or
  // not. We may optimize to avoid creating the map relation later.
  virtual bool ReadTuple(TupleTableSlot *slot) = 0;

  // ------------------------------------------
  // below interface different with `ReadTuple`
  //
  // direct read with `Group` from current `MicroPartitionReader` with group
  // index. The group index will not be changed, and won't have any middle state
  // in this process.
  // ------------------------------------------
  virtual bool GetTuple(TupleTableSlot *slot, size_t row_index) = 0;

  virtual size_t GetTupleCountsInGroup(size_t group_index) = 0;

  virtual size_t GetGroupNums() = 0;

  virtual std::unique_ptr<Group> ReadGroup(size_t group_index) = 0;

  virtual std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) = 0;

#ifdef VEC_BUILD
 private:
  friend class PaxVecReader;
#endif
};

class MicroPartitionReaderProxy : public MicroPartitionReader {
 public:
  MicroPartitionReaderProxy() = default;

  virtual ~MicroPartitionReaderProxy() override;

  void Open(const MicroPartitionReader::ReaderOptions &options) override;

  // Close the current reader. It may be re-Open.
  void Close() override;

  // read tuple from the micro partition with a filter.
  // the default behavior doesn't push the predicate down to
  // the low-level storage code.
  // returns the offset of the tuple in the micro partition
  // NOTE: the ctid is stored in slot, mapping from block_id to micro partition
  // is also created during this stage, no matter the map relation is needed or
  // not. We may optimize to avoid creating the map relation later.
  bool ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  size_t GetTupleCountsInGroup(size_t group_index) override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

  std::unique_ptr<Group> ReadGroup(size_t index) override;

  void SetReader(std::unique_ptr<MicroPartitionReader> &&reader);
  MicroPartitionReader *GetReader() { return reader_.get(); }
  const MicroPartitionReader *GetReader() const { return reader_.get(); }

 protected:
  // Allow different MicroPartitionReader shared columns
  // but should not let export columns out of micro partition
  //
  // In MicroPartition writer/reader implementation, all in-memory data should
  // be accessed by pax column This is because most of the common logic of
  // column operation is done in pax column, such as type mapping, bitwise
  // fetch, compression/encoding. At the same time, pax column can also be used
  // as a general interface for internal using, because it's zero copy from
  // buffer. more details in `storage/columns`

  std::unique_ptr<MicroPartitionReader> reader_;
};

}  // namespace pax
