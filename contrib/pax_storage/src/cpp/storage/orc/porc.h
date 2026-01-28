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
 * porc.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/porc.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/orc_format_reader.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
class MicroPartitionStats;
class OrcFormatReader;

class OrcWriter : public MicroPartitionWriter {
 public:
  OrcWriter(const MicroPartitionWriter::WriterOptions &orc_writer_options,
            const std::vector<pax::porc::proto::Type_Kind> &column_types,
            std::unique_ptr<File> file, std::unique_ptr<File> toast_file = nullptr);

  ~OrcWriter() override;

  void Flush() override;

  void WriteTuple(TupleTableSlot *slot) override;

  void MergeTo(MicroPartitionWriter *writer) override;

  void Close() override;

  size_t PhysicalSize() const override;

  static std::vector<pax::porc::proto::Type_Kind> BuildSchema(TupleDesc desc,
                                                              bool is_vec);

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

#ifdef RUN_GTEST
  // only for test
  static std::unique_ptr<MicroPartitionWriter> CreateWriter(
      MicroPartitionWriter::WriterOptions options,
      const std::vector<pax::porc::proto::Type_Kind> &column_types, std::unique_ptr<File> file,
      std::unique_ptr<File> toast_file = nullptr) {
    std::vector<std::tuple<ColumnEncoding_Kind, int>> all_no_encoding_types;
    for (auto _ : column_types) {
      (void)_;
      all_no_encoding_types.emplace_back(std::make_tuple(
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED, 0));
    }

    options.encoding_opts = all_no_encoding_types;

    return std::make_unique<OrcWriter>(options, column_types, std::move(file), std::move(toast_file));
  }
#endif

  std::vector<std::pair<int, Datum>> PrepareWriteTuple(TupleTableSlot *slot);
  void EndWriteTuple(TupleTableSlot *slot);

  void BuildFooterType();
  bool WriteStripe(BufferedOutputStream *buffer_mem_stream,
                   DataBuffer<char> *toast_mem, PaxColumns *pax_columns,
                   MicroPartitionStats *stripe_stats,
                   MicroPartitionStats *file_stats);
  bool WriteStripe(BufferedOutputStream *buffer_mem_stream,
                   DataBuffer<char> *toast_mem);
  void WriteFileFooter(BufferedOutputStream *buffer_mem_stream);
  void WritePostscript(BufferedOutputStream *buffer_mem_stream);

  void MergePaxColumns(OrcWriter *writer);
  void MergeGroups(OrcWriter *orc_writer);
  void MergeGroup(OrcWriter *orc_writer, int group_index,
                  std::shared_ptr<DataBuffer<char>> &merge_buffer);
  void DeleteUnstateFile();

 protected:
  bool is_closed_;
  std::unique_ptr<PaxColumns> pax_columns_;
  // Hold the detoast and new generate toast datum
  // pair.first save the origin datum
  // pair.secord save the detoast datum which need free
  std::vector<std::pair<int, Datum>> origin_datum_holder_;

  // buffers to hold pax toasted values
  std::vector<std::shared_ptr<MemoryObject>> toast_memory_holder_;

  // detoasted values, needs to free memory after the writing tuple
  // If exception happens, the detoasted values should not be touched.
  std::vector<void*> detoast_memory_holder_;

  const std::vector<pax::porc::proto::Type_Kind> column_types_;
  std::unique_ptr<File> file_;
  std::unique_ptr<File> toast_file_;
  int32 current_written_phy_size_;
  WriteSummary summary_;

  int32 row_index_;
  uint64 total_rows_;
  uint64 current_offset_;
  uint64 current_toast_file_offset_;

  ::pax::porc::proto::Footer file_footer_;
  ::pax::porc::proto::PostScript post_script_;
  ::pax::MicroPartitionStats group_stats_;

  // indices of columns that are non-byval and have typlen == -1 (varlena)
  std::vector<int> varlena_slowpath_indices_;
};

class OrcReader : public MicroPartitionReader {
 public:
  explicit OrcReader(std::unique_ptr<File> file, std::unique_ptr<File> toast_file = nullptr);

  ~OrcReader() override = default;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(TupleTableSlot *cslot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  size_t GetTupleCountsInGroup(size_t group_index) override;

  std::unique_ptr<MicroPartitionReader::Group> ReadGroup(size_t group_index) override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

#ifndef RUN_GTEST
 protected:  // NOLINT
#endif

  // Clean up reading status
  void ResetCurrentReading();

 protected:
  std::unique_ptr<MicroPartitionReader::Group> working_group_;

  // used to cache the group in `GetTuple`
  std::unique_ptr<MicroPartitionReader::Group> cached_group_;
  size_t current_group_index_;

  // hold filtler to ensure that the projection is always vaild.
  std::shared_ptr<PaxFilter> filter_;

  OrcFormatReader format_reader_;
  bool is_closed_;

  // only reference
  std::shared_ptr<Bitmap8> visibility_bitmap_ = nullptr;
};

};  // namespace pax
