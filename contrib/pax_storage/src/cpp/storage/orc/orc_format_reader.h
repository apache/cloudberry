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
 * orc_format_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_format_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/proto/proto_wrappers.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
namespace tools {
class OrcDumpReader;
}
class OrcFormatReader final {
 public:
  explicit OrcFormatReader(std::shared_ptr<File> file, std::shared_ptr<File> toast_file = nullptr);

  ~OrcFormatReader();

  void SetReusedBuffer(std::shared_ptr<DataBuffer<char>> data_buffer);

  void Open();

  void Close();

  size_t GetStripeNums() const;

  size_t GetStripeNumberOfRows(size_t stripe_index);

  size_t GetStripeOffset(size_t stripe_index);

  std::unique_ptr<PaxColumns> ReadStripe(size_t group_index, const std::vector<bool> &proj_cols);

 private:
  pax::porc::proto::StripeFooter ReadStripeWithProjection(
      std::shared_ptr<DataBuffer<char>> data_buffer,
      const ::pax::porc::proto::StripeInformation &stripe_info,
      const std::vector<bool> &proj_cols, size_t group_index);

  pax::porc::proto::StripeFooter ReadStripeFooter(std::shared_ptr<DataBuffer<char>> data_buffer,
                                                  size_t sf_length,
                                                  off64_t sf_offset,
                                                  size_t sf_data_len,
                                                  size_t group_index);

  pax::porc::proto::StripeFooter ReadStripeFooter(std::shared_ptr<DataBuffer<char>> data_buffer,
                                                  size_t stripe_index);

  void BuildProtoTypes();

 private:
  friend class tools::OrcDumpReader;
  friend class OrcGroupStatsProvider;
  std::vector<pax::porc::proto::Type_Kind> column_types_;
  std::vector<std::map<std::string, std::string>> column_attrs_;
  std::shared_ptr<File> file_;
  std::shared_ptr<File> toast_file_;
  std::shared_ptr<DataBuffer<char>> reused_buffer_;
  size_t num_of_stripes_;
  bool is_vec_;

  std::vector<size_t> stripe_row_offsets_;

  pax::porc::proto::PostScript post_script_;
  pax::porc::proto::Footer file_footer_;
};

}  // namespace pax
