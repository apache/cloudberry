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
 * pax_columns.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_columns.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <map>
#include <utility>
#include <vector>

#include "exceptions/CException.h"
#include "storage/columns/pax_column.h"

namespace pax {
// PaxColumns are similar to the kind_struct in orc
// It is designed to be nested and some interfaces have semantic differences
// Inheriting PaxCommColumn use to be able to nest itself
class PaxColumns : public PaxColumn {
 public:
  PaxColumns();

  ~PaxColumns() override;

  inline const std::unique_ptr<PaxColumn> &operator[](uint64 i) {
    return columns_[i];
  }

  void Append(std::unique_ptr<PaxColumn> &&column);

  void Append(char *buffer, size_t size) override;

  void AppendToast(char *buffer, size_t size) override;

  void Set(std::shared_ptr<DataBuffer<char>> data);

  void SetStorageFormat(PaxStorageFormat format);

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetOffsetsOriginLength() const override;

  int32 GetTypeLength() const override;

  PaxStorageFormat GetStorageFormat() const override;

  // Get number of column in columns
  virtual size_t GetColumns() const;

  // Get number of toast in columns
  size_t ToastCounts() override;

  // Set the external toast buffer
  void SetExternalToastDataBuffer(
      std::shared_ptr<DataBuffer<char>> external_toast_data,
      const std::vector<size_t> &column_sizes);

  // Get the external toast data buffer
  std::shared_ptr<DataBuffer<char>> GetExternalToastDataBuffer() override;

  // verify the external toast buffer
  void VerifyAllExternalToasts(const std::vector<uint64> &ext_toast_lens);

  // Get the combine buffer of all columns
  std::pair<char *, size_t> GetBuffer() override;

  // Get the combine buffer of single column
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  // can't call this function on columns
  Datum GetDatum(size_t position) override {
    CBDB_RAISE(cbdb::CException::kExTypeUnImplements);
  }

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  using ColumnStreamsFunc = std::function<void(
      const pax::porc::proto::Stream_Kind &, size_t, size_t, size_t)>;

  using ColumnEncodingFunc = std::function<void(
      const ColumnEncoding_Kind &, const uint64, const int64,
      const ColumnEncoding_Kind &, const uint64, const int64)>;

  // Get the combined data buffer of all columns
  // TODO(jiaqizho): consider add a new api which support split IO from
  // different column
  virtual std::shared_ptr<DataBuffer<char>> GetDataBuffer(
      const ColumnStreamsFunc &column_streams_func,
      const ColumnEncodingFunc &column_encoding_func);

  inline void AddRows(size_t row_num) { row_nums_ += row_num; }
  inline size_t GetRows() const override { return row_nums_; }

 protected:
  static size_t AlignSize(size_t buf_len, size_t len, size_t align_size);

  size_t MeasureOrcDataBuffer(const ColumnStreamsFunc &column_streams_func,
                              const ColumnEncodingFunc &column_encoding_func);

  size_t MeasureVecDataBuffer(const ColumnStreamsFunc &column_streams_func,
                              const ColumnEncodingFunc &column_encoding_func);

  void CombineOrcDataBuffer();
  void CombineVecDataBuffer();

 protected:
  std::vector<std::unique_ptr<PaxColumn>> columns_;
  std::vector<std::shared_ptr<DataBuffer<char>>> data_holder_;
  std::shared_ptr<DataBuffer<char>> data_;
  size_t row_nums_;

  PaxStorageFormat storage_format_;
};

}  //  namespace pax
