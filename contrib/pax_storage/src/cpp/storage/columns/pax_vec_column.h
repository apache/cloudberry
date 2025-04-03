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
 * pax_vec_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_column.h"

namespace pax {

template <typename T>
class PaxVecCommColumn : public PaxColumn {
 public:
  explicit PaxVecCommColumn(uint32 capacity);

  ~PaxVecCommColumn() override;

  PaxVecCommColumn();

  virtual void Set(std::shared_ptr<DataBuffer<T>> data, size_t non_null_rows);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  void Append(char *buffer, size_t size) override;

  void AppendNull() override;

  void AppendToast(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  Datum GetDatum(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetOffsetsOriginLength() const override;

  int32 GetTypeLength() const override;

  // directly pass the buffer to vec
  std::shared_ptr<DataBuffer<T>> GetDataBuffer();

 protected:
  void AppendInternal(char *buffer, size_t size);

 protected:  // NOLINT
  std::shared_ptr<DataBuffer<T>> data_;
};

extern template class PaxVecCommColumn<char>;
extern template class PaxVecCommColumn<int8>;
extern template class PaxVecCommColumn<int16>;
extern template class PaxVecCommColumn<int32>;
extern template class PaxVecCommColumn<int64>;
extern template class PaxVecCommColumn<float>;
extern template class PaxVecCommColumn<double>;

class PaxVecNonFixedColumn : public PaxColumn {
 public:
  explicit PaxVecNonFixedColumn(uint32 data_capacity, uint32 offsets_capacity);

  PaxVecNonFixedColumn();

  ~PaxVecNonFixedColumn() override;

  virtual void Set(std::shared_ptr<DataBuffer<char>> data,
                   std::shared_ptr<DataBuffer<int32>> offsets,
                   size_t total_size, size_t non_null_rows);

  void Append(char *buffer, size_t size) override;

  void AppendNull() override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetOffsetsOriginLength() const override;

  int32 GetTypeLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  Datum GetDatum(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  virtual std::pair<char *, size_t> GetOffsetBuffer(bool append_last);

  // directly pass the buffer to vec
  std::shared_ptr<DataBuffer<char>> GetDataBuffer();
#ifndef RUN_GTEST
 protected:
#endif
  void AppendLastOffset();

  std::shared_ptr<DataBuffer<int32>> GetOffsetDataBuffer() { return offsets_; }

 protected:
  size_t estimated_size_;
  std::shared_ptr<DataBuffer<char>> data_;
  std::shared_ptr<DataBuffer<int32>> offsets_;

  // used to record next offset in write path
  // in read path, next_offsets_ always be -1
  int32 next_offsets_;

  std::vector<std::shared_ptr<MemoryObject>> buffer_holders_;
};

}  // namespace pax
