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
 * pax_vec_encoding_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_encoding_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_vec_column.h"

namespace pax {

template <typename T>
class PaxVecEncodingColumn : public PaxVecCommColumn<T> {
 public:
  PaxVecEncodingColumn(uint32 capacity,
                       const PaxEncoder::EncodingOption &encoding_option);

  PaxVecEncodingColumn(uint32 capacity,
                       const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecEncodingColumn() override;

  void Set(std::shared_ptr<DataBuffer<T>> data, size_t non_null_rows) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t PhysicalSize() const override;

  size_t GetAlignSize() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  ColumnEncoding_Kind GetDefaultColumnType();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  std::shared_ptr<PaxEncoder> encoder_;

  PaxDecoder::DecodingOption decoder_options_;
  std::shared_ptr<PaxDecoder> decoder_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
};

extern template class PaxVecEncodingColumn<int8>;
extern template class PaxVecEncodingColumn<int16>;
extern template class PaxVecEncodingColumn<int32>;
extern template class PaxVecEncodingColumn<int64>;

class PaxVecNonFixedEncodingColumn : public PaxVecNonFixedColumn {
 public:
  PaxVecNonFixedEncodingColumn(
      uint32 data_capacity, uint32 offsets_capacity,
      const PaxEncoder::EncodingOption &encoder_options);

  PaxVecNonFixedEncodingColumn(
      uint32 data_capacity, uint32 offsets_capacity,
      const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecNonFixedEncodingColumn() override;

  void Set(std::shared_ptr<DataBuffer<char>> data,
           std::shared_ptr<DataBuffer<int32>> offsets, size_t total_size,
           size_t non_null_rows) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

  std::pair<char *, size_t> GetOffsetBuffer(bool append_last) override;

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> offsets_compressor_;
  std::shared_ptr<DataBuffer<char>> shared_offsets_data_;
};

}  // namespace pax
