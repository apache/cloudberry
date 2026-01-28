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
 * pax_encoding_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

template <typename T>
class PaxEncodingColumn : public PaxCommColumn<T> {
 public:
  PaxEncodingColumn(uint32 capacity,
                    const PaxEncoder::EncodingOption &encoding_option);

  PaxEncodingColumn(uint32 capacity,
                    const PaxDecoder::DecodingOption &decoding_option);

  ~PaxEncodingColumn() override;

  void Set(std::unique_ptr<DataBuffer<T>> data) override;

  std::pair<char *, size_t> GetBuffer() override;

  int64 GetOriginLength() const override;

  size_t PhysicalSize() const override;

  size_t GetAlignSize() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  virtual ColumnEncoding_Kind GetDefaultColumnType();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  std::shared_ptr<PaxEncoder> encoder_;

  PaxDecoder::DecodingOption decoder_options_;
  std::shared_ptr<PaxDecoder> decoder_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
};

extern template class PaxEncodingColumn<int8>;
extern template class PaxEncodingColumn<int16>;
extern template class PaxEncodingColumn<int32>;
extern template class PaxEncodingColumn<int64>;

}  // namespace pax
