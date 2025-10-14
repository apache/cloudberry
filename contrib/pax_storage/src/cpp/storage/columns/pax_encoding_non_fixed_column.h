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
 * pax_encoding_non_fixed_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_non_fixed_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {
class PaxNonFixedEncodingColumn : public PaxNonFixedColumn {
 public:
  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 offsets_capacity,
                            const PaxEncoder::EncodingOption &encoder_options);

  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 offsets_capacity,
                            const PaxDecoder::DecodingOption &decoding_option);

  ~PaxNonFixedEncodingColumn() override;

  void Set(std::unique_ptr<DataBuffer<char>> data,
           std::unique_ptr<DataBuffer<int32>> offsets,
           size_t total_size) override;

  std::pair<char *, size_t> GetBuffer() override;

  std::pair<char *, size_t> GetOffsetBuffer(bool append_last) override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

#ifdef BUILD_RB_RET_DICT
  inline std::shared_ptr<DataBuffer<char>> GetUndecodedBuffer() {
    return shared_data_;
  }
#endif

  // The reason why `PaxNonFixedEncodingColumn` not override the
  // method `GetRangeBuffer` and `GetNonNullRows` is that
  // `PaxNonFixedEncodingColumn` don't have any streaming encoding, also
  // `shared_data_` will own the same buffer with `PaxNonFixedColumn::data_`.

 protected:
  void InitEncoder();
  void InitOffsetStreamCompressor();
  void InitDecoder();
  void InitOffsetStreamDecompressor();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  std::shared_ptr<PaxEncoder> encoder_;
  std::shared_ptr<PaxDecoder> decoder_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> offsets_compressor_;
  // Optional encoder/decoder for offsets stream (alternative to compression)
  std::shared_ptr<PaxEncoder> offsets_encoder_;
  std::shared_ptr<PaxDecoder> offsets_decoder_;
  std::shared_ptr<DataBuffer<char>> shared_offsets_data_;
};

}  // namespace pax
