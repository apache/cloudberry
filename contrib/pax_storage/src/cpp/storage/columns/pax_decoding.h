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
 * pax_decoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_decoding.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxDecoder {
 public:
  struct DecodingOption {
    ColumnEncoding_Kind column_encode_type;
    bool is_sign;
    int compress_level;

    ColumnEncoding_Kind offsets_encode_type;
    int offsets_compress_level;

    DecodingOption()
        : column_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED),
          is_sign(true),
          compress_level(0),
          offsets_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
          offsets_compress_level(0) {}
  };

  explicit PaxDecoder(const DecodingOption &decoder_options);

  virtual ~PaxDecoder() = default;

  virtual PaxDecoder *SetSrcBuffer(char *data, size_t data_len) = 0;

  virtual PaxDecoder *SetDataBuffer(
      std::shared_ptr<DataBuffer<char>> result_buffer) = 0;

  virtual size_t Next(const char *not_null) = 0;

  virtual size_t Decoding() = 0;

  virtual size_t Decoding(const char *not_null, size_t not_null_len) = 0;

  virtual const char *GetBuffer() const = 0;

  virtual size_t GetBufferSize() const = 0;

  template <typename T>
  static std::shared_ptr<PaxDecoder> CreateDecoder(
      const DecodingOption &decoder_options);

 protected:
  const DecodingOption &decoder_options_;
};

extern template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int64>(
    const DecodingOption &);
extern template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int32>(
    const DecodingOption &);
extern template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int16>(
    const DecodingOption &);
extern template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int8>(
    const DecodingOption &);

}  // namespace pax
