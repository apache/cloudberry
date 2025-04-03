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
 * pax_encoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <map>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxEncoder {
 public:
  struct EncodingOption {
    ColumnEncoding_Kind column_encode_type;
    bool is_sign;
    int compress_level;

    ColumnEncoding_Kind offsets_encode_type;
    int offsets_compress_level;

    EncodingOption()
        : column_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED),
          is_sign(true),
          compress_level(0),
          offsets_encode_type(
              ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
          offsets_compress_level(0) {}
  };

 public:
  explicit PaxEncoder(const EncodingOption &encoder_options);

  void SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer);

  virtual ~PaxEncoder() = default;

  virtual void Append(char *data, size_t size) = 0;

  virtual bool SupportAppendNull() const = 0;

  virtual void Flush() = 0;

  virtual char *GetBuffer() const;

  virtual size_t GetBufferSize() const;

  /**
   * steaming encoder
   *
   * streaming means it need hold two DataBuffers
   * - one of DataBuffer used to temp save buffer
   * - one of DataBuffer used to keep result
   *
   * compared with the block method, streaming can reduce one memory copy
   */
  static std::shared_ptr<PaxEncoder> CreateStreamingEncoder(
      const EncodingOption &encoder_options, bool non_fixed = false);

 protected:
  const EncodingOption &encoder_options_;
  std::shared_ptr<DataBuffer<char>> result_buffer_;
};

}  // namespace pax
