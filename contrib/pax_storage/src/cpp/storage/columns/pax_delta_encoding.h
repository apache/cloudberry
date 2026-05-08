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
 * pax_delta_encoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_delta_encoding.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_decoding.h"
#include <vector>

namespace pax {
  
struct BitReader64 {
    const uint8_t*& p;
    uint32_t& remaining;
    uint64_t bit_buffer = 0;
    uint32_t bit_count = 0;

    BitReader64(const uint8_t*& ptr, uint32_t& size) : p(ptr), remaining(size) {}

    inline void Ensure(uint32_t need_bits) {
        while (bit_count < need_bits && remaining > 0) {
            bit_buffer |= (static_cast<uint64_t>(*p) << bit_count);
            ++p;
            --remaining;
            bit_count += 8;
        }
    }

    inline uint32_t Read(uint8_t width) {
        if (width == 0) return 0;
        Ensure(width);
        uint32_t result;
        if (width == 32) {
            result = static_cast<uint32_t>(bit_buffer & 0xFFFFFFFFull);
        } else {
            result = static_cast<uint32_t>(bit_buffer & ((1ull << width) - 1));
        }
        bit_buffer >>= width;
        bit_count -= width;
        return result;
    }

    inline void AlignToByte() {
        uint32_t drop = bit_count % 8;
        if (drop) {
            bit_buffer >>= drop;
            bit_count -= drop;
        }
    }
};

struct DeltaBlockHeader {
  uint32_t value_per_block;
  uint32_t values_per_mini_block;
  uint32_t total_count;
};

template <typename T>
class PaxDeltaEncoder : public PaxEncoder {
 public:
  explicit PaxDeltaEncoder(const EncodingOption &encoder_options);

  virtual void Append(char *data, size_t size) override;

  virtual bool SupportAppendNull() const override;

  virtual void Flush() override;

  virtual size_t GetBoundSize(size_t src_len) const override;

 private:

  void Encode(T *data, size_t size);

 private:
  static constexpr uint32_t value_per_block_ = 128;
  static constexpr uint32_t mini_blocks_per_block_ = 4;
  static constexpr uint32_t values_per_mini_block_ =
      value_per_block_ / mini_blocks_per_block_;

 private:
  bool has_append_ = false;
  // Reusable working buffer to avoid per-block allocations during encoding
  std::vector<uint32_t> deltas_scratch_;
};

template <typename T>
class PaxDeltaDecoder : public PaxDecoder {
 public:
  explicit PaxDeltaDecoder(const PaxDecoder::DecodingOption &encoder_options);

  virtual PaxDecoder *SetSrcBuffer(char *data, size_t data_len) override;

  virtual PaxDecoder *SetDataBuffer(
      std::shared_ptr<DataBuffer<char>> result_buffer) override;

  virtual size_t Next(const char *not_null) override;

  virtual size_t Decoding() override;

  virtual size_t Decoding(const char *not_null, size_t not_null_len) override;

  virtual const char *GetBuffer() const override;

  virtual size_t GetBufferSize() const override;

 private:
  std::shared_ptr<DataBuffer<char>> data_buffer_;
  std::shared_ptr<DataBuffer<char>> result_buffer_;
};

}  // namespace pax