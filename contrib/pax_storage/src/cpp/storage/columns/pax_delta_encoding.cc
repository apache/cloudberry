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
 * pax_delta_encoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_delta_encoding.cc
 *
 *-------------------------------------------------------------------------
 */
#include "storage/columns/pax_delta_encoding.h"

#include <algorithm>
#include <cstring>
#include <vector>

namespace pax {

// delta bitpack encoder
template <typename T>
PaxDeltaEncoder<T>::PaxDeltaEncoder(const EncodingOption &encoder_options)
    : PaxEncoder(encoder_options) {}

template <typename T>
void PaxDeltaEncoder<T>::Append(char *data, size_t size) {
  CBDB_CHECK(!has_append_, cbdb::CException::kExTypeAbort,
             fmt("PaxDeltaEncoder::Append only support Append Once"));
  has_append_ = true;

  auto T_data = reinterpret_cast<T *>(data);
  auto T_data_len = size / sizeof(T);
  Encode(T_data, T_data_len);
}

template <typename T>
void PaxDeltaEncoder<T>::WriteVarInt(T value) {
  while (value >= 0x80) {
    result_buffer_->Write(value | 0x80);
    result_buffer_->Brush(1);
    value >>= 7;
  }
  result_buffer_->Write(value);
  result_buffer_->Brush(1);
}

inline uint8_t NumBitsAllowZero(uint32_t value) {
  if (value == 0) return 0;
  uint8_t bits = 0;
  while (value) {
    bits++;
    value >>= 1;
  }
  return bits;
}

// Fast bit width calculation (0 -> 0)
inline uint8_t FastNumBits(uint32_t v) {
#if defined(__GNUC__) || defined(__clang__)
  return v == 0 ? 0 : static_cast<uint8_t>(32 - __builtin_clz(v));
#else
  uint8_t bits = 0;
  while (v) {
    ++bits;
    v >>= 1;
  }
  return bits;
#endif
}

// 64-bit bit writer based on raw pointer (writes to reserved DataBuffer range)
struct BitWriter64Ptr {
  uint8_t *out;
  size_t capacity;
  size_t index;
  uint64_t bit_buffer;
  uint32_t bit_count;

  BitWriter64Ptr(uint8_t *p, size_t cap)
      : out(p), capacity(cap), index(0), bit_buffer(0), bit_count(0) {}

  inline void Write(uint32_t value, uint8_t width) {
    if (width == 0) return;
    bit_buffer |= (static_cast<uint64_t>(value) << bit_count);
    bit_count += width;
    while (bit_count >= 8) {
      out[index++] = static_cast<uint8_t>(bit_buffer & 0xFF);
      bit_buffer >>= 8;
      bit_count -= 8;
    }
  }

  inline void FlushToByte() {
    if (bit_count > 0) {
      out[index++] = static_cast<uint8_t>(bit_buffer & 0xFF);
      bit_buffer = 0;
      bit_count = 0;
    }
  }
};

// 64-bit bit reader based on raw pointer (limited to specified payload bytes)
struct BitReader64Ptr {
  const uint8_t *in;
  size_t size;
  size_t index;
  uint64_t bit_buffer;
  uint32_t bit_count;

  BitReader64Ptr(const uint8_t *p, size_t len)
      : in(p), size(len), index(0), bit_buffer(0), bit_count(0) {}

  inline void Ensure(uint32_t need_bits) {
    while (bit_count < need_bits && index < size) {
      bit_buffer |= (static_cast<uint64_t>(in[index]) << bit_count);
      ++index;
      bit_count += 8;
    }
  }

  inline uint32_t Read(uint8_t width) {
    if (width == 0) return 0;
    Ensure(width);
    uint32_t result;
    if (width == 32)
      result = static_cast<uint32_t>(bit_buffer & 0xFFFFFFFFull);
    else
      result = static_cast<uint32_t>(bit_buffer & ((1ull << width) - 1));
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

template <typename T>
void PaxDeltaEncoder<T>::Encode(T *data, size_t count) {
  // Estimate allocation: by element byte count, sufficient to accommodate
  // header and bit stream
  result_buffer_->ReSize(count * sizeof(T));

  WriteVarInt(static_cast<T>(value_per_block));
  WriteVarInt(static_cast<T>(values_per_mini_block));
  WriteVarInt(static_cast<T>(count));
  // add base value
  WriteVarInt(static_cast<T>(data[0]));

  size_t values_emitted = 1;
  T previous_value = data[0];

  while (values_emitted < count) {
    uint32_t values_in_block = std::min(
        value_per_block, static_cast<uint32_t>(count - values_emitted));

    if (deltas_scratch_.size() < values_in_block) {
      deltas_scratch_.resize(values_in_block);
    }
    uint32_t *deltas = deltas_scratch_.data();
    uint32_t min_delta = UINT32_MAX;
    uint32_t mini_max[mini_blocks_per_block] = {0};

    for (uint32_t i = 0; i < values_in_block; ++i) {
      T current = data[values_emitted + i];
      uint32_t delta = static_cast<uint32_t>(current - previous_value);
      deltas[i] = delta;
      previous_value = current;
      if (delta < min_delta) min_delta = delta;
      uint32_t mini_index = i / values_per_mini_block;
      if (delta > mini_max[mini_index]) mini_max[mini_index] = delta;
    }

    // write block header: min_delta
    WriteVarInt(static_cast<T>(min_delta));

    // calculate bit widths and total bits (V3-like)
    uint8_t bit_widths[mini_blocks_per_block] = {0};
    uint64_t total_bits = 0;
    for (uint32_t i = 0; i < mini_blocks_per_block; ++i) {
      uint32_t start = i * values_per_mini_block;
      if (start >= values_in_block) {
        bit_widths[i] = 0;
        continue;
      }
      uint32_t adjusted_max =
          (mini_max[i] >= min_delta) ? (mini_max[i] - min_delta) : 0;
      uint8_t w = FastNumBits(adjusted_max);
      bit_widths[i] = w;
      uint32_t end = std::min(start + values_per_mini_block, values_in_block);
      total_bits += static_cast<uint64_t>(w) * (end - start);
    }
    uint32_t payload_bytes = static_cast<uint32_t>((total_bits + 7) / 8);

    // write bit_widths and payload_size (VLQ)
    result_buffer_->Write(reinterpret_cast<char *>(bit_widths),
                          mini_blocks_per_block);
    result_buffer_->Brush(mini_blocks_per_block);
    WriteVarInt(static_cast<T>(payload_bytes));

    // write payload
    if (result_buffer_->Available() < payload_bytes) {
      result_buffer_->ReSize(result_buffer_->Used() + payload_bytes);
    }
    uint8_t *payload_ptr =
        reinterpret_cast<uint8_t *>(result_buffer_->GetAvailableBuffer());
    BitWriter64Ptr bw(payload_ptr, payload_bytes);
    for (uint32_t i = 0; i < mini_blocks_per_block; ++i) {
      uint32_t start = i * values_per_mini_block;
      if (start >= values_in_block) break;
      uint32_t end = std::min(start + values_per_mini_block, values_in_block);
      uint8_t w = bit_widths[i];
      if (w == 0) continue;
      for (uint32_t j = start; j < end; ++j) {
        uint32_t adjusted = deltas[j] - min_delta;
        bw.Write(adjusted, w);
      }
    }
    bw.FlushToByte();
    result_buffer_->Brush(payload_bytes);

    values_emitted += values_in_block;
  }
}

template <typename T>
bool PaxDeltaEncoder<T>::SupportAppendNull() const {
  return false;
}

template <typename T>
void PaxDeltaEncoder<T>::Flush() {
  // do nothing
}

// ---------------- Delta bitpack decoder (DecodeV2 compatible) ----------------

template <typename T>
static inline uint32_t ReadVlq(const uint8_t *&p, uint32_t &remaining) {
  uint32_t result = 0;
  uint32_t shift = 0;
  while (remaining > 0) {
    uint8_t byte = *p++;
    --remaining;
    result |= static_cast<uint32_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) break;
    shift += 7;
  }
  return result;
}

// Specialized reading of one mini-block and batch writing results
// (BitReader64Ptr)
template <typename T>
inline void ReadMiniBlockSpecializedPtr(BitReader64Ptr &br, T *out_values,
                                        T &current_value, uint32_t count_in_mb,
                                        uint32_t min_delta, uint8_t w) {
  switch (w) {
    case 0: {
      for (uint32_t j = 0; j < count_in_mb; ++j) {
        current_value =
            static_cast<T>(static_cast<uint64_t>(current_value) + min_delta);
        out_values[j] = current_value;
      }
      return;
    }
    case 8: {
      for (uint32_t j = 0; j < count_in_mb; ++j) {
        uint32_t adjusted = br.Read(8);
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       adjusted + min_delta);
        out_values[j] = current_value;
      }
      return;
    }
    case 16: {
      for (uint32_t j = 0; j < count_in_mb; ++j) {
        uint32_t adjusted = br.Read(16);
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       adjusted + min_delta);
        out_values[j] = current_value;
      }
      return;
    }
    case 32: {
      for (uint32_t j = 0; j < count_in_mb; ++j) {
        uint32_t adjusted = br.Read(32);
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       adjusted + min_delta);
        out_values[j] = current_value;
      }
      return;
    }
    default: {
      uint32_t j = 0;
      const uint32_t n4 = count_in_mb & ~3u;
      for (; j < n4; j += 4) {
        uint32_t a0 = br.Read(w);
        uint32_t a1 = br.Read(w);
        uint32_t a2 = br.Read(w);
        uint32_t a3 = br.Read(w);
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       a0 + min_delta);
        out_values[j] = current_value;
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       a1 + min_delta);
        out_values[j + 1] = current_value;
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       a2 + min_delta);
        out_values[j + 2] = current_value;
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       a3 + min_delta);
        out_values[j + 3] = current_value;
      }
      for (; j < count_in_mb; ++j) {
        uint32_t a = br.Read(w);
        current_value = static_cast<T>(static_cast<uint64_t>(current_value) +
                                       a + min_delta);
        out_values[j] = current_value;
      }
      return;
    }
  }
}

// Specialized reading of one mini-block and batch writing results
template <typename T>
PaxDeltaDecoder<T>::PaxDeltaDecoder(
    const PaxDecoder::DecodingOption &encoder_options)
    : PaxDecoder(encoder_options),
      data_buffer_(nullptr),
      result_buffer_(nullptr) {
  CBDB_CHECK(encoder_options.column_encode_type ==
                 ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA,
             cbdb::CException::kExTypeAbort,
             fmt("PaxDeltaDecoder only support DIRECT_DELTA encoding"));
  // TODO: if sign is true, should use zigzag encoding, now use delta encoding
  // for offsets in non-fixed columns
  CBDB_CHECK(encoder_options.is_sign == false,
             cbdb::CException::kExTypeUnImplements,
             fmt("PaxDeltaDecoder is not supported for signed data, "
                 "will support zigzag later"));
}

template <typename T>
PaxDecoder *PaxDeltaDecoder<T>::SetSrcBuffer(char *data, size_t data_len) {
  if (data) {
    data_buffer_ =
        std::make_shared<DataBuffer<char>>(data, data_len, false, false);
    data_buffer_->Brush(data_len);
  }
  return this;
}

template <typename T>
PaxDecoder *PaxDeltaDecoder<T>::SetDataBuffer(
    std::shared_ptr<DataBuffer<char>> result_buffer) {
  result_buffer_ = result_buffer;
  return this;
}

template <typename T>
const char *PaxDeltaDecoder<T>::GetBuffer() const {
  return result_buffer_ ? result_buffer_->GetBuffer() : nullptr;
}

template <typename T>
size_t PaxDeltaDecoder<T>::GetBufferSize() const {
  return result_buffer_ ? result_buffer_->Used() : 0;
}

template <typename T>
size_t PaxDeltaDecoder<T>::Next(const char * /*not_null*/) {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements);
}

template <typename T>
size_t PaxDeltaDecoder<T>::Decoding() {
  if (!data_buffer_) return 0;
  Assert(result_buffer_);

  const uint8_t *p =
      reinterpret_cast<const uint8_t *>(data_buffer_->GetBuffer());
  uint32_t remaining = static_cast<uint32_t>(data_buffer_->Used());

  // read header: values_per_block, values_per_mini_block, total_count,
  // first_value
  uint32_t values_per_block = ReadVlq<T>(p, remaining);
  uint32_t values_per_mini_block = ReadVlq<T>(p, remaining);
  uint32_t total_count = ReadVlq<T>(p, remaining);
  uint32_t first_value = ReadVlq<T>(p, remaining);

  // reserve output buffer
  if (result_buffer_->Capacity() < total_count * sizeof(T)) {
    result_buffer_->ReSize(total_count * sizeof(T));
  }

  // write first value
  T current_value = static_cast<T>(first_value);
  result_buffer_->Write(reinterpret_cast<char *>(&current_value), sizeof(T));
  result_buffer_->Brush(sizeof(T));
  uint32_t decoded = 1;

  const uint32_t mini_blocks_per_block =
      values_per_block / values_per_mini_block;

  while (decoded < total_count && remaining > 0) {
    uint32_t min_delta = ReadVlq<T>(p, remaining);

    if (remaining < mini_blocks_per_block) break;
    uint8_t bit_widths[16] = {0};
    for (uint32_t i = 0; i < mini_blocks_per_block; ++i) {
      bit_widths[i] = *p++;
      --remaining;
    }

    uint32_t payload_size = ReadVlq<T>(p, remaining);

    uint32_t values_in_block =
        std::min(values_per_block, total_count - decoded);

    // read payload within bounded size
    BitReader64Ptr br(p, payload_size);

    for (uint32_t i = 0; i < mini_blocks_per_block && decoded < total_count;
         ++i) {
      uint32_t start = i * values_per_mini_block;
      if (start >= values_in_block) break;
      uint32_t end = std::min(start + values_per_mini_block, values_in_block);
      uint32_t cnt = end - start;
      uint8_t w = bit_widths[i];

      T *out_base = reinterpret_cast<T *>(result_buffer_->GetAvailableBuffer());
      ReadMiniBlockSpecializedPtr<T>(br, out_base, current_value, cnt,
                                     min_delta, w);
      result_buffer_->Brush(cnt * sizeof(T));
      decoded += cnt;
    }

    br.AlignToByte();

    p += payload_size;
    remaining -= payload_size;
  }

  return result_buffer_->Used();
}

template <typename T>
size_t PaxDeltaDecoder<T>::Decoding(const char * /*not_null*/,
                                    size_t /*not_null_len*/) {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements);
}

template class PaxDeltaEncoder<uint32_t>;
template class PaxDeltaDecoder<uint32_t>;
// Add explicit instantiations for signed integral types used by CreateDecoder
template class PaxDeltaDecoder<long>;
template class PaxDeltaDecoder<int>;
template class PaxDeltaDecoder<short>;
template class PaxDeltaDecoder<signed char>;

}  // namespace pax