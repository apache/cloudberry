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
 * pax_rlev2_decoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_rlev2_decoding.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_rlev2_decoding.h"

#include <algorithm>
#include <cmath>
#include <vector>

#include "comm/fmt.h"
#include "comm/pax_memory.h"

namespace pax {

#ifndef RUN_GTEST
template <typename T>
void ReadLongs(TreatedDataBuffer<int64> *data_buffer, T *data, uint64 offset,
               uint64 len, uint64 fbs, uint32 *bits_left);
#endif

unsigned char ReadByte(TreatedDataBuffer<int64> *data_buffer);
unsigned char ReadByteWithoutBrush(TreatedDataBuffer<int64> *data_buffer);
int64 ReadLongBE(TreatedDataBuffer<int64> *data_buffer, uint64 bsz);
int64 ReadSignedLong(TreatedDataBuffer<int64> *data_buffer);
uint64 ReadUnsignedLong(TreatedDataBuffer<int64> *data_buffer);
void UnpackNonAlignedLongs(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                           uint64 offset, uint64 len, uint64 fbs,
                           uint32 *bits_left);

void UnrolledUnpack4(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                     uint64 offset, uint64 len, uint32 *bits_left);
void UnrolledUnpack8(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                     uint64 offset, uint64 len);
void UnrolledUnpack16(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack24(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack32(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack40(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack48(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack56(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);
void UnrolledUnpack64(TreatedDataBuffer<int64> *data_buffer, int64 *data,
                      uint64 offset, uint64 len);

static inline unsigned char ReadByte(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer) {
  return ReadByte(data_buffer.get());
}
static inline unsigned char ReadByteWithoutBrush(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer) {
  return ReadByteWithoutBrush(data_buffer.get());
}
static inline int64 ReadLongBE(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, uint64 bsz) {
  return ReadLongBE(data_buffer.get(), bsz);
}
static inline int64 ReadSignedLong(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer) {
  return ReadSignedLong(data_buffer.get());
}
static inline uint64 ReadUnsignedLong(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer) {
  return ReadUnsignedLong(data_buffer.get());
}

/**
 * Decode the next gap and patch from 'unpackedPatch' and update the index on
 * it. Used by PATCHED_BASE.
 *
 * patch_bits  bit size of the patch value
 * patch_mask     mask for the patch value
 * res_gap        result of gap
 * res_patch      result of patch
 * patch_idx      current index in the 'unpackedPatch' buffer
 */
void AdjustGapAndPatch(DataBuffer<int64> *unpacked, uint32 patch_bits,
                       int64 patch_mask, int64 *res_gap, int64 *res_patch,
                       uint64 *patch_idx);
static inline void AdjustGapAndPatch(const std::shared_ptr<DataBuffer<int64>> &unpacked, uint32 patch_bits,
                       int64 patch_mask, int64 *res_gap, int64 *res_patch,
                       uint64 *patch_idx) {
  AdjustGapAndPatch(unpacked.get(), patch_bits, patch_mask, res_gap, res_patch, patch_idx);
}

/*
 * copy temp data to data which will skip the null field
 */
template <typename T>
uint64 CopyData(T *data, const int64 *temp_data, uint64 len, uint64 offset,
                const char *not_null);

template uint64 CopyData(int64 *data, const int64 *temp_data, uint64 len,
                         uint64 offset, const char *not_null);

unsigned char ReadByte(TreatedDataBuffer<int64> *data_buffer) {
  unsigned char result = data_buffer->GetTreatedRawBuffer()[0];
  data_buffer->BrushTreated(1);
  return result;
}

unsigned char ReadByteWithoutBrush(TreatedDataBuffer<int64> *data_buffer) {
  unsigned char result = data_buffer->GetTreatedRawBuffer()[0];
  return result;
}

int64 ReadLongBE(TreatedDataBuffer<int64> *data_buffer, uint64 bsz) {
  int64 ret = 0, val;
  uint64 n = bsz;
  while (n > 0) {
    n--;
    val = ReadByte(data_buffer);
    ret |= (val << (n * 8));
  }
  return ret;
}

int64 ReadSignedLong(TreatedDataBuffer<int64> *data_buffer) {
  return UnZigZag<uint64>(ReadUnsignedLong(data_buffer));
}

uint64 ReadUnsignedLong(TreatedDataBuffer<int64> *data_buffer) {
  uint64 ret = 0, b;
  uint64 offset = 0;
  do {
    b = ReadByte(data_buffer);
    ret |= (0x7f & b) << offset;
    offset += 7;
  } while (b >= 0x80);
  return ret;
}

template <typename T>
void UnrolledUnpack4(TreatedDataBuffer<int64> *data_buffer, T *data,
                     uint64 offset, uint64 len, uint32 *bits_left) {
  uint64 cur_idx = offset;
  uint32 cur_byte = 0;
  uint64 num_groups = 0;
  uint32 local_byte;

  Assert(*bits_left == 0);

  while (cur_idx < offset + len) {
    // Make sure bits_left is 0 before the loop. bits_left can only be 0, 4,
    // or 8.
    while (*bits_left > 0 && cur_idx < offset + len) {
      *bits_left -= 4;
      data[cur_idx++] = (cur_byte >> *bits_left) & 15;
    }
    if (cur_idx == offset + len) return;

    num_groups = (offset + len - cur_idx) / 2;
    num_groups =
        std::min(num_groups, static_cast<uint64>(data_buffer->UnTreated()));
    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());

    for (uint64 i = 0; i < num_groups; ++i) {
      local_byte = *buffer++;
      data[cur_idx] = (local_byte >> 4) & 15;
      data[cur_idx + 1] = local_byte & 15;
      cur_idx += 2;
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    cur_byte = ReadByte(data_buffer);
    *bits_left = 8;
  }
}

template <typename T>
void UnrolledUnpack8(TreatedDataBuffer<int64> *data_buffer, T *data,
                     uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = data_buffer->UnTreated();
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));

    auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      data[cur_idx++] = *buffer++;
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());

    if (cur_idx == offset + len) return;
    data[cur_idx++] = ReadByte(data_buffer);
  }
}

template <typename T>
void UnrolledUnpack16(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;
  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 2;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint16 b0, b1;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint16>(*buffer);
      b1 = static_cast<uint16>(*(buffer + 1));
      buffer += 2;
      data[cur_idx++] = (b0 << 8) | b1;
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    data[cur_idx++] = (b0 << 8) | b1;
  }
}

template <typename T>
void UnrolledUnpack24(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 3;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint32 b0, b1, b2;
    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      buffer += 3;
      data[cur_idx++] = static_cast<T>((b0 << 16) | (b1 << 8) | b2);
    }
    data_buffer->BrushTreated(buff_len * 3);

    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    data[cur_idx++] = static_cast<T>((b0 << 16) | (b1 << 8) | b2);
  }
}

template <typename T>
void UnrolledUnpack32(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 4;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint32 b0, b1, b2, b3;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      b3 = static_cast<uint32>(*(buffer + 3));
      buffer += 4;
      data[cur_idx++] =
          static_cast<T>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    b3 = ReadByte(data_buffer);
    data[cur_idx++] = static_cast<T>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
  }
}

template <typename T>
void UnrolledUnpack40(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 5;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint64 b0, b1, b2, b3, b4;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      b3 = static_cast<uint32>(*(buffer + 3));
      b4 = static_cast<uint32>(*(buffer + 4));
      buffer += 5;
      data[cur_idx++] =
          static_cast<T>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    b3 = ReadByte(data_buffer);
    b4 = ReadByte(data_buffer);
    data[cur_idx++] =
        static_cast<T>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
  }
}

template <typename T>
void UnrolledUnpack48(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 6;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint64 b0, b1, b2, b3, b4, b5;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      b3 = static_cast<uint32>(*(buffer + 3));
      b4 = static_cast<uint32>(*(buffer + 4));
      b5 = static_cast<uint32>(*(buffer + 5));
      buffer += 6;
      data[cur_idx++] = static_cast<T>((b0 << 40) | (b1 << 32) | (b2 << 24) |
                                       (b3 << 16) | (b4 << 8) | b5);
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    b3 = ReadByte(data_buffer);
    b4 = ReadByte(data_buffer);
    b5 = ReadByte(data_buffer);
    data[cur_idx++] = static_cast<T>((b0 << 40) | (b1 << 32) | (b2 << 24) |
                                     (b3 << 16) | (b4 << 8) | b5);
  }
}

template <typename T>
void UnrolledUnpack56(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 7;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint64 b0, b1, b2, b3, b4, b5, b6;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      b3 = static_cast<uint32>(*(buffer + 3));
      b4 = static_cast<uint32>(*(buffer + 4));
      b5 = static_cast<uint32>(*(buffer + 5));
      b6 = static_cast<uint32>(*(buffer + 6));
      buffer += 7;
      data[cur_idx++] =
          static_cast<T>((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) |
                         (b4 << 16) | (b5 << 8) | b6);
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    b3 = ReadByte(data_buffer);
    b4 = ReadByte(data_buffer);
    b5 = ReadByte(data_buffer);
    b6 = ReadByte(data_buffer);
    data[cur_idx++] = static_cast<T>((b0 << 48) | (b1 << 40) | (b2 << 32) |
                                     (b3 << 24) | (b4 << 16) | (b5 << 8) | b6);
  }
}

template <typename T>
void UnrolledUnpack64(TreatedDataBuffer<int64> *data_buffer, T *data,
                      uint64 offset, uint64 len) {
  uint64 cur_idx = offset;
  int64 buff_len;

  while (cur_idx < offset + len) {
    buff_len = (data_buffer->UnTreated()) / 8;
    buff_len = std::min(buff_len, static_cast<int64>(offset + len - cur_idx));
    uint64 b0, b1, b2, b3, b4, b5, b6, b7;

    const auto *buffer = reinterpret_cast<const unsigned char *>(
        data_buffer->GetTreatedRawBuffer());
    for (int i = 0; i < buff_len; ++i) {
      b0 = static_cast<uint32>(*buffer);
      b1 = static_cast<uint32>(*(buffer + 1));
      b2 = static_cast<uint32>(*(buffer + 2));
      b3 = static_cast<uint32>(*(buffer + 3));
      b4 = static_cast<uint32>(*(buffer + 4));
      b5 = static_cast<uint32>(*(buffer + 5));
      b6 = static_cast<uint32>(*(buffer + 6));
      b7 = static_cast<uint32>(*(buffer + 7));
      buffer += 8;
      data[cur_idx++] =
          static_cast<T>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) |
                         (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
    }

    data_buffer->BrushTreated(reinterpret_cast<const char *>(buffer) -
                              data_buffer->GetTreatedRawBuffer());
    if (cur_idx == offset + len) return;

    b0 = ReadByte(data_buffer);
    b1 = ReadByte(data_buffer);
    b2 = ReadByte(data_buffer);
    b3 = ReadByte(data_buffer);
    b4 = ReadByte(data_buffer);
    b5 = ReadByte(data_buffer);
    b6 = ReadByte(data_buffer);
    b7 = ReadByte(data_buffer);
    data[cur_idx++] =
        static_cast<T>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) |
                       (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
  }
}

template <typename T>
void UnpackNonAlignedLongs(TreatedDataBuffer<int64> *data_buffer, T *data,
                           uint64 offset, uint64 len, uint64 fbs,
                           uint32 *bits_left) {
  unsigned char cur_byte = data_buffer->GetTreatedRawBuffer()[0];
  for (uint64 i = offset; i < (offset + len); i++) {
    uint64 result = 0;
    uint64 bits_left_to_read = fbs;

    while (bits_left_to_read > *bits_left) {
      result <<= *bits_left;
      result |= cur_byte & ((1 << *bits_left) - 1);
      bits_left_to_read -= *bits_left;
      cur_byte = ReadByte(data_buffer);
      *bits_left = 8;
    }

    // handle the left over bits
    if (bits_left_to_read > 0) {
      result <<= bits_left_to_read;
      *bits_left -= static_cast<uint32>(bits_left_to_read);
      result |= (cur_byte >> *bits_left) & ((1 << bits_left_to_read) - 1);
    }
    data[i] = static_cast<T>(result);
  }
}

template <typename T>
void ReadLongs(TreatedDataBuffer<int64> *data_buffer, T *data, uint64 offset,
               uint64 len, uint64 fbs, uint32 *bits_left) {
  switch (fbs) {
    case 4:
      UnrolledUnpack4<T>(data_buffer, data, offset, len, bits_left);
      return;
    case 8:
      UnrolledUnpack8<T>(data_buffer, data, offset, len);
      return;
    case 16: {
      Assert(sizeof(T) >= 2);
      UnrolledUnpack16<T>(data_buffer, data, offset, len);
      return;
    }
    case 24: {
      Assert(sizeof(T) >= 3);
      UnrolledUnpack24<T>(data_buffer, data, offset, len);
      return;
    }
    case 32: {
      Assert(sizeof(T) >= 4);
      UnrolledUnpack32<T>(data_buffer, data, offset, len);
      return;
    }
    case 40: {
      Assert(sizeof(T) >= 5);
      UnrolledUnpack40<T>(data_buffer, data, offset, len);
      return;
    }
    case 48: {
      Assert(sizeof(T) >= 6);
      UnrolledUnpack48<T>(data_buffer, data, offset, len);
      return;
    }
    case 56: {
      Assert(sizeof(T) >= 7);
      UnrolledUnpack56<T>(data_buffer, data, offset, len);
      return;
    }
    case 64: {
      Assert(sizeof(T) >= 8);
      UnrolledUnpack64<T>(data_buffer, data, offset, len);
      return;
    }
    default:
      // Fallback to the default implementation for deprecated bit size.
      UnpackNonAlignedLongs<T>(data_buffer, data, offset, len, fbs, bits_left);
      return;
  }
}

void AdjustGapAndPatch(DataBuffer<int64> *unpacked, uint32 patch_bits,
                       int64 patch_mask, int64 *res_gap, int64 *res_patch,
                       uint64 *patch_idx) {
  uint64 idx = *patch_idx;
  uint64 gap = static_cast<uint64>((*unpacked)[idx]) >> patch_bits;
  int64 patch = (*unpacked)[idx] & patch_mask;
  int64 actual_gap = 0;

  // special case: gap is >255 then patch value will be 0.
  // if gap is <=255 then patch value cannot be 0
  while (gap == 255 && patch == 0) {
    actual_gap += 255;
    ++idx;
    gap = static_cast<uint64>((*unpacked)[idx]) >> patch_bits;
    patch = (*unpacked)[idx] & patch_mask;
  }
  // add the left over gap
  actual_gap += gap;

  *res_gap = actual_gap;
  *res_patch = patch;
  *patch_idx = idx;
}

template <typename T>
uint64 CopyData(T *data, const int64 *const temp_data, uint64 len,
                uint64 offset, const char *not_null) {
  if (not_null) {
    size_t already_fill = 0;

    for (uint64 pos = 0; pos < len;) {
      if (!not_null[pos + already_fill + offset]) {
        // should never add offset, cause not data is not from start();
        already_fill++;
      } else {
        data[pos + already_fill] = temp_data[pos];
        pos++;
      }
    }
    return already_fill + len;
  } else {
    for (size_t i = 0; i < len; i++) {
      data[i] = static_cast<T>(temp_data[i]);
    }
    return len;
  }

  // never reach
  Assert(false);
}

template <typename T>
PaxOrcDecoder<T>::PaxOrcDecoder(
    const PaxDecoder::DecodingOption &encoder_options)
    : PaxDecoder(encoder_options),
      data_buffer_(nullptr),
      copy_data_buffer_(nullptr),
      unpacked_data_(nullptr),
      result_buffer_(nullptr) {}

template <typename T>
PaxOrcDecoder<T>::~PaxOrcDecoder() { }

template <typename T>
PaxDecoder *PaxOrcDecoder<T>::SetSrcBuffer(char *data, size_t data_len) {
  Assert(!data_buffer_);
  if (data) {
    data_buffer_ = std::make_shared<TreatedDataBuffer<int64>>(
        reinterpret_cast<int64 *>(data), data_len);
    copy_data_buffer_ =
        std::make_shared<DataBuffer<int64>>(ORC_MAX_LITERAL_SIZE * sizeof(int64));
  }

  return this;
}

template <typename T>
PaxDecoder *PaxOrcDecoder<T>::SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer) {
  result_buffer_ = result_buffer;
  return this;
}

template <typename T>
const char *PaxOrcDecoder<T>::GetBuffer() const {
  return result_buffer_->GetBuffer();
}

template <typename T>
size_t PaxOrcDecoder<T>::GetBufferSize() const {
  return result_buffer_->Used();
}

template <typename T>
size_t PaxOrcDecoder<T>::Next(const char *const not_null) {
  Assert(result_buffer_);
  size_t n_read = result_buffer_->Used();
  uint64 read_round = 0;

  if (unlikely(!data_buffer_)) {
    return n_read;
  }

  if (data_buffer_->UnTreated() == 0) {
    return result_buffer_->Used();
  }

  unsigned char first_byte = ReadByteWithoutBrush(data_buffer_);

  // brush the null field

  if (not_null) {
    uint64 null_index = n_read / sizeof(T);
    uint64 null_read = n_read;
    while (!not_null[null_index++]) {
      n_read += sizeof(T);
    }

    null_read = n_read - null_read;
    result_buffer_->Brush(null_read);
  }
  Assert(n_read % sizeof(T) == 0);

  auto enc = static_cast<EncodingType>((first_byte >> 6) & 0x03);
  auto result_data =
      reinterpret_cast<T *>(result_buffer_->GetAvailableBuffer());
  switch (enc) {
    case EncodingType::kShortRepeat:
      read_round = NextShortRepeats(data_buffer_, result_data,
                                    n_read / sizeof(T), not_null);
      break;
    case EncodingType::kDirect:
      read_round =
          NextDirect(data_buffer_, result_data, n_read / sizeof(T), not_null);
      break;
    case EncodingType::kPatchedBase:
      read_round =
          NextPatched(data_buffer_, result_data, n_read / sizeof(T), not_null);
      break;
    case EncodingType::kDelta:
      read_round =
          NextDelta(data_buffer_, result_data, n_read / sizeof(T), not_null);
      break;
    default:
      Assert(false);
  }

  result_buffer_->Brush(read_round * sizeof(T));
  n_read += read_round;

  return n_read;
}

template <typename T>
size_t PaxOrcDecoder<T>::Decoding() {
  return Decoding(nullptr, 0);
}

template <typename T>
size_t PaxOrcDecoder<T>::Decoding(const char *const not_null,
                                  size_t not_null_len) {
  size_t n_read = 0;
  size_t last_read = 0;
  size_t result_cap = result_buffer_->Available();
  Assert(result_buffer_);
  Assert(result_cap > 0);

  if (data_buffer_) {
    do {
      last_read = n_read;
      n_read = Next(not_null);

      CBDB_CHECK(n_read <= result_cap,
                 cbdb::CException::ExType::kExTypeOutOfRange,
                 fmt("RLEV2 decoder, No enough memory in result buffer "
                     "[n_read=%lu, result_cap=%lu]",
                     n_read, result_cap));
    } while (n_read != last_read);
  }

  if (not_null) {
    Assert(n_read <= (not_null_len * sizeof(T)));

    if (n_read < (not_null_len * sizeof(T))) {
      uint64 null_index = n_read / sizeof(T);
      uint64 null_read = n_read;
      while (!not_null[null_index] && null_index < not_null_len) {
        n_read += sizeof(T);
        null_index++;
      }

      null_read = n_read - null_read;
      result_buffer_->Brush(null_read);
    }
    Assert(n_read == (not_null_len * sizeof(T)));
  }

  return n_read;
}

template <typename T>
uint64 PaxOrcDecoder<T>::NextShortRepeats(const std::shared_ptr<TreatedDataBuffer<int64>> &p_data_buffer,
                                          T *const data, uint64 offset,
                                          const char *const not_null) {
  int64 value = 0;
  uint16 data_lens = 0;  // 3 - 10
  auto data_buffer = p_data_buffer.get();

  unsigned char first_byte = ReadByte(data_buffer);

  // extract the number of fixed bytes
  uint64 byte_size = (first_byte >> 3) & 0x07;
  byte_size += 1;

  data_lens = first_byte & 0x07;
  // run lengths values are stored only after MIN_REPEAT value is met
  data_lens += ORC_MIN_REPEAT;

  // read the repeated value which is store using fixed bytes
  value = ReadLongBE(data_buffer, byte_size);

  if (decoder_options_.is_sign) {
    value = UnZigZag<uint64>(static_cast<uint64>(value));
  }

  // It different with orc
  // Not sure why orc just fill the null field
  // But in our storage, should insert null rather fill null field
  if (not_null) {
    size_t already_fill = 0;

    for (uint64 pos = 0; pos < data_lens;) {
      if (!not_null[pos + already_fill + offset]) {
        // should never add offset, cause not data is not from start();
        already_fill++;
      } else {
        data[pos + already_fill] = static_cast<T>(value);
        pos++;
      }
    }
    return already_fill + data_lens;
  }

  for (uint64 pos = 0; pos < data_lens; ++pos) {
    data[pos] = static_cast<T>(value);
  }
  return data_lens;
}

template <typename T>
uint64 PaxOrcDecoder<T>::NextDirect(const std::shared_ptr<TreatedDataBuffer<int64>> &p_data_buffer,
                                    T *const data, uint64 offset,
                                    const char *const not_null) {
  auto data_buffer = p_data_buffer.get();

  // extract the number of fixed bits
  unsigned char first_byte = ReadByte(data_buffer);
  unsigned char fbo = (first_byte >> 1) & 0x1f;
  uint32 bits = DecodeBits(fbo);
  uint32 data_lens = 0;
  uint32 bits_left = 0;

  // extract the run length
  data_lens = static_cast<uint64>(first_byte & 0x01) << 8;
  data_lens |= ReadByte(data_buffer);

  // runs are one off
  data_lens += 1;

  if (!not_null) {
    ReadLongs<T>(data_buffer, data, 0, data_lens, bits, &bits_left);
    if (decoder_options_.is_sign) {
      for (uint64 i = 0; i < data_lens; ++i) {
        data[i] = UnZigZagWithUnsigned<T>(data[i]);
      }
    }

    return data_lens;
  }

  ReadLongs(data_buffer, copy_data_buffer_->StartT(), 0, data_lens, bits,
            &bits_left);

  if (decoder_options_.is_sign) {
    for (uint64 i = 0; i < data_lens; ++i) {
      (*copy_data_buffer_)[i] =
          UnZigZag<uint64>(static_cast<uint64>((*copy_data_buffer_)[i]));
    }
  }

  return CopyData(data, copy_data_buffer_->StartT(), data_lens, offset,
                  not_null);
}

template <typename T>
uint64 PaxOrcDecoder<T>::NextDelta(const std::shared_ptr<TreatedDataBuffer<int64>> &p_data_buffer,
                                   T *data, uint64 offset,
                                   const char *const not_null) {
  auto data_buffer = p_data_buffer.get();
  unsigned char first_byte = ReadByte(data_buffer);

  // extract the number of fixed bits
  unsigned char fbo = (first_byte >> 1) & 0x1f;
  uint32 bits;
  uint32 data_lens = 0;
  uint32 bits_left = 0;

  bits = fbo != 0 ? DecodeBits(fbo) : 0;

  // extract the run length
  data_lens = static_cast<uint64>(first_byte & 0x01) << 8;
  data_lens |= ReadByte(data_buffer);
  ++data_lens;  // account for first value

  // it is safe to make no copy here
  if (!not_null) {
    // read the first value stored as int
    T prev_val = decoder_options_.is_sign
                     ? static_cast<T>(ReadSignedLong(data_buffer))
                     : static_cast<T>(ReadUnsignedLong(data_buffer));

    data[0] = static_cast<T>(prev_val);
    int64 delta_base = ReadSignedLong(data_buffer);

    if (bits == 0) {
      // add fixed deltas to adjacent values
      for (uint64 i = 1; i < data_lens; ++i) {
        data[i] = data[i - 1] + delta_base;
      }
    } else {
      prev_val = data[1] = prev_val + delta_base;
      if (data_lens < 2) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("RLEV2 decode delta failed, [position=%lu]",
                       data_buffer->Used()));
      }

      // write the unpacked values, add it to previous value and store final
      // value to result buffer. if the delta base value is negative then it
      // is a decreasing sequence else an increasing sequence.
      // read deltas using the curr_literals buffer.
      ReadLongs<T>(data_buffer, data, 2, data_lens - 2, bits, &bits_left);
      if (delta_base < 0) {
        for (uint64 i = 2; i < data_lens; ++i) {
          prev_val = data[i] = prev_val - data[i];
        }
      } else {
        for (uint64 i = 2; i < data_lens; ++i) {
          prev_val = data[i] = prev_val + data[i];
        }
      }
    }

    return data_lens;
  }

  int64 prev_val = decoder_options_.is_sign
                       ? ReadSignedLong(data_buffer)
                       : static_cast<int64>(ReadUnsignedLong(data_buffer));

  int64 *curr_literals = copy_data_buffer_->StartT();
  curr_literals[0] = prev_val;

  int64 delta_base = ReadSignedLong(data_buffer);

  if (bits == 0) {
    // TODO(jiaqi zhou): still can no copy here
    // add fixed deltas to adjacent values
    for (uint64 i = 1; i < data_lens; ++i) {
      curr_literals[i] = curr_literals[i - 1] + delta_base;
    }
  } else {
    prev_val = curr_literals[1] = prev_val + delta_base;
    if (data_lens < 2) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("RLEV2 decode delta failed, [position=%lu]",
                     data_buffer->Used()));
    }

    ReadLongs(data_buffer, curr_literals, 2, data_lens - 2, bits, &bits_left);
    if (delta_base < 0) {
      for (uint64 i = 2; i < data_lens; ++i) {
        prev_val = curr_literals[i] = prev_val - curr_literals[i];
      }
    } else {
      for (uint64 i = 2; i < data_lens; ++i) {
        prev_val = curr_literals[i] = prev_val + curr_literals[i];
      }
    }
  }

  return CopyData(data, curr_literals, data_lens, offset, not_null);
}

template <typename T>
uint64 PaxOrcDecoder<T>::NextPatched(const std::shared_ptr<TreatedDataBuffer<int64>> &p_data_buffer,
                                     T *const data, uint64 offset,
                                     const char *const not_null) {
  auto data_buffer = p_data_buffer.get();
  unsigned char first_byte = ReadByte(data_buffer);
  unsigned char fbo = (first_byte >> 1) & 0x1f;
  uint32 bits = DecodeBits(fbo);
  uint32 data_lens = 0;
  uint32 bits_left = 0;

  data_lens = static_cast<uint64>(first_byte & 0x01) << 8;
  data_lens |= ReadByte(data_buffer);
  data_lens += 1;

  uint64 third_byte = ReadByte(data_buffer);
  uint64 byte_size = (third_byte >> 5) & 0x07;

  // base width is one off
  byte_size += 1;

  // extract patch width
  uint32 pwo = third_byte & 0x1f;
  uint32 patch_bits = DecodeBits(pwo);

  // read fourth byte and extract patch gap width
  uint64 fourth_byte = ReadByte(data_buffer);
  uint32 pgw = (fourth_byte >> 5) & 0x07;

  // patch gap width is one off
  pgw += 1;

  // extract the length of the patch list
  size_t pl = fourth_byte & 0x1f;
  CBDB_CHECK(
      pl != 0, cbdb::CException::ExType::kExTypeCompressError,
      fmt("RLEV2 decode patched failed, [position=%lu]", data_buffer->Used()));

  int64 base = ReadLongBE(data_buffer, byte_size);
  int64 mask = (static_cast<int64>(1) << ((byte_size * 8) - 1));

  // if mask of base value is 1 then base is negative value else positive
  if ((base & mask) != 0) {
    base = base & ~mask;
    base = -base;
  }

  /// FIXME(jiaqizho): consider just use result_buffer to reduce copy if no
  /// null field here
  ReadLongs(data_buffer, copy_data_buffer_->StartT(), 0, data_lens, bits,
            &bits_left);
  // reset the bit left
  bits_left = 0;

  if ((patch_bits + pgw) > 64) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
               fmt("RLEV2 decode patched failed, [position=%lu]",
                   data_buffer->Used()));
  }

  if (unpacked_data_ == nullptr) {
    unpacked_data_ = std::make_shared<DataBuffer<int64>>(pl * sizeof(int64));
  } else {
    unpacked_data_->BrushBackAll();
    if (unpacked_data_->Capacity() < (pl * sizeof(int64))) {
      unpacked_data_->ReSize(pl * sizeof(int64));
    }
  }

  uint32 cfb = GetClosestBits(patch_bits + pgw);
  size_t old_treaded = data_buffer->Treated();
  ReadLongs(data_buffer, unpacked_data_->GetAvailableBuffer(), 0, pl, cfb,
            &bits_left);
  size_t treaded_last_read = data_buffer->Treated() - old_treaded;

  CBDB_CHECK(
      treaded_last_read < (pl * sizeof(int64)),
      cbdb::CException::ExType::kExTypeOutOfRange,
      fmt("RLEV2 decode patched failed, [position=%lu]", data_buffer->Used()));

  int64 patch_mask = ((static_cast<int64>(1) << patch_bits) - 1);

  int64 gap = 0;
  int64 patch = 0;
  uint64 patch_idx = 0;
  AdjustGapAndPatch(unpacked_data_, patch_bits, patch_mask, &gap, &patch,
                    &patch_idx);

  for (uint64 i = 0; i < data_lens; ++i) {
    if (static_cast<int64>(i) != gap) {
      // no patching required. add base to unpacked value to get final value
      (*copy_data_buffer_)[i] += base;
    } else {
      // extract the patch value
      int64 patched_val = (*copy_data_buffer_)[i] | (patch << bits);

      // add base to patched value
      (*copy_data_buffer_)[i] = base + patched_val;

      // increment the patch to point to next entry in patch list
      ++patch_idx;

      if (patch_idx < (unpacked_data_->Capacity())) {
        AdjustGapAndPatch(unpacked_data_, patch_bits, patch_mask, &gap, &patch,
                          &patch_idx);

        // next gap is relative to the current gap
        gap += i;
      }
    }
  }

  return CopyData(data, copy_data_buffer_->StartT(), data_lens, offset,
                  not_null);
}

template class PaxOrcDecoder<int64>;
template class PaxOrcDecoder<int32>;
template class PaxOrcDecoder<int16>;
template class PaxOrcDecoder<int8>;

}  // namespace pax
