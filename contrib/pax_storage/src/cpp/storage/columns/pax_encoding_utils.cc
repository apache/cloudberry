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
 * pax_encoding_utils.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_utils.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_utils.h"

namespace pax {
// Map FBS enum to bit width value.
const uint8 kFBSToBitWidthMap[FixedBitSizes::kSIZE] = {
    1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 30, 32, 40, 48, 56, 64};

// Map bit length i to closest fixed bit width that can contain i bits.
const uint8 kClosestBitsMap[65] = {
    1,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 26, 26, 28, 28, 30, 30, 32, 32, 40,
    40, 40, 40, 40, 40, 40, 40, 48, 48, 48, 48, 48, 48, 48, 48, 56, 56,
    56, 56, 56, 56, 56, 56, 64, 64, 64, 64, 64, 64, 64, 64};

// Map bit length i to closest aligned fixed bit width that can contain i bits.
const uint8 kClosestAlignedBitsMap[65] = {
    1,  1,  2,  4,  4,  8,  8,  8,  8,  16, 16, 16, 16, 16, 16, 16, 16,
    24, 24, 24, 24, 24, 24, 24, 24, 32, 32, 32, 32, 32, 32, 32, 32, 40,
    40, 40, 40, 40, 40, 40, 40, 48, 48, 48, 48, 48, 48, 48, 48, 56, 56,
    56, 56, 56, 56, 56, 56, 64, 64, 64, 64, 64, 64, 64, 64};

// Map bit width to FBS enum.
const uint8 kBitWidthToFBSMap[65] = {
    FixedBitSizes::kONE,         FixedBitSizes::kONE,
    FixedBitSizes::kTWO,         FixedBitSizes::kTHREE,
    FixedBitSizes::kFOUR,        FixedBitSizes::kFIVE,
    FixedBitSizes::kSIX,         FixedBitSizes::kSEVEN,
    FixedBitSizes::kEIGHT,       FixedBitSizes::kNINE,
    FixedBitSizes::kTEN,         FixedBitSizes::kELEVEN,
    FixedBitSizes::kTWELVE,      FixedBitSizes::kTHIRTEEN,
    FixedBitSizes::kFOURTEEN,    FixedBitSizes::kFIFTEEN,
    FixedBitSizes::kSIXTEEN,     FixedBitSizes::kSEVENTEEN,
    FixedBitSizes::kEIGHTEEN,    FixedBitSizes::kNINETEEN,
    FixedBitSizes::kTWENTY,      FixedBitSizes::kTWENTYONE,
    FixedBitSizes::kTWENTYTWO,   FixedBitSizes::kTWENTYTHREE,
    FixedBitSizes::kTWENTYFOUR,  FixedBitSizes::kTWENTYSIX,
    FixedBitSizes::kTWENTYSIX,   FixedBitSizes::kTWENTYEIGHT,
    FixedBitSizes::kTWENTYEIGHT, FixedBitSizes::kTHIRTY,
    FixedBitSizes::kTHIRTY,      FixedBitSizes::kTHIRTYTWO,
    FixedBitSizes::kTHIRTYTWO,   FixedBitSizes::kFORTY,
    FixedBitSizes::kFORTY,       FixedBitSizes::kFORTY,
    FixedBitSizes::kFORTY,       FixedBitSizes::kFORTY,
    FixedBitSizes::kFORTY,       FixedBitSizes::kFORTY,
    FixedBitSizes::kFORTY,       FixedBitSizes::kFORTYEIGHT,
    FixedBitSizes::kFORTYEIGHT,  FixedBitSizes::kFORTYEIGHT,
    FixedBitSizes::kFORTYEIGHT,  FixedBitSizes::kFORTYEIGHT,
    FixedBitSizes::kFORTYEIGHT,  FixedBitSizes::kFORTYEIGHT,
    FixedBitSizes::kFORTYEIGHT,  FixedBitSizes::kFIFTYSIX,
    FixedBitSizes::kFIFTYSIX,    FixedBitSizes::kFIFTYSIX,
    FixedBitSizes::kFIFTYSIX,    FixedBitSizes::kFIFTYSIX,
    FixedBitSizes::kFIFTYSIX,    FixedBitSizes::kFIFTYSIX,
    FixedBitSizes::kFIFTYSIX,    FixedBitSizes::kSIXTYFOUR,
    FixedBitSizes::kSIXTYFOUR,   FixedBitSizes::kSIXTYFOUR,
    FixedBitSizes::kSIXTYFOUR,   FixedBitSizes::kSIXTYFOUR,
    FixedBitSizes::kSIXTYFOUR,   FixedBitSizes::kSIXTYFOUR,
    FixedBitSizes::kSIXTYFOUR};

uint32 GetClosestBits(uint32 n) {
  if (n <= 64) {
    return kClosestBitsMap[n];
  } else {
    return 64;
  }
}

uint32 GetClosestAlignedBits(uint32 n) {
  if (n <= 64) {
    return kClosestAlignedBitsMap[n];
  } else {
    return 64;
  }
}

uint32 FindClosestBits(int64 value) {
  if (value < 0) {
    return GetClosestBits(64);
  }

  uint32 count = 0;
  while (value != 0) {
    count++;
    value = value >> 1;
  }
  return GetClosestBits(count);
}

void BuildHistogram(int32 *histogram, int64_t *data, size_t number) {
  // histogram that store the encoded bit requirement for each values.
  // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
  memset(histogram, 0, FixedBitSizes::kSIZE * sizeof(int32_t));

  // compute the histogram
  for (size_t i = 0; i < number; i++) {
    uint32_t idx = EncodeBits(FindClosestBits((int64_t)data[i]));
    histogram[idx] += 1;
  }
}

uint32_t GetPercentileBits(const int32 *const histogram, size_t histogram_len,
                           double p) {
  Assert((p <= 1.0) || (p > 0.0));

  auto per_len =
      static_cast<int32>(static_cast<double>(histogram_len) * (1.0 - p));

  // return the bits required by pth percentile length
  for (int32_t i = ORC_HIST_LEN - 1; i >= 0; i--) {
    per_len -= histogram[i];
    if (per_len < 0) {
      return DecodeBits(static_cast<uint32_t>(i));
    }
  }
  return 0;
}

void ZigZagBuffers(int64_t *input, int64_t *output, size_t number) {
  Assert(input && output);
  for (size_t i = 0; i < number; i++) {
    output[i] = ZigZag(input[i]);
  }
}

}  // namespace pax
