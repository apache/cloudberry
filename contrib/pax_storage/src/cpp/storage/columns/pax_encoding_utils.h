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
 * pax_encoding_utils.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_utils.h
 *
 *-------------------------------------------------------------------------
 */


#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

/* This is a limitation of the orc protocol.
 * This parameters should not be changed under any circumstances.
 */
#define ORC_MAX_LITERAL_SIZE 512
#define ORC_MIN_REPEAT 3
#define ORC_HIST_LEN 32
#define ORC_MAX_SHORT_REPEAT_LENGTH (ORC_MIN_REPEAT + 7)

enum EncodingType {
  kShortRepeat = 0,
  kDirect,
  kPatchedBase,
  kDelta,
  // internal used, will not be serialized to disk
  kInvalidType,
};

struct FixedBitSizes {
  enum FBS {
    kONE = 0,
    kTWO,
    kTHREE,
    kFOUR,
    kFIVE,
    kSIX,
    kSEVEN,
    kEIGHT,
    kNINE,
    kTEN,
    kELEVEN,
    kTWELVE,
    kTHIRTEEN,
    kFOURTEEN,
    kFIFTEEN,
    kSIXTEEN,
    kSEVENTEEN,
    kEIGHTEEN,
    kNINETEEN,
    kTWENTY,
    kTWENTYONE,
    kTWENTYTWO,
    kTWENTYTHREE,
    kTWENTYFOUR,
    kTWENTYSIX,
    kTWENTYEIGHT,
    kTHIRTY,
    kTHIRTYTWO,
    kFORTY,
    kFORTYEIGHT,
    kFIFTYSIX,
    kSIXTYFOUR,
    kSIZE
  };
};

extern const uint8_t kFBSToBitWidthMap[FixedBitSizes::kSIZE];
extern const uint8_t kClosestBitsMap[65];
extern const uint8_t kClosestAlignedBitsMap[65];
extern const uint8_t kBitWidthToFBSMap[65];

inline uint32 DecodeBits(uint32 n) {  //
  return kFBSToBitWidthMap[n];
}

inline uint32 EncodeBits(uint32 n) {
  if (n <= 64) {
    return kBitWidthToFBSMap[n];
  } else {
    return FixedBitSizes::kSIXTYFOUR;
  }
}

uint32 GetClosestBits(uint32 n);
uint32 GetClosestAlignedBits(uint32 n);
uint32 FindClosestBits(int64 value);

// histogram functions
void BuildHistogram(int32 *histogram, int64_t *data, size_t number);
uint32_t GetPercentileBits(const int32 *histogram, size_t histogram_len,
                           double p);

// zig zag encoding for the sign number
inline int64 ZigZag(int64 value) {  //
  return (value << 1) ^ (value >> 63);
}

template <typename T>
inline int64 UnZigZag(T value) {  //
  return (value >> 1) ^ -(value & 1);
}

template <typename T>
inline int64 UnZigZagWithUnsigned(T value) {
  switch (sizeof(T)) {
    case 1: {
      auto us_value = static_cast<uint8>(value);
      return (us_value >> 1) ^ -(us_value & 1);
    }
    case 2: {
      auto us_value = static_cast<uint16>(value);
      return (us_value >> 1) ^ -(us_value & 1);
    }
    case 4: {
      auto us_value = static_cast<uint32>(value);
      return (us_value >> 1) ^ -(us_value & 1);
    }
    case 8: {
      auto us_value = static_cast<uint64>(value);
      return (us_value >> 1) ^ -(us_value & 1);
    }
    default: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
    }
  }
  CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError);
}

void ZigZagBuffers(int64_t *input, int64_t *output, size_t number);

}  // namespace pax
