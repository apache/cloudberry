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
 * bloomfilter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/bloomfilter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/bloomfilter.h"

#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {
#define MAX_HASH_FUNCS 10

BloomFilter::BloomFilter()
    : k_hash_funcs_(0), seed_(0), m_(0), bitset_(nullptr), readonly_(true) {}

BloomFilter::~BloomFilter() { PAX_FREE(bitset_); }

static int optimal_k(uint64 bitset_bits, int64 total_elems) {
  int k = rint(log(2.0) * bitset_bits / total_elems);
  return Max(1, Min(k, MAX_HASH_FUNCS));
}

static inline uint32 mod_m(uint32 val, uint64 bits) {
  Assert(bits <= PG_UINT32_MAX + UINT64CONST(1));
  Assert(((bits - 1) & bits) == 0);

  return val & (bits - 1);
}

void BloomFilter::KHashes(uint32 *hashes, unsigned char *elem, size_t len) {
  uint64 hash;
  uint32 x, y;
  int i;

  // Use 64-bit hashing to get two independent 32-bit hashes
  hash = hash_bytes_extended(elem, len, seed_);
  x = (uint32)hash;
  y = (uint32)(hash >> 32);

  x = mod_m(x, m_);
  y = mod_m(y, m_);

  // Accumulate hashes
  hashes[0] = x;
  for (i = 1; i < k_hash_funcs_; i++) {
    x = mod_m(x + y, m_);
    y = mod_m(y + i, m_);

    hashes[i] = x;
  }
}

void BloomFilter::Create(size_t total_elems, int bloom_work_mem, uint64 seed) {
  uint64 bitset_bytes;
  uint64 bitset_bits;
  int bloom_power = -1;
  uint64 target_bitset_bits;

  Assert(!bitset_ && m_ == 0);

  // Aim for two bytes per element; this is sufficient to get a false
  // positive rate below 1%, independent of the size of the bitset or total
  // number of elements.  Also, if rounding down the size of the bitset to
  // the next lowest power of two turns out to be a significant drop, the
  // false positive rate still won't exceed 2% in almost all cases.
  bitset_bytes = Min((uint64)bloom_work_mem, total_elems * 2);

  // Size in bits should be the highest power of two <= target.  bitset_bits
  // is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
  target_bitset_bits = bitset_bytes * BITS_PER_BYTE;
  while (target_bitset_bits > 0 && bloom_power < 32) {
    bloom_power++;
    target_bitset_bits >>= 1;
  }
  bitset_bits = UINT64CONST(1) << bloom_power;
  bitset_bytes = bitset_bits / BITS_PER_BYTE;

  PAX_LOG_IF(pax_enable_debug, "Build a writable bloom filter [bytes=%lu]",
             bitset_bytes);

  bitset_ = PAX_ALLOC0<unsigned char>(bitset_bytes);
  k_hash_funcs_ = optimal_k(bitset_bits, total_elems);
  seed_ = seed;
  m_ = bitset_bits;
  readonly_ = false;
}

void BloomFilter::Create(const char *bs, uint64 bits, uint64 seed,
                         int32 khashfuncs) {
  uint64 bitset_bytes;
  Assert(!bitset_ && m_ == 0);

  bitset_bytes = (bits + 7) / 8;

  seed_ = seed;
  k_hash_funcs_ = khashfuncs;
  m_ = bits;

  PAX_LOG_IF(pax_enable_debug, "Build a readable bloom filter [bytes=%lu]",
             bits / BITS_PER_BYTE);

  bitset_ = PAX_ALLOC<unsigned char>(bitset_bytes);
  memcpy(bitset_, bs, bitset_bytes);
  readonly_ = true;
}

void BloomFilter::CreateFixed() {
  Create(pax_max_tuples_per_file, pax_bloom_filter_work_memory_bytes, 0);
}

void BloomFilter::Reset() {
  size_t bitset_bytes;

  CBDB_CHECK(!readonly_, cbdb::CException::kExTypeLogicError,
             "Current bloom filter is read only");
  Assert(bitset_);

  bitset_bytes = (m_ + 7) / 8;
  memset(reinterpret_cast<void *>(bitset_), 0, bitset_bytes);
}

void BloomFilter::Add(unsigned char *elem, size_t len) {
  uint32 hashes[MAX_HASH_FUNCS];
  int i;

  Assert(bitset_);
  CBDB_CHECK(!readonly_, cbdb::CException::kExTypeLogicError,
             "Current bloom filter is read only");
  KHashes(hashes, elem, len);

  // Map a bit-wise address to a byte-wise address + bit offset
  for (i = 0; i < k_hash_funcs_; i++) {
    bitset_[hashes[i] >> 3] |= 1 << (hashes[i] & 7);
  }
}

bool BloomFilter::Test(unsigned char *elem, size_t len) {
  uint32 hashes[MAX_HASH_FUNCS];
  int i;

  Assert(bitset_);
  KHashes(hashes, elem, len);

  // Map a bit-wise address to a byte-wise address + bit offset
  for (i = 0; i < k_hash_funcs_; i++) {
    if (!(bitset_[hashes[i] >> 3] & (1 << (hashes[i] & 7)))) return true;
  }

  return false;
}

void BloomFilter::MergeFrom(BloomFilter *filter) {
  size_t bitset_bytes;

  CBDB_CHECK(!readonly_, cbdb::CException::kExTypeLogicError,
             "Current bloom filter is read only");
  Assert(bitset_ && filter->bitset_);

  if (k_hash_funcs_ != filter->k_hash_funcs_ || m_ != filter->m_ ||
      seed_ != filter->seed_) {
    CBDB_RAISE(cbdb::CException::kExTypeLogicError,
               pax::fmt("Different bloom filter can't merge, left: [%d, %lu, "
                        "%lu], right: [%d, %lu, %lu]",
                        k_hash_funcs_, seed_, m_, filter->k_hash_funcs_,
                        filter->seed_, filter->m_));
  }

  bitset_bytes = (m_ + 7) / 8;
  for (size_t i = 0; i < bitset_bytes; ++i) {
    bitset_[i] |= filter->bitset_[i];
  }
}

}  // namespace pax
