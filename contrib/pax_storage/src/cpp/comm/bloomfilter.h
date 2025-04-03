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
 * bloomfilter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <assert.h>

#include <cstddef>

#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {

class BloomFilter {
 public:
  BloomFilter();
  ~BloomFilter();

  // Create Bloom filter in caller's memory context.  We aim for a false
  // positive rate of between 1% and 2% when bitset size is not constrained by
  // memory availability.
  //
  // total_elems is an estimate of the final size of the set.  It should be
  // approximately correct, but the implementation can cope well with it being
  // off by perhaps a factor of five or more.  See "Bloom Filters in
  // Probabilistic Verification" (Dillinger & Manolios, 2004) for details of why
  // this is the case.
  //
  // bloom_work_mem is sized in KB, in line with the general work_mem
  // convention. This determines the size of the underlying bitset (trivial
  // bookkeeping space isn't counted).  The bitset is always sized as a power of
  // two number of bits, and the largest possible bitset is 512MB (2^32 bits).
  // The implementation allocates only enough memory to target its standard
  // false positive rate, using a simple formula with caller's total_elems
  // estimate as an input.  The bitset might be as small as 1MB, even when
  // bloom_work_mem is much higher.
  //
  // The Bloom filter is seeded using a value provided by the caller.  Using a
  // distinct seed value on every call makes it unlikely that the same false
  // positives will reoccur when the same set is fingerprinted a second time.
  // Callers that don't care about this pass a constant as their seed, typically
  // 0.  Callers can use a pseudo-random seed in the range of 0 - INT_MAX by
  // calling random().
  void Create(size_t total_elems, int bloom_work_mem, uint64 seed);

  // Create a READ_ONLY bloom filter
  //
  // After we serialize the bloomfilter to disk, we should no longer be allowed
  // to change the bloom filter (call Add() function to expand it) when we read
  // it out.
  void Create(const char *bs, uint64 bits, uint64 bf_seed, int32 khashfuncs);

  // Create a fixed args bloom filter
  //
  // This is useful for pax because a fixed bloom filter is required for the
  // merge operation (call the MergeFrom()) when pax generate statistics.
  void CreateFixed();

  // Add element to Bloom filter
  void Add(unsigned char *elem, size_t len);

  // Test if Bloom filter definitely lacks element.
  // Returns true if the element is definitely not in the set of elements
  // observed by Add().  Otherwise, returns false, indicating that
  // element is probably present in set.
  bool Test(unsigned char *elem, size_t len);

  // Merge the another filter
  void MergeFrom(BloomFilter *filter);

  // Reset the bloom filter
  void Reset();

  // Destroy the bloom filter
  void Destroy();

  // Get the bitset used to serialize to disk
  // Return the bitset and bits in the bitsets
  inline std::pair<unsigned char *, uint64> GetBitSet() {
    return std::make_pair(bitset_, m_);
  }

  inline uint64 GetSeed() { return seed_; }

  inline int32 GetKHashFuncs() { return k_hash_funcs_; }

  inline uint64 GetM() { return m_; }

 private:
  void KHashes(uint32 *hashes, unsigned char *elem, size_t len);

 private:
  // K hash functions are used, seeded by caller's seed
  int32 k_hash_funcs_;
  uint64 seed_;
  // m is bitset size, in bits.  Must be a power of two <= 2^32.
  uint64 m_;
  unsigned char *bitset_;
  bool readonly_;
};

}  // namespace pax
