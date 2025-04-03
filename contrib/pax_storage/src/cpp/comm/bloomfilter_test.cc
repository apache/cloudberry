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
 * bloomfilter_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/bloomfilter_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include <gtest/gtest.h>

#include "comm/bloomfilter.h"

#include "comm/guc.h"
#include "pax_gtest_helper.h"

namespace pax::tests {
class BloomFilterTest : public ::testing::Test {};

TEST_F(BloomFilterTest, TestFilter) {
  BloomFilter filter;
  pax_enable_debug = false;
  filter.Create(1000, 1024 * 1024, 0);

  for (int i = 0; i < 100; i++) {
    filter.Add((unsigned char *)&i, sizeof(int));
  }

  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(filter.Test((unsigned char *)(&i), sizeof(int)))
        << "value: " << i << ", should not get true";
    int i100 = (i + 100);
    ASSERT_TRUE(filter.Test((unsigned char *)(&i100), sizeof(int)))
        << "value: " << i100 << ", should not get false";
  }
}

TEST_F(BloomFilterTest, TestFilterMerge) {
  BloomFilter filter1;
  BloomFilter filter2;
  BloomFilter filter3;
  pax_enable_debug = false;
  filter1.Create(200, 1024 * 1024, 0);
  filter2.Create(200, 1024 * 1024, 0);
  filter3.Create(200, 1024 * 1024, 0);
  for (int i = 0; i < 100; i++) {
    filter1.Add((unsigned char *)&i, sizeof(int));
  }

  filter2.MergeFrom(&filter1);
  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(filter2.Test((unsigned char *)(&i), sizeof(int)))
        << "value: " << i << ", should not get true";
    int i100 = (i + 100);
    ASSERT_TRUE(filter2.Test((unsigned char *)(&i100), sizeof(int)))
        << "value: " << i100 << ", should not get false";
  }

  for (int i = 100; i < 200; i++) {
    filter3.Add((unsigned char *)&i, sizeof(int));
  }

  filter2.MergeFrom(&filter3);
  for (int i = 0; i < 200; i++) {
    ASSERT_FALSE(filter2.Test((unsigned char *)(&i), sizeof(int)))
        << "value: " << i << ", should not get true";
    int i200 = (i + 200);
    ASSERT_TRUE(filter2.Test((unsigned char *)(&i200), sizeof(int)))
        << "value: " << i200 << ", should not get false";
  }
}

TEST_F(BloomFilterTest, TestFilterSerializeAndRestore) {
  BloomFilter filter1;
  BloomFilter filter2;
  unsigned char *bitset;
  uint64 m;
  pax_enable_debug = false;
  filter1.Create(200, 1024 * 1024, 0);
  for (int i = 0; i < 100; i++) {
    filter1.Add((unsigned char *)&i, sizeof(int));
  }

  std::tie(bitset, m) = filter1.GetBitSet();
  filter2.Create((const char *)bitset, m, filter1.GetSeed(),
                 filter1.GetKHashFuncs());

  for (int i = 0; i < 100; i++) {
    ASSERT_FALSE(filter2.Test((unsigned char *)(&i), sizeof(int)))
        << "value: " << i << ", should not get true";
    int i100 = (i + 100);
    ASSERT_TRUE(filter2.Test((unsigned char *)(&i100), sizeof(int)))
        << "value: " << i100 << ", should not get false";
  }
}

}  // namespace pax::tests