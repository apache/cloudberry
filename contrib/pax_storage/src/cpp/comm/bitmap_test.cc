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
 * bitmap_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/bitmap_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include <gtest/gtest.h>

#include "comm/bitmap.h"

namespace pax::tests {
class BitMapTest : public ::testing::Test {};

TEST_F(BitMapTest, Bitmap8) {
  Bitmap8 bm(20);

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= 128; i++) {
    ASSERT_FALSE(bm.Test(i));  // zeros
    ASSERT_FALSE(bm.Toggle(i));
    ASSERT_TRUE(bm.Test(i));
    ASSERT_TRUE(bm.Toggle(i));
    ASSERT_FALSE(bm.Test(i));

    ASSERT_FALSE(bm.Test(i));  // zeros
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));

    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));
    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
  }
}

TEST_F(BitMapTest, Bitmap8SetN) {
  Bitmap8 bm(10);
  const uint32 nbits = 128;

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    bm.ClearAll();
    for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
    bm.SetN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_TRUE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap8ClearN) {
  Bitmap8 bm(10);
  const uint32 nbits = 128;

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    for (uint32 i = 0; i <= nbits; i++) {
      bm.Set(i);
      ASSERT_TRUE(bm.Test(i));
    }
    bm.ClearN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_FALSE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_TRUE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap64) {
  Bitmap64 bm(100);

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= 128; i++) {
    ASSERT_FALSE(bm.Test(i));  // zeros
    ASSERT_FALSE(bm.Toggle(i));
    ASSERT_TRUE(bm.Test(i));
    ASSERT_TRUE(bm.Toggle(i));
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));

    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));
    bm.Clear(i);
    ASSERT_FALSE(bm.Test(i));

    bm.Set(i);
    ASSERT_TRUE(bm.Test(i));
  }
}
TEST_F(BitMapTest, Bitmap64SetN) {
  Bitmap64 bm(1);
  const uint32 nbits = 512;

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, nbits](uint32 index) {
    bm.ClearAll();
    for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
    bm.SetN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_TRUE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Bitmap64ClearN) {
  Bitmap64 bm(1);
  const uint32 nbits = 512;

  ASSERT_TRUE(bm.Empty());
  for (uint32 i = 0; i <= nbits; i++) ASSERT_FALSE(bm.Test(i));

  auto fn = [&bm, &nbits](uint32 index) {
    for (uint32 i = 0; i <= nbits; i++) {
      bm.Set(i);
      ASSERT_TRUE(bm.Test(i));
    }
    bm.ClearN(index);
    for (uint32 i = 0; i <= index; i++) ASSERT_FALSE(bm.Test(i));
    for (uint32 i = index + 1; i <= nbits; i++) ASSERT_TRUE(bm.Test(i));
  };
  for (uint32 i = 0; i <= nbits; i++) fn(i);
}

TEST_F(BitMapTest, Clone) {
  Bitmap8 bm8(1024);

  for (int i = 0; i < 1024; i++) {
    if (i % 2 == 0) {
      bm8.Set(i);
    }
  }

  ASSERT_EQ(bm8.CountBits(0, 1023), 512UL);

  auto bm8_copy = bm8.Clone();

  ASSERT_EQ(bm8.Raw().size, bm8_copy->Raw().size);

  ASSERT_EQ(bm8.CountBits(0, 1023), 512UL);
  for (int i = 0; i < 1024; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(bm8_copy->Test(i));
    }
  }

  Bitmap64 bm(1024);

  for (int i = 0; i < 1024; i++) {
    if (i % 2 == 0) {
      bm.Set(i);
    }
  }

  ASSERT_EQ(bm.CountBits(0, 1023), 512UL);

  auto bm_copy = bm.Clone();

  ASSERT_EQ(bm.Raw().size, bm_copy->Raw().size);

  ASSERT_EQ(bm.CountBits(0, 1023), 512UL);
  for (int i = 0; i < 1024; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(bm_copy->Test(i));
    }
  }
}

TEST_F(BitMapTest, Union) {
  {
    Bitmap8 bm8_1(1024);
    Bitmap8 bm8_2(1024);

    for (int i = 0; i < 1024; i++) {
      if (i % 2 == 0) {
        bm8_1.Set(i);
      } else {
        bm8_2.Set(i);
      }
    }

    ASSERT_EQ(bm8_1.CountBits(0, 1023), 512UL);
    ASSERT_EQ(bm8_2.CountBits(0, 1023), 512UL);

    auto bm_union = Bitmap8::Union(&bm8_1, &bm8_2);

    ASSERT_EQ(bm_union->CurrentBytes(), bm8_1.CurrentBytes());
    ASSERT_EQ(bm_union->CountBits(0, 1023), 1024UL);

    for (int i = 0; i < 1024; i++) {
      ASSERT_TRUE(bm_union->Test(i));
    }
  }

  {
    Bitmap8 bm8_1(1024);
    Bitmap8 bm8_2(2048);

    for (int i = 0; i < 1024; i++) {
      if (i % 2 == 0) {
        bm8_1.Set(i);
      } else {
        bm8_2.Set(i);
      }
    }

    ASSERT_EQ(bm8_1.CountBits(0, 1023), 512UL);
    ASSERT_EQ(bm8_2.CountBits(0, 2047), 512UL);

    auto bm_union = Bitmap8::Union(&bm8_1, &bm8_2);

    ASSERT_EQ(bm_union->CurrentBytes(), bm8_2.CurrentBytes());
    ASSERT_EQ(bm_union->CountBits(0, 2047), 1024UL);

    for (int i = 0; i < 1024; i++) {
      ASSERT_TRUE(bm_union->Test(i));
    }
  }
}

TEST_F(BitMapTest, CountBits) {
  const uint32 starts[] = {0, 1, 3, 7};
  const uint32 ends[] = {0, 1, 7, 8, 9, 15, 16, 17};
  Bitmap8 bm(11);

  auto fill_bits = [&bm](uint32 bits) {
    uint32 k = 0;
    bm.ClearAll();
    while (bits) {
      if (bits & 1) bm.Set(k);
      bits = bits >> 1;
      k++;
    }
  };
  auto plain_count = [](uint32 bits, uint32 start, uint32 end) {
    size_t nbits = 0;
    for (auto i = start; i <= end; i++) {
      if (bits & (1ULL << i)) nbits++;
    }
    return nbits;
  };

  for (uint32 i = 0; i < 0x3ffff; i++) {
    fill_bits(i);
    for (auto start : starts) {
      for (auto end : ends) {
        if (end < start) continue;
        ASSERT_EQ(bm.CountBits(start, end), plain_count(i, start, end));
        ASSERT_EQ(bm.CountBits(end), plain_count(i, 0, end));
      }
    }
  }
}

}  // namespace pax::tests
