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
 * zorder_utils_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/zorder_utils_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "clustering/zorder_utils.h"

#include <cmath>
#include <random>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "utils/float.h"

#define NUM_TESTS 10000
namespace pax::tests {

template <class T>
void convert_to_bytes(T value, char *result);
template <>
void convert_to_bytes<float>(float value, char *result) {
  pax::datum_to_bytes(Float4GetDatum(value), FLOAT4OID, false, result);
}
template <>
void convert_to_bytes<int>(int value, char *result) {
  pax::datum_to_bytes(Int32GetDatum(value), INT4OID, false, result);
}

float generateRandomFloat(float min_value, float max_value) {
  std::random_device rd;
  std::mt19937 generator(rd());

  std::uniform_real_distribution<float> distribution(0.0f, 1.0f);

  float random_number = distribution(generator);
  return min_value + (max_value - min_value) * random_number;
}

float randomFloat() {
  float min_value = -65536.0;
  float max_value = 65536.0;
  return generateRandomFloat(min_value, max_value);
}

int compareFloat(float a, float b) {
  if (fabs(a - b) < EPSILON) {
    return 0;
  }
  return a < b ? -1 : 1;
}

class ZOrderTest : public ::testing::Test {
 public:
  template <class T>
  class OriginValueWrappper {
   public:
    int index;
    T originValue;
    OriginValueWrappper(int index, T origin_value) : index(index) {
      originValue = origin_value;
    }

    OriginValueWrappper(const OriginValueWrappper &other) : index(other.index) {
      originValue = other.originValue;
    }
  };

  template <class T>
  class ConvertResultWrappper {
   public:
    int index;
    char result[N_BYTES];
    ConvertResultWrappper(int index, char *result) : index(index) {
      memcpy(this->result, result, N_BYTES);
    }
  };

  template <class T>
  std::vector<OriginValueWrappper<T>> buildOriginValueArrays(
      std::vector<T> values) {
    std::vector<OriginValueWrappper<T>> origin_arrays;
    for (auto it = values.begin(); it != values.end(); it++) {
      OriginValueWrappper<T> originValueWrappper(it - values.begin(), *it);
      origin_arrays.push_back(originValueWrappper);
    }
    return origin_arrays;
  }

  template <class T>
  std::vector<ConvertResultWrappper<T>> buildConvertResultArrays(
      std::vector<OriginValueWrappper<T>> origin_arrays) {
    std::vector<ConvertResultWrappper<T>> convertResultArrays;
    for (auto it = origin_arrays.begin(); it != origin_arrays.end(); it++) {
      char result[N_BYTES] = {0};
      pax::tests::convert_to_bytes(it->originValue, result);
      ConvertResultWrappper<T> convertResultWrappper(it->index, result);
      convertResultArrays.push_back(convertResultWrappper);
    }
    return convertResultArrays;
  }

 public:
  void SetUp() override {
    MemoryContext comm_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "ZOrderTestMemoryContext", 1 * 1024 * 1024,
        1 * 1024 * 1024, 1 * 1024 * 1024);
    MemoryContextSwitchTo(comm_test_memory_context);
  }
};

TEST_F(ZOrderTest, TestFloat4ToBytes2) {
  std::vector<float4> origin_arrays = {std::numeric_limits<float>::lowest(),
                                       -100.0f,
                                       -INFINITY,
                                       -1.0f,
                                       0.0f,
                                       INFINITY,
                                       NAN,
                                       1.9f,
                                       std::numeric_limits<float>::max(),
                                       23.24f,
                                       203.14f};

  std::vector<OriginValueWrappper<float4>> originValueArrays =
      buildOriginValueArrays(origin_arrays);
  std::vector<ConvertResultWrappper<float4>> convertResultArrays =
      buildConvertResultArrays(originValueArrays);

  // sort by origin value from small to large
  std::vector<int> expected_index = {2, 0, 1, 3, 4, 7, 9, 10, 8, 5, 6};

  std::sort(originValueArrays.begin(), originValueArrays.end(),
            [](const OriginValueWrappper<float4> &a,
               const OriginValueWrappper<float4> &b) {
              return float8_lt(a.originValue, b.originValue);
            });

  std::sort(convertResultArrays.begin(), convertResultArrays.end(),
            [](const ConvertResultWrappper<float4> &a,
               const ConvertResultWrappper<float4> &b) {
              return pax::bytes_compare(a.result, b.result, 1) < 0;
            });
  for (size_t i = 0; i < originValueArrays.size(); i++) {
    ASSERT_EQ(originValueArrays[i].index, expected_index[i]);
    ASSERT_EQ(originValueArrays[i].index, convertResultArrays[i].index);
  }
}

TEST_F(ZOrderTest, TestDatumToBytes) {
  Datum datum = 0;
  char result[N_BYTES] = {0};
  pax::datum_to_bytes(datum, INT4OID, false, result);
  ASSERT_EQ(*(uint64 *)result, 0x0000000000000080UL);

  datum = 1;
  pax::datum_to_bytes(datum, INT4OID, false, result);
  ASSERT_EQ(*(uint64 *)result, 0x0100000000000080UL);

  datum = -1;
  pax::datum_to_bytes(datum, INT4OID, false, result);
  ASSERT_EQ(*(uint64 *)result, 0xffffffffffffff7fUL);
}

TEST_F(ZOrderTest, TestInterleaveBits) {
  char buffer[N_BYTES * 2] = {0};
  char result[N_BYTES * 2] = {0};
  Datum datum1 = 0;
  Datum datum2 = -1;

  pax::datum_to_bytes(datum1, INT4OID, false, buffer);
  pax::datum_to_bytes(datum2, INT4OID, false, buffer + N_BYTES);
  pax::interleave_bits(buffer, result, 2);
  ASSERT_EQ(*(uint64 *)result, 0x5555555555555595UL);
  ASSERT_EQ(*(uint64 *)(result + 8), 0x5555555555555555UL);
}

}  // namespace pax::tests