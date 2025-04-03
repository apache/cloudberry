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
 * pax_compress_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_compress_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_compress.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/columns/pax_encoding_utils.h"

namespace pax::tests {
class PaxCompressTest : public ::testing::TestWithParam<
                            ::testing::tuple<ColumnEncoding_Kind, uint32>> {
  void SetUp() override { CreateMemoryContext(); }
};

TEST_P(PaxCompressTest, TestCompressAndDecompress) {
  ColumnEncoding_Kind type = ::testing::get<0>(GetParam());
  uint32 data_len = ::testing::get<1>(GetParam());
  size_t dst_len = 0;

  char *data = pax::PAX_ALLOC<char>(data_len);
  char *result_data;
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  auto compressor = PaxCompressor::CreateBlockCompressor(type);

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data = pax::PAX_ALLOC<char>(bound_size);
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  pax::PAX_FREE(data);
  pax::PAX_FREE(result_data);
}

INSTANTIATE_TEST_SUITE_P(
    PaxCompressTestCombined, PaxCompressTest,
    testing::Combine(testing::Values(ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB),
                     testing::Values(1, 128, 4096, 1024 * 1024,
                                     64 * 1024 * 1024)));

class PaxCompressTest2 : public ::testing::Test {
  void SetUp() override { CreateMemoryContext(); }
};

TEST_F(PaxCompressTest2, TestPgLZCompress) {
  size_t dst_len = 0;
  PaxCompressor *compressor = new PgLZCompressor();
  // too small may cause compress failed
  uint32 data_len = 512;

  char *data = pax::PAX_ALLOC<char>(data_len);
  char *result_data;
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data = pax::PAX_ALLOC<char>(bound_size);
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  pax::PAX_FREE(data);
  pax::PAX_FREE(result_data);
}

#ifdef USE_LZ4
TEST_F(PaxCompressTest2, TestLZ4Compress) {
  size_t dst_len = 0;
  PaxCompressor *compressor = new PaxLZ4Compressor();
  uint32 data_len = 100;

  char *data = pax::PAX_ALLOC<char>(data_len);
  char *result_data;
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0UL);
  result_data = pax::PAX_ALLOC<char>(bound_size);
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0UL);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0UL);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  pax::PAX_FREE(data);
  pax::PAX_FREE(result_data);
}
#endif

}  // namespace pax::tests
