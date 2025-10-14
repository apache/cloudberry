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
 * pax_column_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_column_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_column.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/columns/pax_column_traits.h"

namespace pax::tests {
using namespace pax::traits;
static void AppendInt4All(const std::unique_ptr<PaxColumn> &pax_column,
                          size_t bits) {
  int64 data;
  for (int16 i = INT16_MIN;; ++i) {  // dead loop
    data = i;
    pax_column->Append(reinterpret_cast<char *>(&data), bits / 8);
    if (i == INT16_MAX) {
      break;
    }
  }
}

template <typename T>
static void VerifyInt4All(char *verify_buff, size_t verify_len) {
  ASSERT_NE(verify_buff, nullptr);
  ASSERT_EQ(verify_len, (UINT16_MAX + 1) * sizeof(T));

  auto verify_int64_buff = reinterpret_cast<T *>(verify_buff);

  uint32 index = 0;
  for (int16 i = INT16_MIN;; ++i) {
    ASSERT_EQ(i, verify_int64_buff[index++]);
    if (i == INT16_MAX) {
      break;
    }
  }
}

static void VerifyInt4All(char *verify_buff, size_t verify_len, size_t bits) {
  switch (bits) {
    case 16:
      VerifyInt4All<int16>(verify_buff, verify_len);
      break;
    case 32:
      VerifyInt4All<int32>(verify_buff, verify_len);
      break;
    case 64:
      VerifyInt4All<int64>(verify_buff, verify_len);
      break;
    default:
      ASSERT_TRUE(false);
  }
}

static std::unique_ptr<PaxColumn> CreateEncodeColumn(
    uint8 bits, const PaxEncoder::EncodingOption &encoding_option,
    PaxStorageFormat storage_type = PaxStorageFormat::kTypeStoragePorcNonVec) {
  std::unique_ptr<PaxColumn> int_column;

  switch (bits) {
    case 16:
      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int16>::create_encoding(
                1024, std::move(encoding_option));
      } else {
        int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int16>::create_encoding(
                1024, std::move(encoding_option));
      }
      break;
    case 32:
      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int32>::create_encoding(
                1024, std::move(encoding_option));
      } else {
        int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int32>::create_encoding(
                1024, std::move(encoding_option));
      }
      break;
    case 64:
      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int64>::create_encoding(
                1024, std::move(encoding_option));
      } else {
        int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int64>::create_encoding(
                1024, std::move(encoding_option));
      }
      break;
    default:
      int_column = nullptr;
      break;
  }

  return int_column;
}

static std::unique_ptr<PaxColumn> CreateDecodeColumn(
    uint8 bits, size_t origin_len, size_t origin_rows,
    const PaxDecoder::DecodingOption &decoding_option, char *encoded_buff,
    size_t encoded_len,
    PaxStorageFormat storage_type = PaxStorageFormat::kTypeStoragePorcNonVec,
    size_t column_not_nulls = 0) {
  std::unique_ptr<PaxColumn> column_rc;
  switch (bits) {
    case 16: {
      auto buffer_for_read = std::make_unique<DataBuffer<int16>>(
          reinterpret_cast<int16 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        auto int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int16>::create_decoding(
                origin_len / sizeof(int16), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read));
        column_rc = std::move(int_column);
      } else {
        auto int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int16>::create_decoding(
                origin_len / sizeof(int16), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read), column_not_nulls);
        column_rc = std::move(int_column);
      }
      break;
    }
    case 32: {
      auto buffer_for_read = std::make_unique<DataBuffer<int32>>(
          reinterpret_cast<int32 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        auto int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int32>::create_decoding(
                origin_len / sizeof(int32), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read));
        column_rc = std::move(int_column);
      } else {
        auto int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int32>::create_decoding(
                origin_len / sizeof(int32), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read), column_not_nulls);
        column_rc = std::move(int_column);
      }
      break;
    }
    case 64: {
      auto buffer_for_read = std::make_unique<DataBuffer<int64>>(
          reinterpret_cast<int64 *>(encoded_buff), encoded_len, false, false);
      buffer_for_read->Brush(encoded_len);

      if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
        auto int_column =
            ColumnOptCreateTraits<PaxEncodingColumn, int64>::create_decoding(
                origin_len / sizeof(int64), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read));
        column_rc = std::move(int_column);
      } else {
        auto int_column =
            ColumnOptCreateTraits<PaxVecEncodingColumn, int64>::create_decoding(
                origin_len / sizeof(int64), std::move(decoding_option));
        int_column->Set(std::move(buffer_for_read), column_not_nulls);
        column_rc = std::move(int_column);
      }
      break;
    }
    default: {
      return nullptr;
    }
  }

  if (column_rc) {
    column_rc->SetRows(origin_rows);
  }
  return column_rc;
}

class PaxColumnTest : public ::testing::TestWithParam<PaxStorageFormat> {
 public:
  void SetUp() override { CreateMemoryContext(); }
};

class PaxColumnEncodingTest : public ::testing::TestWithParam<
                                  ::testing::tuple<uint8, PaxStorageFormat>> {
 public:
  void SetUp() override { CreateMemoryContext(); }
};

class PaxColumnCompressTest
    : public ::testing::TestWithParam<
          ::testing::tuple<uint8, ColumnEncoding_Kind>> {
 public:
  void SetUp() override { CreateMemoryContext(); }
};

class PaxNonFixedColumnCompressTest
    : public ::testing::TestWithParam<
          ::testing::tuple<uint8, ColumnEncoding_Kind, bool, bool>> {
 public:
  void SetUp() override { CreateMemoryContext(); }
};

TEST_P(PaxColumnTest, FixColumnGetRangeBufferTest) {
  std::unique_ptr<PaxColumn> column;
  auto storage_type = GetParam();
  char *buffer = nullptr;
  size_t buffer_len = 0;

  if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
    column = ColumnCreateTraits<PaxCommColumn, int32>::create(200);
  } else {
    column = ColumnCreateTraits<PaxVecCommColumn, int32>::create(200);
  }

  for (int32 i = 0; i < 16; i++) {
    column->Append(reinterpret_cast<char *>(&i), sizeof(int32));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int32));

  for (size_t i = 5; i < 16; i++) {
    auto *i_32 = reinterpret_cast<int32 *>(buffer + ((i - 5) * sizeof(int32)));
    ASSERT_EQ(*i_32, (int32)i);
  }
  ASSERT_EQ(column->GetRows(), 16UL);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16UL);

  if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
    column = ColumnCreateTraits<PaxCommColumn, int32>::create(200);
  } else {
    column = ColumnCreateTraits<PaxVecCommColumn, int32>::create(200);
  }

  for (int32 i = 0; i < 16; i++) {
    if (i % 3 == 0) {
      column->AppendNull();
    }
    column->Append(reinterpret_cast<char *>(&i), sizeof(int32));
  }

  switch (storage_type) {
    case kTypeStoragePorcNonVec: {
      std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
      ASSERT_EQ(buffer_len, 10 * sizeof(int32));

      for (size_t i = 5; i < 16; i++) {
        auto *i_32 =
            reinterpret_cast<int32 *>(buffer + ((i - 5) * sizeof(int32)));
        ASSERT_EQ(*i_32, (int32)i);
      }
      break;
    }
    case kTypeStoragePorcVec: {
      std::tie(buffer, buffer_len) = column->GetRangeBuffer(0, 10);
      ASSERT_EQ(buffer_len, 10 * sizeof(int32));

      size_t nulls_count = 0;
      for (size_t i = 0; i < 10; i++) {
        auto *i_32 = reinterpret_cast<int32 *>(buffer + (i * sizeof(int32)));
        if (i % 4 == 0) {
          nulls_count++;
          ASSERT_EQ(*i_32, 0);
        } else {
          ASSERT_EQ(*i_32, static_cast<int32>((int32)i - nulls_count));
        }
      }

      break;
    }
    default:
      break;
  }

  ASSERT_EQ(column->GetRows(), static_cast<size_t>(16 + 6));
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16UL);
}

TEST_P(PaxColumnTest, NonFixColumnGetRangeBufferTest) {
  std::unique_ptr<PaxColumn> column;
  auto storage_type = GetParam();
  char *buffer = nullptr;
  size_t buffer_len = 0;

  if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
    column = ColumnCreateTraits2<PaxNonFixedColumn>::create(200, 200);
  } else {
    column = ColumnCreateTraits2<PaxVecNonFixedColumn>::create(200, 200);
  }

  for (int64 i = 0; i < 16; i++) {
    column->Append(reinterpret_cast<char *>(&i), sizeof(int64));
  }

  std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
  ASSERT_EQ(buffer_len, 10 * sizeof(int64));

  for (size_t i = 5; i < 16; i++) {
    auto *i_64 = reinterpret_cast<int64 *>(buffer + ((i - 5) * sizeof(int64)));
    ASSERT_EQ(*i_64, (int64)i);
  }
  ASSERT_EQ(column->GetRows(), 16UL);
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16UL);

  std::tie(buffer, buffer_len) =
      column->GetRangeBuffer(0, column->GetNonNullRows());
  ASSERT_EQ(buffer_len, column->GetNonNullRows() * sizeof(int64));

  for (int64 start = 0; start < 16; start++) {
    for (int64 len = 1; len <= 16 - start; len++) {
      std::tie(buffer, buffer_len) = column->GetRangeBuffer(start, len);
      ASSERT_EQ(buffer_len, len * sizeof(int64));
    }
  }

  for (int64 i = 0; i < 16; i++) {
    std::tie(buffer, buffer_len) = column->GetBuffer(i);
    ASSERT_EQ(i, *reinterpret_cast<uint64 *>(buffer));
  }

  if (storage_type == PaxStorageFormat::kTypeStoragePorcNonVec) {
    column = ColumnCreateTraits2<PaxNonFixedColumn>::create(200, 200);
  } else {
    column = ColumnCreateTraits2<PaxVecNonFixedColumn>::create(200, 200);
  }

  for (int64 i = 0; i < 16; i++) {
    if (i % 3 == 0) {
      column->AppendNull();
    }
    column->Append(reinterpret_cast<char *>(&i), sizeof(int64));
  }

  switch (storage_type) {
    case kTypeStoragePorcNonVec: {
      std::tie(buffer, buffer_len) = column->GetRangeBuffer(5, 10);
      ASSERT_EQ(buffer_len, 10 * sizeof(int64));

      for (size_t i = 5; i < 16; i++) {
        auto *i_64 =
            reinterpret_cast<int64 *>(buffer + ((i - 5) * sizeof(int64)));
        ASSERT_EQ(*i_64, (int64)i);
      }
      break;
    }
    case kTypeStoragePorcVec: {
      size_t nulls_count = 0;
      for (size_t i = 0; i < 10; i++) {
        std::tie(buffer, buffer_len) = column->GetBuffer(i);
        if (buffer) {
          ASSERT_EQ(i - nulls_count, *reinterpret_cast<uint64 *>(buffer));
        } else {
          nulls_count++;
        }
      }

      std::tie(buffer, buffer_len) = column->GetRangeBuffer(0, 10);

      // 0 4 8 is null
      ASSERT_EQ(buffer_len, 7 * sizeof(int64));

      nulls_count = 0;
      for (size_t i = 0; i < 10; i++) {
        auto *i_64 = reinterpret_cast<int64 *>(
            buffer + ((i - nulls_count) * sizeof(int64)));
        if (i % 4 == 0) {
          nulls_count++;
        } else {
          ASSERT_EQ(static_cast<size_t>(*i_64), i - nulls_count);
        }
      }

      break;
    }
    default:
      break;
  }

  ASSERT_EQ(column->GetRows(), static_cast<size_t>(16 + 6));
  ASSERT_EQ(column->GetRangeNonNullRows(0, column->GetRows()), 16UL);
}

TEST_P(PaxColumnEncodingTest, GetRangeEncodingColumnTest) {
  std::unique_ptr<PaxColumn> int_column;
  auto bits = ::testing::get<0>(GetParam());
  auto storage_type = ::testing::get<1>(GetParam());
  if (bits < 32) {
    return;
  }

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED;
  encoding_option.is_sign = true;

  int_column =
      CreateEncodeColumn(bits, std::move(encoding_option), storage_type);
  ASSERT_TRUE(int_column);

  int64 data;
  for (int16 i = 0; i < 100; ++i) {
    data = i;
    int_column->Append(reinterpret_cast<char *>(&data), bits / 8);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, static_cast<size_t>(UINT16_MAX));

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(static_cast<size_t>(origin_len),
            static_cast<size_t>(origin_rows * (bits / 8)));

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  decoding_option.is_sign = false;

  auto int_column_for_read = CreateDecodeColumn(
      bits, 100 * (bits / 8), origin_rows, std::move(decoding_option),
      encoded_buff, encoded_len, storage_type, 100);

  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 0);
  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) =
      int_column_for_read->GetRangeBuffer(30, 20);

  for (int16 i = 0; i < 20; ++i) {
    switch (bits) {
      case 32:
        ASSERT_EQ((reinterpret_cast<int32 *>(verify_buff))[i], 30 + i);
        break;
      case 64:
        ASSERT_EQ((reinterpret_cast<int64 *>(verify_buff))[i], 30 + i);
        break;
    }
  }
}

TEST_P(PaxColumnCompressTest, FixedCompressColumnGetRangeTest) {
  auto bits = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_level = 5;
  encoding_option.is_sign = true;

  auto int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  int64 data;
  for (int16 i = 0; i < 100; ++i) {
    data = i;
    int_column->Append(reinterpret_cast<char *>(&data), bits / 8);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, static_cast<size_t>(UINT16_MAX));

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(origin_len, 100 * (bits / 8));

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;
  decoding_option.compress_level = 5;

  auto int_column_for_read =
      CreateDecodeColumn(bits, 100 * (bits / 8), origin_rows,
                         std::move(decoding_option), encoded_buff, encoded_len);

  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 5);
  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) =
      int_column_for_read->GetRangeBuffer(30, 20);

  for (int16 i = 0; i < 20; ++i) {
    switch (bits) {
      case 32:
        ASSERT_EQ((reinterpret_cast<int32 *>(verify_buff))[i], 30 + i);
        break;
      case 64:
        ASSERT_EQ((reinterpret_cast<int64 *>(verify_buff))[i], 30 + i);
        break;
    }
  }
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnDefault) {
  auto bits = ::testing::get<0>(GetParam());
  auto storage_type = ::testing::get<1>(GetParam());
  if (bits < 32) {
    return;
  }

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED;
  encoding_option.is_sign = true;

  auto int_column =
      CreateEncodeColumn(bits, std::move(encoding_option), storage_type);
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_GT(encoded_len, static_cast<size_t>(UINT16_MAX));

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(static_cast<size_t>(origin_len), origin_rows * (bits / 8));

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  decoding_option.is_sign = false;

  auto int_column_for_read = CreateDecodeColumn(
      bits, (UINT16_MAX + 1) * (bits / 8), origin_rows,
      std::move(decoding_option), encoded_buff, encoded_len, storage_type);

  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 0);
  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnSpecType) {
  auto bits = ::testing::get<0>(GetParam());
  auto storage_type = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoding_option.is_sign = true;

  auto int_column =
      CreateEncodeColumn(bits, std::move(encoding_option), storage_type);
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);
  ASSERT_LT(encoded_len, static_cast<size_t>(UINT16_MAX));

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(origin_len, (UINT16_MAX + 1) * bits / 8);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, origin_len, origin_rows, std::move(decoding_option), encoded_buff,
      encoded_len, storage_type);
  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 0);

  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);
}

TEST_P(PaxColumnEncodingTest, PaxEncodingColumnNoEncoding) {
  auto bits = ::testing::get<0>(GetParam());
  auto storage_type = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  encoding_option.is_sign = true;

  auto int_column =
      CreateEncodeColumn(bits, std::move(encoding_option), storage_type);
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(static_cast<size_t>(origin_len), origin_rows * (bits / 8));

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
  decoding_option.is_sign = true;

  auto int_column_for_read = CreateDecodeColumn(
      bits, encoded_len, origin_rows, std::move(decoding_option), encoded_buff,
      encoded_len, storage_type);
  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 0);
  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);
}

TEST_P(PaxColumnCompressTest, PaxEncodingColumnCompressDecompress) {
  auto bits = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_level = 5;
  encoding_option.is_sign = true;

  auto int_column = CreateEncodeColumn(bits, std::move(encoding_option));
  ASSERT_TRUE(int_column);

  AppendInt4All(int_column, bits);

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = int_column->GetBuffer();
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = int_column->GetOriginLength();
  auto origin_rows = int_column->GetRows();
  ASSERT_EQ(origin_len, (UINT16_MAX + 1) * (bits / 8));

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;
  decoding_option.compress_level = 5;

  auto int_column_for_read =
      CreateDecodeColumn(bits, (UINT16_MAX + 1) * bits / 8, origin_rows,
                         std::move(decoding_option), encoded_buff, encoded_len);

  ASSERT_EQ(int_column_for_read->GetCompressLevel(), 5);
  char *verify_buff;
  size_t verify_len;
  std::tie(verify_buff, verify_len) = int_column_for_read->GetBuffer();
  VerifyInt4All(verify_buff, verify_len, bits);
}

TEST_P(PaxNonFixedColumnCompressTest,
       PaxEncodingNonFixedColumnCompressDecompress) {
  PaxNonFixedColumn *non_fixed_column;
  auto number = ::testing::get<0>(GetParam());
  auto kind = ::testing::get<1>(GetParam());
  auto verify_range = ::testing::get<2>(GetParam());
  const size_t number_of_rows = 1024;

  PaxEncoder::EncodingOption encoding_option;
  encoding_option.column_encode_type = kind;
  encoding_option.compress_level = 5;
  encoding_option.is_sign = true;

  encoding_option.offsets_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
  encoding_option.offsets_compress_level = 5;

  non_fixed_column = new PaxNonFixedEncodingColumn(
      number_of_rows, number_of_rows, std::move(encoding_option));

  std::srand(static_cast<unsigned int>(std::time(0)));
  char *data = new char[number_of_rows * number];

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);

  for (size_t i = 0; i < number_of_rows; ++i) {
    for (size_t j = 0; j < number; ++j) {
      data[j + i * number] = static_cast<char>(dis(gen));
    }
    non_fixed_column->Append((data + i * number), number);
  }

  char *encoded_buff;
  size_t encoded_len;
  std::tie(encoded_buff, encoded_len) = non_fixed_column->GetBuffer();
  char *offset_stream_buff;
  size_t offset_stream_len;
  std::tie(offset_stream_buff, offset_stream_len) =
      non_fixed_column->GetOffsetBuffer(false);
  ASSERT_NE(encoded_buff, nullptr);

  auto origin_len = non_fixed_column->GetOriginLength();
  ASSERT_EQ(static_cast<size_t>(origin_len), number_of_rows * number);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = kind;
  decoding_option.is_sign = true;
  decoding_option.compress_level = 5;

  decoding_option.offsets_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
  decoding_option.offsets_compress_level = 5;

  auto non_fixed_column_for_read = new PaxNonFixedEncodingColumn(
      number_of_rows * number, sizeof(int32) * number_of_rows,
      std::move(decoding_option));
  auto data_buffer_for_read = std::make_unique<DataBuffer<char>>(
      encoded_buff, encoded_len, false, false);
  data_buffer_for_read->Brush(encoded_len);
  auto length_buffer_cpy = std::make_unique<DataBuffer<int32>>(
      (int32 *)offset_stream_buff, offset_stream_len, false, false);
  length_buffer_cpy->BrushAll();
  non_fixed_column_for_read->Set(std::move(data_buffer_for_read),
                                 std::move(length_buffer_cpy), origin_len);
  ASSERT_EQ(non_fixed_column_for_read->GetCompressLevel(), 5);
  char *verify_buff;
  size_t verify_len;

  if (verify_range) {
    std::tie(verify_buff, verify_len) =
        non_fixed_column_for_read->GetRangeBuffer(30, 50);
    ASSERT_EQ(verify_len, static_cast<size_t>(number * (50)));

    for (size_t i = 0; i < verify_len; ++i) {
      EXPECT_EQ(verify_buff[i], data[i + (30 * number)]);
    }
  } else {
    std::tie(verify_buff, verify_len) = non_fixed_column_for_read->GetBuffer();
    ASSERT_EQ(verify_len, number_of_rows * number);

    for (size_t i = 0; i < number_of_rows * number; ++i) {
      EXPECT_EQ(verify_buff[i], data[i]);
    }
  }

  delete[] data;
  delete non_fixed_column;
  delete non_fixed_column_for_read;
}

INSTANTIATE_TEST_SUITE_P(
    PaxColumnTestCombine, PaxColumnTest,
    testing::Values(PaxStorageFormat::kTypeStoragePorcNonVec,
                    PaxStorageFormat::kTypeStoragePorcVec));

INSTANTIATE_TEST_SUITE_P(
    PaxColumnEncodingTestCombine, PaxColumnEncodingTest,
    testing::Combine(testing::Values(16, 32, 64),
                     testing::Values(PaxStorageFormat::kTypeStoragePorcNonVec,
                                     PaxStorageFormat::kTypeStoragePorcVec)));

INSTANTIATE_TEST_SUITE_P(
    PaxColumnEncodingTestCombine, PaxColumnCompressTest,
    testing::Combine(testing::Values(16, 32, 64),
                     testing::Values(ColumnEncoding_Kind_NO_ENCODED,
#ifdef USE_LZ4
                                     ColumnEncoding_Kind_COMPRESS_LZ4,
#endif
                                     ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB)));

INSTANTIATE_TEST_SUITE_P(
    PaxColumnEncodingTestCombine, PaxNonFixedColumnCompressTest,
    testing::Combine(testing::Values(16, 32, 64),
                     testing::Values(ColumnEncoding_Kind_NO_ENCODED,
#ifdef USE_LZ4
                                     ColumnEncoding_Kind_COMPRESS_LZ4,
#endif
                                     ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB),
                     testing::Values(true, false),
                     testing::Values(true, false)));

};  // namespace pax::tests
