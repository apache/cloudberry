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
 * pax_encoding_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding.h"

#include <cmath>
#include <random>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "pax_gtest_helper.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/columns/pax_rlev2_decoding.h"
#include "storage/columns/pax_rlev2_encoding.h"
namespace pax::tests {

std::shared_ptr<PaxDecoder> GetDecoderByBits(
    uint8 data_bits, std::shared_ptr<DataBuffer<char>> shared_data,
    const PaxDecoder::DecodingOption &decoder_options) {
  std::shared_ptr<PaxDecoder> decoder;
  switch (data_bits) {
    case 8:
      decoder = PaxDecoder::CreateDecoder<int8>(decoder_options);
      break;
    case 16:
      decoder = PaxDecoder::CreateDecoder<int16>(decoder_options);
      break;
    case 32:
      decoder = PaxDecoder::CreateDecoder<int32>(decoder_options);
      break;
    case 64:
      decoder = PaxDecoder::CreateDecoder<int64>(decoder_options);
      break;
    default:
      decoder = nullptr;
      break;
  }

  if (decoder)
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  return decoder;
}

class PaxEncodingTest : public ::testing::Test {
  void SetUp() override { CreateMemoryContext(); }
};

class PaxEncodingRangeTest
    : public ::testing::TestWithParam<::testing::tuple<uint64, bool>> {
  void SetUp() override { CreateMemoryContext(); }
};

class PaxEncodingRangeWithBitsTest
    : public ::testing::TestWithParam<::testing::tuple<uint64, bool, uint8>> {
 public:
  void SetUp() override { CreateMemoryContext(); }
};

class PaxEncodingShortRepeatRangeTest : public PaxEncodingRangeWithBitsTest {};
class PaxEncodingDeltaRangeTest : public PaxEncodingRangeWithBitsTest {};
class PaxEncodingWriteReadLongsRangeTest : public PaxEncodingRangeTest {};

class PaxEncodingDeltaIncDecRangeTest
    : public ::testing::TestWithParam<::testing::tuple<uint64, uint64, bool>> {
  void SetUp() override { CreateMemoryContext(); }
};

class PaxEncodingDirectRangeTest : public PaxEncodingDeltaIncDecRangeTest {};

class PaxEncodingRawDataTest
    : public testing::TestWithParam<
          ::testing::tuple<std::vector<int64>, uint8>> {
  void SetUp() override { CreateMemoryContext(); }
};

class PaxEncodingPBTest : public PaxEncodingRawDataTest {};

TEST_F(PaxEncodingTest, TestPaxUntreatedBuffer) {
  auto *data_buffer = new UntreatedDataBuffer<char>(1024);

  for (size_t i = 0; i < 100; i++) {
    data_buffer->Write(i);
    data_buffer->Brush(1);
    data_buffer->BrushUnTreated(1);
  }
  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  for (size_t i = 0; i < 100; i++) {
    data_buffer->Write(2);
    data_buffer->Brush(1);
  }

  EXPECT_EQ(data_buffer->Used(), 200UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);
  EXPECT_EQ(data_buffer->UnTouched(), 100UL);

  data_buffer->TreatedAll();

  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 0UL);
  EXPECT_EQ(data_buffer->UnTouched(), 100UL);

  for (size_t i = 0; i < 100; i++) {
    EXPECT_EQ((*data_buffer)[i], (char)2);
  }

  data_buffer->BrushUnTreatedAll();
  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->TreatedAll();
  EXPECT_EQ(data_buffer->Used(), 0UL);
  EXPECT_EQ(data_buffer->UnTreated(), 0UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->BrushUnTreatedAll();
  EXPECT_EQ(data_buffer->Used(), 0UL);
  EXPECT_EQ(data_buffer->UnTreated(), 0UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->Brush(100);
  data_buffer->BrushUnTreated(100);

  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->ReSize(2048);
  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->Brush(2048 - 100);
  data_buffer->BrushUnTreated(2048 - 100);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->ReSize(2148);
  EXPECT_EQ(data_buffer->Used(), 2048UL);
  EXPECT_EQ(data_buffer->UnTreated(), 2048UL);
  EXPECT_EQ(data_buffer->UnTouched(), 0UL);

  data_buffer->Brush(100);
  EXPECT_EQ(data_buffer->Used(), 2148UL);
  EXPECT_EQ(data_buffer->UnTreated(), 2048UL);
  EXPECT_EQ(data_buffer->UnTouched(), 100UL);

  data_buffer->ReSize(2248);
  EXPECT_EQ(data_buffer->Used(), 2148UL);
  EXPECT_EQ(data_buffer->UnTreated(), 2048UL);
  EXPECT_EQ(data_buffer->UnTouched(), 100UL);

  delete data_buffer;
}

TEST_F(PaxEncodingTest, TestPaxTreatedBuffer) {
  char data[100];
  auto *data_buffer = new TreatedDataBuffer<char>(data, 100);

  for (size_t i = 0; i < 100; i++) {
    data[i] = i;
  }

  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->Treated(), 0UL);
  EXPECT_EQ(data_buffer->UnTreated(), 100UL);

  data_buffer->BrushTreated(100);
  EXPECT_EQ(data_buffer->Used(), 100UL);
  EXPECT_EQ(data_buffer->Treated(), 100UL);
  EXPECT_EQ(data_buffer->UnTreated(), 0UL);

  delete data_buffer;
}

TEST_P(PaxEncodingShortRepeatRangeTest, TestOrcShortRepeatEncoding) {
  int64 *data;
  auto shared_data = std::make_shared<DataBuffer<char>>(1024);
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);
  auto sr_len = ::testing::get<0>(GetParam());
  auto sign = ::testing::get<1>(GetParam());
  auto data_bits = ::testing::get<2>(GetParam());

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = new int64[1];
  *data = sign ? -2 : 2;
  for (size_t i = 0; i < sr_len; i++) {
    encoder->Append((char *)data, sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_EQ(encoder->GetBufferSize(), 2UL);

  auto encoding_buff = encoder->GetBuffer();
  // type(2 bytes): 0
  // type len(3 bytes)
  // len(3 bytes)
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kShortRepeat);
  EXPECT_EQ(static_cast<size_t>(encoding_buff[0] & 0x07),
            sr_len - ORC_MIN_REPEAT);
  EXPECT_EQ(((encoding_buff[0] >> 3) & 0x07) + 1, 1);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;

  auto decoder =
      GetDecoderByBits(data_bits, shared_data, std::move(decoder_options));
  EXPECT_TRUE(decoder);

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), sr_len * data_bits / 8);

  switch (data_bits) {
    case 8: {
      auto result_dst_data = new DataBuffer<int8>(
          reinterpret_cast<int8 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < sr_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 16: {
      auto result_dst_data = new DataBuffer<int16>(
          reinterpret_cast<int16 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < sr_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 32: {
      auto result_dst_data = new DataBuffer<int32>(
          reinterpret_cast<int32 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < sr_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 64: {
      auto result_dst_data = new DataBuffer<int64>(
          reinterpret_cast<int64 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < sr_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    default:
      break;
  }

  delete[] data;
}

INSTANTIATE_TEST_SUITE_P(PaxEncodingRangeTestCombine,
                         PaxEncodingShortRepeatRangeTest,
                         testing::Combine(testing::Values(3, 4, 5, 6, 7, 8, 9,
                                                          10),
                                          testing::Values(true, false),
                                          testing::Values(8, 16, 32, 64)));

TEST_P(PaxEncodingDeltaRangeTest, TestOrcDeltaEncoding) {
  int64 *data;
  auto delta_len = ::testing::get<0>(GetParam());
  auto sign = ::testing::get<1>(GetParam());
  auto data_bits = ::testing::get<2>(GetParam());

  auto shared_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = pax::PAX_ALLOC<int64>(sizeof(int64));
  *data = sign ? -10 : 1;
  for (size_t i = 0; i < delta_len; i++) {
    encoder->Append((char *)data, sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_EQ(encoder->GetBufferSize(), 4UL);

  // type(2 bytes): 0
  // type len(5 bytes)
  // len(9 bytes)
  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);
  EXPECT_EQ((encoding_buff[0] >> 1) & 0x1f, 0);
  EXPECT_EQ(((encoding_buff[0] & 0x01) << 8) | (unsigned char)encoding_buff[1],
            static_cast<int>(delta_len - 1));

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;

  auto decoder =
      GetDecoderByBits(data_bits, shared_data, std::move(decoder_options));
  EXPECT_TRUE(decoder);

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), delta_len * data_bits / 8);

  switch (data_bits) {
    case 8: {
      auto result_dst_data = new DataBuffer<int8>(
          reinterpret_cast<int8 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < delta_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 16: {
      auto result_dst_data = new DataBuffer<int16>(
          reinterpret_cast<int16 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < delta_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 32: {
      auto result_dst_data = new DataBuffer<int32>(
          reinterpret_cast<int32 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < delta_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    case 64: {
      auto result_dst_data = new DataBuffer<int64>(
          reinterpret_cast<int64 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < delta_len; i++) {
        EXPECT_EQ((*result_dst_data)[i], *data);
      }
      delete result_dst_data;
      break;
    }
    default:
      break;
  }

  pax::PAX_FREE(data);
}

INSTANTIATE_TEST_SUITE_P(PaxEncodingRangeTestCombine, PaxEncodingDeltaRangeTest,
                         testing::Combine(testing::Values(11, 100, 256, 345,
                                                          511, 512),
                                          testing::Values(true, false),
                                          testing::Values(16, 32, 64)));

TEST_P(PaxEncodingDeltaIncDecRangeTest, TestOrcIncDeltaEncoding) {
  int64 *data;
  auto delta_len = ::testing::get<0>(GetParam());
  auto delta_inc = ::testing::get<1>(GetParam());
  auto sign = ::testing::get<2>(GetParam());
  auto shared_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = pax::PAX_ALLOC<int64>(delta_len * sizeof(int64));
  for (size_t i = 0; i < delta_len; i++) {
    data[i] = i * delta_inc;
  }

  for (size_t i = 0; i < delta_len; i++) {
    if (sign) {
      data[i] = -data[i];
    }
    encoder->Append((char *)&(data[i]), sizeof(int64));
  }

  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  // eq 4 or 5 depends on lens of max value
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);
  EXPECT_EQ((encoding_buff[0] >> 1) & 0x1f, 0);
  EXPECT_EQ(((encoding_buff[0] & 0x01) << 8) | (unsigned char)encoding_buff[1],
            static_cast<int>(delta_len - 1));

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;
  auto decoder =
      PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), delta_len * sizeof(int64));

  auto result_dst_data =
      std::make_shared<DataBuffer<int64>>(reinterpret_cast<int64 *>(shared_dst_data->Start()),
                            shared_dst_data->Used(), false, false);

  for (size_t i = 0; i < delta_len; i++) {
    EXPECT_EQ((*result_dst_data)[i], data[i]);
  }

  pax::PAX_FREE(data);
}

TEST_P(PaxEncodingDeltaIncDecRangeTest, TestOrcIncWithoutFixedDeltaEncoding) {
  int64 *data;
  auto delta_len = ::testing::get<0>(GetParam());
  auto delta_inc = ::testing::get<1>(GetParam());
  auto sign = ::testing::get<2>(GetParam());
  auto shared_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = new int64[delta_len];
  for (size_t i = 0; i < delta_len; i++) {
    data[i] = i * delta_inc;
    if (i < delta_inc && i % 2 == 0) {
      data[i] -= i;
    }
  }

  for (size_t i = 0; i < delta_len; i++) {
    if (sign) {
      data[i] = -data[i];
    }
    encoder->Append((char *)&(data[i]), sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  // eq 4 or 5 depends on lens of max value
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;
  auto decoder =
      PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), delta_len * sizeof(int64));

  auto result_dst_data =
      std::make_shared<DataBuffer<int64>>(reinterpret_cast<int64 *>(shared_dst_data->Start()),
                            shared_dst_data->Used(), false, false);

  for (size_t i = 0; i < delta_len; i++) {
    EXPECT_EQ((*result_dst_data)[i], data[i]);
  }

  delete[] data;
}

TEST_P(PaxEncodingDeltaIncDecRangeTest, TestOrcDecDeltaEncoding) {
  int64 *data;
  auto delta_len = ::testing::get<0>(GetParam());
  auto delta_dec = ::testing::get<1>(GetParam());
  auto sign = ::testing::get<2>(GetParam());
  auto shared_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);
  data = new int64[delta_len];

  size_t j = 0;
  for (int64 i = (static_cast<int64>(delta_len - 1) * delta_dec); i >= 0;
       i -= delta_dec) {
    data[j] = i;
    j++;
  }

  for (size_t i = 0; i < delta_len; i++) {
    if (sign) {
      data[i] = -data[i];
    }
    encoder->Append((char *)&(data[i]), sizeof(int64));
  }

  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_GT(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);
  EXPECT_EQ((encoding_buff[0] >> 1) & 0x1f, 0);
  EXPECT_EQ(((encoding_buff[0] & 0x01) << 8) | (unsigned char)encoding_buff[1],
            static_cast<int>(delta_len - 1));

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;
  auto decoder =
      PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), delta_len * sizeof(int64));

  auto result_dst_data =
      std::make_shared<DataBuffer<int64>>(reinterpret_cast<int64 *>(shared_dst_data->Start()),
                            shared_dst_data->Used(), false, false);

  for (size_t i = 0; i < delta_len; i++) {
    EXPECT_EQ((*result_dst_data)[i], data[i]);
  }

  delete[] data;
}

TEST_P(PaxEncodingDeltaIncDecRangeTest, TestOrcDecWithoutFixedDeltaEncoding) {
  int64 *data;
  auto delta_len = ::testing::get<0>(GetParam());
  auto delta_dec = ::testing::get<1>(GetParam());
  auto sign = ::testing::get<2>(GetParam());
  auto shared_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(delta_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);
  data = new int64[delta_len];

  size_t j = 0;
  for (int64 i = (static_cast<int64>(delta_len - 1) * delta_dec); i >= 0;
       i -= delta_dec) {
    data[j] = i;
    if (j < delta_dec && j % 2 == 0) {
      data[j] += j;
    }
    j++;
  }

  for (size_t i = 0; i < delta_len; i++) {
    if (sign) {
      data[i] = -data[i];
    }
    encoder->Append((char *)&(data[i]), sizeof(int64));
  }

  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;
  auto decoder =
      PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), delta_len * sizeof(int64));

  auto result_dst_data =
      std::make_shared<DataBuffer<int64>>(reinterpret_cast<int64 *>(shared_dst_data->Start()),
                            shared_dst_data->Used(), false, false);

  for (size_t i = 0; i < delta_len; i++) {
    EXPECT_EQ((*result_dst_data)[i], data[i]);
  }

  delete[] data;
}

INSTANTIATE_TEST_SUITE_P(PaxEncodingRangeTestCombine,
                         PaxEncodingDeltaIncDecRangeTest,
                         testing::Combine(testing::Values(11, 100, 256, 345,
                                                          511, 512),
                                          testing::Values(1, 7, 99, 4294967295,
                                                          18014398509481984ULL),
                                          testing::Values(true, false)));

TEST_P(PaxEncodingWriteReadLongsRangeTest, TestOrcDirectWriteReadLong) {
  auto write_max = ::testing::get<0>(GetParam());
  auto sign = ::testing::get<1>(GetParam());
  write_max--;

  EXPECT_FALSE(sign);

  DataBuffer<char> *write_dst_buffer;
  TreatedDataBuffer<int64> *read_src_buffer;
  int64 data[3];
  int64 result[3];

  write_dst_buffer = new DataBuffer<char>(1024);
  read_src_buffer = new TreatedDataBuffer<int64>(
      reinterpret_cast<int64 *>(write_dst_buffer->GetBuffer()), 1024);

  data[0] = 0;
  std::random_device rd;
  std::mt19937_64 eng(rd());
  std::uniform_int_distribution<uint64> distr;
  data[1] = distr(eng) % write_max;
  data[1] = sign ? -data[1] : data[1];
  data[2] = sign ? -write_max : write_max;

  auto bits = write_max == 1 ? 1 : static_cast<int>(log2(write_max)) + 1;
  auto bits_align = GetClosestAlignedBits(bits);

  WriteLongs(write_dst_buffer, data, 0, 3, bits_align);
  read_src_buffer->Brush(write_dst_buffer->Used());

  uint32 bits_left = 0;
  ReadLongs<int64>(read_src_buffer, result, 0, 3, bits_align, &bits_left);

  ASSERT_EQ(result[0], data[0]);
  ASSERT_EQ(result[1], data[1]);
  ASSERT_EQ(result[2], data[2]);
}

// Do not change to foreach(2ULL ^ n)
// Then it will
INSTANTIATE_TEST_SUITE_P(
    PaxEncodingRangeTestCombine, PaxEncodingWriteReadLongsRangeTest,
    testing::Combine(
        testing::Values(
            pow(2ULL, 1), pow(2ULL, 2), pow(2ULL, 3), pow(2ULL, 4),
            pow(2ULL, 5), pow(2ULL, 6), pow(2ULL, 7), pow(2ULL, 8),
            pow(2ULL, 9), pow(2ULL, 10), pow(2ULL, 11), pow(2ULL, 12),
            pow(2ULL, 13), pow(2ULL, 14), pow(2ULL, 15), pow(2ULL, 16),
            pow(2ULL, 17), pow(2ULL, 18), pow(2ULL, 19), pow(2ULL, 20),
            pow(2ULL, 21), pow(2ULL, 22), pow(2ULL, 23), pow(2ULL, 24),
            pow(2ULL, 25), pow(2ULL, 26), pow(2ULL, 27), pow(2ULL, 28),
            pow(2ULL, 29), pow(2ULL, 30), pow(2ULL, 31), pow(2ULL, 32),
            pow(2ULL, 33), pow(2ULL, 34), pow(2ULL, 35), pow(2ULL, 36),
            pow(2ULL, 37), pow(2ULL, 38), pow(2ULL, 39), pow(2ULL, 40),
            pow(2ULL, 41), pow(2ULL, 42), pow(2ULL, 43), pow(2ULL, 44),
            pow(2ULL, 45), pow(2ULL, 46), pow(2ULL, 47), pow(2ULL, 48),
            pow(2ULL, 49), pow(2ULL, 50), pow(2ULL, 51), pow(2ULL, 52),
            pow(2ULL, 53), pow(2ULL, 54), pow(2ULL, 55), pow(2ULL, 56),
            pow(2ULL, 57), pow(2ULL, 58), pow(2ULL, 59), pow(2ULL, 60),
            pow(2ULL, 61), pow(2ULL, 62), pow(2ULL, 63)),
        testing::Values(false)));

TEST_P(PaxEncodingDirectRangeTest, TestOrcDirectEncoding) {
  int64 *data;
  auto direct_len = ::testing::get<0>(GetParam());
  auto direct_range = ::testing::get<1>(GetParam());
  auto sign = ::testing::get<2>(GetParam());
  // will auto expanded
  auto shared_data = std::make_shared<DataBuffer<char>>(direct_len * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(direct_len * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = sign;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);
  encoder->SetDataBuffer(shared_data);

  data = new int64[direct_len];
  for (size_t i = 0; i < direct_len; i++) {
    data[i] = i * direct_range;
  }

  for (size_t i = 1; i < direct_len; i += 2) {
    int64 temp = data[i];
    data[i] = data[i - 1];
    data[i - 1] = temp;
  }

  for (size_t i = 0; i < direct_len; i++) {
    if (sign) {
      data[i] = -data[i];
    }

    encoder->Append((char *)&(data[i]), sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDirect);
  // EXPECT_EQ((encoding_buff[0] >> 1) & 0x1f, 0);
  // EXPECT_EQ(((encoding_buff[0] & 0x01) << 8) | (unsigned
  // char)encoding_buff[1],
  //           direct_len - 1);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = sign;
  auto decoder =
      PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), direct_len * sizeof(int64));

  auto result_dst_data =
      std::make_shared<DataBuffer<int64>>(reinterpret_cast<int64 *>(shared_dst_data->Start()),
                            shared_dst_data->Used(), false, false);

  for (size_t i = 0; i < direct_len; i++) {
    EXPECT_EQ((*result_dst_data)[i], data[i]);
  }

  delete[] data;
}

INSTANTIATE_TEST_SUITE_P(
    PaxEncodingRangeTestCombine, PaxEncodingDirectRangeTest,
    testing::Combine(testing::Values(4, 10, 128, 256, 512, 1024),
                     testing::Values(7, 99, 4294967295, 18014398509481984ULL),
                     testing::Values(true, false)));

TEST_P(PaxEncodingPBTest, TestOrcPBEncoding) {
  auto data_vec = ::testing::get<0>(GetParam());
  auto data_bits = ::testing::get<1>(GetParam());
  auto data_lens = data_vec.size();
  auto shared_data = std::make_shared<DataBuffer<char>>(data_lens * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(data_lens * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = true;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);
  encoder->SetDataBuffer(shared_data);

  for (size_t i = 0; i < data_lens; i++) {
    encoder->Append((char *)&(data_vec[i]), sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kPatchedBase);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = true;

  auto decoder =
      GetDecoderByBits(data_bits, shared_data, std::move(decoder_options));
  EXPECT_TRUE(decoder);

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), static_cast<size_t>(20 * data_bits / 8));

  switch (data_bits) {
    case 32: {
      auto result_dst_data = new DataBuffer<int32>(
          reinterpret_cast<int32 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < 20; i++) {
        EXPECT_EQ((*result_dst_data)[i], data_vec[i]);
      }
      delete result_dst_data;
      break;
    }
    case 64: {
      auto result_dst_data = new DataBuffer<int64>(
          reinterpret_cast<int64 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < 20; i++) {
        EXPECT_EQ((*result_dst_data)[i], data_vec[i]);
      }
      delete result_dst_data;
      break;
    }
    default:
      break;
  }
}

INSTANTIATE_TEST_SUITE_P(
    PaxEncodingRangeTestCombine, PaxEncodingPBTest,
    testing::Combine(
        testing::Values(
            std::vector<int64>{2030, 2000, 2020, 1000000, 2040, 2050, 2060,
                               2070, 2080, 2090, 2100,    2110, 2120, 2130,
                               2140, 2150, 2160, 2170,    2180, 2190},
            std::vector<int64>{2030, 2000, 2020, 2040,    2050, 2060, 2070,
                               2080, 2090, 2100, 1000000, 2110, 2120, 2130,
                               2140, 2150, 2160, 2170,    2180, 2190},
            std::vector<int64>{2030, 3333, 1111, 4444,    9991, 33213, 3213,
                               1,    2090, 2100, 1000000, 2110, 2120,  2130,
                               2140, 11,   2160, 2170,    2180, 2190}),
        testing::Values(32, 64)));

TEST_P(PaxEncodingRawDataTest, TestOrcMixEncoding) {
  auto data_vec = ::testing::get<0>(GetParam());
  auto data_bits = ::testing::get<1>(GetParam());
  auto data_lens = data_vec.size();
  auto shared_data = std::make_shared<DataBuffer<char>>(data_lens * sizeof(int64));
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(data_lens * sizeof(int64));

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = true;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);
  encoder->SetDataBuffer(shared_data);

  for (size_t i = 0; i < data_lens; i++) {
    encoder->Append((char *)&(data_vec[i]), sizeof(int64));
  }
  encoder->Flush();
  // should allow call flush multi times
  encoder->Flush();
  encoder->Flush();
  encoder->Flush();
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_NE(encoder->GetBufferSize(), 0UL);

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = true;

  auto decoder =
      GetDecoderByBits(data_bits, shared_data, std::move(decoder_options));
  EXPECT_TRUE(decoder);

  decoder->SetDataBuffer(shared_dst_data);
  decoder->Decoding();

  EXPECT_EQ(shared_dst_data->Used(), data_lens * data_bits / 8);

  switch (data_bits) {
    case 32: {
      auto result_dst_data = new DataBuffer<int32>(
          reinterpret_cast<int32 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < data_lens; i++) {
        EXPECT_EQ((*result_dst_data)[i], data_vec[i]);
      }
      delete result_dst_data;
      break;
    }
    case 64: {
      auto result_dst_data = new DataBuffer<int64>(
          reinterpret_cast<int64 *>(shared_dst_data->Start()),
          shared_dst_data->Used(), false, false);

      for (size_t i = 0; i < data_lens; i++) {
        EXPECT_EQ((*result_dst_data)[i], data_vec[i]);
      }
      delete result_dst_data;
      break;
    }
    default:
      break;
  }
}

INSTANTIATE_TEST_SUITE_P(
    PaxEncodingRangeTestCombine, PaxEncodingRawDataTest,
    testing::Combine(
        testing::Values(
            std::vector<int64>{1, 23, 4, 5123, 123123, 3213214, 543123, 3213,
                               34, 123, 5213, 23, 52},
            std::vector<int64>{-1, -23, -4, -5123, -123123, -3213214, -543123,
                               -3213, -34, -123, -5213, -23, -52},
            std::vector<int64>{1, 2, 3, 4, 5, 123, 3, 3, 3, 3, 4, 4, 4, 5, 55,
                               5},
            std::vector<int64>{-1, -2, -3, -4, -5, -123, -3, -3, -3, -3, -4, -4,
                               -4, -5, -55, -5},
            std::vector<int64>{2030, 2000, 2020, 1000000, 2040, 2050, 2060,
                               2070, 2080, 2090, 2100,    2110, 2120, 2130,
                               2140, 2150, 2160, 2170,    2180, 2190},
            std::vector<int64>{-2030, -2000, -2020, -1000000, -2040,
                               -2050, -2060, -2070, -2080,    -2090,
                               -2100, -2110, -2120, -2130,    -2140,
                               -2150, -2160, -2170, -2180,    -2190},
            std::vector<int64>{1, 2, 3, 4, 5, 123, 3, 3, 3, 3, 3, 3,  3, 3,
                               3, 3, 3, 3, 3, 3,   3, 4, 4, 4, 5, 55, 5},
            std::vector<int64>{-1, -2, -3, -4, -5, -123, -3, -3,  -3,
                               -3, -3, -3, -3, -3, -3,   -3, -3,  -3,
                               -3, -3, -3, -4, -4, -4,   -5, -55, -5},
            std::vector<int64>{1, 2, 3, 4, 5, 123, 3, 3, 3, 3, 3,  3, 3, 3,
                               3, 3, 3, 3, 3, 3,   3, 4, 4, 4, 4,  4, 4, 4,
                               4, 4, 4, 4, 4, 4,   4, 4, 4, 5, 55, 5},
            std::vector<int64>{-1, -2, -3, -4, -5, -123, -3, -3, -3,  -3,
                               -3, -3, -3, -3, -3, -3,   -3, -3, -3,  -3,
                               -3, -4, -4, -4, -4, -4,   -4, -4, -4,  -4,
                               -4, -4, -4, -4, -4, -4,   -4, -5, -55, -5},
            std::vector<int64>{1, 2, 3, 4,   5, 123, 3, 3, 3, 3, 3,  3,   3, 3,
                               3, 3, 3, 3,   3, 3,   3, 4, 4, 4, 4,  333, 4, 4,
                               4, 4, 4, 823, 4, 4,   4, 4, 4, 5, 55, 5},
            std::vector<int64>{-1, -2,   -3, -4, -5, -123, -3, -3, -3,  -3,
                               -3, -3,   -3, -3, -3, -3,   -3, -3, -3,  -3,
                               -3, -4,   -4, -4, -4, -333, -4, -4, -4,  -4,
                               -4, -823, -4, -4, -4, -4,   -4, -5, -55, -5},
            std::vector<int64>{-1, -2,   -3, -4, -5, 123, -3, -3, -3,  -3,
                               -3, -3,   -3, 3,  -3, 3,   -3, -3, -3,  -3,
                               -3, -4,   4,  -4, -4, 333, -4, -4, -4,  -4,
                               -4, -823, -4, -4, 4,  -4,  -4, -5, -55, -5}),
        testing::Values(32, 64)));

TEST_F(PaxEncodingTest, TestOrcShortRepeatWithNULL) {
  int64 *data;
  auto shared_data = std::make_shared<DataBuffer<char>>(1024);

  size_t sr_len = 10;
  size_t total_len = 15;

  PaxEncoder::EncodingOption encoder_options;

  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = true;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = pax::PAX_ALLOC<int64>(sizeof(int64));
  *data = 2;
  for (size_t i = 0; i < sr_len; i++) {
    encoder->Append((char *)data, sizeof(int64));
  }
  encoder->Flush();

  EXPECT_NE(encoder->GetBuffer(), nullptr);
  EXPECT_EQ(encoder->GetBufferSize(), 2UL);

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kShortRepeat);

  char *cpy_data = new char[shared_data->Used()];
  memcpy(cpy_data, shared_data->GetBuffer(), shared_data->Used());

  {
    // case 1 null in header
    // (total_len - sr_len) * null , sr data
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i >= (total_len - sr_len));
    }
    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = total_len - sr_len; i < total_len; i++) {
      ASSERT_EQ(2, (*result_dst_data)[i]);
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  {
    // case 2 null in tail
    // sr data, (total_len - sr_len) * null
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i < sr_len);
    }
    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = 0; i < sr_len; i++) {
      ASSERT_EQ(2, (*result_dst_data)[i]);
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  {
    // case 3 null inside delta
    // sr data, (total_len - sr_len) * null
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i % 3 != 0);
    }

    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = 0; i < total_len; i++) {
      if (i % 3 != 0) {
        ASSERT_EQ(2, (*result_dst_data)[i]);
      }
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  delete[] cpy_data;
  pax::PAX_FREE(data);
}

TEST_F(PaxEncodingTest, TestOrcDeltaEncodingWithNULL) {
  int64 *data;
  auto shared_data = std::make_shared<DataBuffer<char>>(1024);

  size_t delta_len = 20;
  size_t total_len = 30;

  PaxEncoder::EncodingOption encoder_options;
  encoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  encoder_options.is_sign = true;
  auto encoder = PaxEncoder::CreateStreamingEncoder(encoder_options);

  EXPECT_TRUE(encoder);

  encoder->SetDataBuffer(shared_data);

  data = new int64;
  *data = 2;
  for (size_t i = 0; i < delta_len; i++) {
    encoder->Append((char *)data, sizeof(int64));
  }
  encoder->Flush();

  auto encoding_buff = encoder->GetBuffer();
  EXPECT_EQ(static_cast<EncodingType>((encoding_buff[0] >> 6) & 0x03),
            EncodingType::kDelta);

  char *cpy_data = new char[shared_data->Used()];
  memcpy(cpy_data, shared_data->GetBuffer(), shared_data->Used());

  {
    // case 1 null in header
    // (total_len - delta_len) * null , delta data
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i >= (total_len - delta_len));
    }
    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = total_len - delta_len; i < total_len; i++) {
      ASSERT_EQ(2, (*result_dst_data)[i]);
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  {
    // case 2 null in tail
    // delta data, (total_len - delta_len) * null
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i < delta_len);
    }
    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = 0; i < delta_len; i++) {
      ASSERT_EQ(2, (*result_dst_data)[i]);
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  {
    // case 3 null inside delta
    auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

    std::vector<char> not_null;
    for (size_t i = 0; i < total_len; ++i) {
      not_null.push_back(i % 3 != 0);
    }

    PaxDecoder::DecodingOption decoder_options;
    decoder_options.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
    decoder_options.is_sign = true;
    auto decoder =
        PaxDecoder::CreateDecoder<int64>(decoder_options);
    decoder->SetSrcBuffer(shared_data->GetBuffer(), shared_data->Used());

    decoder->SetDataBuffer(shared_dst_data);
    auto n_read = decoder->Decoding(not_null.data(), total_len);
    ASSERT_EQ(n_read, shared_dst_data->Used());

    auto result_dst_data = std::make_shared<DataBuffer<int64>>(
        reinterpret_cast<int64 *>(shared_dst_data->Start()),
        shared_dst_data->Used(), false, false);

    ASSERT_EQ(total_len * sizeof(int64), shared_dst_data->Used());
    for (size_t i = 0; i < total_len; i++) {
      if (i % 3 != 0) {
        ASSERT_EQ(2, (*result_dst_data)[i]);
      }
    }

    // should no changed
    for (size_t i = 0; i < shared_data->Used(); i++) {
      ASSERT_EQ(cpy_data[i], (*shared_data)[i]);
    }
  }

  delete[] cpy_data;
  delete data;
}

TEST_F(PaxEncodingTest, TestEncodingWithAllNULL) {
  auto shared_dst_data = std::make_shared<DataBuffer<char>>(10240);

  std::vector<char> not_null;
  for (size_t i = 0; i < 20; ++i) {
    not_null.push_back(false);
  }

  PaxDecoder::DecodingOption decoder_options;
  decoder_options.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2;
  decoder_options.is_sign = true;
  auto decoder = PaxDecoder::CreateDecoder<int64>(decoder_options);
  decoder->SetSrcBuffer(nullptr, 0);

  decoder->SetDataBuffer(shared_dst_data);
  auto n_read = decoder->Decoding(not_null.data(), 20);
  ASSERT_EQ(n_read, shared_dst_data->Used());
}

}  // namespace pax::tests
