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
 * pax_delta_encoding_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_delta_encoding_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_delta_encoding.h"

#include <random>
#include <vector>

#include "comm/gtest_wrappers.h"
#include "pax_gtest_helper.h"

namespace pax {

class PaxDeltaEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create encoding options
    encoding_options_.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
    encoding_options_.is_sign = false;

    // Create decoding options
    decoding_options_.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
    decoding_options_.is_sign = false;
  }

  void TearDown() override {}

  // Fast bit width calculation (0 -> 0)
  inline uint8_t FastNumBits(uint32_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return v == 0 ? 0 : static_cast<uint8_t>(32 - __builtin_clz(v));
#else
    uint8_t bits = 0;
    while (v) {
      ++bits;
      v >>= 1;
    }
    return bits;
#endif
  }

  // Helper function to encode and decode data
  template <typename T>
  std::vector<T> EncodeAndDecode(const std::vector<T> &input) {
    // Create encoder
    PaxDeltaEncoder<T> encoder(encoding_options_);

    size_t bound_size = encoder.GetBoundSize(input.size() * sizeof(T));

    encoder.SetDataBuffer(std::make_shared<DataBuffer<char>>(bound_size));

    // Encode data
    encoder.Append(reinterpret_cast<char *>(const_cast<T *>(input.data())),
                   input.size() * sizeof(T));

    // Get encoded buffer
    const char *encoded_data = encoder.GetBuffer();
    size_t encoded_size = encoder.GetBufferSize();

    // Create decoder
    PaxDeltaDecoder<T> decoder(decoding_options_);

    // Set source buffer
    decoder.SetSrcBuffer(const_cast<char *>(encoded_data), encoded_size);

    // Create result buffer
    auto result_buffer =
        std::make_shared<DataBuffer<char>>(input.size() * sizeof(T));
    decoder.SetDataBuffer(result_buffer);

    // Decode
    size_t decoded_size = decoder.Decoding();

    // Convert result back to vector
    const T *decoded_data = reinterpret_cast<const T *>(decoder.GetBuffer());
    size_t count = decoded_size / sizeof(T);

    return std::vector<T>(decoded_data, decoded_data + count);
  }

  PaxEncoder::EncodingOption encoding_options_;
  PaxDecoder::DecodingOption decoding_options_;
};

// Test basic functionality
TEST_F(PaxDeltaEncodingTest, BasicEncodeDecode) {
  std::vector<uint32_t> input = {1, 2, 3, 4, 5};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test example from documentation - consecutive sequence
TEST_F(PaxDeltaEncodingTest, ConsecutiveSequence) {
  std::vector<uint32_t> input = {1, 2, 3, 4, 5};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);

  // Verify deltas would be [1, 1, 1, 1] with min_delta = 1
  // and adjusted deltas [0, 0, 0, 0] with bit_width = 0
}

// Test example from documentation - sequence with variation
TEST_F(PaxDeltaEncodingTest, SequenceWithVariation) {
  std::vector<uint32_t> input = {7, 5, 3, 1, 2, 3, 4, 5};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);

  // Verify deltas would be [-2, -2, -2, 1, 1, 1, 1] with min_delta = -2
  // Since we cast to uint32, -2 becomes a large positive number
  // adjusted deltas would be [0, 0, 0, 3, 3, 3, 3] with bit_width = 2
}

// Test single value
TEST_F(PaxDeltaEncodingTest, SingleValue) {
  std::vector<uint32_t> input = {42};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test two values
TEST_F(PaxDeltaEncodingTest, TwoValues) {
  std::vector<uint32_t> input = {10, 15};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test large values
TEST_F(PaxDeltaEncodingTest, LargeValues) {
  std::vector<uint32_t> input = {1000000, 1000001, 1000002, 1000003};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test values with large deltas
TEST_F(PaxDeltaEncodingTest, LargeDeltas) {
  std::vector<uint32_t> input = {1, 1000, 2000, 3000};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test full block (128 values)
TEST_F(PaxDeltaEncodingTest, FullBlock) {
  std::vector<uint32_t> input;
  for (uint32_t i = 0; i < 128; ++i) {
    input.push_back(i);
  }
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test multiple blocks
TEST_F(PaxDeltaEncodingTest, MultipleBlocks) {
  std::vector<uint32_t> input;
  for (uint32_t i = 0; i < 250; ++i) {
    input.push_back(i);
  }
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test random data
TEST_F(PaxDeltaEncodingTest, RandomData) {
  std::mt19937 gen(12345);
  std::uniform_int_distribution<uint32_t> dis(0, 1000000);

  std::vector<uint32_t> input;
  for (int i = 0; i < 100; ++i) {
    input.push_back(dis(gen));
  }

  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test payload size calculation
TEST_F(PaxDeltaEncodingTest, PayloadSizeCalculation) {
  std::vector<uint32_t> input = {
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18,
      19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 56, 63, 89};
  // Test the specific example: deltas [0,0,0,0,0,0,0,0,...,0,22,6,25] with
  // bit_width 0,5,0,0

  PaxDeltaEncoder<uint32_t> encoder(encoding_options_);
  size_t bound_size = encoder.GetBoundSize(input.size() * sizeof(uint32_t));
  encoder.SetDataBuffer(std::make_shared<DataBuffer<char>>(bound_size));
  encoder.Append(reinterpret_cast<char *>(input.data()),
                 input.size() * sizeof(uint32_t));

  // Verify the encoded data structure manually
  const char *encoded_data = encoder.GetBuffer();
  size_t encoded_size = encoder.GetBufferSize();

  EXPECT_GT(encoded_size, 0);

  // Parse the encoded data
  const uint8_t *p = reinterpret_cast<const uint8_t *>(encoded_data);

  // Read header
  DeltaBlockHeader header;
  std::memcpy(&header, p, sizeof(header));
  p += sizeof(header);

  EXPECT_EQ(header.value_per_block, 128);
  EXPECT_EQ(header.values_per_mini_block, 32);
  EXPECT_EQ(header.total_count, input.size());

  // Read first value
  uint32_t first_value;
  std::memcpy(&first_value, p, sizeof(first_value));
  p += sizeof(first_value);
  EXPECT_EQ(first_value, 1);

  // Read block data
  uint32_t min_delta;
  std::memcpy(&min_delta, p, sizeof(min_delta));
  p += sizeof(min_delta);

  // Read allbit widths
  uint8_t bit_widths[4];
  for (int i = 0; i < 4; ++i) {
    bit_widths[i] = *p++;
  }

  // bit_widths should be [0, 6, 0, 0]
  ASSERT_EQ(bit_widths[0], 0);
  ASSERT_EQ(bit_widths[1], 5);
  ASSERT_EQ(bit_widths[2], 0);
  ASSERT_EQ(bit_widths[3], 0);

  // Compute payload size from bit_widths and counts
  uint32_t values_in_block =
      input.size() - 1;  // we constructed input with 35 deltas in first block
  uint64_t total_bits = 0;
  for (uint32_t i = 0; i < 4; ++i) {
    uint32_t start = i * 32;
    if (start >= values_in_block) break;
    uint32_t end = std::min(start + 32u, values_in_block);
    uint8_t w = bit_widths[i];
    total_bits += static_cast<uint64_t>(w) * (end - start);
  }
  uint32_t payload_size = static_cast<uint32_t>((total_bits + 7) / 8);

  // For this example, we expect payload_size = 2 bytes
  EXPECT_EQ(payload_size, 2);

  // Assert payload bitmap is correct
  uint8_t payload[4];
  std::memcpy(payload, p, 4);
  p += 4;

  // payload should be LSB-Last, value is(22,6,25)
  // [0b10110, 0b00110, 0b11001]
  EXPECT_EQ(payload[0], 0b11010110);
  EXPECT_EQ(payload[1], 0b01100100);
}

// Test bit width calculation helper
TEST_F(PaxDeltaEncodingTest, BitWidthCalculation) {
  EXPECT_EQ(FastNumBits(0), 0);
  EXPECT_EQ(FastNumBits(1), 1);
  EXPECT_EQ(FastNumBits(2), 2);
  EXPECT_EQ(FastNumBits(3), 2);
  EXPECT_EQ(FastNumBits(4), 3);
  EXPECT_EQ(FastNumBits(7), 3);
  EXPECT_EQ(FastNumBits(8), 4);
  EXPECT_EQ(FastNumBits(15), 4);
  EXPECT_EQ(FastNumBits(16), 5);
  EXPECT_EQ(FastNumBits(255), 8);
  EXPECT_EQ(FastNumBits(256), 9);
}

// Test zero deltas (all same values)
TEST_F(PaxDeltaEncodingTest, ZeroDeltas) {
  std::vector<uint32_t> input = {42, 42, 42, 42, 42};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test decreasing sequence (negative deltas)
TEST_F(PaxDeltaEncodingTest, DecreasingSequence) {
  std::vector<uint32_t> input = {100, 90, 80, 70, 60};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test mixed pattern
TEST_F(PaxDeltaEncodingTest, MixedPattern) {
  std::vector<uint32_t> input = {10, 20, 15, 25, 5, 30, 1, 35};
  auto output = EncodeAndDecode(input);
  EXPECT_EQ(input, output);
}

// Test empty input (edge case)
TEST_F(PaxDeltaEncodingTest, EmptyInput) {
  std::vector<uint32_t> input = {};
  // This should handle gracefully or throw expected exception
  // For now, let's skip this test until we clarify expected behavior
}

// Test different data types
TEST_F(PaxDeltaEncodingTest, DifferentTypes) {
  // Test int32_t (with non-negative values)
  std::vector<uint32_t> input32 = {1, 2, 3, 4, 5};
  auto output32 = EncodeAndDecode(input32);
  EXPECT_EQ(input32, output32);
}

}  // namespace pax

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
