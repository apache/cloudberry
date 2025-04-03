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
 * pax_rlev2_encoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_rlev2_encoding.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_rlev2_encoding.h"

#include <algorithm>
#include <cmath>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"

namespace pax {

#ifndef RUN_GTEST
void WriteLongs(DataBuffer<char> *data_buffer, const int64 *input,
                uint32 offset, size_t len, uint32 bits);

static inline void WriteLongs(const std::shared_ptr<DataBuffer<char>> &data_buffer, const int64 *input,
                uint32 offset, size_t len, uint32 bits) {
  WriteLongs(data_buffer.get(), input, offset, len, bits);
}
#endif
void WriteUnsignedLong(DataBuffer<char> *data_buffer, int64 val);
void WriteSignedLong(DataBuffer<char> *data_buffer, int64 val);

static inline void WriteUnsignedLong(const std::shared_ptr<DataBuffer<char>> &data_buffer, int64 val) {
  WriteUnsignedLong(data_buffer.get(), val);
}

static inline void WriteSignedLong(const std::shared_ptr<DataBuffer<char>> &data_buffer, int64 val) {
  WriteSignedLong(data_buffer.get(), val);
}

void WriteUnsignedLong(DataBuffer<char> *data_buffer, int64 val) {
  while (true) {
    if ((val & ~0x7f) == 0) {
      data_buffer->Write(val);
      data_buffer->Brush(1);
      return;
    } else {
      data_buffer->Write(static_cast<char>(0x80 | (val & 0x7f)));
      data_buffer->Brush(1);
      // cast val to unsigned so as to force 0-fill right shift
      val = (static_cast<uint64>(val) >> 7);
    }
  }
}

void WriteSignedLong(DataBuffer<char> *data_buffer, int64 val) {
  WriteUnsignedLong(data_buffer, ZigZag(val));
}

void WriteLongs(DataBuffer<char> *data_buffer, const int64 *const input,
                uint32 offset, size_t len, uint32 bits) {
  if (input == nullptr || len < 1 || bits < 1) {
    return;
  }

  if (GetClosestAlignedBits(bits) == bits) {
    uint32 num_of_bytes;
    auto end_offset = static_cast<uint32>(offset + len);
    if (bits < 8) {
      char bit_mask = static_cast<char>((1 << bits) - 1);
      uint32 num_hops = 8 / bits;
      auto remainder = static_cast<uint32>(len % num_hops);
      uint32 end_unroll = end_offset - remainder;
      for (uint32 i = offset; i < end_unroll; i += num_hops) {
        char to_write = 0;
        for (uint32 j = 0; j < num_hops; ++j) {
          to_write |= static_cast<char>((input[i + j] & bit_mask)
                                        << (8 - (j + 1) * bits));
        }
        data_buffer->Write(to_write);
        data_buffer->Brush(1);
      }

      if (remainder > 0) {
        uint32 shift = 8 - bits;
        char to_write = 0;
        for (uint32 i = end_unroll; i < end_offset; ++i) {
          to_write |= static_cast<char>((input[i] & bit_mask) << shift);
          shift -= bits;
        }
        data_buffer->Write(to_write);
        data_buffer->Brush(1);
      }

    } else {
      num_of_bytes = bits / 8;
      for (uint32 i = offset; i < end_offset; ++i) {
        for (uint32 j = 0; j < num_of_bytes; ++j) {
          char to_write = static_cast<char>(
              (input[i] >> (8 * (num_of_bytes - j - 1))) & 255);
          data_buffer->Write(to_write);
          data_buffer->Brush(1);
        }
      }
    }

    return;
  }

  // write for unaligned bit size
  uint32 bits_left = 8;
  char current = 0;
  for (uint32 i = offset; i < (offset + len); i++) {
    int64 value = input[i];
    uint32 bits_to_write = bits;
    while (bits_to_write > bits_left) {
      // add the bits to the bottom of the current word
      current |= static_cast<char>(value >> (bits_to_write - bits_left));
      // subtract out the bits we just added
      bits_to_write -= bits_left;
      // zero out the bits above bits_to_write
      value &= (static_cast<uint64>(1) << bits_to_write) - 1;
      data_buffer->Write(current);
      data_buffer->Brush(1);
      current = 0;
      bits_left = 8;
    }
    bits_left -= bits_to_write;
    current |= static_cast<char>(value << bits_left);
    if (bits_left == 0) {
      data_buffer->Write(current);
      data_buffer->Brush(1);
      current = 0;
      bits_left = 8;
    }
  }

  // flush
  if (bits_left != 8) {
    data_buffer->Write(current);
    data_buffer->Brush(1);
  }
}

struct PaxOrcEncoder::EncoderContext::DeltaContext {
  int64 adj_deltas[ORC_MAX_LITERAL_SIZE];
  int64 adj_deltas_idx;
  bool is_fixed_delta;
  int64 fixed_delta_val;
  uint32 bits_delta_max;
};

struct PaxOrcEncoder::EncoderContext::DirectContext {
  uint32 zigzag_bits_100_p;
  bool zz_bits_100_p_inited;
};

struct PaxOrcEncoder::EncoderContext::PatchBaseContext {
  int32 histogram[ORC_HIST_LEN];
  size_t histogram_len;

  int64 base_patch_buffer[ORC_MAX_LITERAL_SIZE];
  int64 base_patch_buffer_count;

  int64 gap_sign_patch_list[ORC_MAX_LITERAL_SIZE];
  int64 gap_sign_patch_list_count;

  uint32 patch_width;
  uint32 patch_gap_width;
  uint32 patch_len;
  uint32 hist_bits_95_p;
  uint32 hist_bits_100_p;
  int64 min;
  int64 max;
};

PaxOrcEncoder::EncoderContext::EncoderContext()
    : is_sign(true), fixed_len(0), var_len(0), prev_delta(0), current_delta(0) {
  auto size = sizeof(struct DeltaContext) + sizeof(struct DirectContext) + sizeof(struct PatchBaseContext);
  internal_buffer_ = pax::PAX_ALLOC0<char>(size);

  delta_ctx = reinterpret_cast<struct DeltaContext *>(internal_buffer_);
  direct_ctx = reinterpret_cast<struct DirectContext *>(
      internal_buffer_ + sizeof(struct DeltaContext));
  pb_ctx = reinterpret_cast<struct PatchBaseContext *>(
      internal_buffer_ + sizeof(struct DeltaContext) +
      sizeof(struct DirectContext));
}

PaxOrcEncoder::EncoderContext::~EncoderContext() {
  pax::PAX_FREE(internal_buffer_);
}

void PaxOrcEncoder::EncoderContext::ResetDirectCtx() const {
  direct_ctx->zz_bits_100_p_inited = false;
}

void PaxOrcEncoder::EncoderContext::ResetDeltaCtx() const {
  delta_ctx->adj_deltas_idx = 0;
  delta_ctx->is_fixed_delta = false;
  delta_ctx->fixed_delta_val = 0;
  delta_ctx->bits_delta_max = 0;
}

void PaxOrcEncoder::EncoderContext::ResetPbCtx() const {
  pb_ctx->histogram_len = 0;
  pb_ctx->base_patch_buffer_count = 0;
  pb_ctx->gap_sign_patch_list_count = 0;

  pb_ctx->patch_width = 0;
  pb_ctx->patch_gap_width = 0;
  pb_ctx->patch_len = 0;
  pb_ctx->hist_bits_95_p = 0;
  pb_ctx->hist_bits_100_p = 0;
  pb_ctx->min = 0;
  pb_ctx->max = 0;
}

PaxOrcEncoder::PaxOrcEncoder(const EncodingOption &encoder_options)
    : PaxEncoder(encoder_options),
      data_buffer_(std::make_shared<UntreatedDataBuffer<int64>>(1024)),
      zigzag_buffer_(std::make_shared<DataBuffer<int64>>(128)),
      status_(EncoderStatus::kInit) {
  encoder_context_.is_sign = encoder_options_.is_sign;
}

PaxOrcEncoder::~PaxOrcEncoder() { }

void PaxOrcEncoder::Append(char *data, size_t len) {
  Assert(len <= 8);
  switch (len) {
    case 1:
      AppendInternal(*((int8 *)data), false);
      break;
    case 2:
      AppendInternal(*((int16 *)data), false);
      break;
    case 4:
      AppendInternal(*((int32 *)data), false);
      break;
    case 8:
      AppendInternal(*((int64 *)data), false);
      break;
    default:
      Assert(false);
  }
}

bool PaxOrcEncoder::SupportAppendNull() const { return false; }

void PaxOrcEncoder::Flush() { AppendInternal(0, true); }

void PaxOrcEncoder::AppendData(const int64 data) {
  if (data_buffer_->Available() < sizeof(int64)) {
    data_buffer_->ReSize(data_buffer_->Capacity() * 2);
  }

  data_buffer_->Write(data);

  data_buffer_->Brush(sizeof(int64));
  data_buffer_->BrushUnTreated(sizeof(int64));
}

void PaxOrcEncoder::SwitchStatusTo(EncoderStatus new_status) {
  status_ = new_status;
}

void PaxOrcEncoder::AppendInternal(const int64 data, bool is_flush) {
  bool already_append_before_delta_changed = false;
  bool keep_push_status = true;

  if (is_flush) {
    SwitchStatusTo(kFlush);
  }

  while (keep_push_status) {
    keep_push_status = false;

    switch (status_) {
      case EncoderStatus::kInvalid: {
        Assert(false);
        break;
      }
      case EncoderStatus::kInit: {
        AppendData(data);
        encoder_context_.fixed_len = 1;
        encoder_context_.var_len = 1;
        SwitchStatusTo(kTwoElements);
        break;
      }
      case EncoderStatus::kTwoElements: {
        encoder_context_.prev_delta =
            data - ((*data_buffer_)[data_buffer_->GetSize() - 1]);

        AppendData(data);

        if (encoder_context_.prev_delta == 0) {
          encoder_context_.fixed_len = 2;
          encoder_context_.var_len = 0;
        } else {
          encoder_context_.fixed_len = 0;
          encoder_context_.var_len = 2;
        }

        SwitchStatusTo(kUntreated);
        break;
      }
      case kUntreated: {
        if (data_buffer_->UnTreated() == 0) {
          keep_push_status = true;
          SwitchStatusTo(kInit);
          break;
        }

        encoder_context_.current_delta =
            data - ((*data_buffer_)[data_buffer_->GetSize() - 1]);

        // Temporary data is duplicated(minimum of 3 repetitions, eq.
        // ORC_MIN_REPEAT) But it is not confirmed whether it is already
        // duplicated at the beginning
        if (encoder_context_.current_delta == encoder_context_.prev_delta &&
            encoder_context_.current_delta == 0) {
          AppendData(data);
          if (encoder_context_.var_len > 0) {
            // If variable run is non-zero then we are seeing repeating
            // values at the end of variable run in which case fixed Run
            // length is 2
            encoder_context_.fixed_len = 2;
          }
          encoder_context_.fixed_len++;

          if (encoder_context_.fixed_len >= ORC_MIN_REPEAT &&
              encoder_context_.var_len > 0) {
            // Ok, got at lease 3 repetitions
            // Encode and flush the non-repeating data
            keep_push_status = true;
            SwitchStatusTo(kTreatPrevBuffer);
            break;
          }

          // The data becomes repeated at the beginning
          if (encoder_context_.fixed_len == ORC_MAX_LITERAL_SIZE) {
            encoder_context_.delta_ctx->is_fixed_delta = true;

            keep_push_status = true;
            SwitchStatusTo(kTreatDelta);
            break;
          }

          /// no need switch to next type
          break;
        }

        // Below is encoder_context_.current_delta !=
        // encoder_context_.prev_delta != 0 That can happen in two ways:
        // 1. Repeating consecutive sequences become non-repeating
        // 2. Temporary data is not repeated at the beginning

        if (encoder_context_.fixed_len >= ORC_MIN_REPEAT) {
          keep_push_status = true;
          SwitchStatusTo(kUntreatedDiscontinuous);
          break;
        }

        // Repetitions data numbers can less than 3 here
        // ex. 0 0 1, then current_delta != 0. and data will transfer to
        // non-repeating data
        if (encoder_context_.fixed_len > 0 &&
            encoder_context_.fixed_len < ORC_MIN_REPEAT &&
            encoder_context_.current_delta != 0) {
          encoder_context_.var_len = encoder_context_.fixed_len;
          encoder_context_.fixed_len = 0;
        }

        // No data remain
        if (data_buffer_->UnTreated() == 0) {
          keep_push_status = true;
          SwitchStatusTo(kInit);
          break;
        }

        // non-repeating data
        encoder_context_.prev_delta = encoder_context_.current_delta;
        AppendData(data);
        encoder_context_.var_len++;

        if (encoder_context_.var_len == ORC_MAX_LITERAL_SIZE) {
          keep_push_status = true;
          SwitchStatusTo(kDetermineFlushPrevBuffer);
          break;
        }

        /// no need switch to next type
        break;
      }
      case EncoderStatus::kUntreatedDiscontinuous: {
        already_append_before_delta_changed = true;
        keep_push_status = true;

        if (encoder_context_.fixed_len <= ORC_MAX_SHORT_REPEAT_LENGTH) {
          SwitchStatusTo(kTreatShortRepeat);
        } else {
          encoder_context_.delta_ctx->is_fixed_delta = true;
          SwitchStatusTo(kTreatDelta);
        }
        break;
      }
      case EncoderStatus::kTreatPrevBuffer: {
        Assert(data_buffer_->UnTreated());
        Assert(data_buffer_->UnTouched() == 0);

        // fixed_len must equal to ORC_MIN_REPEAT
        // the status kTreatPrevBuffer will only occur when
        // non-repeating elements change to ORC_MIN_REPEAT repeating elements
        // means there must be non-repeating elements before repeating elements
        Assert(encoder_context_.fixed_len == ORC_MIN_REPEAT);

        // will shift tail fixed in kTreatDone
        data_buffer_->BrushBackUnTreated(sizeof(int64) * ORC_MIN_REPEAT);
        encoder_context_.var_len -= (ORC_MIN_REPEAT - 1);

        keep_push_status = true;
        SwitchStatusTo(kDetermineFlushPrevBuffer);
        break;
      }
      case EncoderStatus::kFlush:
        if (data_buffer_->Used() == 0) {
          keep_push_status = true;
          SwitchStatusTo(kFinish);
          break;
        }

        Assert(data_buffer_->UnTreated() == data_buffer_->Used());

        // must check delta & short repeat can treat or not.
        if (encoder_context_.fixed_len != 0) {
          if (encoder_context_.fixed_len < ORC_MIN_REPEAT) {
            encoder_context_.var_len = encoder_context_.fixed_len;
            encoder_context_.fixed_len = 0;
          } else if (  // encoder_context_.fixed_len >= ORC_MIN_REPEAT &&
              encoder_context_.fixed_len <= ORC_MAX_SHORT_REPEAT_LENGTH) {
            keep_push_status = true;
            SwitchStatusTo(kTreatShortRepeat);
            break;
          } else {  // encoder_context_.fixed_len > ORC_MAX_SHORT_REPEAT_LENGTH
            keep_push_status = true;
            encoder_context_.delta_ctx->is_fixed_delta = true;
            SwitchStatusTo(kTreatDelta);
            break;
          }
        }

        keep_push_status = true;
        SwitchStatusTo(kDetermineFlushPrevBuffer);
        break;
      case EncoderStatus::kDetermineFlushPrevBuffer: {
        size_t data_lens = data_buffer_->UnTreated() / sizeof(int64);

        if (data_lens <= ORC_MIN_REPEAT) {
          keep_push_status = true;
          SwitchStatusTo(kTreatDirect);
          break;
        }

        bool increasing = true;
        bool decreasing = true;
        auto delta_ctx = encoder_context_.delta_ctx;
        auto direct_ctx = encoder_context_.direct_ctx;
        auto pb_ctx = encoder_context_.pb_ctx;
        int64 init_delta_val = (*data_buffer_)[1] - (*data_buffer_)[0];
        int64 curr_delta = 0;
        int64 max_delta = 0;
        uint32 zigzag_bits_90_p = 0;

        delta_ctx->is_fixed_delta = true;
        pb_ctx->min = (*data_buffer_)[0];
        pb_ctx->max = (*data_buffer_)[0];

        delta_ctx->adj_deltas[delta_ctx->adj_deltas_idx++] = init_delta_val;

        for (size_t i = 1; i < data_lens; i++) {
          const int64 l1 = (*data_buffer_)[i];
          const int64 l0 = (*data_buffer_)[i - 1];
          curr_delta = l1 - l0;
          pb_ctx->min = std::min(pb_ctx->min, l1);
          pb_ctx->max = std::max(pb_ctx->max, l1);

          increasing &= (l0 <= l1);
          decreasing &= (l0 >= l1);

          delta_ctx->is_fixed_delta &= (curr_delta == init_delta_val);
          if (i > 1) {
            delta_ctx->adj_deltas[delta_ctx->adj_deltas_idx++] =
                std::abs(curr_delta);
            max_delta = std::max(max_delta, delta_ctx->adj_deltas[i - 1]);
          }
        }

        // it's faster to exit under delta overflow condition without checking
        // for PATCHED_BASE condition as encoding using DIRECT is faster and has
        // less overhead than PATCHED_BASE
        auto is_safe_subtract = [](int64 left, int64 right) -> bool {
          return ((left ^ right) >= 0) || ((left ^ (left - right)) >= 0);
        };

        if (!is_safe_subtract(pb_ctx->max, pb_ctx->min)) {
          keep_push_status = true;
          SwitchStatusTo(kTreatDirect);
          break;
        }

        // invariant - subtracting any number from any other in the literals
        // after option point won't overflow

        // if min is equal to max then the delta is 0, option condition happens
        // for fixed values run >10 which cannot be encoded with SHORT_REPEAT
        if (pb_ctx->min == pb_ctx->max) {
          Assert(delta_ctx->is_fixed_delta);
          Assert(!curr_delta);

          delta_ctx->fixed_delta_val = 0;

          keep_push_status = true;
          SwitchStatusTo(kTreatDelta);
          break;
        }

        if (delta_ctx->is_fixed_delta) {
          Assert(curr_delta == init_delta_val);
          delta_ctx->fixed_delta_val = curr_delta;

          keep_push_status = true;
          SwitchStatusTo(kTreatDelta);
          break;
        }

        if (init_delta_val != 0) {
          delta_ctx->bits_delta_max = FindClosestBits(max_delta);

          // monotonic condition
          if (increasing || decreasing) {
            keep_push_status = true;
            SwitchStatusTo(kTreatDelta);
            break;
          }
        }

        // Without flush as delta then reset delta ctx
        encoder_context_.ResetDeltaCtx();

        pb_ctx->histogram_len = data_lens;
        if (encoder_context_.is_sign) {
          zigzag_buffer_->BrushBackAll();
          if (zigzag_buffer_->Capacity() < data_lens * sizeof(int64)) {
            zigzag_buffer_->ReSize(data_lens * sizeof(int64));
          }
          ZigZagBuffers(data_buffer_->StartT(), zigzag_buffer_->StartT(),
                        data_lens);
          zigzag_buffer_->Brush(data_lens * sizeof(int64));
        }

        // PATCHED_BASE encoding check

        // percentile values are computed for the zigzag encoded values. if the
        // number of bit requirement between 90th and 100th percentile varies
        // beyond a threshold then we need to patch the values. if the variation
        // is not significant then we can use direct encoding

        BuildHistogram(pb_ctx->histogram,
                       encoder_context_.is_sign ? zigzag_buffer_->StartT()
                                                : data_buffer_->StartT(),
                       data_lens);

        direct_ctx->zz_bits_100_p_inited = true;
        direct_ctx->zigzag_bits_100_p =
            GetPercentileBits(pb_ctx->histogram, pb_ctx->histogram_len, 1.0);
        zigzag_bits_90_p =
            GetPercentileBits(pb_ctx->histogram, pb_ctx->histogram_len, 0.9);

        // if the difference between 90th percentile and 100th percentile fixed
        // bits is > 1 then we need patch the values
        if (direct_ctx->zigzag_bits_100_p - zigzag_bits_90_p > 1) {
          for (size_t i = 0; i < data_lens; i++) {
            pb_ctx->base_patch_buffer[pb_ctx->base_patch_buffer_count++] =
                ((*data_buffer_)[i] - pb_ctx->min);
          }

          pb_ctx->histogram_len = data_lens;
          // rebuild histogram with literals[*] - min
          BuildHistogram(pb_ctx->histogram, pb_ctx->base_patch_buffer,
                         data_lens);

          // 95th percentile width is used to determine max allowed value
          // after which patching will be done
          pb_ctx->hist_bits_95_p =
              GetPercentileBits(pb_ctx->histogram, pb_ctx->histogram_len, 0.95);

          // 100th percentile is used to compute the max patch width
          pb_ctx->hist_bits_100_p =
              GetPercentileBits(pb_ctx->histogram, pb_ctx->histogram_len, 1.0);

          // after base reducing the values, if the difference in bits between
          // 95th percentile and 100th percentile value is zero then there
          // is no point in patching the values, in which case we will
          // fallback to DIRECT encoding.
          // The decision to use patched base was based on zigzag values, but
          // the actual patching is done on base reduced literals.
          if ((pb_ctx->hist_bits_100_p - pb_ctx->hist_bits_95_p) != 0) {
            keep_push_status = true;
            SwitchStatusTo(kTreatPatchedBase);
            break;
          }
        }

        encoder_context_.ResetPbCtx();

        keep_push_status = true;
        SwitchStatusTo(kTreatDirect);
        break;
      }
      case EncoderStatus::kTreatShortRepeat: {
        TreatShortRepeat();
        encoder_context_.fixed_len = 0;

        keep_push_status = true;
        SwitchStatusTo(kTreatDone);
        break;
      }
      case EncoderStatus::kTreatDirect: {
        TreatDirect();
        encoder_context_.var_len = 0;

        keep_push_status = true;
        SwitchStatusTo(kTreatDone);
        break;
      }
      case EncoderStatus::kTreatPatchedBase: {
        TreatPatchedBase();
        encoder_context_.var_len = 0;

        keep_push_status = true;
        SwitchStatusTo(kTreatDone);
        break;
      }
      case EncoderStatus::kTreatDelta: {
        bool reset_fix = TreatDelta();
        if (reset_fix) {
          encoder_context_.fixed_len = 0;
        } else {
          encoder_context_.var_len = 0;
        }

        keep_push_status = true;
        SwitchStatusTo(kTreatDone);

        break;
      }
      case EncoderStatus::kTreatDone: {
        Assert(data_buffer_->UnTreated() != 0);
        encoder_context_.ResetDeltaCtx();
        encoder_context_.ResetDirectCtx();
        encoder_context_.ResetPbCtx();

        // left shift
        data_buffer_->TreatedAll();
        data_buffer_->BrushUnTreatedAll();

        if (is_flush) {
          keep_push_status = true;
          SwitchStatusTo(
              (encoder_context_.fixed_len == 0 && encoder_context_.var_len == 0)
                  ? kFinish
                  : kFlush);
        } else {
          keep_push_status = already_append_before_delta_changed;
          SwitchStatusTo(kUntreated);
          already_append_before_delta_changed = false;
        }

        break;
      }
      case EncoderStatus::kFinish:
        keep_push_status = false;
        SwitchStatusTo(kInvalid);
        break;
      default:
        break;
    }
  }
}

void PaxOrcEncoder::TreatShortRepeat() {
  int64 repeat_val = encoder_context_.is_sign ? ZigZag((*data_buffer_)[0])
                                              : (*data_buffer_)[0];
  const uint32 num_of_bits = FindClosestBits(repeat_val);
  const uint32 num_of_bytes =
      num_of_bits % 8 == 0 ? (num_of_bits >> 3) : ((num_of_bits >> 3) + 1);

  auto header = (uint8)(EncodingType::kShortRepeat << 6);

  header |= encoder_context_.fixed_len - ORC_MIN_REPEAT;
  header |= ((num_of_bytes - 1) << 3);

  while (result_buffer_->Available() < (1) + num_of_bytes) {
    result_buffer_->ReSize(result_buffer_->Capacity() * 2);
  }

  result_buffer_->Write(header);
  result_buffer_->Brush(1);
  for (auto i = static_cast<int32>(num_of_bytes - 1); i >= 0; i--) {
    int64 b = ((repeat_val >> (i * 8)) & 0xff);
    result_buffer_->Write(static_cast<char>(b));
    result_buffer_->Brush(1);
  }
}

void PaxOrcEncoder::TreatDirect() {
  size_t data_lens = data_buffer_->UnTreated() / sizeof(int64);
  auto direct_ctx = encoder_context_.direct_ctx;
  int64 *data_write = nullptr;
  auto result_buffer = result_buffer_.get();

  if (!direct_ctx->zz_bits_100_p_inited) {
    auto pb_ctx = encoder_context_.pb_ctx;
    if (encoder_context_.is_sign) {
      ZigZagBuffers(data_buffer_->StartT(), data_buffer_->StartT(), data_lens);
    }
    // need to borrow patched base ctx to get the max zigzag bits
    pb_ctx->histogram_len = data_lens;
    BuildHistogram(pb_ctx->histogram, data_buffer_->StartT(), data_lens);
    direct_ctx->zigzag_bits_100_p =
        GetPercentileBits(pb_ctx->histogram, pb_ctx->histogram_len, 1.0);
    encoder_context_.ResetPbCtx();
    data_write = data_buffer_->StartT();
  } else if (encoder_context_.is_sign) {
    Assert(zigzag_buffer_->Used() == data_buffer_->UnTreated());
    data_write = zigzag_buffer_->StartT();
  } else {  // inited hist and not sign
    data_write = data_buffer_->StartT();
  }

  uint32 fb = GetClosestAlignedBits(direct_ctx->zigzag_bits_100_p);

  const uint32 efb = EncodeBits(fb) << 1;
  const uint32 tail_bits = ((encoder_context_.var_len - 1) & 0x100) >> 8;
  const char first_byte =
      static_cast<char>((EncodingType::kDirect << 6) | efb | tail_bits);
  const char second_byte =
      static_cast<char>((encoder_context_.var_len - 1) & 0xff);

  // worst case: will write (3 bytes header + untreated buffer)
  while (result_buffer->Available() <
         static_cast<size_t>((3) + data_buffer_->UnTreated())) {
    result_buffer->ReSize(result_buffer->Capacity() * 2);
  }

  result_buffer->Write(first_byte);
  result_buffer->Brush(1);
  result_buffer->Write(second_byte);
  result_buffer->Brush(1);

  WriteLongs(result_buffer, data_write, 0, data_lens, fb);

  encoder_context_.ResetDirectCtx();
}

void PaxOrcEncoder::PreparePatchedBlob() {
  size_t data_lens = data_buffer_->UnTreated() / sizeof(int64);
  auto pb_ctx = encoder_context_.pb_ctx;

  // mask will be max value beyond which patch will be generated
  int64 mask =
      static_cast<int64>(static_cast<uint64>(1) << pb_ctx->hist_bits_95_p) - 1;

  // since we are considering only 95 percentile, the size of gap and
  // patch array can contain only be 5% values
  pb_ctx->patch_len = static_cast<uint32>(std::ceil((data_lens / 20)));

  // #bit for patch
  pb_ctx->patch_width = pb_ctx->hist_bits_100_p - pb_ctx->hist_bits_95_p;
  pb_ctx->patch_width = GetClosestBits(pb_ctx->patch_width);

  // if patch bit requirement is 64 then it will not possible to pack
  // gap and patch together in a long. To make sure gap and patch can be
  // packed together adjust the patch width
  if (pb_ctx->patch_width == 64) {
    pb_ctx->patch_width = 56;
    pb_ctx->hist_bits_95_p = 8;
    mask =
        static_cast<int64>(static_cast<uint64>(1) << pb_ctx->hist_bits_95_p) -
        1;
  }

  uint32 gap_idx = 0;
  uint32 patch_idx = 0;
  size_t prev = 0;
  size_t max_gap = 0;

  std::vector<int64> gap_list;
  std::vector<int64> patch_list;

  for (size_t i = 0; i < data_lens; i++) {
    // if value is above mask then create the patch and record the gap
    if (pb_ctx->base_patch_buffer[i] > mask) {
      size_t gap = i - prev;
      if (gap > max_gap) {
        max_gap = gap;
      }

      // gaps are relative, so store the previous patched value index
      prev = i;
      gap_list.push_back(static_cast<int64>(gap));
      gap_idx++;

      // extract the most significant bits that are over mask bits
      int64 patch = pb_ctx->base_patch_buffer[i] >> pb_ctx->hist_bits_95_p;
      patch_list.push_back(patch);
      patch_idx++;

      // strip off the MSB to enable safe bit packing
      pb_ctx->base_patch_buffer[i] &= mask;
    }
  }

  // adjust the patch length to number of entries in gap list
  pb_ctx->patch_len = gap_idx;

  // if the element to be patched is the first and only element then
  // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
  if (max_gap == 0 && pb_ctx->patch_len != 0) {
    pb_ctx->patch_gap_width = 1;
  } else {
    pb_ctx->patch_gap_width = FindClosestBits(static_cast<int64>(max_gap));
  }

  // special case: if the patch gap width is greater than 256, then
  // we need 9 bits to encode the gap width. But we only have 3 bits in
  // header to record the gap width. To deal with this case, we will save
  // two entries in patch list in the following way
  // 256 gap width => 0 for patch value
  // actual gap - 256 => actual patch value
  // We will do the same for gap width = 511. If the element to be patched is
  // the last element in the scope then gap width will be 511. In this case we
  // will have 3 entries in the patch list in the following way
  // 255 gap width => 0 for patch value
  // 255 gap width => 0 for patch value
  // 1 gap width => actual patch value
  if (pb_ctx->patch_gap_width > 8) {
    pb_ctx->patch_gap_width = 8;
    // for gap = 511, we need two additional entries in patch list
    if (max_gap == 511) {
      pb_ctx->patch_len += 2;
    } else {
      pb_ctx->patch_len += 1;
    }
  }

  // create gap vs patch list
  gap_idx = 0;
  patch_idx = 0;
  for (size_t i = 0; i < pb_ctx->patch_len; i++) {
    int64 g = gap_list[gap_idx++];
    int64 p = patch_list[patch_idx++];
    while (g > 255) {
      pb_ctx->gap_sign_patch_list[pb_ctx->gap_sign_patch_list_count++] =
          (255L << pb_ctx->patch_width);
      i++;
      g -= 255;
    }

    // store patch value in LSBs and gap in MSBs
    pb_ctx->gap_sign_patch_list[pb_ctx->gap_sign_patch_list_count++] =
        ((g << pb_ctx->patch_width) | p);
  }
}

void PaxOrcEncoder::TreatPatchedBase() {
  // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
  // because patch is applied to MSB bits. For example: If fixed bit width of
  // base value is 7 bits and if patch is 3 bits, the actual value is
  // constructed by shifting the patch to left by 7 positions.
  // actual_value = patch << 7 | base_value
  // So, if we align base_value then actual_value can not be reconstructed.
  size_t data_lens = data_buffer_->UnTreated() / sizeof(int64);
  auto pb_ctx = encoder_context_.pb_ctx;

  PreparePatchedBlob();

  // write the number of fixed bits required in next 5 bits
  const uint32 efb = EncodeBits(pb_ctx->hist_bits_95_p) << 1;
  // adjust variable run length, they are one off
  uint16 var_len = encoder_context_.var_len - 1;

  // extract the 9th bit of run length
  const uint32 tail_bits = (var_len & 0x100) >> 8;

  // create first byte of the header
  const char first_byte =
      static_cast<char>((EncodingType::kPatchedBase << 6) | efb | tail_bits);

  // second byte of the header stores the remaining 8 bits of runlength
  const char second_byte = static_cast<char>(var_len & 0xff);

  // if the min value is negative toggle the sign
  const bool neg = (pb_ctx->min < 0);
  if (neg) {
    pb_ctx->min = -pb_ctx->min;
  }

  // find the number of bytes required for base and shift it by 5 bits
  // to accommodate patch width. The additional bit is used to store the sign
  // of the base value.
  const uint32 base_width = FindClosestBits(pb_ctx->min) + 1;
  const uint32 base_bytes =
      base_width % 8 == 0 ? base_width / 8 : (base_width / 8) + 1;
  const uint32 bb = (base_bytes - 1) << 5;

  // if the base value is negative then set MSB to 1
  if (neg) {
    pb_ctx->min |= (1LL << ((base_bytes * 8) - 1));
  }

  // third byte contains 3 bits for number of bytes occupied by base
  // and 5 bits for patch_width
  const char third_byte =
      static_cast<char>(bb | EncodeBits(pb_ctx->patch_width));

  // fourth byte contains 3 bits for page gap width and 5 bits for
  // patch length
  const char fourth_byte =
      static_cast<char>((pb_ctx->patch_gap_width - 1) << 5 | pb_ctx->patch_len);

  auto result_buffer = result_buffer_.get();
  // In fact, Worst case will not write more than header(4) + UnTreated()
  // But the bytes of writes is not easy to estimate
  while (result_buffer->Available() <
         static_cast<size_t>((4) + data_buffer_->UnTreated())) {
    result_buffer->ReSize(result_buffer->Capacity() * 2);
  }

  // write header
  result_buffer->Write(first_byte);
  result_buffer->Brush(1);
  result_buffer->Write(second_byte);
  result_buffer->Brush(1);
  result_buffer->Write(third_byte);
  result_buffer->Brush(1);
  result_buffer->Write(fourth_byte);
  result_buffer->Brush(1);

  // write the base value using fixed bytes in big endian order
  for (auto i = static_cast<int32>(base_bytes - 1); i >= 0; i--) {
    char b = static_cast<char>(((pb_ctx->min >> (i * 8)) & 0xff));
    result_buffer->Write(b);
    // TODO(jiaqizho): brush out loop
    result_buffer->Brush(1);
  }

  // base reduced literals are bit packed
  uint32 closest_bits = GetClosestBits(pb_ctx->hist_bits_95_p);

  WriteLongs(result_buffer, pb_ctx->base_patch_buffer, 0, data_lens,
             closest_bits);

  // write patch list
  closest_bits = GetClosestBits(pb_ctx->patch_gap_width + pb_ctx->patch_width);

  WriteLongs(result_buffer, pb_ctx->gap_sign_patch_list, 0, pb_ctx->patch_len,
             closest_bits);
}

bool PaxOrcEncoder::TreatDelta() {
  bool reset_fix = true;
  auto delta_ctx = encoder_context_.delta_ctx;
  uint32 len = 0;
  uint32 fb = delta_ctx->bits_delta_max;
  uint32 efb = 0;

  fb = GetClosestAlignedBits(fb);
  size_t data_lens = data_buffer_->UnTreated() / sizeof(int64);

  if (delta_ctx->is_fixed_delta) {
    // if fixed run length is greater than threshold then it will be fixed
    // delta sequence with delta value 0 else fixed delta sequence with
    // non-zero delta value
    if (encoder_context_.fixed_len > ORC_MIN_REPEAT) {
      // ex. sequence: 2 2 2 2 2 2 2 2
      len = encoder_context_.fixed_len - 1;
      reset_fix = true;
    } else {
      // ex. sequence: 4 6 8 10 12 14 16
      len = encoder_context_.var_len - 1;
      reset_fix = false;
    }
  } else {
    if (fb == 1) {
      fb = 2;
    }
    efb = EncodeBits(fb) << 1;
    len = encoder_context_.var_len - 1;
    reset_fix = false;
  }

  const uint32 tail_bits = (len & 0x100) >> 8;

  // create first byte of the header
  const char first_byte =
      static_cast<char>((EncodingType::kDelta << 6) | efb | tail_bits);

  // second byte of the header stores the remaining 8 bits of runlength
  const char second_byte = static_cast<char>(len & 0xff);

  auto result_buffer = result_buffer_.get();
  // In fact, Worst case will not write more than header(2) + UnTreated()
  // But the bytes of writes is not easy to estimate
  while (result_buffer->Available() <
         static_cast<size_t>((2) + data_buffer_->UnTreated())) {
    result_buffer->ReSize(result_buffer->Capacity() * 2);
  }

  result_buffer->Write(first_byte);
  result_buffer->Brush(1);
  result_buffer->Write(second_byte);
  result_buffer->Brush(1);

  if (encoder_context_.is_sign) {
    WriteSignedLong(result_buffer, (*data_buffer_)[0]);
  } else {
    WriteUnsignedLong(result_buffer, (*data_buffer_)[0]);
  }

  if (delta_ctx->is_fixed_delta) {
    WriteSignedLong(result_buffer, delta_ctx->fixed_delta_val);
  } else {
    // store the first value as delta value using zigzag encoding
    WriteSignedLong(result_buffer, delta_ctx->adj_deltas[0]);

    // adjacent delta values are bit packed. The length of adj_deltas array is
    // always one less than the number of literals (delta difference for n
    // elements is n-1). We have already written one element, write the
    // remaining data_lens - 2 elements here
    WriteLongs(result_buffer, delta_ctx->adj_deltas, 1, data_lens - 2, fb);
  }

  return reset_fix;
}

}  // namespace pax
