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
 * pax_rlev2_encoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_rlev2_encoding.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

class PaxOrcEncoder final : public PaxEncoder {
 public:
  explicit PaxOrcEncoder(const EncodingOption &encoder_options);

  ~PaxOrcEncoder() override;

  void Append(char *data, size_t size) override;

  bool SupportAppendNull() const override;

  void Flush() override;

 private:
  struct EncoderContext {
    bool is_sign;

    // repeat lengths
    uint16 fixed_len;

    // non-repeat lengths
    uint16 var_len;

    int64 prev_delta;
    int64 current_delta;

    EncoderContext();
    ~EncoderContext();

    struct DeltaContext;
    struct DirectContext;
    struct PatchBaseContext;

    struct DeltaContext *delta_ctx;
    struct DirectContext *direct_ctx;
    struct PatchBaseContext *pb_ctx;

    inline void ResetDirectCtx() const;
    inline void ResetDeltaCtx() const;
    inline void ResetPbCtx() const;

   private:
    char *internal_buffer_;
  };

  enum EncoderStatus {
    // current encoder have been flushed or no init
    kInvalid = 0,
    // no elements in buffer, accept the first element
    kInit,
    // 1 element in buffer, accept the second element
    kTwoElements,
    // at lease 2 elements in buffer
    kUntreated,
    // at lease `ORC_MIN_REPEAT` repeating elements change to non-repeating
    // elements
    kUntreatedDiscontinuous,
    // non-repeating elements change to `ORC_MIN_REPEAT` repeating elements
    kTreatPrevBuffer,
    // flush all buffer
    kFlush,
    // treat the non-repeating buffer which is before repeating datas
    kDetermineFlushPrevBuffer,
    // treat the buffer which belongs to the Short-Repeat rule
    kTreatShortRepeat,
    // treat the buffer which can deal with other types
    kTreatDirect,
    // treat the buffer which belongs to the Patched-Base rule
    kTreatPatchedBase,
    // treat the buffer which belongs to the Delta rule
    kTreatDelta,
    // done with treat or flush buffer
    kTreatDone,
    // all done, will change to invalid
    kFinish,
  };

 private:
  void AppendInternal(int64 data, bool is_flush);

  void AppendData(int64 data);

  void SwitchStatusTo(EncoderStatus new_status);

  void TreatShortRepeat();

  void TreatDirect();

  void PreparePatchedBlob();

  void TreatPatchedBase();

  bool TreatDelta();

 private:
  EncoderContext encoder_context_;
  std::shared_ptr<UntreatedDataBuffer<int64>> data_buffer_;
  std::shared_ptr<DataBuffer<int64>> zigzag_buffer_;
  EncoderStatus status_;
};

#ifdef RUN_GTEST
void WriteLongs(DataBuffer<char> *data_buffer, const int64 *input,
                uint32 offset, size_t len, uint32 bits);
#endif

}  // namespace pax
