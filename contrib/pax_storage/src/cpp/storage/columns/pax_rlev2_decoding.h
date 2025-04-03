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
 * pax_rlev2_decoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_rlev2_decoding.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "comm/cbdb_wrappers.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"

namespace pax {

template <typename T>
class PaxOrcDecoder final : public PaxDecoder {
 public:
  explicit PaxOrcDecoder(const PaxDecoder::DecodingOption &encoder_options);

  ~PaxOrcDecoder() override;

  PaxDecoder *SetSrcBuffer(char *data, size_t data_len) override;

  PaxDecoder *SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer) override;

  const char *GetBuffer() const override;

  size_t GetBufferSize() const override;

  size_t Next(const char *not_null) override;

  size_t Decoding() override;

  size_t Decoding(const char *not_null, size_t not_null_len) override;

 private:
  uint64 NextShortRepeats(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, T *data,
                          uint64 offset, const char *not_null);
  uint64 NextDirect(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, T *data,
                    uint64 offset, const char *not_null);
  uint64 NextPatched(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, T *data,
                     uint64 offset, const char *not_null);
  uint64 NextDelta(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, T *data,
                   uint64 offset, const char *not_null);

 private:
  std::shared_ptr<TreatedDataBuffer<int64>> data_buffer_;
  // Used to fill null field
  std::shared_ptr<DataBuffer<int64>> copy_data_buffer_;
  // Used by PATCHED_BASE
  std::shared_ptr<DataBuffer<int64>> unpacked_data_;
  // result buffer
  std::shared_ptr<DataBuffer<char>> result_buffer_;
};

extern template class PaxOrcDecoder<int64>;
extern template class PaxOrcDecoder<int32>;
extern template class PaxOrcDecoder<int16>;
extern template class PaxOrcDecoder<int8>;

#ifdef RUN_GTEST
template <typename T>
void ReadLongs(TreatedDataBuffer<int64> *data_buffer, T *data, uint64 offset,
               uint64 len, uint64 fbs, uint32 *bits_left);
template <typename T>
void ReadLongs(const std::shared_ptr<TreatedDataBuffer<int64>> &data_buffer, T *data, uint64 offset,
               uint64 len, uint64 fbs, uint32 *bits_left) {
    ReadLongs(data_buffer.get(), data, offset, len, fbs, bits_left);
}
                 
#endif

}  // namespace pax
