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
 * pax_dict_encoding.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_dict_encoding.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/pax_memory.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

struct PaxDictHead {
  uint64_t indexsz;
  uint64_t dictsz;
  uint64_t dict_descsz;
};

static_assert(sizeof(struct PaxDictHead) == 24,
              "PaxDictHead is not a align structure.");

class PaxDictEncoder final : public PaxEncoder {
 public:
  explicit PaxDictEncoder(const EncodingOption &encoder_options);

  ~PaxDictEncoder() override = default;

  void Append(char *data, size_t len) override;

  bool SupportAppendNull() const override;

  void Flush() override;

 private:
  size_t AppendInternal(char *data, size_t len);

  struct DictEntry {
    DictEntry(const char *buffer, size_t length) : data(buffer), len(length) {}
    const char *data;
    size_t len;
  };

  struct LessThan {
    bool operator()(const DictEntry &left, const DictEntry &right) const {
      int ret = memcmp(left.data, right.data, std::min(left.len, right.len));

      if (ret != 0) {
        return ret < 0;
      }

      return left.len < right.len;
    }
  };

 private:
  bool flushed_;

  std::vector<std::vector<char>> data_holder_;
  std::map<DictEntry, size_t, LessThan> dict_;
};

class PaxDictDecoder final : public PaxDecoder {
 public:
  explicit PaxDictDecoder(const PaxDecoder::DecodingOption &encoder_options);

  ~PaxDictDecoder() override;

  PaxDecoder *SetSrcBuffer(char *data, size_t data_len) override;

  PaxDecoder *SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer) override;

  const char *GetBuffer() const override;

  size_t GetBufferSize() const override;

  size_t Next(const char *not_null) override;

  size_t Decoding() override;

  size_t Decoding(const char *not_null, size_t not_null_len) override;

#ifdef BUILD_RB_RET_DICT
  static std::tuple<std::shared_ptr<DataBuffer<int32>>,
                    std::shared_ptr<DataBuffer<char>>,
                    std::shared_ptr<DataBuffer<int32>>>
  GetRawDictionary(const std::shared_ptr<DataBuffer<char>> &src_buff) {
    std::shared_ptr<DataBuffer<int32>> index_buffer;
    std::shared_ptr<DataBuffer<char>> entry_buffer;
    std::shared_ptr<DataBuffer<int32>> desc_buffer;
    PaxDictHead head;
    char *buffer;

    Assert(src_buff->Capacity() >= sizeof(struct PaxDictHead));

    memcpy(
        &head,
        src_buff->GetBuffer() + src_buff->Used() - sizeof(struct PaxDictHead),
        sizeof(struct PaxDictHead));

    buffer = src_buff->GetBuffer();

    index_buffer =
        std::make_shared<DataBuffer<int32>>((int32 *)buffer, head.indexsz, false, false);
    index_buffer->BrushAll();

    desc_buffer = std::make_shared<DataBuffer<int32>>(
        (int32 *)(buffer + head.indexsz + head.dictsz), head.dict_descsz, false,
        false);
    desc_buffer->BrushAll();

    entry_buffer = std::make_shared<DataBuffer<char>>(buffer + head.indexsz, head.dictsz,
                                             false, false);
    entry_buffer->BrushAll();

    return std::make_tuple(index_buffer, entry_buffer, desc_buffer);
  }
#endif

 private:
  std::tuple<uint64, uint64, uint64> DecodeLens();

 private:
  std::shared_ptr<DataBuffer<char>> data_buffer_;
  std::shared_ptr<DataBuffer<char>> result_buffer_;
};

}  // namespace pax
