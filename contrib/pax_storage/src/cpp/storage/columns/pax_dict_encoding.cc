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
 * pax_dict_encoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_dict_encoding.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_dict_encoding.h"

namespace pax {

PaxDictEncoder::PaxDictEncoder(const EncodingOption &encoder_options)
    : PaxEncoder(encoder_options), flushed_(false) {}

size_t PaxDictEncoder::AppendInternal(char *data, size_t len) {
  auto ret = dict_.insert({DictEntry(data, len), dict_.size()});
  if (ret.second) {
    data_holder_.push_back(std::vector<char>(len));
    memcpy(data_holder_.back().data(), data, len);

    // update dictionary entry to link pointer to internal storage
    auto entry = const_cast<DictEntry *>(&(ret.first->first));
    entry->data = data_holder_.back().data();
  }
  return ret.first->second;
}

void PaxDictEncoder::Append(char *data, size_t len) {
  size_t index;

  CBDB_CHECK(!flushed_, cbdb::CException::ExType::kExTypeLogicError);

  Assert(result_buffer_);
  Assert(result_buffer_->Capacity() >= sizeof(int32));
  if (result_buffer_->Available() == 0) {
    result_buffer_->ReSize(result_buffer_->Used() + sizeof(int32), 2);
  }

  index = AppendInternal(data, len);
  result_buffer_->Write((char *)&index, sizeof(int32));
  result_buffer_->Brush(sizeof(int32));
}

bool PaxDictEncoder::SupportAppendNull() const { return true; }

void PaxDictEncoder::Flush() {
  PaxDictHead head;
  uint64 total_len;
  std::vector<const DictEntry *> reorder_dict;

  if (flushed_) {
    return;
  }

  head.indexsz = result_buffer_->Used();
  head.dictsz = 0;
  head.dict_descsz = dict_.size() * sizeof(int32);
  reorder_dict.resize(dict_.size());

  // reorder the `dict` which will index be right
  // because the for each order in std::map is not by insert order
  for (const auto &it : dict_) {
    head.dictsz += it.first.len;
    reorder_dict[it.second] = &(it.first);
  }

  total_len = head.indexsz + head.dictsz + head.dict_descsz +
              sizeof(struct PaxDictHead);

  if (result_buffer_->Capacity() < total_len) {
    result_buffer_->ReSize(total_len);
  }

  for (auto entry : reorder_dict) {
    result_buffer_->Write(entry->data, entry->len);
    result_buffer_->Brush(entry->len);
  }

  Assert(result_buffer_->Used() == head.indexsz + head.dictsz);
  for (auto entry : reorder_dict) {
    result_buffer_->Write((char *)&(entry->len), sizeof(int32));
    result_buffer_->Brush(sizeof(int32));
  }

  Assert(result_buffer_->Used() ==
         head.indexsz + head.dictsz + head.dict_descsz);

  result_buffer_->Write((char *)&head, sizeof(struct PaxDictHead));
  result_buffer_->Brush(sizeof(struct PaxDictHead));

  Assert(result_buffer_->Used() == total_len);
  flushed_ = true;
}

PaxDictDecoder::PaxDictDecoder(
    const PaxDecoder::DecodingOption &encoder_options)
    : PaxDecoder(encoder_options),
      data_buffer_(nullptr),
      result_buffer_(nullptr) {}

PaxDictDecoder::~PaxDictDecoder() { }

PaxDecoder *PaxDictDecoder::SetSrcBuffer(char *data, size_t data_len) {
  if (data) {
    data_buffer_ = std::make_shared<DataBuffer<char>>(data, data_len, false, false);
  }
  return this;
}

PaxDecoder *PaxDictDecoder::SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer) {
  result_buffer_ = result_buffer;
  return this;
}

const char *PaxDictDecoder::GetBuffer() const {
  return result_buffer_->GetBuffer();
}

size_t PaxDictDecoder::GetBufferSize() const { return result_buffer_->Used(); }

size_t PaxDictDecoder::Next(const char * /*not_null*/) {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements);
}

size_t PaxDictDecoder::Decoding() {
  PaxDictHead head;
  std::vector<int32> offsets;
  int32 index, offset;
  char *buffer;

  if (!data_buffer_) {
    return 0;
  }

  Assert(result_buffer_);
  Assert(data_buffer_->Capacity() >= sizeof(struct PaxDictHead));

  memcpy(&head,
         data_buffer_->GetBuffer() + data_buffer_->Capacity() -
             sizeof(struct PaxDictHead),
         sizeof(struct PaxDictHead));

  buffer = data_buffer_->GetBuffer();

  DataBuffer<int32> index_buffer((int32 *)buffer, head.indexsz, false, false);
  index_buffer.BrushAll();

  DataBuffer<int32> desc_buffer((int32 *)(buffer + head.indexsz + head.dictsz),
                                 head.dict_descsz, false, false);
  desc_buffer.BrushAll();

  for (size_t i = 0; i < desc_buffer.GetSize(); i++) {
    offsets.emplace_back(i == 0 ? 0 : offsets[i - 1] + (desc_buffer)[i - 1]);
  }

  for (size_t i = 0; i < index_buffer.GetSize(); i++) {
    index = (index_buffer)[i];

    CBDB_CHECK(index >= 0 && (size_t)index < desc_buffer.GetSize(),
               cbdb::CException::kExTypeOutOfRange);

    offset = offsets[index] + head.indexsz;
    result_buffer_->Write(buffer + offset, (desc_buffer)[index]);
    result_buffer_->Brush((desc_buffer)[index]);
  }

  return data_buffer_->Used();
}

size_t PaxDictDecoder::Decoding(const char * /*not_null*/,
                                size_t /*not_null_len*/) {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements);
}

}  // namespace pax
