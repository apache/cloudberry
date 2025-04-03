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
 * pax_vec_bpchar_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_bpchar_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_vec_bpchar_column.h"

namespace pax {

#define NUMBER_OF_CHAR_KEY "CHAR_N_KEY"
#define NUMBER_OF_CHAR_UNINIT -1

PaxVecBpCharColumn::PaxVecBpCharColumn(
    uint32 capacity, uint32 offsets_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxVecNonFixedEncodingColumn(capacity, offsets_capacity, encoder_options),
      number_of_char_(NUMBER_OF_CHAR_UNINIT) {}

PaxVecBpCharColumn::PaxVecBpCharColumn(
    uint32 capacity, uint32 offsets_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecNonFixedEncodingColumn(capacity, offsets_capacity, decoding_option),
      number_of_char_(NUMBER_OF_CHAR_UNINIT) {}

PaxVecBpCharColumn::~PaxVecBpCharColumn() {}

void PaxVecBpCharColumn::Append(char *buffer, size_t size) {
  if (number_of_char_ == NUMBER_OF_CHAR_UNINIT) {
#ifdef ENABLE_DEBUG
    if (HasAttributes()) {
      auto ret = GetAttribute(NUMBER_OF_CHAR_KEY);
      if (ret.second) {
        auto value = atoll(ret.first.c_str());
        Assert(size == (size_t)value);
      }
    }
#endif

    // no need pass the n (which from `char(n)`)
    // we can direct get n from size
    number_of_char_ = size;
    PutAttribute(NUMBER_OF_CHAR_KEY, std::to_string(number_of_char_));
  }
  Assert(size == (size_t)number_of_char_);
  auto real_len = bpchartruelen(buffer, size);
  Assert(real_len <= (int)size);
  PaxVecNonFixedEncodingColumn::Append(buffer, real_len);
}

std::pair<char *, size_t> PaxVecBpCharColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < offsets_->GetSize(),
             cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                 position, offsets_->GetSize(), DebugString().c_str()));

  if (number_of_char_ == NUMBER_OF_CHAR_UNINIT) {
    auto attr = GetAttribute(NUMBER_OF_CHAR_KEY);
    CBDB_CHECK(attr.second, cbdb::CException::ExType::kExTypeInvalidPORCFormat,
               fmt("No set the %s in attribute, \n %s", NUMBER_OF_CHAR_KEY,
                   DebugString().c_str()));
    Assert(attr.first.length() > 0);
    number_of_char_ = atoll(attr.first.c_str());
    CBDB_CHECK(number_of_char_ > 0,
               cbdb::CException::ExType::kExTypeInvalidPORCFormat,
               fmt("Invalid %s in attribute [value=%ld], \n %s",
                   NUMBER_OF_CHAR_KEY, number_of_char_, DebugString().c_str()));
  }

  auto pair = PaxVecNonFixedColumn::GetBuffer(position);
  Assert(pair.second <= (size_t)number_of_char_);
  if (pair.second < (size_t)number_of_char_) {
    ByteBuffer buff(number_of_char_, number_of_char_);
    auto bpchar_buff = reinterpret_cast<char *>(buff.Addr());
    memcpy(bpchar_buff, pair.first, pair.second);
    memset(bpchar_buff + pair.second, ' ', number_of_char_ - pair.second);
    bpchar_holder_.emplace_back(std::move(buff));

    return {bpchar_buff, number_of_char_};
  }

  return pair;
}

PaxColumnTypeInMem PaxVecBpCharColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeVecBpChar;
}
}  // namespace pax
