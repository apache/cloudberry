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
 * pax_encoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding.h"

#include <utility>

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_dict_encoding.h"
#include "storage/columns/pax_rlev2_encoding.h"

namespace pax {

static std::set<ColumnEncoding_Kind> non_fixed_column_white_list = {
    ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY};

std::shared_ptr<PaxEncoder> PaxEncoder::CreateStreamingEncoder(
    const EncodingOption &encoder_options, bool non_fixed) {
  std::shared_ptr<PaxEncoder> encoder;

  // non-fixed only support dict encoder
  if (non_fixed &&
      non_fixed_column_white_list.find(encoder_options.column_encode_type) ==
          non_fixed_column_white_list.end()) {
    return encoder;
  }

  switch (encoder_options.column_encode_type) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2: {
      encoder = std::make_shared<PaxOrcEncoder>(encoder_options);
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA: {
      // TODO(jiaqizho): support direct delta encoding
      // not support yet, then direct return a nullptr(means no encoding)
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                 fmt("Invalid encoding type %d",
                     ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED));
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY: {
      encoder = std::make_shared<PaxDictEncoder>(encoder_options);
      break;
    }
    // two cases here:
    //  - `encoded type` is not a encoding type.
    //  - `encoded type` is the no_encoding type.
    default: {
      // do nothing
    }
  }

  return encoder;
}

PaxEncoder::PaxEncoder(const EncodingOption &encoder_options)
    : encoder_options_(encoder_options), result_buffer_(nullptr) {}

void PaxEncoder::SetDataBuffer(std::shared_ptr<DataBuffer<char>> result_buffer) {
  Assert(!result_buffer_ && result_buffer);
  Assert(result_buffer->IsMemTakeOver());
  result_buffer_ = result_buffer;
}

char *PaxEncoder::GetBuffer() const { return result_buffer_->GetBuffer(); }

size_t PaxEncoder::GetBufferSize() const { return result_buffer_->Used(); }

}  // namespace pax
