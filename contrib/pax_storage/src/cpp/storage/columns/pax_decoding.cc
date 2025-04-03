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
 * pax_decoding.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_decoding.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_decoding.h"

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_dict_encoding.h"
#include "storage/columns/pax_rlev2_decoding.h"

namespace pax {

template <typename T>
std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder(const DecodingOption &decoder_options) {
  std::shared_ptr<PaxDecoder> decoder;
  switch (decoder_options.column_encode_type) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED: {
      // do nothing
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2: {
      decoder = std::make_shared<PaxOrcDecoder<T>>(decoder_options);
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA: {
      /// TODO(jiaqizho) support it
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY: {
      decoder = std::make_shared<PaxDictDecoder>(decoder_options);
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                 fmt("Invalid encoding type %d",
                     ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED));
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD:
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB:
    default: {
      // do nothing
    }
  }

  return decoder;
}

template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int64>(const DecodingOption &);
template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int32>(const DecodingOption &);
template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int16>(const DecodingOption &);
template std::shared_ptr<PaxDecoder> PaxDecoder::CreateDecoder<int8>(const DecodingOption &);

PaxDecoder::PaxDecoder(const DecodingOption &decoder_options)
    : decoder_options_(decoder_options) {}

}  // namespace pax
