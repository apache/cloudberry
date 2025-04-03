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
 * pax_vec_no_hdr_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_no_hdr_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecNoHdrColumn final : public PaxVecNonFixedEncodingColumn {
 public:
  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxEncoder::EncodingOption &encoder_options)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     encoder_options) {}

  PaxVecNoHdrColumn(uint32 data_capacity, uint32 length_capacity,
                    const PaxDecoder::DecodingOption &decoding_option)
      : PaxVecNonFixedEncodingColumn(data_capacity, length_capacity,
                                     decoding_option) {}

  Datum GetDatum(size_t position) override {
    CBDB_CHECK(position < offsets_->GetSize(),
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                   position, GetRows(), DebugString().c_str()));
    // This situation happend when writing
    // The `offsets_` have not fill the last one
    if (unlikely(position == offsets_->GetSize() - 1)) {
      if (null_bitmap_ && null_bitmap_->Test(position)) {
        return PointerGetDatum(nullptr);
      }
      return PointerGetDatum(data_->GetBuffer() + (*offsets_)[position]);
    }

    auto start_offset = (*offsets_)[position];
    auto last_offset = (*offsets_)[position + 1];

    if (start_offset == last_offset) {
      return PointerGetDatum(nullptr);
    }

    return PointerGetDatum(data_->GetBuffer() + start_offset);
  }

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override {
    return PaxColumnTypeInMem::kTypeVecNoHeader;
  }
};

}  // namespace pax
