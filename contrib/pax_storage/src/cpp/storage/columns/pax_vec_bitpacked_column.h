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
 * pax_vec_bitpacked_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_bitpacked_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecBitPackedColumn final : public PaxVecEncodingColumn<int8> {
 public:
  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxEncoder::EncodingOption &encoding_option);

  PaxVecBitPackedColumn(uint32 capacity,
                        const PaxDecoder::DecodingOption &decoding_option);

  void Set(std::shared_ptr<DataBuffer<int8>> data, size_t non_null_rows) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  void AppendNull() override;

  void Append(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  Datum GetDatum(size_t position) override;

 private:
  void CheckExpandBitmap();

 private:
  BitmapRaw<uint8> bitmap_raw_;
  std::unique_ptr<DataBuffer<bool>> flat_buffer_;
};
}  // namespace pax
