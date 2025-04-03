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
 * pax_vec_bpchar_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_bpchar_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/byte_buffer.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax {

class PaxVecBpCharColumn final : public PaxVecNonFixedEncodingColumn {
 public:
  PaxVecBpCharColumn(uint32 capacity, uint32 offsets_capacity,
                     const PaxEncoder::EncodingOption &encoder_options);

  PaxVecBpCharColumn(uint32 capacity, uint32 offsets_capacity,
                     const PaxDecoder::DecodingOption &decoding_option);

  ~PaxVecBpCharColumn();

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

 private:
  int64 number_of_char_;
  std::vector<ByteBuffer> bpchar_holder_;
};

}  // namespace pax
