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
 * pax_vec_bitpacked_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_bitpacked_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_vec_bitpacked_column.h"

namespace pax {

#define BYTES_TO_BITS 8
#define RESIZE_REQUIRE_BYTES 8

PaxVecBitPackedColumn::PaxVecBitPackedColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxVecEncodingColumn(capacity, encoding_option), flat_buffer_(nullptr) {
  bitmap_raw_.bitmap = (uint8 *)data_->Start();
  bitmap_raw_.size = data_->Capacity() * BYTES_TO_BITS;
}

PaxVecBitPackedColumn::PaxVecBitPackedColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecEncodingColumn(capacity, decoding_option), flat_buffer_(nullptr) {}

void PaxVecBitPackedColumn::Set(std::shared_ptr<DataBuffer<int8>> data, size_t non_null_rows) {
  PaxVecEncodingColumn::Set(data, non_null_rows);
  bitmap_raw_.bitmap = (uint8 *)data_->Start();
  bitmap_raw_.size = data_->Used() * BYTES_TO_BITS;
}

void PaxVecBitPackedColumn::CheckExpandBitmap() {
  Assert(data_->Capacity() >= RESIZE_REQUIRE_BYTES);
  if (data_->Available() == 0) {
    data_->ReSize(data_->Used() + RESIZE_REQUIRE_BYTES, 2);
    bitmap_raw_.bitmap = (uint8 *)data_->Start();
    bitmap_raw_.size = data_->Capacity() * BYTES_TO_BITS;
  }
}

void PaxVecBitPackedColumn::AppendNull() {
  PaxColumn::AppendNull();
  CheckExpandBitmap();

  bitmap_raw_.Clear(total_rows_ - 1);
  if (total_rows_ % BYTES_TO_BITS == 1) {
    data_->Brush(1);
  }
}

void PaxVecBitPackedColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);

  CheckExpandBitmap();

  // check buffer
  if (*buffer) {
    bitmap_raw_.Set(total_rows_ - 1);
  } else {
    bitmap_raw_.Clear(total_rows_ - 1);
  }

  if (total_rows_ % BYTES_TO_BITS == 1) {
    data_->Brush(1);
  }
}

std::pair<char *, size_t> PaxVecBitPackedColumn::GetBuffer(size_t position) {
  CBDB_CHECK(position < GetRows(), cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                 position, GetRows(), DebugString().c_str()));
  if (!flat_buffer_) {
    flat_buffer_ = std::make_unique<DataBuffer<bool>>(non_null_rows_ * sizeof(bool));
  }
  Assert(flat_buffer_->Available() >= sizeof(bool));
  flat_buffer_->Write(bitmap_raw_.Test(position));
  auto buffer = (char *)flat_buffer_->GetAvailableBuffer();
  flat_buffer_->Brush(sizeof(bool));
  return std::make_pair(buffer, sizeof(bool));
}

Datum PaxVecBitPackedColumn::GetDatum(size_t position) {
  Assert(position < GetRows());
  if (!flat_buffer_) {
    flat_buffer_ = std::make_unique<DataBuffer<bool>>(non_null_rows_ * sizeof(bool));
  }
  Assert(flat_buffer_->Available() >= sizeof(bool));
  flat_buffer_->Write(bitmap_raw_.Test(position));
  auto buffer = (char *)flat_buffer_->GetAvailableBuffer();
  flat_buffer_->Brush(sizeof(bool));
  return cbdb::Int8ToDatum(*reinterpret_cast<int8 *>(buffer));
}

PaxColumnTypeInMem PaxVecBitPackedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeVecBitPacked;
}

}  // namespace pax
