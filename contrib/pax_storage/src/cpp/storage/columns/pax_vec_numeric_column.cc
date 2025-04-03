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
 * pax_vec_numeric_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_numeric_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_vec_numeric_column.h"

#include "comm/vec_numeric.h"

namespace pax {

#define NUMERIC_SHORT_HDRSZ sizeof(uint16)

PaxShortNumericColumn::PaxShortNumericColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxVecEncodingColumn(capacity, encoding_option),
      already_combined_(false),
      width_(VEC_SHORT_NUMERIC_STORE_BYTES) {
  Assert(capacity >= VEC_SHORT_NUMERIC_STORE_BYTES);
}

PaxShortNumericColumn::PaxShortNumericColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecEncodingColumn(capacity, decoding_option),
      already_combined_(false),
      width_(VEC_SHORT_NUMERIC_STORE_BYTES) {}

PaxColumnTypeInMem PaxShortNumericColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeVecDecimal;
}

PaxShortNumericColumn::~PaxShortNumericColumn() { }

void PaxShortNumericColumn::AppendNull() {
  static char null_buffer[sizeof(int64) * 2] = {0};

  PaxColumn::AppendNull();

  // Check that null_buffer is large enough
  Assert(width_ * sizeof(int8) <= 2 * sizeof(int64));
  Assert(data_->Capacity() >= width_);

  if (data_->Available() < width_) {
    data_->ReSize(data_->Used() + width_, 2);
  }

  data_->Write((int8 *)null_buffer, width_);
  data_->Brush(width_);
}

void PaxShortNumericColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  struct varlena *vl;
  size_t vl_len;
  Numeric numeric;

  Assert(data_->Capacity() >= width_);
  if (data_->Available() < width_) {
    data_->ReSize(data_->Used() + width_, 2);
  }

  vl = (struct varlena *)DatumGetPointer(buffer);
  vl_len = VARSIZE_ANY_EXHDR(vl);

  numeric = cbdb::DatumToNumeric(PointerGetDatum(buffer));

  int8 *dest_buff = data_->GetAvailableBuffer();
  pg_short_numeric_to_vec_short_numeric(numeric, vl_len, (int64 *)dest_buff,
                                        (int64 *)(dest_buff + sizeof(int64)));
  data_->Brush(width_);
}

std::pair<char *, size_t> PaxShortNumericColumn::GetRangeBuffer(
    size_t start_pos, size_t len) {
  CBDB_CHECK((start_pos + len) <= GetRows(),
             cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("Fail to get buffer [pos=%lu, len=%lu, total rows=%lu], \n %s",
                 start_pos, len, GetRows(), DebugString().c_str()));

  return std::make_pair(data_->Start() + (width_ * start_pos), width_ * len);
}

std::pair<char *, size_t> PaxShortNumericColumn::GetBuffer(size_t position) {
  int8 *raw_buffer;
  Datum datum;
  struct varlena *vl;

  CBDB_CHECK(position < GetRows(), cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                 position, GetRows(), DebugString().c_str()));

  Assert(data_->Used() > position * width_);

  raw_buffer = data_->GetBuffer() + (position * width_);
  datum = vec_short_numeric_to_datum((int64 *)raw_buffer,
                                     (int64 *)(raw_buffer + sizeof(int64)));
  Assert(datum != 0);

  numeric_holder_.emplace_back(cbdb::DatumToNumeric(datum));
  vl = (struct varlena *)DatumGetPointer(datum);
  return {DatumGetPointer(datum), VARSIZE_ANY_EXHDR(vl) + VARHDRSZ};
}

Datum PaxShortNumericColumn::GetDatum(size_t position) {
  Assert(position < GetRows());
  int8 *raw_buffer;
  Datum datum;

  CBDB_CHECK(position < GetRows(), cbdb::CException::ExType::kExTypeOutOfRange,
             fmt("Fail to get buffer [pos=%lu, total rows=%lu], \n %s",
                 position, GetRows(), DebugString().c_str()));

  Assert(data_->Used() > position * width_);

  raw_buffer = data_->GetBuffer() + (position * width_);
  datum = vec_short_numeric_to_datum((int64 *)raw_buffer,
                                     (int64 *)(raw_buffer + sizeof(int64)));
  Assert(datum != 0);

  numeric_holder_.emplace_back(cbdb::DatumToNumeric(datum));
  return datum;
}

std::shared_ptr<DataBuffer<int8>> PaxShortNumericColumn::GetDataBuffer() { return data_; }

int32 PaxShortNumericColumn::GetTypeLength() const {
  return VEC_SHORT_NUMERIC_STORE_BYTES;
}

}  // namespace pax
