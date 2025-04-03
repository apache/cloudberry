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
 * pax_vec_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_vec_column.h"

#include "comm/pax_memory.h"
#include "storage/toast/pax_toast.h"

namespace pax {

template <typename T>
PaxVecCommColumn<T>::PaxVecCommColumn(uint32 capacity) {
  data_ = std::make_shared<DataBuffer<T>>(
      TYPEALIGN(MEMORY_ALIGN_SIZE, capacity * sizeof(T)));
}

template <typename T>
PaxVecCommColumn<T>::~PaxVecCommColumn() {}

template <typename T>  // NOLINT: redirect constructor
PaxVecCommColumn<T>::PaxVecCommColumn() : PaxVecCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxVecCommColumn<T>::Set(std::shared_ptr<DataBuffer<T>> data,
                              size_t non_null_rows) {
  data_ = std::move(data);
  non_null_rows_ = non_null_rows;
}

template <typename T>
void PaxVecCommColumn<T>::AppendInternal(char *buffer, size_t size) {  // NOLINT
  auto buffer_t = reinterpret_cast<T *>(buffer);
  Assert(size % sizeof(T) == 0);
  Assert(data_->Capacity() >= sizeof(T));

  if (data_->Available() == 0) {
    data_->ReSize(data_->Capacity() * 2);
  }

  data_->Write(buffer_t, sizeof(T));
  data_->Brush(sizeof(T));
}

template <typename T>
void PaxVecCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  AppendInternal(buffer, size);
}

static char null_buffer[sizeof(int64)] = {0};

template <typename T>
void PaxVecCommColumn<T>::AppendNull() {
  PaxColumn::AppendNull();
  static_assert(sizeof(T) <= sizeof(int64), "invalid append null");
  AppendInternal(null_buffer, sizeof(T));
}

template <typename T>
void PaxVecCommColumn<T>::AppendToast(char * /*buffer*/, size_t /*size*/) {
  // fixed-column won't get toast datum
  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

template <typename T>
PaxColumnTypeInMem PaxVecCommColumn<T>::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeFixed;
}

template <typename T>
PaxStorageFormat PaxVecCommColumn<T>::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcVec;
}

template <typename T>
size_t PaxVecCommColumn<T>::PhysicalSize() const {
  return data_->Used();
}

template <typename T>
int64 PaxVecCommColumn<T>::GetOriginLength() const {
  return data_->Used();
}

template <typename T>
int64 PaxVecCommColumn<T>::GetOffsetsOriginLength() const {
  return 0;
}

template <typename T>
int32 PaxVecCommColumn<T>::GetTypeLength() const {
  return sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetBuffer(size_t position) {
  Assert(position < GetRows());
  return std::make_pair(data_->Start() + (sizeof(T) * position), sizeof(T));
}

template <typename T>
Datum PaxVecCommColumn<T>::GetDatum(size_t position) {
  Assert(position < GetRows());
  auto ptr = data_->Start() + (sizeof(T) * position);
  return (Datum)(*reinterpret_cast<T *>(ptr));
}

template <typename T>
std::pair<char *, size_t> PaxVecCommColumn<T>::GetRangeBuffer(size_t start_pos,
                                                              size_t len) {
  Assert((start_pos + len) <= GetRows());
  return std::make_pair(data_->Start() + (sizeof(T) * start_pos),
                        sizeof(T) * len);
}

template <typename T>
std::shared_ptr<DataBuffer<T>> PaxVecCommColumn<T>::GetDataBuffer() {
  return data_;
}

template class PaxVecCommColumn<char>;
template class PaxVecCommColumn<int8>;
template class PaxVecCommColumn<int16>;
template class PaxVecCommColumn<int32>;
template class PaxVecCommColumn<int64>;
template class PaxVecCommColumn<float>;
template class PaxVecCommColumn<double>;

PaxVecNonFixedColumn::PaxVecNonFixedColumn(uint32 data_capacity,
                                           uint32 offsets_capacity)
    : estimated_size_(0),
      data_(std::make_shared<DataBuffer<char>>(
          TYPEALIGN(MEMORY_ALIGN_SIZE, data_capacity))),
      offsets_(std::make_shared<DataBuffer<int32>>(offsets_capacity)),
      next_offsets_(0) {
  Assert(data_capacity % sizeof(int64) == 0);
}

PaxVecNonFixedColumn::PaxVecNonFixedColumn()
    : PaxVecNonFixedColumn(DEFAULT_CAPACITY, DEFAULT_CAPACITY) {}

PaxVecNonFixedColumn::~PaxVecNonFixedColumn() {}

void PaxVecNonFixedColumn::Set(std::shared_ptr<DataBuffer<char>> data,
                               std::shared_ptr<DataBuffer<int32>> offsets,
                               size_t total_size, size_t non_null_rows) {
  Assert(data && offsets);

  estimated_size_ = total_size;
  data_ = std::move(data);
  offsets_ = std::move(offsets);
  non_null_rows_ = non_null_rows;
  next_offsets_ = -1;
}

void PaxVecNonFixedColumn::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  // vec format will remove the val header
  // so we don't need do align with the datum

  Assert(data_->Capacity() > 0);
  if (data_->Available() < size) {
    data_->ReSize(data_->Used() + size, 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, size);
  data_->Brush(size);

  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Used() + sizeof(int32), 2);
  }

  Assert(next_offsets_ != -1);
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ += size;
}

void PaxVecNonFixedColumn::AppendNull() {
  PaxColumn::AppendNull();
  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Capacity() * 2);
  }

  Assert(next_offsets_ != -1);
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
}

void PaxVecNonFixedColumn::AppendLastOffset() {
  Assert(next_offsets_ != -1);
  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Capacity() + sizeof(int32));
  }
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ = -1;
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetOffsetBuffer(
    bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  return std::make_pair((char *)offsets_->GetBuffer(), offsets_->Used());
}

PaxColumnTypeInMem PaxVecNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeNonFixed;
}

PaxStorageFormat PaxVecNonFixedColumn::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcVec;
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetBuffer() {
  return std::make_pair(data_->GetBuffer(), data_->Used());
}

size_t PaxVecNonFixedColumn::PhysicalSize() const { return estimated_size_; }

int64 PaxVecNonFixedColumn::GetOriginLength() const { return data_->Used(); }

int64 PaxVecNonFixedColumn::GetOffsetsOriginLength() const {
  Assert(next_offsets_ == -1);
  return offsets_->Used();
}

int32 PaxVecNonFixedColumn::GetTypeLength() const { return -1; }

std::pair<char *, size_t> PaxVecNonFixedColumn::GetBuffer(size_t position) {
  Assert(position < offsets_->GetSize());
  // This situation happend when writing
  // The `offsets_` have not fill the last one
  if (unlikely(position == offsets_->GetSize() - 1)) {
    if (null_bitmap_ && null_bitmap_->Test(position)) {
      return std::make_pair(nullptr, 0);
    }
    return std::make_pair(data_->GetBuffer() + (*offsets_)[position],
                          next_offsets_);
  }

  auto start_offset = (*offsets_)[position];
  auto last_offset = (*offsets_)[position + 1];

  if (start_offset == last_offset) {
    return std::make_pair(nullptr, 0);
  }

  return std::make_pair(data_->GetBuffer() + start_offset,
                        last_offset - start_offset);
}

Datum PaxVecNonFixedColumn::GetDatum(size_t position) {
  Assert(position < GetRows());
  Datum datum;
  char *buffer;
  std::shared_ptr<MemoryObject> ref;
  size_t buffer_len;
  std::tie(buffer, buffer_len) = GetBuffer(position);

  if (IsToast(position)) {
    datum = PointerGetDatum(buffer);
    auto external_buffer = GetExternalToastDataBuffer();
    std::tie(datum, ref) =
        pax_detoast(datum, external_buffer ? external_buffer->Start() : nullptr,
                    external_buffer ? external_buffer->Used() : 0);

  } else {
    auto size = TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len + VARHDRSZ);
    ByteBuffer bb(size, size);
    auto tmp = bb.Addr();
    SET_VARSIZE(tmp, buffer_len + VARHDRSZ);
    memcpy(VARDATA(tmp), buffer, buffer_len);
    datum = PointerGetDatum(tmp);
    ref = std::make_unique<ExternalToastValue>(std::move(bb));
  }

  if (ref) {
    buffer_holders_.emplace_back(ref);
  }
  return datum;
}

std::pair<char *, size_t> PaxVecNonFixedColumn::GetRangeBuffer(size_t start_pos,
                                                               size_t len) {
  AssertImply(next_offsets_ == -1,
              (start_pos + len) <= (offsets_->GetSize() - 1));
  AssertImply(next_offsets_ != -1, (start_pos + len) <= (offsets_->GetSize()));

  // same as (start_pos + len - 1 == offsets_->GetSize() - 1)
  if (unlikely(start_pos + len == offsets_->GetSize())) {
    Assert(next_offsets_ != -1);
    if (offsets_->GetSize() == 0) {  // all null in write
      Assert(len == 0);
      return std::make_pair(data_->GetBuffer(), 0);
    } else {
      Assert(next_offsets_ != 0);
      return std::make_pair(data_->GetBuffer() + (*offsets_)[start_pos],
                            next_offsets_ - (*offsets_)[start_pos]);
    }
  }

  return std::make_pair(data_->GetBuffer() + (*offsets_)[start_pos],
                        (*offsets_)[start_pos + len] - (*offsets_)[start_pos]);
}

std::shared_ptr<DataBuffer<char>> PaxVecNonFixedColumn::GetDataBuffer() {
  return data_;
}

}  // namespace pax
