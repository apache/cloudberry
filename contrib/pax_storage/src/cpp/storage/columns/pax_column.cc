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
 * pax_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_column.h"

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "storage/columns/pax_column_traits.h"
#include "storage/pax_defined.h"
#include "storage/toast/pax_toast.h"

namespace pax {

PaxColumn::PaxColumn()
    : null_bitmap_(nullptr),
      total_rows_(0),
      non_null_rows_(0),
      encoded_type_(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
      compress_level_(0),
      offsets_encoded_type_(
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED),
      offsets_compress_level_(0),
      type_align_size_(PAX_DATA_NO_ALIGN),
      toast_indexes_(nullptr),
      toast_flat_map_(nullptr),
      numeber_of_external_toast_(0),
      external_toast_data_(nullptr) {}

PaxColumn::~PaxColumn() {}

PaxColumnTypeInMem PaxColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeInvalid;
}

void PaxColumn::SetAttributes(const std::map<std::string, std::string> &attrs) {
  attrs_map_ = attrs;
}

const std::map<std::string, std::string> &PaxColumn::GetAttributes() const {
  return attrs_map_;
}

size_t PaxColumn::GetRows() const { return total_rows_; }

size_t PaxColumn::GetNonNullRows() const { return non_null_rows_; }

size_t PaxColumn::GetRangeNonNullRows(size_t start_pos, size_t len) {
  Assert((start_pos + len) <= GetRows());
  if (!null_bitmap_) return len;
  if (len == 0) {
    return 0;
  }
  return null_bitmap_->CountBits(start_pos, start_pos + len - 1);
}

void PaxColumn::CreateNulls(size_t cap) {
  Assert(!null_bitmap_);
  null_bitmap_ = std::make_unique<Bitmap8>(cap);
  null_bitmap_->SetN(total_rows_);
}

void PaxColumn::AppendNull() {
  if (!null_bitmap_) {
    CreateNulls(DEFAULT_CAPACITY);
  }
  null_bitmap_->Clear(total_rows_);
  ++total_rows_;
}

void PaxColumn::AppendToast(char *buffer, size_t size) {
  Assert(VARATT_IS_PAX_SUPPORT_TOAST(buffer));
  if (VARATT_IS_PAX_EXTERNAL_TOAST(buffer)) {
    auto ref = PaxExtRefGetDatum(buffer);
    Assert(ref->data_ref);
    Assert(ref->data_size > 0);
    auto off = AppendExternalToastData(ref->data_ref, ref->data_size);
    PAX_VARATT_EXTERNAL_SET_OFFSET(ref, off);
  }

  Append(buffer, size);
  Assert(total_rows_ > 0);
  AddToastIndex(total_rows_ - 1);
}

void PaxColumn::Append(char * /*buffer*/, size_t /*size*/) {
  if (null_bitmap_) null_bitmap_->Set(total_rows_);
  ++total_rows_;
  ++non_null_rows_;
}

void PaxColumn::SetAlignSize(size_t align_size) {
  Assert(align_size > 0 && (align_size & (align_size - 1)) == 0);
  type_align_size_ = align_size;
}

size_t PaxColumn::GetAlignSize() const { return type_align_size_; }

size_t PaxColumn::ToastCounts() {
  if (!toast_indexes_) {
    return 0;
  }

  Assert(toast_indexes_->Used() > 0);
  return toast_indexes_->GetSize();
}

void PaxColumn::SetToastIndexes(
    std::shared_ptr<DataBuffer<int32>> toast_indexes) {
  Assert(!toast_indexes_ && !toast_flat_map_);
  toast_indexes_ = toast_indexes;
  Assert(total_rows_ > 0 && toast_indexes->Used() > 0);
  toast_flat_map_ = std::make_shared<Bitmap8>(total_rows_ / 8 + 1);
  toast_flat_map_->SetN(total_rows_);

  for (size_t i = 0; i < toast_indexes_->GetSize(); i++) {
    auto toast_index = (*toast_indexes_)[i];
    Assert(toast_index >= 0 && (size_t)toast_index < total_rows_);
    toast_flat_map_->Clear(toast_index);
  }
}

void PaxColumn::SetExternalToastDataBuffer(
    std::shared_ptr<DataBuffer<char>> external_toast_data) {
  Assert(!external_toast_data_);
  external_toast_data_ = std::move(external_toast_data);
}

std::shared_ptr<DataBuffer<char>> PaxColumn::GetExternalToastDataBuffer() {
  return external_toast_data_;
}

size_t PaxColumn::AppendExternalToastData(char *data, size_t size) {
  size_t rc;
  if (!external_toast_data_) {
    external_toast_data_ = std::make_unique<DataBuffer<char>>(DEFAULT_CAPACITY);
  }

  if (external_toast_data_->Available() < size) {
    external_toast_data_->ReSize(external_toast_data_->Used() + size, 2);
  }
  rc = external_toast_data_->Used();
  external_toast_data_->Write(data, size);
  external_toast_data_->Brush(size);

  return rc;
}

void PaxColumn::AddToastIndex(int32 index_of_toast) {
  if (!toast_indexes_) {
    toast_indexes_ = std::make_shared<DataBuffer<int32>>(DEFAULT_CAPACITY);

    Assert(!toast_flat_map_);
    toast_flat_map_ = std::make_shared<Bitmap8>(DEFAULT_CAPACITY);
    toast_flat_map_->SetN(total_rows_);
  }

  if (toast_indexes_->Available() == 0) {
    toast_indexes_->ReSize(toast_indexes_->Used() + sizeof(int32), 2);
  }

  toast_indexes_->Write(index_of_toast);
  toast_indexes_->Brush(sizeof(int32));
  toast_flat_map_->Clear(total_rows_);
}

std::string PaxColumn::DebugString() {
  // Notice that: the method that needs to be called below MUST not cause a
  // second exception.
  return fmt(
      "Column info [type=%d, format=%d, typelen=%d, hasnull=%d, allnull=%d,"
      "hasattrs=%d, totalrows=%lu, alignsz=%lu, encoded/compress type=%d, len "
      "stream encoded/compress type=%d,"
      "compresslvl=%d, len stream compresslv=%d, ToastCounts=%lu, has external "
      "toast=%d]",
      GetPaxColumnTypeInMem(), GetStorageFormat(), GetTypeLength(), HasNull(),
      AllNull(), HasAttributes(), GetRows(), GetAlignSize(), GetEncodingType(),
      GetOffsetsEncodingType(), GetCompressLevel(), GetOffsetsCompressLevel(),
      ToastCounts(), GetExternalToastDataBuffer() != nullptr);
}

template <typename T>
PaxCommColumn<T>::PaxCommColumn(uint32 capacity) {
  data_ = std::make_unique<DataBuffer<T>>(capacity * sizeof(T));
}

template <typename T>
PaxCommColumn<T>::~PaxCommColumn() {}

template <typename T>  // NOLINT: redirect constructor
PaxCommColumn<T>::PaxCommColumn() : PaxCommColumn(DEFAULT_CAPACITY) {}

template <typename T>
void PaxCommColumn<T>::Set(std::unique_ptr<DataBuffer<T>> data) {
  data_ = std::move(data);
}

template <typename T>
void PaxCommColumn<T>::Append(char *buffer, size_t size) {
  PaxColumn::Append(buffer, size);
  auto buffer_t = reinterpret_cast<T *>(buffer);

  // TODO(jiaqizho): Is it necessary to support multiple buffer insertions for
  // bulk insert push to mirco partition?
  Assert(size == sizeof(T));
  Assert(data_->Capacity() >= sizeof(T));

  if (data_->Available() == 0) {
    data_->ReSize(data_->Used() + size, 2);
  }

  data_->Write(buffer_t, sizeof(T));
  data_->Brush(sizeof(T));
}

template <typename T>
void PaxCommColumn<T>::AppendToast(char * /*buffer*/, size_t /*size*/) {
  // fixed-column won't get toast datum
  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

template <typename T>
PaxStorageFormat PaxCommColumn<T>::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcNonVec;
}

template <typename T>
PaxColumnTypeInMem PaxCommColumn<T>::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeFixed;
}

template <typename T>
size_t PaxCommColumn<T>::GetNonNullRows() const {
  return data_->GetSize();
}

template <typename T>
size_t PaxCommColumn<T>::PhysicalSize() const {
  return data_->Used();
}

template <typename T>
int64 PaxCommColumn<T>::GetOriginLength() const {
  return data_->Used();
}

template <typename T>
int64 PaxCommColumn<T>::GetOffsetsOriginLength() const {
  return 0;
}

template <typename T>
int32 PaxCommColumn<T>::GetTypeLength() const {
  return sizeof(T);
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer() {
  return std::make_pair(data_->Start(), data_->Used());
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetBuffer(size_t position) {
  Assert(position < GetNonNullRows());
  return std::make_pair(data_->Start() + (sizeof(T) * position), sizeof(T));
}

template <typename T>
Datum PaxCommColumn<T>::GetDatum(size_t position) {
  Assert(position < GetNonNullRows());
  auto ptr = data_->Start() + (sizeof(T) * position);
  return (Datum)(*reinterpret_cast<T *>(ptr));
}

template <typename T>
std::pair<char *, size_t> PaxCommColumn<T>::GetRangeBuffer(size_t start_pos,
                                                           size_t len) {
  Assert((start_pos + len) <= GetNonNullRows());
  return std::make_pair(data_->Start() + (sizeof(T) * start_pos),
                        sizeof(T) * len);
}

template class PaxCommColumn<char>;
template class PaxCommColumn<int8>;
template class PaxCommColumn<int16>;
template class PaxCommColumn<int32>;
template class PaxCommColumn<int64>;
template class PaxCommColumn<float>;
template class PaxCommColumn<double>;

PaxNonFixedColumn::PaxNonFixedColumn(uint32 data_capacity,
                                     uint32 offsets_capacity)
    : estimated_size_(0),
      data_(std::make_unique<DataBuffer<char>>(data_capacity)),
      offsets_(std::make_unique<DataBuffer<int32>>(offsets_capacity)),
      next_offsets_(0) {}

PaxNonFixedColumn::PaxNonFixedColumn()
    : PaxNonFixedColumn(DEFAULT_CAPACITY, DEFAULT_CAPACITY) {}

PaxNonFixedColumn::~PaxNonFixedColumn() {}

void PaxNonFixedColumn::Set(std::unique_ptr<DataBuffer<char>> data,
                            std::unique_ptr<DataBuffer<int32>> offsets,
                            size_t total_size) {
  estimated_size_ = total_size;
  data_ = std::move(data);
  offsets_ = std::move(offsets);
  next_offsets_ = -1;
}

void PaxNonFixedColumn::AppendAlign(char *buffer, size_t size) {
  size_t origin_size;
  origin_size = size;

  PaxColumn::Append(buffer, size);

  Assert(likely(reinterpret_cast<char *> TYPEALIGN(
                    type_align_size_, data_->Position()) == data_->Position()));
  Assert(next_offsets_ != -1);
  size = TYPEALIGN(type_align_size_, size);

  if (data_->Available() < size) {
    data_->ReSize(data_->Used() + size, 2);
  }

  if (offsets_->Available() < sizeof(int32)) {
    offsets_->ReSize(offsets_->Used() + sizeof(int32), 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, origin_size);
  data_->Brush(origin_size);
  if (size - origin_size != 0) {
    data_->WriteZero(size - origin_size);
    data_->Brush(size - origin_size);
  }

  Assert(size <= INT32_MAX);
  Assert(next_offsets_ + size <= INT32_MAX);

  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ += size;
}

void PaxNonFixedColumn::Append(char *buffer, size_t size) {
  if (type_align_size_ != PAX_DATA_NO_ALIGN) {
    AppendAlign(buffer, size);
    return;
  }

  PaxColumn::Append(buffer, size);

  if (data_->Available() < size) {
    data_->ReSize(data_->Used() + size, 2);
  }

  if (offsets_->Available() < sizeof(int32)) {
    offsets_->ReSize(offsets_->Used() + sizeof(int32), 2);
  }

  estimated_size_ += size;
  data_->Write(buffer, size);
  data_->Brush(size);

  Assert(size <= INT32_MAX);
  Assert(next_offsets_ + size <= INT32_MAX);

  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ += size;
}

void PaxNonFixedColumn::AppendLastOffset() {
  Assert(next_offsets_ != -1);
  Assert(offsets_->Capacity() >= sizeof(int32));
  if (offsets_->Available() == 0) {
    offsets_->ReSize(offsets_->Capacity() + sizeof(int32));
  }
  offsets_->Write(next_offsets_);
  offsets_->Brush(sizeof(next_offsets_));
  next_offsets_ = -1;
}

std::pair<char *, size_t> PaxNonFixedColumn::GetOffsetBuffer(bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  return std::make_pair((char *)offsets_->GetBuffer(), offsets_->Used());
}

PaxColumnTypeInMem PaxNonFixedColumn::GetPaxColumnTypeInMem() const {
  return PaxColumnTypeInMem::kTypeNonFixed;
}

PaxStorageFormat PaxNonFixedColumn::GetStorageFormat() const {
  return PaxStorageFormat::kTypeStoragePorcNonVec;
}

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer() {
  return std::make_pair(data_->GetBuffer(), data_->Used());
}

size_t PaxNonFixedColumn::PhysicalSize() const { return estimated_size_; }

int64 PaxNonFixedColumn::GetOriginLength() const { return data_->Used(); }

int64 PaxNonFixedColumn::GetOffsetsOriginLength() const {
  Assert(next_offsets_ == -1);
  return offsets_->Used();
}

int32 PaxNonFixedColumn::GetTypeLength() const { return -1; }

std::pair<char *, size_t> PaxNonFixedColumn::GetBuffer(size_t position) {
  Assert(position < offsets_->GetSize());

  // This situation happend when writing
  // The `offsets_` have not fill the last one
  if (unlikely(position == offsets_->GetSize() - 1)) {
    Assert(next_offsets_ != -1);
    return std::make_pair(data_->GetBuffer() + (*offsets_)[position],
                          next_offsets_ - (*offsets_)[position]);
  }

  Assert((*offsets_)[position] != (*offsets_)[position + 1]);

  return std::make_pair(data_->GetBuffer() + (*offsets_)[position],
                        (*offsets_)[position + 1] - (*offsets_)[position]);
}

Datum PaxNonFixedColumn::GetDatum(size_t position) {
  Assert(position < GetNonNullRows());
  const char *buffer = nullptr;
  // safe to call without length
  const auto &start_offset = (*offsets_)[position];
  buffer = data_->GetBuffer() + start_offset;
  Datum datum = PointerGetDatum(buffer);

  if (unlikely(IsToast(position))) {
    std::shared_ptr<MemoryObject> ref;
    auto external_buffer = GetExternalToastDataBuffer();
    std::tie(datum, ref) =
        pax_detoast(datum, external_buffer ? external_buffer->Start() : nullptr,
                    external_buffer ? external_buffer->Used() : 0);

    if (ref) {
      buffer_holders_.emplace_back(ref);
    }
  }

  return datum;
}

std::pair<char *, size_t> PaxNonFixedColumn::GetRangeBuffer(size_t start_pos,
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

  AssertImply(len != 0, (*offsets_)[start_pos + len] > (*offsets_)[start_pos]);

  return std::make_pair(data_->GetBuffer() + (*offsets_)[start_pos],
                        (*offsets_)[start_pos + len] - (*offsets_)[start_pos]);
}

size_t PaxNonFixedColumn::GetNonNullRows() const {
  return next_offsets_ == -1 ? offsets_->GetSize() - 1 : offsets_->GetSize();
}

};  // namespace pax
