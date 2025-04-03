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
 * pax_buffer.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_buffer.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/pax_buffer.h"

#include "comm/fmt.h"
#include "exceptions/CException.h"

namespace pax {

#define MEMORY_RESIZE_LIMIT (768UL * 1024 * 1024)

BlockBuffer::BlockBuffer(char *begin_offset, char *end_offset)
    : begin_offset_(begin_offset), end_offset_(end_offset) {}

BlockBufferBase::BlockBufferBase(char *ptr, size_t size, size_t offset)
    : block_pos_(ptr + offset), block_buffer_(ptr, ptr + size) {
  Assert(offset <= size);
}

void BlockBufferBase::Set(char *ptr, size_t size, size_t offset) {
  Assert(offset <= size);
  block_buffer_ = BlockBuffer(ptr, ptr + size);
  block_pos_ = ptr + offset;
}

void BlockBufferBase::Set(char *ptr, size_t size) {
  block_buffer_ = BlockBuffer(ptr, ptr + size);
  block_pos_ = ptr;
}

void BlockBufferBase::Combine(const BlockBufferBase &buffer) {
  Assert(Available() > buffer.Used());
  Write(buffer.block_buffer_.Start(), buffer.Used());
}

template <typename T>
DataBuffer<T>::DataBuffer(T *data_buffer, size_t size, bool allow_null,
                          bool mem_take_over)
    : BlockBufferBase(nullptr, 0, 0),
      mem_take_over_(mem_take_over),
      data_buffer_(data_buffer) {
  if (!allow_null && !data_buffer_ && size != 0) {
    data_buffer_ = BlockBuffer::Alloc<T>(size);
  }
  auto ptr = reinterpret_cast<char *>(data_buffer_);
  block_buffer_ = BlockBuffer(ptr, ptr + size);
  block_pos_ = ptr;
}

template <typename T>
DataBuffer<T>::DataBuffer(const DataBuffer &data_buffer)
    : BlockBufferBase(data_buffer),
      mem_take_over_(false),
      data_buffer_(data_buffer.data_buffer_) {}

template <typename T>  // NOLINT: redirect constructor
DataBuffer<T>::DataBuffer(size_t size)
    : DataBuffer(nullptr, size, false, true) {}

template <typename T>
DataBuffer<T>::~DataBuffer() {
  if (mem_take_over_ && data_buffer_) {
    BlockBuffer::Free(data_buffer_);
  }
}

template <typename T>
void DataBuffer<T>::Set(char *ptr, size_t size) {
  Assert(data_buffer_ == nullptr);
  BlockBufferBase::Set(ptr, size);
  data_buffer_ = reinterpret_cast<T *>(ptr);
}

template <typename T>
void DataBuffer<T>::Reset() {
  Assert(!mem_take_over_);
  BlockBufferBase::Set(nullptr, 0);
  data_buffer_ = nullptr;
}

template <typename T>
void DataBuffer<T>::ReSize(size_t size, double mul_ratio) {
  Assert(mul_ratio > 1);
  auto cap = Capacity();

  if (size <= cap) {
    return;
  }

  while (cap < size) {
    cap = cap * mul_ratio;
  }

  ReSize(cap);
}

template <typename T>
void DataBuffer<T>::ReSize(size_t size) {
  if (!mem_take_over_) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalidMemoryOperation);
  }

  if (unlikely(size > MEMORY_RESIZE_LIMIT)) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeOOM,
               fmt("Current size require: %lu, The max limit resize is %lu",
                   size, MEMORY_RESIZE_LIMIT));
  }

  size_t used = Used();
  if (data_buffer_) {
    data_buffer_ = BlockBuffer::Realloc<T>(data_buffer_, size);
  } else {
    data_buffer_ = BlockBuffer::Alloc<T>(size);
  }
  BlockBufferBase::Set(reinterpret_cast<char *>(data_buffer_), size, used);
}

template class DataBuffer<char>;
template class DataBuffer<int8>;
template class DataBuffer<int16>;
template class DataBuffer<uint32>;
template class DataBuffer<int32>;
template class DataBuffer<int64>;
template class DataBuffer<float>;
template class DataBuffer<double>;
template class DataBuffer<bool>;

template <typename T>
UntreatedDataBuffer<T>::UntreatedDataBuffer(size_t size)
    : DataBuffer<T>(nullptr, size, false, true) {
  untreated_pos_ = BlockBufferBase::block_buffer_.Start();
}

template <typename T>
void UntreatedDataBuffer<T>::BrushUnTreated(size_t size) {
  Assert(untreated_pos_ + size <= BlockBufferBase::block_pos_);
  untreated_pos_ += size;
}

template <typename T>
void UntreatedDataBuffer<T>::BrushBackUnTreated(size_t size) {
  auto new_offset = UnTreated() - size;
  Assert(new_offset >= 0 && UnTreated() <= BlockBufferBase::Used());

  untreated_pos_ = BlockBufferBase::block_buffer_.Start() + new_offset;
}

template <typename T>
void UntreatedDataBuffer<T>::TreatedAll() {
  Assert(UnTreated() <= BlockBufferBase::Used());
  size_t treated = UnTreated();
  size_t untouched = UnTouched();
  if (untouched <= treated) {
    memcpy(BlockBufferBase::block_buffer_.Start(), untreated_pos_, untouched);
    untreated_pos_ = BlockBufferBase::block_buffer_.Start();
    BlockBufferBase::block_pos_ = untreated_pos_ + untouched;
    return;
  }

  char *write_pos = BlockBufferBase::block_buffer_.Start();
  size_t batch_size = 0;
  while (untouched != 0) {
    batch_size = untouched > treated ? treated : untouched;

    memcpy(write_pos, untreated_pos_, batch_size);
    untreated_pos_ = untreated_pos_ + batch_size;
    write_pos = write_pos + batch_size;

    untouched -= batch_size;
  }

  BlockBufferBase::block_pos_ = write_pos;
  untreated_pos_ = BlockBufferBase::block_buffer_.Start();
}

template <typename T>
void UntreatedDataBuffer<T>::ReSize(size_t size, double mul_ratio) {
  size_t untreated = UnTreated();
  DataBuffer<T>::ReSize(size, mul_ratio);
  untreated_pos_ = BlockBufferBase::block_buffer_.Start() + untreated;
}

template <typename T>
void UntreatedDataBuffer<T>::ReSize(size_t size) {
  size_t untreated = UnTreated();
  DataBuffer<T>::ReSize(size);
  untreated_pos_ = BlockBufferBase::block_buffer_.Start() + untreated;
}

template class UntreatedDataBuffer<char>;
template class UntreatedDataBuffer<int64>;

template <typename T>
TreatedDataBuffer<T>::TreatedDataBuffer(T *data_buffer, size_t size)
    : DataBuffer<T>(data_buffer, size, false, false) {
  Assert(data_buffer);
  Assert(size != 0);
  BlockBufferBase::Brush(size);
  treated_pos_ = BlockBufferBase::block_buffer_.Start();
}

template class TreatedDataBuffer<char>;
template class TreatedDataBuffer<int64>;

}  // namespace pax
