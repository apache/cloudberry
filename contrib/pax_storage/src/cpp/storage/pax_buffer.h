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
 * pax_buffer.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_buffer.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <stddef.h>

#include <cstring>
#include <iostream>
#include <utility>

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"

namespace pax {

struct BlockBuffer {
  BlockBuffer(char *begin_offset, char *end_offset);

  BlockBuffer(const BlockBuffer &block_buffer) = default;

  inline char *Start() const { return begin_offset_; }

  inline char *End() const { return end_offset_; }

  inline size_t Size() const { return size_t(end_offset_ - begin_offset_); }

  inline void Resize(size_t size) { end_offset_ = begin_offset_ + size; }

  inline bool IsEmpty() const { return Size() == 0; }

  inline size_t Stripe(size_t size) const {
    return size_t(end_offset_ - begin_offset_) / size;
  }

  inline void Swap(BlockBuffer &other) {
    std::swap(begin_offset_, other.begin_offset_);
    std::swap(end_offset_, other.end_offset_);
  }

  template <typename T=char>
  static inline T* Alloc(size_t size) { return PAX_ALLOC<T>(size); }

  template <typename T=char>
  static inline T* Alloc0(size_t size) { return PAX_ALLOC0<T>(size); }

  template <typename T=char>
  static inline T* Realloc(void *ptr, size_t new_size) {
    return PAX_REALLOC<T>(ptr, new_size);
  }
  template <typename T>
  static inline void Free(T *ptr) { PAX_FREE(ptr); }

 private:
  char *begin_offset_;
  char *end_offset_;
};

class BlockBufferBase {
 public:
  BlockBufferBase(char *ptr, size_t size, size_t offset);

  BlockBufferBase(const BlockBufferBase &block_buffer_base) = default;

  inline BlockBuffer &Buffer() { return block_buffer_; }
  inline char *Position() { return block_pos_; }

  /* Should not call Brush inside BlockBuffer or DataBuffer */
  inline void Brush(size_t size) { block_pos_ += size; }
  inline void BrushAll() { block_pos_ = block_buffer_.End(); }

  inline void BrushBack(size_t size) {
    size_t used = Used();
    CBDB_CHECK(used >= size, cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Invalid brush back [used=%lu, require=%lu]", used, size));
    block_pos_ = block_buffer_.Start() + used - size;
  }

  inline void BrushBackAll() { block_pos_ = block_buffer_.Start(); }

  inline char *Start() const { return block_buffer_.Start(); }

  inline size_t Used() const {
    ptrdiff_t res = block_pos_ - block_buffer_.Start();
    Assert(res >= 0);
    return size_t(res);
  }

  inline size_t Available() const {
    ptrdiff_t res = block_buffer_.End() - block_pos_;
    Assert(res >= 0);
    return size_t(res);
  }

  inline size_t Capacity() const {
    ptrdiff_t res = block_buffer_.End() - block_buffer_.Start();
    Assert(res >= 0);
    return size_t(res);
  }

  inline void Write(char *ptr, size_t size) {
    Assert(block_pos_ + size <= block_buffer_.End());
    memcpy(block_pos_, ptr, size);
  }

  inline void WriteZero(size_t size) {
    Assert(block_pos_ + size <= block_buffer_.End());
    memset(block_pos_, 0, size);
  }

  void Combine(const BlockBufferBase &buffer);

  virtual ~BlockBufferBase() = default;

 protected:
  void Set(char *ptr, size_t size, size_t offset);

  void Set(char *ptr, size_t size);


  char *block_pos_;
  BlockBuffer block_buffer_;
};

// DataBuffer used to manage a chunk of memory buffer.
// It provides a series of methods for template access,
// the internal buffer(T* data_buffer_) are ordered and can be used as a
// array. The internal buffer have a working pointer(block_pos_) which
// distinguishes the used buffer and available buffer.
// Below is the internal buffer visualization
//
//        internal buffer
// ----------------------------------
// | used buffer  | available buffer|
// ----------------------------------
//                ↑
//         working pointer
template <typename T>
class DataBuffer : public BlockBufferBase {
 public:
  // `data_buffer` can be exist buffer or nullptr
  // `size means` size of current buffer
  // `allow_null` if true then will not used `size` to alloc new buffer,
  // otherwise DataBuffer will used `size` to alloc a new buffer.
  // `mem_take_over` if true the internal buffer which passed by `data_buffer`
  // or new alloced will be freed when `DataBuffer` been freed, otherwise the
  // internal buffer should be freed by caller also the method `ReSize` can't be
  // called if `mem_take_over` is false.
  DataBuffer(T *data_buffer, size_t size, bool allow_null = true,
             bool mem_take_over = true);

  // will alloc a size of buffer and memory will take over with DataBuffer
  explicit DataBuffer(size_t size);

  DataBuffer(const DataBuffer &data_buffer);

  friend class DataBuffer<char>;

  // copy constructor for DataBuffer<T>
  // at the same time, this is also a way to convert templates <typename T2> to
  // templates <typename T>.
  //
  // must pay attention that after origin DataBuffer<T2> call `ReSize`, The
  // copied DataBuffer<T> will become illegal this is because there is no way
  // for the internal pointer to be updated.
  //
  template <typename T2>
  explicit DataBuffer(const DataBuffer<T2> &data_buffer)
      : BlockBufferBase(data_buffer),
        mem_take_over_(false),
        data_buffer_(reinterpret_cast<T *>(data_buffer.data_buffer_)) {}

  // Direct access elements of internal buffer
  inline T &operator[](size_t i) { return data_buffer_[i]; }

  inline T *StartT() const { return data_buffer_; }

  // Get size of elements of internal buffer
  inline size_t GetSize() { return Used() / sizeof(T); }

  ~DataBuffer() override;

  // Set a memory buffer, should make sure internal buffer is nullptr.
  // This method is split from the constructor.

  void Set(char *ptr, size_t size);

  // Reset the DataBuffer
  void Reset();

  // Direct write a element into available buffer
  // Should call `Brush` after write
  inline void Write(T value) {
    Assert(block_pos_ + sizeof(T) <= block_buffer_.End());
    *(reinterpret_cast<T *>(block_pos_)) = value;
  }

  inline void Write(T *ptr, size_t size) {
    Assert(size % sizeof(T) == 0 && (block_pos_ + size) <= block_buffer_.End());
    memcpy(block_pos_, reinterpret_cast<const char *>(ptr), size);
  }

  inline void Write(const T *ptr, size_t size) {
    Assert(size % sizeof(T) == 0 && (block_pos_ + size) <= block_buffer_.End());
    memcpy(block_pos_, reinterpret_cast<const char *>(ptr), size);
  }

  // Read all to dst pointer
  inline void Read(T *dst) {
    Assert(Used() > sizeof(T) && Used() <= Capacity());
    memcpy(dst, block_pos_, sizeof(T));
  }

  inline void Read(void *dst, size_t n) {
    Assert(Used() > n && Used() <= Capacity());
    memcpy(dst, block_pos_, n);
  }

  // Get the internal buffer pointer
  inline T *GetBuffer() const { return data_buffer_; }

  // Get the available buffer pointer
  inline T *GetAvailableBuffer() const { return data_buffer_ + Used(); }

  // Resize the internal buffer, size should bigger than capacity of internal
  // buffer `mem_take_over` should be true
  // The `mul_rate` means that the current Buffer will be expanded according to
  // this ratio If `mul_rate` is 0 means derict do realloc
  virtual void ReSize(size_t size, double mul_ratio);

  // Direct resize the internal buffer, size should bigger than capacity of
  // internal buffer `mem_take_over` should be true
  virtual void ReSize(size_t size);

  // Is current internal buffer take over by DataBuffer
  inline bool IsMemTakeOver() const { return mem_take_over_; }

  inline void SetMemTakeOver(bool take_over) { mem_take_over_ = take_over; }

  // Clear up the DataBuffer
  // Caller should call `Set` to reuse current `DataBuffer` after call `Clear`
  inline void Clear() {
    if (mem_take_over_ && data_buffer_) {
      BlockBuffer::Free(data_buffer_);
    }
    data_buffer_ = nullptr;
  }

 protected:
  bool mem_take_over_;
  T *data_buffer_ = nullptr;
};

// extern template DataBuffer<char>::DataBuffer<long>(pax::DataBuffer<long>
// const&);

extern template class DataBuffer<char>;
extern template class DataBuffer<int8>;
extern template class DataBuffer<int16>;
extern template class DataBuffer<uint32>;
extern template class DataBuffer<int32>;
extern template class DataBuffer<int64>;
extern template class DataBuffer<float>;
extern template class DataBuffer<double>;
extern template class DataBuffer<bool>;

template <typename T>
class UntreatedDataBuffer final : public DataBuffer<T> {
 public:
  explicit UntreatedDataBuffer(size_t size);

  void ReSize(size_t size, double mul_ratio) override;

  void ReSize(size_t size) override;

  void BrushUnTreated(size_t size);

  inline void BrushUnTreatedAll() {
    untreated_pos_ = BlockBufferBase::block_pos_;
  }

  void BrushBackUnTreated(size_t size);

  void TreatedAll();

  inline size_t UnTreated() const {
    return size_t(untreated_pos_ - BlockBufferBase::block_buffer_.Start());
  }

  inline size_t UnTouched() const {
    return size_t(BlockBufferBase::block_pos_ - untreated_pos_);
  }

 private:
  char *untreated_pos_ = nullptr;
};

extern template class UntreatedDataBuffer<char>;
extern template class UntreatedDataBuffer<int64>;

template <typename T>
class TreatedDataBuffer final : public DataBuffer<T> {
 public:
  TreatedDataBuffer(T *data_buffer, size_t size);

  inline void BrushTreated(size_t size) {
    Assert(treated_pos_ + size <= BlockBufferBase::block_pos_);
    treated_pos_ += size;
  }

  inline char *GetTreatedRawBuffer() const { return treated_pos_; }

  inline T *GetTreatedBuffer() const {
    return reinterpret_cast<T *>(treated_pos_);
  }

  inline size_t Treated() const {
    Assert(treated_pos_);
    return size_t(treated_pos_ - BlockBufferBase::block_buffer_.Start());
  }

  inline size_t UnTreated() const {
    Assert(treated_pos_);
    return size_t(BlockBufferBase::block_pos_ - treated_pos_);
  }

 private:
  char *treated_pos_ = nullptr;
};

extern template class TreatedDataBuffer<char>;
extern template class TreatedDataBuffer<int64>;

}  // namespace pax
