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
 * byte_buffer.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/byte_buffer.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <memory>

namespace pax {
class ByteBuffer {
 public:
  ByteBuffer(size_t capacity, size_t size);
  ByteBuffer(size_t capacity);
  ByteBuffer();

  ByteBuffer(const ByteBuffer &) = delete;
  ByteBuffer(ByteBuffer &&other);
  ByteBuffer &operator=(const ByteBuffer &) = delete;
  ByteBuffer &operator=(ByteBuffer &&tmp);
  ~ByteBuffer();

  const void *Addr() const { return buffer_; }
  void *Addr() { return buffer_; }
  size_t Size() const { return size_; }
  void SetSize(size_t size) {
    EnsureBufferSize(size);
    size_ = size;
  }
  bool Empty() const { return size_ == 0; }
  operator bool() const { return size_ > 0; }

  size_t Capacity() const { return capacity_; }

  void EnsureBufferSize(size_t size) {
    if (capacity_ >= size) return;
    EnsureBufferSizeInternal(size);
  }
 private:
  void InitializeBuffer();
  void EnsureBufferSizeInternal(size_t size);
  void *buffer_; // owned by me
  size_t size_;
  size_t capacity_;
};

}