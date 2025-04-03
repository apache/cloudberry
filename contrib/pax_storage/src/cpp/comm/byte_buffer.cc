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
 * byte_buffer.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/byte_buffer.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/byte_buffer.h"

#include "comm/pax_memory.h"

namespace pax {
ByteBuffer::ByteBuffer(size_t capacity, size_t size)
  : buffer_(nullptr)
  , size_(size)
  , capacity_(capacity) {
    InitializeBuffer();
  }

ByteBuffer::ByteBuffer(size_t capacity)
  : buffer_(nullptr)
  , size_(0)
  , capacity_(capacity) {
    InitializeBuffer();
  }

ByteBuffer::ByteBuffer()
  : buffer_(nullptr)
  , size_(0)
  , capacity_(0) { }

ByteBuffer::ByteBuffer(ByteBuffer &&other)
  : buffer_(other.buffer_)
  , size_(other.size_)
  , capacity_(other.capacity_) {
    other.buffer_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
}

ByteBuffer::~ByteBuffer() {
  auto p = buffer_;
  buffer_ = nullptr;
  size_ = 0;
  capacity_ = 0;

  PAX_FREE(p);
}

ByteBuffer &ByteBuffer::operator=(ByteBuffer &&tmp) {
  if (this != &tmp) {
    PAX_FREE(buffer_);
    buffer_ = tmp.buffer_;
    size_ = tmp.size_;
    capacity_ = tmp.capacity_;

    tmp.buffer_ = nullptr;
    tmp.size_ = 0;
    tmp.capacity_ = 0;
  }

  return *this;
}

void ByteBuffer::InitializeBuffer() {
  if (size_ > capacity_)
    capacity_ = size_;

  if (capacity_ > 0) {
    buffer_ = PAX_ALLOC(capacity_);
  }
}

#define ALIGN_SIZE(size, pow2) (((size) + ((pow2) - 1)) & ~((pow2) - 1))

void ByteBuffer::EnsureBufferSizeInternal(size_t size) {
  void *p;

  size = ALIGN_SIZE(size, 16);
  if (buffer_) {
    p = PAX_REALLOC(buffer_, size);
  } else {
    p = PAX_ALLOC(size);
  }
  buffer_ = p;
  capacity_ = size;
}

}