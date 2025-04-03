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
 * protobuf_stream.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/proto/protobuf_stream.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <google/protobuf/io/zero_copy_stream.h>

#include <sstream>

#include "storage/pax_buffer.h"

namespace pax {

class BufferedOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit BufferedOutputStream(uint64 block_size);

  virtual void Set(std::shared_ptr<DataBuffer<char>> data_buffer);

  bool Next(void **buffer, int *size) override;

  void BackUp(int count) override;

  google::protobuf::int64 ByteCount() const override;

  bool WriteAliasedRaw(const void *data, int size) override;

  bool AllowsAliasing() const override;

  virtual uint64 GetSize() const;

  virtual std::shared_ptr<DataBuffer<char>> GetDataBuffer() const;

  virtual void StartBufferOutRecord();

  virtual size_t EndBufferOutRecord();

  virtual void DirectWrite(char *ptr, size_t size);

 private:
  size_t last_used_ = 0;
  std::shared_ptr<DataBuffer<char>> data_buffer_;
  uint64 block_size_;
};

class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  SeekableInputStream(char *data_buffer, uint64 length);

  bool Next(const void **buffer, int *size) override;

  void BackUp(int count) override;

  bool Skip(int count) override;

  google::protobuf::int64 ByteCount() const override;

 private:
  DataBuffer<char> data_buffer_;
};

}  // namespace pax
