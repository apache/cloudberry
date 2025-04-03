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
 * pax_compress.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_compress.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <stddef.h>

#include <string>

#include "comm/pax_memory.h"
#include "storage/columns/pax_encoding_utils.h"

namespace pax {

class PaxCompressor {
 public:
  PaxCompressor() = default;

  virtual ~PaxCompressor() = default;

  virtual bool ShouldAlignBuffer() const = 0;

  virtual size_t GetCompressBound(size_t src_len) = 0;

  virtual size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                          size_t src_len, int lvl) = 0;

  virtual size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                            size_t src_len) = 0;

  virtual bool IsError(size_t code) = 0;

  virtual const char *ErrorName(size_t code) = 0;

  /**
   * block compress
   *
   * it has similar interface with `CreateStreamingEncoder`
   * but the timing of compression/decompression method calls is different from
   * encoding/decoding.
   */
  static std::shared_ptr<PaxCompressor> CreateBlockCompressor(ColumnEncoding_Kind kind);
};

class PaxZSTDCompressor final : public PaxCompressor {
 public:
  PaxZSTDCompressor() = default;

  ~PaxZSTDCompressor() override = default;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;
};

class PaxZlibCompressor final : public PaxCompressor {
 public:
  PaxZlibCompressor() = default;

  ~PaxZlibCompressor() override = default;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t Decompress(void *dst_buff, size_t dst_cap, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;

 private:
  std::string err_msg_;
};

class PgLZCompressor final : public PaxCompressor {
 public:
  PgLZCompressor() = default;

  ~PgLZCompressor() override = default;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;
};

class PaxLZ4Compressor final : public PaxCompressor {
 public:
  PaxLZ4Compressor() = default;

  ~PaxLZ4Compressor() override = default;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;
};

}  // namespace pax
