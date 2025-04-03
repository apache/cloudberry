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
 * pax_compress.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_compress.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_compress.h"

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#ifdef USE_LZ4
#include <lz4.h>
#endif
#include "zlib.h"  // NOLINT
#include "zstd.h"  // NOLINT

namespace pax {

std::shared_ptr<PaxCompressor> PaxCompressor::CreateBlockCompressor(
    const ColumnEncoding_Kind kind) {
  std::shared_ptr<PaxCompressor> compressor;
  switch (kind) {
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD: {
      compressor = std::make_shared<PaxZSTDCompressor>();
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB: {
      compressor = std::make_shared<PaxZlibCompressor>();
      break;
    }
    case ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED: {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                 fmt("Invalid compress type %d",
                     ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED));
    }
    // two cases here:
    //  - `encoded type` is not a compress type.
    //  - `encoded type` is the no_encoding type.
    default: {
      // do nothing
    }
  }

  return compressor;
}

bool PaxZSTDCompressor::ShouldAlignBuffer() const { return false; }

size_t PaxZSTDCompressor::GetCompressBound(size_t src_len) {
  Assert(src_len > 0);
  return ZSTD_compressBound(src_len);
}

size_t PaxZSTDCompressor::Compress(void *dst_buff, size_t dst_cap,
                                   void *src_buff, size_t src_len, int lvl) {
  Assert(dst_buff);
  Assert(dst_cap > 0);
  Assert(src_buff);
  Assert(src_len > 0);

  return ZSTD_compress(dst_buff, dst_cap, src_buff, src_len, lvl);
}

size_t PaxZSTDCompressor::Decompress(void *dst_buff, size_t dst_len,
                                     void *src_buff, size_t src_len) {
  Assert(dst_buff);
  Assert(dst_len > 0);
  Assert(src_buff);
  Assert(src_len > 0);

  return ZSTD_decompress(dst_buff, dst_len, src_buff, src_len);
}

bool PaxZSTDCompressor::IsError(size_t code) { return ZSTD_isError(code); }

const char *PaxZSTDCompressor::ErrorName(size_t code) {
  return ZSTD_getErrorName(code);
}

bool PaxZlibCompressor::ShouldAlignBuffer() const { return false; }

size_t PaxZlibCompressor::GetCompressBound(size_t src_len) {
  return compressBound(src_len);
}

size_t PaxZlibCompressor::Compress(void *dst_buff, size_t dst_cap,
                                   void *src_buff, size_t src_len, int lvl) {
  z_stream c_stream; /* compression stream */
  int err;

  c_stream.zalloc = reinterpret_cast<alloc_func>(0);
  c_stream.zfree = reinterpret_cast<free_func>(0);
  c_stream.opaque = reinterpret_cast<voidpf>(0);

  err = deflateInit(&c_stream, lvl);
  if (err != Z_OK) {
    goto error;
  }

  c_stream.next_in = reinterpret_cast<Bytef *>(src_buff);
  c_stream.next_out = reinterpret_cast<Bytef *>(dst_buff);

  while (c_stream.total_in != src_len && c_stream.total_out < dst_cap) {
    /* force small buffers */
    c_stream.avail_in = c_stream.avail_out = 1;
    err = deflate(&c_stream, Z_NO_FLUSH);
    if (err != Z_OK) {
      goto error;
    }
  }

  while (true) {
    c_stream.avail_out = 1;
    err = deflate(&c_stream, Z_FINISH);
    if (err == Z_STREAM_END) break;
    if (err != Z_OK) {
      goto error;
    }
  }

  err = deflateEnd(&c_stream);
  if (err != Z_OK) {
    goto error;
  }

  return c_stream.total_out;
error:
  err_msg_ = c_stream.msg;
  return err;
}

size_t PaxZlibCompressor::Decompress(void *dst_buff, size_t dst_cap,
                                     void *src_buff, size_t src_len) {
  int err;
  z_stream d_stream; /* decompression stream */

  d_stream.zalloc = reinterpret_cast<alloc_func>(0);
  d_stream.zfree = reinterpret_cast<free_func>(0);
  d_stream.opaque = reinterpret_cast<voidpf>(0);

  d_stream.next_in = reinterpret_cast<Bytef *>(src_buff);
  d_stream.avail_in = 0;
  d_stream.next_out = reinterpret_cast<Bytef *>(dst_buff);

  err = inflateInit(&d_stream);
  if (err != Z_OK) {
    goto error;
  }

  while (d_stream.total_out < dst_cap && d_stream.total_in < src_len) {
    d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
    err = inflate(&d_stream, Z_NO_FLUSH);
    if (err == Z_STREAM_END) break;
    if (err != Z_OK) {
      goto error;
    }
  }

  err = inflateEnd(&d_stream);
  if (err != Z_OK) {
    goto error;
  }

  return d_stream.total_out;
error:
  err_msg_ = d_stream.msg;
  return err;
}

bool PaxZlibCompressor::IsError(size_t code) {
  return code != Z_OK && !err_msg_.empty();
}

const char *PaxZlibCompressor::ErrorName(size_t /*code*/) {
  return err_msg_.c_str();
}

bool PgLZCompressor::ShouldAlignBuffer() const { return false; }

size_t PgLZCompressor::GetCompressBound(size_t src_len) {
  return PGLZ_MAX_OUTPUT(src_len);
}

size_t PgLZCompressor::Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                                size_t src_len, int /*lvl*/) {
  Assert(dst_cap >= PGLZ_MAX_OUTPUT(src_len));
  return pglz_compress((char *)src_buff, src_len, (char *)dst_buff, NULL);
}

size_t PgLZCompressor::Decompress(void *dst_buff, size_t dst_len,
                                  void *src_buff, size_t src_len) {
  return pglz_decompress((char *)src_buff, src_len, (char *)dst_buff, dst_len,
                         true);
}

bool PgLZCompressor::IsError(size_t code) { return ((int)code) < 0; }

const char *PgLZCompressor::ErrorName(size_t code) {
  return "PG LZ compress/decompress error";
}

#ifdef USE_LZ4

bool PaxLZ4Compressor::ShouldAlignBuffer() const { return false; }

size_t PaxLZ4Compressor::GetCompressBound(size_t src_len) {
  return LZ4_compressBound(src_len);
}

size_t PaxLZ4Compressor::Compress(void *dst_buff, size_t dst_cap,
                                  void *src_buff, size_t src_len, int /*lvl*/) {
  return LZ4_compress_default((char *)src_buff, (char *)dst_buff, src_len,
                              dst_cap);
}

size_t PaxLZ4Compressor::Decompress(void *dst_buff, size_t dst_len,
                                    void *src_buff, size_t src_len) {
  return LZ4_decompress_safe((char *)src_buff, (char *)dst_buff, src_len,
                             dst_len);
}

bool PaxLZ4Compressor::IsError(size_t code) {
  // 0 is failed
  return ((int)code) <= 0;
}

const char *PaxLZ4Compressor::ErrorName(size_t code) {
  return "LZ4 compress/decompress error";
}

#endif

}  // namespace pax
