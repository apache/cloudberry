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
 * pax_compress_bench.cc
 *
 * IDENTIFICATION
 *   contrib/pax_storage/src/cpp/storage/columns/pax_compress_bench.cc
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "pax_gbench.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_delta_encoding.h"
#include "storage/columns/pax_rlev2_encoding.h"
#include "storage/pax_buffer.h"

namespace pax::bench {

namespace {

// Test data and prebuilt buffers for decode/decompress benchmarks
static const size_t kCount = 1024 * 1024;
static std::vector<uint32_t> g_offsets;
static std::unique_ptr<char[]> g_raw_bytes;
static size_t g_raw_len = 0;

static std::vector<char> g_rle_encoded;
static size_t g_rle_len = 0;

static std::vector<char> g_delta_encoded;
static size_t g_delta_len = 0;

static std::unique_ptr<char[]> g_zstd_compressed;
static size_t g_zstd_len = 0;

static std::shared_ptr<pax::PaxCompressor> g_zstd;

// Simple helpers for bench data persistence
static void EnsureDirExists(const char *dir_path) {
  if (mkdir(dir_path, 0755) != 0) {
    if (errno != EEXIST) {
      std::cerr << "Failed to create directory: " << dir_path << std::endl;
      std::abort();
    }
  }
}

static bool ReadWholeFile(const char *path, std::vector<char> &out) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) return false;
  in.seekg(0, std::ios::end);
  std::streampos size = in.tellg();
  if (size <= 0) return false;
  out.resize(static_cast<size_t>(size));
  in.seekg(0, std::ios::beg);
  in.read(out.data(), size);
  return static_cast<bool>(in);
}

static bool ReadWholeFile(const char *path, std::unique_ptr<char[]> &out,
                          size_t &out_len) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) return false;
  in.seekg(0, std::ios::end);
  std::streampos size = in.tellg();
  if (size <= 0) return false;
  out_len = static_cast<size_t>(size);
  out = std::make_unique<char[]>(out_len);
  in.seekg(0, std::ios::beg);
  in.read(out.get(), size);
  return static_cast<bool>(in);
}

static void WriteWholeFile(const char *path, const char *data, size_t len) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    std::cerr << "Failed to open file for write: " << path << std::endl;
    std::abort();
  }
  out.write(data, static_cast<std::streamsize>(len));
  if (!out) {
    std::cerr << "Failed to write file: " << path << std::endl;
    std::abort();
  }
}

static const char *kBenchDataDir = "bench_data";
static const char *kRLEV2Path = "bench_data/rle_v2_u32.bin";
static const char *kDeltaPath = "bench_data/delta_u32.bin";
static const char *kZSTDPath = "bench_data/zstd_u32.bin";
static const char *kRawPath = "bench_data/raw_u32.bin";

static std::vector<uint32_t> GenerateMonotonicOffsets(size_t n, uint32_t seed) {
  std::vector<uint32_t> offsets;
  offsets.resize(n);
  offsets[0] = 0;
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> step_dist(1, 256);
  for (size_t i = 1; i < n; ++i) {
    offsets[i] = offsets[i - 1] + static_cast<uint32_t>(step_dist(rng));
  }
  return offsets;
}

// Lazily ensure raw bytes are available (prefer loading from disk)
static void EnsureRawData() {
  if (g_raw_len != 0 && g_raw_bytes) return;
  EnsureDirExists(kBenchDataDir);
  std::vector<char> raw_from_file;
  if (ReadWholeFile(kRawPath, raw_from_file)) {
    g_raw_len = raw_from_file.size();
    g_raw_bytes = std::make_unique<char[]>(g_raw_len);
    std::memcpy(g_raw_bytes.get(), raw_from_file.data(), g_raw_len);
    return;
  }
  // Fallback: generate and persist
  g_offsets = GenerateMonotonicOffsets(kCount, /*seed=*/12345);
  g_raw_len = g_offsets.size() * sizeof(uint32_t);
  g_raw_bytes = std::make_unique<char[]>(g_raw_len);
  std::memcpy(g_raw_bytes.get(), g_offsets.data(), g_raw_len);
  WriteWholeFile(kRawPath, g_raw_bytes.get(), g_raw_len);
}

// Lazily ensure RLEv2 encoded buffer exists (load or build from raw)
static void EnsureRleEncoded() {
  if (g_rle_len != 0 && !g_rle_encoded.empty()) return;
  EnsureDirExists(kBenchDataDir);
  if (ReadWholeFile(kRLEV2Path, g_rle_encoded)) {
    g_rle_len = g_rle_encoded.size();
    return;
  }
  EnsureRawData();
  PaxEncoder::EncodingOption enc_opt;
  enc_opt.column_encode_type = ColumnEncoding_Kind_RLE_V2;
  enc_opt.is_sign = false;

  PaxOrcEncoder rle_encoder(enc_opt);
  auto rle_out = std::make_shared<DataBuffer<char>>(g_raw_len);
  rle_encoder.SetDataBuffer(rle_out);
  // encode directly from raw bytes to avoid depending on g_offsets
  size_t count = g_raw_len / sizeof(uint32_t);
  const uint32_t *vals = reinterpret_cast<const uint32_t *>(g_raw_bytes.get());
  for (size_t i = 0; i < count; ++i) {
    uint32_t v = vals[i];
    rle_encoder.Append(reinterpret_cast<char *>(&v), sizeof(uint32_t));
  }
  rle_encoder.Flush();

  g_rle_len = rle_encoder.GetBufferSize();
  g_rle_encoded.assign(rle_encoder.GetBuffer(),
                       rle_encoder.GetBuffer() + g_rle_len);
  WriteWholeFile(kRLEV2Path, g_rle_encoded.data(), g_rle_len);
}

// Lazily ensure Delta encoded buffer exists (load or build from raw)
static void EnsureDeltaEncoded() {
  if (g_delta_len != 0 && !g_delta_encoded.empty()) return;
  EnsureDirExists(kBenchDataDir);
  if (ReadWholeFile(kDeltaPath, g_delta_encoded)) {
    g_delta_len = g_delta_encoded.size();
    return;
  }
  EnsureRawData();
  PaxEncoder::EncodingOption enc_opt;
  enc_opt.is_sign = false;
  // type not used by PaxDeltaEncoder
  PaxDeltaEncoder<uint32_t> delta_encoder(enc_opt);
  auto delta_out = std::make_shared<DataBuffer<char>>(g_raw_len);
  delta_encoder.SetDataBuffer(delta_out);
  // Encode whole array in one shot
  delta_encoder.Append(g_raw_bytes.get(), g_raw_len);
  delta_encoder.Flush();

  g_delta_len = delta_encoder.GetBufferSize();
  g_delta_encoded.assign(delta_encoder.GetBuffer(),
                         delta_encoder.GetBuffer() + g_delta_len);
  WriteWholeFile(kDeltaPath, g_delta_encoded.data(), g_delta_len);
}

// Lazily ensure ZSTD compressed buffer exists (load or build from raw)
static void EnsureZstdCompressed() {
  EnsureDirExists(kBenchDataDir);
  if (!g_zstd) {
    g_zstd =
        PaxCompressor::CreateBlockCompressor(ColumnEncoding_Kind_COMPRESS_ZSTD);
    if (!g_zstd) {
      std::cerr << "Failed to create ZSTD compressor" << std::endl;
      std::abort();
    }
  }
  if (g_zstd_len != 0 && g_zstd_compressed) return;
  if (ReadWholeFile(kZSTDPath, g_zstd_compressed, g_zstd_len)) {
    return;
  }
  EnsureRawData();
  size_t bound = g_zstd->GetCompressBound(g_raw_len);
  g_zstd_compressed = std::make_unique<char[]>(bound);
  g_zstd_len = g_zstd->Compress(g_zstd_compressed.get(), bound,
                                g_raw_bytes.get(), g_raw_len, /*lvl=*/5);
  if (g_zstd->IsError(g_zstd_len) || g_zstd_len == 0) {
    std::cerr << "ZSTD one-time compress failed" << std::endl;
    std::abort();
  }
  WriteWholeFile(kZSTDPath, g_zstd_compressed.get(), g_zstd_len);
}

static void PrepareOnce() {
  // Keep lightweight init only
  pax::bench::CreateMemoryContext();
  EnsureDirExists(kBenchDataDir);
}

}  // namespace

// Register module init with gbench framework
REGISTER_BENCHMARK_INIT(PrepareOnce);

// RLEv2 encode benchmark
static void BM_RLEV2_Encode(::benchmark::State &state) {
  // Prepare raw data only; no encoded buffers are created here
  EnsureRawData();
  for (auto _ : state) {
    PaxEncoder::EncodingOption enc_opt;
    enc_opt.column_encode_type = ColumnEncoding_Kind_RLE_V2;
    enc_opt.is_sign = false;

    PaxOrcEncoder encoder(enc_opt);
    auto out = std::make_shared<DataBuffer<char>>(g_raw_len);
    encoder.SetDataBuffer(out);

    size_t count = g_raw_len / sizeof(uint32_t);
    const uint32_t *vals =
        reinterpret_cast<const uint32_t *>(g_raw_bytes.get());
    for (size_t i = 0; i < count; ++i) {
      uint32_t v = vals[i];
      encoder.Append(reinterpret_cast<char *>(&v), sizeof(uint32_t));
    }
    encoder.Flush();
    g_rle_len = encoder.GetBufferSize();
    benchmark::DoNotOptimize(encoder.GetBuffer());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
  state.counters["raw_kb"] =
      benchmark::Counter(static_cast<double>(g_raw_len) / (1024.0));
  state.counters["rle_kb"] =
      benchmark::Counter(static_cast<double>(g_rle_len) / (1024.0));
}
BENCHMARK(BM_RLEV2_Encode);

// RLEv2 decode benchmark
static void BM_RLEV2_Decode(::benchmark::State &state) {
  // Ensure we have raw size and encoded buffer ready (prefer from disk)
  EnsureRawData();
  EnsureRleEncoded();
  for (auto _ : state) {
    PaxDecoder::DecodingOption dec_opt;
    dec_opt.column_encode_type = ColumnEncoding_Kind_RLE_V2;
    dec_opt.is_sign = false;

    auto decoder = PaxDecoder::CreateDecoder<int32>(dec_opt);
    auto out = std::make_shared<DataBuffer<char>>(g_raw_len);
    decoder->SetSrcBuffer(g_rle_encoded.data(), g_rle_len);
    decoder->SetDataBuffer(out);
    size_t n = decoder->Decoding();
    benchmark::DoNotOptimize(n);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
}
BENCHMARK(BM_RLEV2_Decode);

// Delta encode benchmark
static void BM_Delta_Encode(::benchmark::State &state) {
  EnsureRawData();
  for (auto _ : state) {
    PaxEncoder::EncodingOption enc_opt;
    enc_opt.is_sign = false;
    PaxDeltaEncoder<uint32_t> encoder(enc_opt);
    auto out = std::make_shared<DataBuffer<char>>(g_raw_len);
    encoder.SetDataBuffer(out);
    encoder.Append(g_raw_bytes.get(), g_raw_len);
    encoder.Flush();
    g_delta_len = encoder.GetBufferSize();
    benchmark::DoNotOptimize(encoder.GetBuffer());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
  state.counters["delta_kb"] =
      benchmark::Counter(static_cast<double>(g_delta_len) / (1024.0));
}
BENCHMARK(BM_Delta_Encode);

// Delta decode benchmark
static void BM_Delta_Decode(::benchmark::State &state) {
  EnsureRawData();
  EnsureDeltaEncoded();
  for (auto _ : state) {
    PaxDecoder::DecodingOption dec_opt;
    dec_opt.is_sign = false;
    PaxDeltaDecoder<int32> decoder(dec_opt);
    auto out = std::make_shared<DataBuffer<char>>(g_raw_len);
    decoder.SetSrcBuffer(g_delta_encoded.data(), g_delta_len);
    decoder.SetDataBuffer(out);
    size_t n = decoder.Decoding();
    if (n != g_raw_len / sizeof(uint32_t) && out->Used() != g_raw_len) {
      std::cerr << "Delta decode failed, n: " << n
                << ", g_raw_len: " << g_raw_len
                << ", g_delta_len: " << g_delta_len
                << ", out: Used: " << out->Used() << std::endl;
      std::abort();
    }

    if (memcmp(out->GetBuffer(), g_raw_bytes.get(), g_raw_len) != 0) {
      std::cerr << "Delta decode failed, out: " << out->GetBuffer()
                << ", g_raw_bytes: " << g_raw_bytes.get() << std::endl;
      std::abort();
    }

    benchmark::DoNotOptimize(n);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
}
BENCHMARK(BM_Delta_Decode);

// ZSTD compress benchmark
static void BM_ZSTD_Compress(::benchmark::State &state) {
  EnsureRawData();
  if (!g_zstd) {
    g_zstd =
        PaxCompressor::CreateBlockCompressor(ColumnEncoding_Kind_COMPRESS_ZSTD);
    if (!g_zstd) {
      std::cerr << "Failed to create ZSTD compressor" << std::endl;
      std::abort();
    }
  }
  size_t bound = g_zstd->GetCompressBound(g_raw_len);
  std::unique_ptr<char[]> dst(new char[bound]);
  for (auto _ : state) {
    size_t n = g_zstd->Compress(dst.get(), bound, g_raw_bytes.get(), g_raw_len,
                                /*lvl=*/5);
    g_zstd_len = n;
    benchmark::DoNotOptimize(n);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
  state.counters["zstd_kb"] =
      benchmark::Counter(static_cast<double>(g_zstd_len) / (1024.0));
}
BENCHMARK(BM_ZSTD_Compress);

// ZSTD decompress benchmark
static void BM_ZSTD_Decompress(::benchmark::State &state) {
  EnsureRawData();
  EnsureZstdCompressed();
  std::unique_ptr<char[]> dst(new char[g_raw_len]);
  for (auto _ : state) {
    size_t n = g_zstd->Decompress(dst.get(), g_raw_len, g_zstd_compressed.get(),
                                  g_zstd_len);
    benchmark::DoNotOptimize(n);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(g_raw_len));
}
BENCHMARK(BM_ZSTD_Decompress);

}  // namespace pax::bench
