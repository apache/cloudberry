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
 * pax_encoding_non_fixed_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_non_fixed_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {

void PaxNonFixedEncodingColumn::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
    encoder_options_.compress_level = 5;
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_, true);
  if (encoder_) {
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (compressor_) {
    return;
  }

  PaxColumn::SetEncodeType(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
  PaxColumn::SetCompressLevel(0);
}

void PaxNonFixedEncodingColumn::InitOffsetStreamCompressor() {
  Assert(encoder_options_.offsets_encode_type ==
         ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA);

  SetOffsetsEncodeType(encoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(encoder_options_.offsets_compress_level);

  PaxEncoder::EncodingOption opt = encoder_options_;
  opt.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
  opt.is_sign = false;
  // offsets are fixed-width, do not enable non_fixed streaming restriction
  offsets_encoder_ = PaxEncoder::CreateStreamingEncoder(opt, false);
}

void PaxNonFixedEncodingColumn::InitOffsetStreamDecompressor() {
  Assert(decoder_options_.offsets_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  SetOffsetsEncodeType(decoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(decoder_options_.offsets_compress_level);

  if (decoder_options_.offsets_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA) {
    PaxDecoder::DecodingOption temp_opt = decoder_options_;
    temp_opt.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
    temp_opt.is_sign = false;
    offsets_decoder_ = PaxDecoder::CreateDecoder<int32>(temp_opt);
  } else {
    offsets_compressor_ = PaxCompressor::CreateBlockCompressor(
        decoder_options_.offsets_encode_type);
  }
}

void PaxNonFixedEncodingColumn::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);

  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<int8>(decoder_options_);
  if (decoder_) {
    shared_data_ =
        std::make_shared<DataBuffer<char>>(*PaxNonFixedColumn::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(data_capacity, offsets_capacity),
      encoder_options_(encoder_options),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  InitEncoder();
  InitOffsetStreamCompressor();
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(data_capacity, offsets_capacity),
      decoder_options_(decoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  InitDecoder();
  InitOffsetStreamDecompressor();
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() {}

void PaxNonFixedEncodingColumn::Set(std::unique_ptr<DataBuffer<char>> data,
                                    std::unique_ptr<DataBuffer<int32>> offsets,
                                    size_t total_size) {
  bool exist_decoder;
  Assert(data && offsets);

  auto data_decompress = [&]() {
    Assert(!compress_route_);
    Assert(bool(compressor_) != bool(decoder_));

    if (data->Used() == 0) {
      return;
    }

    if (compressor_) {
      auto d_size = compressor_->Decompress(
          PaxNonFixedColumn::data_->Start(),
          PaxNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::data_->Brush(d_size);
    }

    if (decoder_) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();

      // `data_` have the same buffer with `shared_data_`
      PaxNonFixedColumn::data_->Brush(shared_data_->Used());
      // no delete the origin data
      shared_data_ = std::move(data);
    }
  };

  auto offsets_decompress = [&]() {
    Assert(!compress_route_);
    Assert(offsets_compressor_ || offsets_decoder_);

    if (offsets->Used() == 0) {
      return;
    }

    if (offsets_compressor_) {
      auto d_size = offsets_compressor_->Decompress(
          PaxNonFixedColumn::offsets_->Start(),
          PaxNonFixedColumn::offsets_->Capacity(), offsets->Start(),
          offsets->Used());
      if (offsets_compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::offsets_->Brush(d_size);
      return;
    }

    if (offsets_decoder_) {
      // Decode offsets using encoder for int32 stream
      shared_offsets_data_ = std::make_shared<DataBuffer<char>>(
          PaxNonFixedColumn::offsets_->Start(),
          PaxNonFixedColumn::offsets_->Capacity(), false, false);
      offsets_decoder_->SetDataBuffer(shared_offsets_data_);
      offsets_decoder_->SetSrcBuffer(offsets->Start(), offsets->Used());
      offsets_decoder_->Decoding();
      PaxNonFixedColumn::offsets_->Brush(shared_offsets_data_->Used());
      return;
    }
  };

  exist_decoder = compressor_ || decoder_;
  bool has_offsets_processor = offsets_compressor_ || offsets_decoder_;

  if (exist_decoder && has_offsets_processor) {
    data_decompress();
    offsets_decompress();
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else if (exist_decoder && !has_offsets_processor) {
    data_decompress();
    PaxNonFixedColumn::offsets_ = std::move(offsets);
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else if (!exist_decoder && has_offsets_processor) {
    PaxNonFixedColumn::data_ = std::move(data);
    offsets_decompress();
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else {  // (!compressor_ && !offsets_compressor_)
    PaxNonFixedColumn::Set(std::move(data), std::move(offsets), total_size);
  }
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer() {
  bool exist_encoder;
  exist_encoder = compressor_ || encoder_;

  if (exist_encoder && compress_route_) {
    Assert(!compressor_ || !encoder_);

    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (PaxNonFixedColumn::data_->Used() == 0) {
      return PaxNonFixedColumn::GetBuffer();
    }

    // do compressed
    if (compressor_) {
      size_t bound_size =
          compressor_->GetCompressBound(PaxNonFixedColumn::data_->Used());
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      auto c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxNonFixedColumn::data_->Start(), PaxNonFixedColumn::data_->Used(),
          encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (encoder_) {
      shared_data_ =
          std::make_shared<DataBuffer<char>>(PaxNonFixedColumn::data_->Used());
      encoder_->SetDataBuffer(shared_data_);

      char *data_buffer = PaxNonFixedColumn::data_->GetBuffer();

      for (size_t i = 0; i < offsets_->GetSize() - 1; i++) {
        encoder_->Append(data_buffer, (*offsets_)[i + 1] - (*offsets_)[i]);
        data_buffer += (*offsets_)[i + 1] - (*offsets_)[i];
      }

      if (next_offsets_ != -1) {
        encoder_->Append(data_buffer,
                         next_offsets_ - (*offsets_)[offsets_->GetSize() - 1]);
      }

      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetBuffer();
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetOffsetBuffer(
    bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  if (shared_offsets_data_) {
    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  if (PaxNonFixedColumn::offsets_->Used() == 0) {
    // should never append last offset again
    return PaxNonFixedColumn::GetOffsetBuffer(false);
  }

  if (offsets_compressor_ && compress_route_) {
    size_t bound_size = offsets_compressor_->GetCompressBound(
        PaxNonFixedColumn::offsets_->Used());
    shared_offsets_data_ = std::make_shared<DataBuffer<char>>(bound_size);

    auto c_size = offsets_compressor_->Compress(
        shared_offsets_data_->Start(), shared_offsets_data_->Capacity(),
        PaxNonFixedColumn::offsets_->Start(),
        PaxNonFixedColumn::offsets_->Used(), encoder_options_.compress_level);

    if (offsets_compressor_->IsError(c_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
    }

    shared_offsets_data_->Brush(c_size);
    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  if (offsets_encoder_ && compress_route_) {
    // For delta encoder, allocate a buffer sized by raw bytes for safety
    size_t bound_size = offsets_encoder_->GetBoundSize(offsets_->Used());
    shared_offsets_data_ = std::make_shared<DataBuffer<char>>(bound_size);
    offsets_encoder_->SetDataBuffer(shared_offsets_data_);

    // Encode entire offsets buffer as a single stream
    offsets_encoder_->Append(offsets_->Start(), offsets_->Used());
    offsets_encoder_->Flush();

    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  // no compress or uncompressed
  // should never append last offset again
  return PaxNonFixedColumn::GetOffsetBuffer(false);
}

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return PaxNonFixedColumn::data_->Used();
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

}  // namespace pax
