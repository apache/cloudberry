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
 * pax_vec_encoding_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_vec_encoding_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_vec_encoding_column.h"

#include "comm/fmt.h"
#include "comm/pax_memory.h"

namespace pax {

template <typename T>
PaxVecEncodingColumn<T>::PaxVecEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxVecCommColumn<T>(capacity),
      encoder_options_(encoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(true) {
  PaxVecEncodingColumn<T>::InitEncoder();
}

template <typename T>
PaxVecEncodingColumn<T>::PaxVecEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecCommColumn<T>(capacity),
      encoder_(nullptr),
      decoder_options_{decoding_option},
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(false) {
  PaxVecEncodingColumn<T>::InitDecoder();
}

template <typename T>
PaxVecEncodingColumn<T>::~PaxVecEncodingColumn() {}

template <typename T>
void PaxVecEncodingColumn<T>::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = GetDefaultColumnType();
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_);
  if (encoder_) {
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (!compressor_) {
    PaxColumn::SetEncodeType(
        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
    PaxColumn::SetCompressLevel(0);
  }
}

template <typename T>
void PaxVecEncodingColumn<T>::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<T>(decoder_options_);
  if (decoder_) {
    // init the shared_data_ with the buffer from PaxVecCommColumn<T>::data_
    // cause decoder_ need a DataBuffer<char> * as dst buffer
    shared_data_ =
        std::make_shared<DataBuffer<char>>(*PaxVecCommColumn<T>::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

template <typename T>
void PaxVecEncodingColumn<T>::Set(std::shared_ptr<DataBuffer<T>> data,
                                  size_t non_null_rows) {
  PaxColumn::non_null_rows_ = non_null_rows;
  if (decoder_) {
    // should not decoding null
    if (data->Used() != 0) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();
      PaxVecCommColumn<T>::data_->Brush(shared_data_->Used());
    }

    Assert(!data->IsMemTakeOver());
  } else if (compressor_) {
    if (data->Used() != 0) {
      // should not init `shared_data_`, direct uncompress to `data_`
      Assert(!shared_data_);
      size_t d_size = compressor_->Decompress(
          PaxVecCommColumn<T>::data_->Start(),
          PaxVecCommColumn<T>::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }

      PaxVecCommColumn<T>::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
  } else {
    PaxVecCommColumn<T>::Set(data, non_null_rows);
  }
}

template <typename T>
std::pair<char *, size_t> PaxVecEncodingColumn<T>::GetBuffer() {
  if (compress_route_) {
    // already done with decoding/compress
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no data for encoding
    if (PaxVecCommColumn<T>::data_->Used() == 0) {
      return PaxVecCommColumn<T>::GetBuffer();
    }

    if (encoder_) {
      // changed streaming encode to blocking encode
      // because we still need store a origin data in `PaxVecCommColumn<T>`
      auto origin_data_buffer = PaxVecCommColumn<T>::data_;

      shared_data_ =
          std::make_shared<DataBuffer<char>>(origin_data_buffer->Used());
      encoder_->SetDataBuffer(shared_data_);
      for (size_t i = 0; i < origin_data_buffer->GetSize(); i++) {
        encoder_->Append((char *)(origin_data_buffer->GetBuffer() + i),
                         sizeof(T));
      }
      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    } else if (compressor_) {
      size_t bound_size =
          compressor_->GetCompressBound(PaxVecCommColumn<T>::data_->Used());
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      size_t c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxVecCommColumn<T>::data_->Start(),
          PaxVecCommColumn<T>::data_->Used(), encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  return PaxVecCommColumn<T>::GetBuffer();
}

template <typename T>
int64 PaxVecEncodingColumn<T>::GetOriginLength() const {
  return PaxVecCommColumn<T>::data_->Used();
}

template <typename T>
size_t PaxVecEncodingColumn<T>::PhysicalSize() const {
  if (shared_data_) {
    return shared_data_->Used();
  }

  return PaxVecCommColumn<T>::PhysicalSize();
}

template <typename T>
size_t PaxVecEncodingColumn<T>::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

template <typename T>
ColumnEncoding_Kind PaxVecEncodingColumn<T>::GetDefaultColumnType() {
  return ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
}

template class PaxVecEncodingColumn<int8>;
template class PaxVecEncodingColumn<int16>;
template class PaxVecEncodingColumn<int32>;
template class PaxVecEncodingColumn<int64>;

PaxVecNonFixedEncodingColumn::PaxVecNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxVecNonFixedColumn(data_capacity, offsets_capacity),
      encoder_options_(encoder_options),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  if (encoder_options.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (!compressor_) {
    PaxColumn::SetEncodeType(
        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
    PaxColumn::SetCompressLevel(0);
  }

  Assert(encoder_options_.offsets_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  offsets_compressor_ = PaxCompressor::CreateBlockCompressor(
      encoder_options_.offsets_encode_type);
  SetOffsetsEncodeType(encoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(encoder_options_.offsets_compress_level);
}

PaxVecNonFixedEncodingColumn::PaxVecNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxVecNonFixedColumn(data_capacity, offsets_capacity),
      decoder_options_(decoding_option),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);
  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());

  Assert(decoder_options_.offsets_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  offsets_compressor_ = PaxCompressor::CreateBlockCompressor(
      decoder_options_.offsets_encode_type);
  SetOffsetsEncodeType(decoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(decoder_options_.offsets_compress_level);
}

PaxVecNonFixedEncodingColumn::~PaxVecNonFixedEncodingColumn() {}

void PaxVecNonFixedEncodingColumn::Set(
    std::shared_ptr<DataBuffer<char>> data,
    std::shared_ptr<DataBuffer<int32>> offsets, size_t total_size,
    size_t non_null_rows) {
  PaxColumn::non_null_rows_ = non_null_rows;

  auto data_decompress = [&]() {
    Assert(!compress_route_);
    Assert(compressor_);

    if (data->Used() != 0) {
      auto d_size = compressor_->Decompress(
          PaxVecNonFixedColumn::data_->Start(),
          PaxVecNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxVecNonFixedColumn::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
  };

  auto offsets_decompress = [&]() {
    Assert(!compress_route_);
    Assert(offsets_compressor_);

    if (offsets->Used() != 0) {
      auto d_size = offsets_compressor_->Decompress(
          PaxVecNonFixedColumn::offsets_->Start(),
          PaxVecNonFixedColumn::offsets_->Capacity(), offsets->Start(),
          offsets->Used());
      if (offsets_compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxVecNonFixedColumn::offsets_->Brush(d_size);
    }
  };

  if (compressor_ && offsets_compressor_) {
    data_decompress();
    offsets_decompress();
    PaxVecNonFixedColumn::estimated_size_ = total_size;
    PaxVecNonFixedColumn::next_offsets_ = -1;
  } else if (compressor_ && !offsets_compressor_) {
    data_decompress();
    PaxVecNonFixedColumn::offsets_ = std::move(offsets);
    PaxVecNonFixedColumn::estimated_size_ = total_size;
    PaxVecNonFixedColumn::next_offsets_ = -1;
  } else if (!compressor_ && offsets_compressor_) {
    PaxVecNonFixedColumn::data_ = std::move(data);
    offsets_decompress();
    PaxVecNonFixedColumn::estimated_size_ = total_size;
    PaxVecNonFixedColumn::next_offsets_ = -1;
  } else {  // (!compressor_ && !offsets_compressor_)
    PaxVecNonFixedColumn::Set(data, offsets_, total_size, non_null_rows);
  }
}

std::pair<char *, size_t> PaxVecNonFixedEncodingColumn::GetBuffer() {
  if (compressor_ && compress_route_) {
    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // do compressed
    if (PaxVecNonFixedColumn::data_->Used() == 0) {
      return PaxVecNonFixedColumn::GetBuffer();
    }

    size_t bound_size =
        compressor_->GetCompressBound(PaxVecNonFixedColumn::data_->Used());
    shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

    auto c_size = compressor_->Compress(
        shared_data_->Start(), shared_data_->Capacity(),
        PaxVecNonFixedColumn::data_->Start(),
        PaxVecNonFixedColumn::data_->Used(), encoder_options_.compress_level);

    if (compressor_->IsError(c_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
    }

    shared_data_->Brush(c_size);
    return std::make_pair(shared_data_->Start(), shared_data_->Used());
  }

  // no compress or uncompressed
  return PaxVecNonFixedColumn::GetBuffer();
}

int64 PaxVecNonFixedEncodingColumn::GetOriginLength() const {
  return PaxVecNonFixedColumn::data_->Used();
}

size_t PaxVecNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

std::pair<char *, size_t> PaxVecNonFixedEncodingColumn::GetOffsetBuffer(
    bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  if (offsets_compressor_ && compress_route_) {
    if (shared_offsets_data_) {
      return std::make_pair(shared_offsets_data_->Start(),
                            shared_offsets_data_->Used());
    }

    if (PaxVecNonFixedColumn::offsets_->Used() == 0) {
      // should never append last offset again
      return PaxVecNonFixedColumn::GetOffsetBuffer(false);
    }

    size_t bound_size = offsets_compressor_->GetCompressBound(
        PaxVecNonFixedColumn::offsets_->Used());
    shared_offsets_data_ = std::make_shared<DataBuffer<char>>(bound_size);

    auto c_size = offsets_compressor_->Compress(
        shared_offsets_data_->Start(), shared_offsets_data_->Capacity(),
        PaxVecNonFixedColumn::offsets_->Start(),
        PaxVecNonFixedColumn::offsets_->Used(),
        encoder_options_.compress_level);

    if (offsets_compressor_->IsError(c_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
    }

    shared_offsets_data_->Brush(c_size);
    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  // no compress or uncompressed
  // should never append last offset again
  return PaxVecNonFixedColumn::GetOffsetBuffer(false);
}

}  // namespace pax
