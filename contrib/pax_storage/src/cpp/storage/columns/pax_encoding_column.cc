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
 * pax_encoding_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_column.h"

#include "comm/fmt.h"
#include "storage/pax_defined.h"
#include "storage/proto/proto_wrappers.h"
namespace pax {

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_options_(encoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(true) {
  InitEncoder();
}

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_(nullptr),
      decoder_options_{decoding_option},
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(false) {
  InitDecoder();
}

template <typename T>
PaxEncodingColumn<T>::~PaxEncodingColumn() { }

template <typename T>
void PaxEncodingColumn<T>::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = GetDefaultColumnType();
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  // Create a streaming encoder
  // If current `encoded_type_` can not create a streaming encoder,
  // `CreateStreamingEncoder` will return a nullptr. This may be
  // caused by three scenarios:
  //   - `encoded_type_` is not a encoding type.
  //   - `encoded_type_` is a encoding type, but not support it yet.
  //   - `encoded_type_` is no_encoding type.
  //
  // Not allow pass `default`type` of `encoded_type_` into
  // `CreateStreamingEncoder`, caller should change it before create a encoder.
  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_);
  if (encoder_) {
    return;
  }

  // Create a block compressor
  // Compressor have a different interface with pax encoder
  // If no pax encoder no provided, then try to create a compressor.
  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (compressor_) {
    return;
  }

  // can't find any encoder or compressor
  // then should reset encode type
  // or will got origin length is -1 but still have encode type
  PaxColumn::SetEncodeType(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
  PaxColumn::SetCompressLevel(0);
}

template <typename T>
void PaxEncodingColumn<T>::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);

  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<T>(decoder_options_);
  if (decoder_) {
    // init the shared_data_ with the buffer from PaxCommColumn<T>::data_
    // cause decoder_ need a DataBuffer<char> * as dst buffer
    shared_data_ = std::make_shared<DataBuffer<char>>(*PaxCommColumn<T>::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

template <typename T>
void PaxEncodingColumn<T>::Set(std::unique_ptr<DataBuffer<T>> data) {
  if (decoder_) {
    // should not decoding null
    if (data->Used() != 0) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();

      // `data_` have the same buffer with `shared_data_`
      PaxCommColumn<T>::data_->Brush(shared_data_->Used());
    }

    Assert(!data->IsMemTakeOver());
  } else if (compressor_) {
    if (data->Used() != 0) {
      // should not init `shared_data_`, direct uncompress to `data_`
      Assert(!shared_data_);
      size_t d_size = compressor_->Decompress(
          PaxCommColumn<T>::data_->Start(), PaxCommColumn<T>::data_->Capacity(),
          data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }

      PaxCommColumn<T>::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
  } else {
    PaxCommColumn<T>::Set(std::move(data));
  }
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetBuffer() {
  if (compress_route_) {
    // already done with decoding/compress
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no data for encoding
    if (PaxCommColumn<T>::data_->Used() == 0) {
      return PaxCommColumn<T>::GetBuffer();
    }

    if (encoder_) {
      // changed streaming encode to blocking encode
      // because we still need store a origin data in `PaxCommColumn<T>`
      auto origin_data_buffer = PaxCommColumn<T>::data_.get();

      shared_data_ = std::make_shared<DataBuffer<char>>(origin_data_buffer->Used());
      encoder_->SetDataBuffer(shared_data_);
      for (size_t i = 0; i < origin_data_buffer->GetSize(); i++) {
        encoder_->Append((char *)(origin_data_buffer->GetBuffer() + i),
                         sizeof(T));
      }
      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    } else if (compressor_) {
      size_t bound_size =
          compressor_->GetCompressBound(PaxCommColumn<T>::data_->Used());
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      size_t c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(),
          PaxCommColumn<T>::data_->Start(), PaxCommColumn<T>::data_->Used(),
          encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  return PaxCommColumn<T>::GetBuffer();
}

template <typename T>
int64 PaxEncodingColumn<T>::GetOriginLength() const {
  return PaxCommColumn<T>::data_->Used();
}

template <typename T>
size_t PaxEncodingColumn<T>::PhysicalSize() const {
  if (shared_data_) {
    return shared_data_->Used();
  }

  return PaxCommColumn<T>::PhysicalSize();
}

template <typename T>
size_t PaxEncodingColumn<T>::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

template <typename T>
ColumnEncoding_Kind PaxEncodingColumn<T>::GetDefaultColumnType() {
  return ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
}

template class PaxEncodingColumn<int8>;
template class PaxEncodingColumn<int16>;
template class PaxEncodingColumn<int32>;
template class PaxEncodingColumn<int64>;

}  // namespace pax
