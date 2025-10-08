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
 * orc_format_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_format_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/orc_format_reader.h"

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/orc/orc_defined.h"

namespace pax {

OrcFormatReader::OrcFormatReader(std::unique_ptr<File> file,
                                 std::unique_ptr<File> toast_file)
    : file_(std::move(file)),
      toast_file_(std::move(toast_file)),
      reused_buffer_(nullptr),
      num_of_stripes_(0),
      is_vec_(false) {}

OrcFormatReader::~OrcFormatReader() {}

void OrcFormatReader::SetReusedBuffer(
    std::shared_ptr<DataBuffer<char>> data_buffer) {
  reused_buffer_ = std::move(data_buffer);
}

void OrcFormatReader::Open() {
  size_t file_length = 0;
  uint64 post_script_len = 0;

  Assert(file_);
  auto read_in_disk = [this](size_t file_size, size_t ps_len,
                             bool skip_read_post_script) {
    // read post script
    if (!skip_read_post_script) {
      char post_script_buffer[ps_len];
      off64_t offset;

      offset = (off64_t)(file_size - PORC_POST_SCRIPT_SIZE - ps_len);
      Assert(offset >= 0);

      file_->PReadN(post_script_buffer, ps_len, offset);

      CBDB_CHECK(post_script_.ParseFromArray(&post_script_buffer,
                                             static_cast<int>(ps_len)),
                 cbdb::CException::ExType::kExTypeInvalidPORCFormat,
                 fmt("Fail to parse the POSTSCRIPT pb structure. [file=%s, "
                     "offset=%ld, len=%lu], %s",
                     file_->GetPath().c_str(), offset, ps_len,
                     file_->DebugString().c_str()));
    }

    size_t footer_len = post_script_.footerlength();
    size_t tail_len = PORC_POST_SCRIPT_SIZE + ps_len + footer_len;
    off64_t footer_offset = file_size - tail_len;

    // read file_footer
    {
      // The footer contains statistical information. The min/max implementation
      // may have a large attribute value. buffer cannot be allocated on the
      // stack, which may cause stack overflow.
      std::unique_ptr<char[]> buffer(new char[footer_len]());

      file_->PReadN(buffer.get(), footer_len, footer_offset);

      SeekableInputStream input_stream(buffer.get(), footer_len);
      CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
                 cbdb::CException::ExType::kExTypeIOError,
                 fmt("Fail to parse the FOOTER pb structure. [file=%s, "
                     "offset=%ld, len=%lu], %s",
                     file_->GetPath().c_str(), footer_offset, footer_len,
                     file_->DebugString().c_str()));
    }
  };

  file_length = file_->FileLength();
  if (file_length > PORC_TAIL_SIZE) {
    size_t footer_len;
    size_t tail_len;
    off64_t footer_offset;
    char tail_buffer[PORC_TAIL_SIZE];

    file_->PReadN(tail_buffer, PORC_TAIL_SIZE,
                  (off64_t)(file_length - PORC_TAIL_SIZE));

    static_assert(sizeof(post_script_len) == PORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    memcpy(&post_script_len,
           &tail_buffer[PORC_TAIL_SIZE - PORC_POST_SCRIPT_SIZE],
           PORC_POST_SCRIPT_SIZE);
    if (post_script_len + PORC_POST_SCRIPT_SIZE > PORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, false);
      goto finish_read;
    }

    auto post_script_offset =
        (off64_t)(PORC_TAIL_SIZE - PORC_POST_SCRIPT_SIZE - post_script_len);
    CBDB_CHECK(
        post_script_.ParseFromArray(tail_buffer + post_script_offset,
                                    static_cast<int>(post_script_len)),
        cbdb::CException::ExType::kExTypeIOError,
        fmt("Fail to parse the POSTSCRIPT pb structure. [file=%s, offset=%ld, "
            "len=%lu], %s",
            file_->GetPath().c_str(),
            (off64_t)(file_length - PORC_POST_SCRIPT_SIZE - post_script_len),
            post_script_len, file_->DebugString().c_str()));

    footer_len = post_script_.footerlength();
    tail_len = PORC_POST_SCRIPT_SIZE + post_script_len + footer_len;
    if (tail_len > PORC_TAIL_SIZE) {
      read_in_disk(file_length, post_script_len, true);
      goto finish_read;
    }

    footer_offset = PORC_TAIL_SIZE - tail_len;
    SeekableInputStream input_stream(tail_buffer + footer_offset, footer_len);
    CBDB_CHECK(file_footer_.ParseFromZeroCopyStream(&input_stream),
               cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to parse the FOOTER pb structure. [file=%s, "
                   "offset=%ld, len=%lu], %s",
                   file_->GetPath().c_str(),
                   (off64_t)(file_length - (PORC_POST_SCRIPT_SIZE +
                                            post_script_len + footer_len)),
                   footer_len, file_->DebugString().c_str()));
  } else {
    static_assert(sizeof(post_script_len) == PORC_POST_SCRIPT_SIZE,
                  "post script type len not match.");
    file_->PReadN(&post_script_len, PORC_POST_SCRIPT_SIZE,
                  (off64_t)(file_length - PORC_POST_SCRIPT_SIZE));
    read_in_disk(file_length, post_script_len, false);
  }

finish_read:
  num_of_stripes_ = file_footer_.stripes_size();
  is_vec_ = file_footer_.storageformat() == kTypeStoragePorcVec;

  // Build schema
  auto max_id = file_footer_.types_size();
  CBDB_CHECK(max_id > 0, cbdb::CException::ExType::kExTypeInvalidPORCFormat,
             fmt("Invalid FOOTER pb structure, the schema is empty or invalid. "
                 "[type mid=%d], %s",
                 max_id, file_->DebugString().c_str()));

  const pax::porc::proto::Type &type = file_footer_.types(0);
  // There is an assumption here: for all pg tables, the outermost structure
  // should be Type_Kind_STRUCT
  CBDB_CHECK(type.kind() == pax::porc::proto::Type_Kind_STRUCT,
             cbdb::CException::ExType::kExTypeInvalidPORCFormat,
             fmt("Invalid FOOTER pb structure, the schema is invalid."
                 "The first type in PORC must be %d but got %d, %s",
                 pax::porc::proto::Type_Kind_STRUCT, type.kind(),
                 file_->DebugString().c_str()));

  CBDB_CHECK(type.subtypes_size() == max_id - 1,
             cbdb::CException::ExType::kExTypeInvalidPORCFormat,
             fmt("Invalid FOOTER pb structure, the schema is invalid."
                 "Subtypes not match the type sizes [type mid=%d, subtypes "
                 "size=%d], %s",
                 max_id, type.subtypes_size(), file_->DebugString().c_str()));

  column_types_.resize(type.subtypes_size());
  column_attrs_.resize(type.subtypes_size());
  for (int j = 0; j < type.subtypes_size(); ++j) {
    int sub_type_id = static_cast<int>(type.subtypes(j)) + 1;
    const pax::porc::proto::Type &sub_type = file_footer_.types(sub_type_id);
    // should allow struct contain struct
    // but not support yet
    CBDB_CHECK(sub_type.kind() != pax::porc::proto::Type_Kind_STRUCT,
               cbdb::CException::ExType::kExTypeInvalidPORCFormat,
               fmt("Invalid FOOTER pb structure, the schema is invalid. "
                   "Current PORC not support the inhert STRUCT type. %s",
                   file_->DebugString().c_str()));

    column_types_[j] = sub_type.kind();
    for (int k = 0; k < sub_type.attributes_size(); k++) {
      auto column_attr = sub_type.attributes(k);
      column_attrs_[j][column_attr.key()] = column_attr.value();
    }
  }

  // Build stripe row offset array
  size_t cur_stripe_row_offset = 0;
  for (size_t i = 0; i < num_of_stripes_; i++) {
    stripe_row_offsets_.emplace_back(cur_stripe_row_offset);
    cur_stripe_row_offset += file_footer_.stripes(i).numberofrows();
  }
}

void OrcFormatReader::Close() {
  file_->Close();
  if (toast_file_) toast_file_->Close();
}

size_t OrcFormatReader::GetStripeNums() const { return num_of_stripes_; }

size_t OrcFormatReader::GetStripeNumberOfRows(size_t stripe_index) {
  Assert(stripe_index < GetStripeNums());
  return file_footer_.stripes(static_cast<int>(stripe_index)).numberofrows();
}

size_t OrcFormatReader::GetStripeOffset(size_t stripe_index) {
  Assert(stripe_index < GetStripeNums());
  return stripe_row_offsets_[stripe_index];
}

pax::porc::proto::StripeFooter OrcFormatReader::ReadStripeFooter(
    std::shared_ptr<DataBuffer<char>> data_buffer, size_t sf_length,
    off64_t sf_offset, size_t sf_data_len, size_t group_index) {
  pax::porc::proto::StripeFooter stripe_footer;

  Assert(data_buffer->Capacity() >= (sf_length - sf_data_len));
  file_->PReadN(data_buffer->GetBuffer(), sf_length - sf_data_len,
                sf_offset + sf_data_len);
  SeekableInputStream input_stream(data_buffer->GetBuffer(),
                                   sf_length - sf_data_len);
  if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
    // fail to do memory io with protobuf
    CBDB_RAISE(cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to parse the STRIPE FOOTER pb structure. [group "
                   "index=%lu, file=%s, offset=%ld, len=%lu], %s",
                   group_index, file_->GetPath().c_str(), sf_offset, sf_length,
                   file_->DebugString().c_str()));
  }

  return stripe_footer;
}

pax::porc::proto::StripeFooter OrcFormatReader::ReadStripeFooter(
    std::shared_ptr<DataBuffer<char>> data_buffer, size_t group_index) {
  size_t sf_data_len;
  off64_t sf_offset;
  size_t sf_length;
  pax::porc::proto::StripeInformation stripe_info;

  Assert(group_index < GetStripeNums());

  stripe_info = file_footer_.stripes(static_cast<int>(group_index));

  sf_data_len = stripe_info.datalength();
  sf_offset = stripe_info.offset();
  sf_length = stripe_info.footerlength();

  Assert(data_buffer->IsMemTakeOver());
  Assert(data_buffer->Used() == 0);

  if (data_buffer->Capacity() < (sf_length - sf_data_len)) {
    data_buffer->ReSize(sf_length - sf_data_len);
  }

  return ReadStripeFooter(data_buffer, sf_length, sf_offset, sf_data_len,
                          group_index);
}

pax::porc::proto::StripeFooter OrcFormatReader::ReadStripeWithProjection(
    std::shared_ptr<DataBuffer<char>> data_buffer,
    const ::pax::porc::proto::StripeInformation &stripe_info,
    const std::vector<bool> &proj_cols, size_t group_index) {
  pax::porc::proto::StripeFooter stripe_footer;
  size_t stripe_footer_data_len;
  off64_t stripe_footer_offset;
  size_t stripe_footer_length;

  size_t streams_index = 0;
  uint64_t batch_len = 0;
  off64_t batch_offset = 0;
  size_t index = 0;

  stripe_footer_data_len = stripe_info.datalength();
  stripe_footer_offset = stripe_info.offset();
  stripe_footer_length = stripe_info.footerlength();

  /* Check all column projection is true, if proj_cols
   * is empty no need do column projection, read all
   * buffer(data + stripe footer) from stripe and decode stripe footer.
   */
  if (proj_cols.empty()) {
    file_->PReadN(data_buffer->GetBuffer(), stripe_footer_length,
                  stripe_footer_offset);
    SeekableInputStream input_stream(
        data_buffer->GetBuffer() + stripe_footer_data_len,
        stripe_footer_length - stripe_footer_data_len);
    if (!stripe_footer.ParseFromZeroCopyStream(&input_stream)) {
      CBDB_RAISE(
          cbdb::CException::ExType::kExTypeIOError,
          fmt("Fail to parse the STRIPE FOOTER pb structure. [group index=%lu, "
              "file=%s, offset=%ld, len=%lu], %s",
              group_index, file_->GetPath().c_str(), stripe_footer_offset,
              stripe_footer_length, file_->DebugString().c_str()));
    }

    return stripe_footer;
  }

  /* If need do column projection here
   * Then read stripe footer and decode it before read data part
   */
  stripe_footer =
      ReadStripeFooter(data_buffer, stripe_footer_length, stripe_footer_offset,
                       stripe_footer_data_len, group_index);
  data_buffer->BrushBackAll();

  batch_offset = stripe_footer_offset;

  while (index < column_types_.size()) {
    // Current column have been skipped
    // Move `batch_offset` and `streams_index` to the right position
    if (!proj_cols[index]) {
      index++;

      const pax::porc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
        batch_offset += n_stream->length();
      } while (n_stream->kind() !=
               ::pax::porc::proto::Stream_Kind::Stream_Kind_DATA);

      continue;
    }

    batch_len = 0;

    /* Current column should be read
     * In this case, did a greedy algorithm to combine io: while
     * the current column is being read, it is necessary
     * to ensure that subsequent columns will be read in the same io.
     *
     * So in `do...while`, only the `batch_size` which io needs to read
     * is calculated, until meet a column which needs to be skipped.
     */
    bool all_null = true;
    do {
      bool has_null = stripe_info.colstats(index).hasnull();
      bool has_toast = stripe_info.colstats(index).hastoast();
      all_null = all_null && stripe_info.colstats(index).allnull();
      if (has_null) {
        const pax::porc::proto::Stream &non_null_stream =
            stripe_footer.streams(streams_index++);
        Assert(non_null_stream.kind() ==
               ::pax::porc::proto::Stream_Kind::Stream_Kind_PRESENT);
        batch_len += non_null_stream.length();
      }

      if (has_toast) {
        const pax::porc::proto::Stream &toast_stream =
            stripe_footer.streams(streams_index++);
        Assert(toast_stream.kind() ==
               ::pax::porc::proto::Stream_Kind::Stream_Kind_TOAST);
        batch_len += toast_stream.length();
      }

      const pax::porc::proto::Stream *len_or_data_stream =
          &stripe_footer.streams(streams_index++);
      batch_len += len_or_data_stream->length();

      if (len_or_data_stream->kind() ==
          ::pax::porc::proto::Stream_Kind::Stream_Kind_OFFSET) {
        len_or_data_stream = &stripe_footer.streams(streams_index++);
        batch_len += len_or_data_stream->length();
      }
    } while ((++index) < column_types_.size() && proj_cols[index]);

    // There is only one situation where batch_len is equal to 0, that is all
    // values in this column are null. null_bitmap does not store data when it
    // is all 0, and data_streams does not store null values.
    // so if the batch_len of a column equals 0, we should check all_null flags
    if (unlikely(batch_len == 0)) {
      CBDB_CHECK(
          all_null, cbdb::CException::kExTypeInvalidPORCFormat,
          fmt("Current stripe data may broken. the length of data is 0, but "
              "still has not null rows inside %s",
              file_->DebugString().c_str()));
      continue;
    }

    file_->PReadN(data_buffer->GetAvailableBuffer(), batch_len, batch_offset);
    data_buffer->Brush(batch_len);
    batch_offset += batch_len;
  }

  return stripe_footer;
}

template <typename T>
static std::unique_ptr<PaxColumn> BuildEncodingColumn(
    std::shared_ptr<DataBuffer<char>> data_buffer,
    const pax::porc::proto::Stream &data_stream,
    const ColumnEncoding &data_encoding, bool is_vec) {
  uint32 not_null_rows = 0;
  uint64 data_stream_len = 0;
  std::unique_ptr<DataBuffer<T>> data_stream_buffer;

  Assert(data_stream.kind() == pax::porc::proto::Stream_Kind_DATA);

  not_null_rows = static_cast<uint32>(data_stream.column());
  data_stream_len = static_cast<uint64>(data_stream.length());

  data_stream_buffer = std::make_unique<DataBuffer<T>>(
      reinterpret_cast<T *>(data_buffer->GetAvailableBuffer()), data_stream_len,
      false, false);

  data_stream_buffer->BrushAll();
  data_buffer->Brush(data_stream_len);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();

  size_t alloc_size = 0;

  if (data_encoding.kind() !=
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    alloc_size = data_encoding.length();
  }

  if (is_vec) {
    auto pax_column =
        traits::ColumnOptCreateTraits<PaxVecEncodingColumn, T>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(std::move(data_stream_buffer), (size_t)not_null_rows);
    return pax_column;
  } else {
    AssertImply(data_encoding.kind() ==
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                not_null_rows == data_stream_buffer->GetSize());
    AssertImply(data_encoding.kind() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                data_encoding.length() / sizeof(T) == not_null_rows);
    auto pax_column =
        traits::ColumnOptCreateTraits<PaxEncodingColumn, T>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(std::move(data_stream_buffer));
    return pax_column;
  }
}

static std::unique_ptr<PaxColumn> BuildEncodingBitPackedColumn(
    std::shared_ptr<DataBuffer<char>> data_buffer,
    const porc::proto::Stream &data_stream, const ColumnEncoding &data_encoding,
    bool is_vec) {
  uint32 not_null_rows = 0;
  uint64 column_data_len = 0;
  std::unique_ptr<DataBuffer<int8>> column_data_buffer;

  Assert(data_stream.kind() == pax::porc::proto::Stream_Kind_DATA);

  not_null_rows = static_cast<uint32>(data_stream.column());
  column_data_len = static_cast<uint64>(data_stream.length());

  column_data_buffer = std::make_unique<DataBuffer<int8>>(
      reinterpret_cast<int8 *>(data_buffer->GetAvailableBuffer()),
      column_data_len, false, false);

  column_data_buffer->BrushAll();
  data_buffer->Brush(column_data_len);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();

  size_t alloc_size = 0;

  if (data_encoding.kind() !=
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    alloc_size = data_encoding.length();
  }

  if (is_vec) {
    auto pax_column =
        traits::ColumnOptCreateTraits2<PaxVecBitPackedColumn>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(std::move(column_data_buffer), (size_t)not_null_rows);
    return pax_column;
  } else {
    AssertImply(data_encoding.kind() ==
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                not_null_rows == column_data_buffer->GetSize());
    AssertImply(data_encoding.kind() !=
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                data_encoding.length() / sizeof(int8) == not_null_rows);
    auto pax_column =
        traits::ColumnOptCreateTraits2<PaxBitPackedColumn>::create_decoding(
            alloc_size, decoding_option);
    pax_column->Set(std::move(column_data_buffer));
    return pax_column;
  }
}

static std::unique_ptr<PaxColumn> BuildEncodingDecimalColumn(
    const std::shared_ptr<DataBuffer<char>> &data_buffer,
    const pax::porc::proto::Stream &data_stream,
    const pax::porc::proto::Stream &len_stream,
    const ColumnEncoding &data_encoding) {
  uint32 not_null_rows = 0;
  uint64 offset_stream_len = 0;
  uint64 data_stream_len = 0;
  std::unique_ptr<DataBuffer<int32>> offset_stream_buffer;
  std::unique_ptr<DataBuffer<char>> data_stream_buffer;
  std::unique_ptr<PaxNonFixedColumn> pax_column;
  uint64 padding = 0;

  not_null_rows = static_cast<uint32>(len_stream.column());
  offset_stream_len = static_cast<uint64>(len_stream.length());
  padding = len_stream.padding();

  offset_stream_buffer = std::make_unique<DataBuffer<int32>>(
      reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
      offset_stream_len, false, false);

  if (data_encoding.offset_stream_kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    Assert(offset_stream_len >= not_null_rows * sizeof(int32));
    offset_stream_buffer->Brush(not_null_rows * sizeof(int32));
  } else {
    // if current length stream compress
    // Then should brush all
    offset_stream_buffer->Brush(offset_stream_len - padding);
  }

  data_buffer->Brush(offset_stream_len);
  data_stream_len = data_stream.length();

#ifdef ENABLE_DEBUG
  if (data_encoding.kind() ==
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED &&
      data_encoding.offset_stream_kind() ==
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    size_t segs_size = 0;
    for (size_t i = 0; i < offset_stream_buffer->GetSize(); i++) {
      segs_size += (*offset_stream_buffer)[i];
    }
    Assert(data_stream_len == segs_size);
  }
#endif

  data_stream_buffer = std::make_unique<DataBuffer<char>>(
      data_buffer->GetAvailableBuffer(), data_stream_len, false, false);
  data_stream_buffer->BrushAll();
  data_buffer->Brush(data_stream_len);

  Assert(static_cast<uint32>(data_stream.column()) == not_null_rows);

  size_t data_cap, offsets_cap;
  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();
  decoding_option.offsets_encode_type = data_encoding.offset_stream_kind();
  decoding_option.offsets_compress_level =
      data_encoding.offset_stream_compress_lvl();

  data_cap = data_encoding.kind() ==
                     ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                 ? 0
                 : data_encoding.length();

  offsets_cap = data_encoding.offset_stream_kind() ==
                        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                    ? 0
                    : data_encoding.offset_stream_length();

  pax_column =
      traits::ColumnOptCreateTraits2<PaxPgNumericColumn>::create_decoding(
          data_cap, offsets_cap, std::move(decoding_option));

  // current memory will be freed in pax_columns->data_
  pax_column->Set(std::move(data_stream_buffer), std::move(offset_stream_buffer), data_stream_len);
  return pax_column;
}

static std::unique_ptr<PaxColumn> BuildVecEncodingDecimalColumn(
    const std::shared_ptr<DataBuffer<char>> &data_buffer,
    const pax::porc::proto::Stream &data_stream,
    const ColumnEncoding &data_encoding, bool is_vec) {
  uint32 not_null_rows = 0;
  uint64 data_stream_len = 0;
  std::unique_ptr<DataBuffer<int8>> data_stream_buffer;

  CBDB_CHECK(is_vec, cbdb::CException::ExType::kExTypeLogicError);

  Assert(data_stream.kind() == pax::porc::proto::Stream_Kind_DATA);

  not_null_rows = static_cast<uint32>(data_stream.column());
  data_stream_len = static_cast<uint64>(data_stream.length());

  data_stream_buffer = std::make_unique<DataBuffer<int8>>(
      reinterpret_cast<int8 *>(data_buffer->GetAvailableBuffer()),
      data_stream_len, false, false);

  data_stream_buffer->BrushAll();
  data_buffer->Brush(data_stream_len);

  PaxDecoder::DecodingOption decoding_option;
  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;

  size_t alloc_size = 0;

  if (data_encoding.kind() !=
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    alloc_size = data_encoding.length();
  }

  auto pax_column = traits::ColumnOptCreateTraits2<PaxShortNumericColumn>::  //
      create_decoding(alloc_size, decoding_option);
  pax_column->Set(std::move(data_stream_buffer), (size_t)not_null_rows);

  return pax_column;
}

static std::unique_ptr<PaxColumn> BuildEncodingVecNonFixedColumn(
    const std::shared_ptr<DataBuffer<char>> &data_buffer,
    const pax::porc::proto::Stream &data_stream,
    const pax::porc::proto::Stream &len_stream,
    const ColumnEncoding &data_encoding, bool is_bpchar, bool is_no_hdr) {
  uint32 not_null_rows = 0;
  uint64 offset_stream_len = 0;
  uint64 padding = 0;
  uint64 data_stream_len = 0;
  std::unique_ptr<DataBuffer<int32>> offset_stream_buffer;
  std::unique_ptr<DataBuffer<char>> data_stream_buffer;
  std::unique_ptr<PaxVecNonFixedColumn> pax_column;
  PaxDecoder::DecodingOption decoding_option;
  size_t data_cap, offsets_cap;

  Assert(!(is_bpchar && is_no_hdr));

  auto total_rows = static_cast<uint32>(len_stream.column());
  not_null_rows = static_cast<uint32>(data_stream.column());
  data_stream_len = static_cast<uint64>(data_stream.length());
  offset_stream_len = static_cast<uint64>(len_stream.length());
  padding = len_stream.padding();

  offset_stream_buffer = std::make_unique<DataBuffer<int32>>(
      reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
      offset_stream_len, false, false);

  if (data_encoding.offset_stream_kind() ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    Assert(offset_stream_len >= ((total_rows + 1) * sizeof(int32)));
    offset_stream_buffer->Brush((total_rows + 1) * sizeof(int32));
    // at lease 2
    Assert(offset_stream_buffer->GetSize() >= 2);

  } else {
    // if current length stream compress
    // Then should brush all
    offset_stream_buffer->Brush(offset_stream_len - padding);
  }

  data_buffer->Brush(offset_stream_len);
  data_stream_buffer = std::make_unique<DataBuffer<char>>(
      data_buffer->GetAvailableBuffer(), data_stream_len, false, false);

  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();
  decoding_option.offsets_encode_type = data_encoding.offset_stream_kind();
  decoding_option.offsets_compress_level =
      data_encoding.offset_stream_compress_lvl();

  data_cap = data_encoding.kind() ==
                     ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                 ? 0
                 : data_encoding.length();

  offsets_cap = data_encoding.offset_stream_kind() ==
                        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                    ? 0
                    : data_encoding.offset_stream_length();

  if (is_bpchar) {
    data_buffer->Brush(data_stream_len);
    data_stream_buffer->BrushAll();

    pax_column =
        traits::ColumnOptCreateTraits2<PaxVecBpCharColumn>::create_decoding(
            data_cap, offsets_cap, std::move(decoding_option));
  } else if (is_no_hdr) {
    data_buffer->Brush(data_stream_len);
    data_stream_buffer->BrushAll();

    pax_column =
        traits::ColumnOptCreateTraits2<PaxVecNoHdrColumn>::create_decoding(
            data_cap, offsets_cap, std::move(decoding_option));
  } else {
    if (data_encoding.kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED &&
        data_encoding.offset_stream_kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
      data_stream_buffer->Brush(
          (*offset_stream_buffer)[offset_stream_buffer->GetSize() - 1]);
      data_buffer->Brush(data_stream_len);

      pax_column =
          traits::ColumnCreateTraits2<PaxVecNonFixedColumn>::create(0, 0);
    } else {
      data_buffer->Brush(data_stream_len);
      data_stream_buffer->BrushAll();

      pax_column =
          traits::ColumnOptCreateTraits2<PaxVecNonFixedEncodingColumn>::  //
          create_decoding(data_cap, offsets_cap, std::move(decoding_option));
    }
  }
  pax_column->Set(std::move(data_stream_buffer), std::move(offset_stream_buffer), data_stream_len,
                  not_null_rows);
  return pax_column;
}

static std::unique_ptr<PaxColumn> BuildEncodingNonFixedColumn(
    std::shared_ptr<DataBuffer<char>> data_buffer,
    const pax::porc::proto::Stream &data_stream,
    const pax::porc::proto::Stream &len_stream,
    const ColumnEncoding &data_encoding, bool is_bpchar) {
  [[maybe_unused]] uint32 not_null_rows = 0;
  uint64 offset_stream_len = 0;
  uint64 data_stream_len = 0;
  std::unique_ptr<DataBuffer<int32>> offset_stream_buffer;
  std::unique_ptr<DataBuffer<char>> data_stream_buffer;
  std::unique_ptr<PaxNonFixedColumn> pax_column;
  uint64 padding = 0;
  PaxDecoder::DecodingOption decoding_option;
  size_t data_cap, offsets_cap;

  not_null_rows = static_cast<uint32>(len_stream.column());
  offset_stream_len = static_cast<uint64>(len_stream.length());
  padding = len_stream.padding();

  offset_stream_buffer = std::make_unique<DataBuffer<int32>>(
      reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
      offset_stream_len, false, false);

  AssertImply(data_encoding.offset_stream_kind() ==
                  ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
              offset_stream_len >= (not_null_rows + 1) * sizeof(int32));
  offset_stream_buffer->Brush(offset_stream_len - padding);
  data_buffer->Brush(offset_stream_len);
  data_stream_len = data_stream.length();

#ifdef ENABLE_DEBUG
  if (data_encoding.kind() ==
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED &&
      data_encoding.offset_stream_kind() ==
          ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    Assert(
        data_stream_len ==
        (uint64)(*offset_stream_buffer)[offset_stream_buffer->GetSize() - 1]);
  }
#endif

  data_stream_buffer = std::make_unique<DataBuffer<char>>(
      data_buffer->GetAvailableBuffer(), data_stream_len, false, false);
  data_stream_buffer->BrushAll();
  data_buffer->Brush(data_stream_len);

  decoding_option.column_encode_type = data_encoding.kind();
  decoding_option.is_sign = true;
  decoding_option.compress_level = data_encoding.compress_lvl();
  decoding_option.offsets_encode_type = data_encoding.offset_stream_kind();
  decoding_option.offsets_compress_level =
      data_encoding.offset_stream_compress_lvl();

  data_cap = data_encoding.kind() ==
                     ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                 ? 0
                 : data_encoding.length();

  offsets_cap = data_encoding.offset_stream_kind() ==
                        ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED
                    ? 0
                    : data_encoding.offset_stream_length();

  Assert(static_cast<uint32>(data_stream.column()) == not_null_rows);
  if (is_bpchar) {
    pax_column =
        traits::ColumnOptCreateTraits2<PaxBpCharColumn>::create_decoding(
            data_cap, offsets_cap, std::move(decoding_option));
  } else {
    if (data_encoding.kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED &&
        data_encoding.offset_stream_kind() ==
            ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
      Assert(data_stream_len == data_stream_buffer->GetSize());
      pax_column = traits::ColumnCreateTraits2<PaxNonFixedColumn>::create(0, 0);
    } else {
      pax_column = traits::ColumnOptCreateTraits2<
          PaxNonFixedEncodingColumn>::create_decoding(data_cap, offsets_cap,
                                                      std::move(
                                                          decoding_option));
    }
  }

  // current memory will be freed in pax_columns->data_
  pax_column->Set(std::move(data_stream_buffer), std::move(offset_stream_buffer), data_stream_len);
  return pax_column;
}

// TODO(jiaqizho): add args buffer
// which can read from a prev read buffer
std::unique_ptr<PaxColumns> OrcFormatReader::ReadStripe(
    size_t group_index, const std::vector<bool> &proj_cols) {
  auto stripe_info = file_footer_.stripes(static_cast<int>(group_index));
  auto pax_columns = std::make_unique<PaxColumns>();
  std::shared_ptr<DataBuffer<char>> data_buffer;
  pax::porc::proto::StripeFooter stripe_footer;
  size_t streams_index = 0;
  size_t streams_size = 0;

  pax_columns->AddRows(stripe_info.numberofrows());

  if (unlikely(stripe_info.footerlength() == 0)) {
    return pax_columns;
  }

  if (reused_buffer_) {
    Assert(reused_buffer_->Capacity() >= 4);
    Assert(reused_buffer_->Used() == 0);
    if (reused_buffer_->Available() < stripe_info.footerlength()) {
      reused_buffer_->ReSize(
          reused_buffer_->Used() + stripe_info.footerlength(), 1.5);
    }
    data_buffer = std::make_shared<DataBuffer<char>>(
        reused_buffer_->GetBuffer(), reused_buffer_->Capacity(), false, false);

  } else {
    data_buffer =
        std::make_shared<DataBuffer<char>>(stripe_info.footerlength());
  }
  pax_columns->Set(data_buffer);
  pax_columns->SetStorageFormat(is_vec_
                                    ? PaxStorageFormat::kTypeStoragePorcVec
                                    : PaxStorageFormat::kTypeStoragePorcNonVec);

  /* `ReadStripeWithProjection` will read the column memory which filter by
   * `proj_cols`, and initialize `stripe_footer`
   *
   * Notice that: should catch `kExTypeIOError` then delete pax columns
   * But for now we will destroy memory context if exception happen.
   * And we don't have a decision that should we use `try...catch` at yet,
   * so it's ok that we just no catch here.
   */
  stripe_footer = ReadStripeWithProjection(data_buffer, stripe_info, proj_cols,
                                           group_index);

  streams_size = stripe_footer.streams_size();

  if (unlikely(streams_size == 0 && column_types_.empty())) {
    return pax_columns;
  }

  data_buffer->BrushBackAll();

  AssertImply(!proj_cols.empty(), column_types_.size() <= proj_cols.size());
  Assert(static_cast<size_t>(stripe_footer.pax_col_encodings_size()) <=
         column_types_.size());
  Assert(column_types_.size() == column_attrs_.size());

  for (size_t index = 0; index < column_types_.size(); index++) {
    /* Skip read current column, just move `streams_index` after
     * `Stream_Kind_DATA` but still need append nullptr into `PaxColumns` to
     * make sure sizeof pax_columns eq with column number
     */
    if (!proj_cols.empty() && !proj_cols[index]) {
      const pax::porc::proto::Stream *n_stream = nullptr;
      do {
        n_stream = &stripe_footer.streams(streams_index++);
      } while (n_stream->kind() !=
               ::pax::porc::proto::Stream_Kind::Stream_Kind_DATA);

      pax_columns->Append(nullptr);
      continue;
    }

    std::unique_ptr<Bitmap8> non_null_bitmap;
    bool has_null = stripe_info.colstats(index).hasnull();
    if (has_null) {
      const pax::porc::proto::Stream &non_null_stream =
          stripe_footer.streams(streams_index++);
      auto bm_nbytes = static_cast<uint32>(non_null_stream.length());
      auto bm_bytes =
          reinterpret_cast<uint8 *>(data_buffer->GetAvailableBuffer());

      Assert(non_null_stream.kind() == pax::porc::proto::Stream_Kind_PRESENT);
      non_null_bitmap = std::make_unique<Bitmap8>(BitmapRaw<uint8>(bm_bytes, bm_nbytes));
      data_buffer->Brush(bm_nbytes);
    }

    std::shared_ptr<DataBuffer<int32>> toast_indexes;
    bool has_toast = stripe_info.colstats(index).hastoast();
    if (has_toast) {
      const pax::porc::proto::Stream &toast_stream =
          stripe_footer.streams(streams_index++);
      auto toast_nbytes = static_cast<uint32>(toast_stream.length());
      auto toast_n = static_cast<uint32>(toast_stream.column());

      Assert(toast_nbytes >= toast_n * sizeof(int32));
      Assert(toast_stream.kind() == pax::porc::proto::Stream_Kind_TOAST);
      toast_indexes = std::make_shared<DataBuffer<int32>>(
          reinterpret_cast<int32 *>(data_buffer->GetAvailableBuffer()),
          toast_n * sizeof(int32), false, false);
      toast_indexes->BrushAll();
      data_buffer->Brush(toast_nbytes);
    }

    switch (column_types_[index]) {
      case (pax::porc::proto::Type_Kind::Type_Kind_BPCHAR):
      case (pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR):
      case (pax::porc::proto::Type_Kind::Type_Kind_VECNOHEADER):
      case (pax::porc::proto::Type_Kind::Type_Kind_STRING): {
        const pax::porc::proto::Stream &len_stream =
            stripe_footer.streams(streams_index++);
        const pax::porc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);

        Assert(len_stream.kind() == pax::porc::proto::Stream_Kind_OFFSET);
        Assert(data_stream.kind() == pax::porc::proto::Stream_Kind_DATA);

        pax_columns->Append(
            is_vec_
                ? BuildEncodingVecNonFixedColumn(
                      data_buffer, data_stream, len_stream, data_encoding,
                      column_types_[index] ==
                          pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR,
                      column_types_[index] ==
                          pax::porc::proto::Type_Kind::Type_Kind_VECNOHEADER)
                : BuildEncodingNonFixedColumn(
                      data_buffer, data_stream, len_stream, data_encoding,
                      column_types_[index] ==
                          pax::porc::proto::Type_Kind::Type_Kind_BPCHAR));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN): {
        pax_columns->Append(BuildEncodingBitPackedColumn(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_BYTE):
        pax_columns->Append(BuildEncodingColumn<int8>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_SHORT):
        pax_columns->Append(BuildEncodingColumn<int16>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      case (pax::porc::proto::Type_Kind::Type_Kind_INT): {
        pax_columns->Append(BuildEncodingColumn<int32>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_LONG): {
        pax_columns->Append(BuildEncodingColumn<int64>(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_DECIMAL): {
        const pax::porc::proto::Stream &len_stream =
            stripe_footer.streams(streams_index++);
        const pax::porc::proto::Stream &data_stream =
            stripe_footer.streams(streams_index++);
        const ColumnEncoding &data_encoding =
            stripe_footer.pax_col_encodings(index);

        Assert(len_stream.kind() == pax::porc::proto::Stream_Kind_OFFSET);
        Assert(data_stream.kind() == pax::porc::proto::Stream_Kind_DATA);

        pax_columns->Append(BuildEncodingDecimalColumn(
            data_buffer, data_stream, len_stream, data_encoding));
        break;
      }
      case (pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL): {
        pax_columns->Append(BuildVecEncodingDecimalColumn(
            data_buffer, stripe_footer.streams(streams_index++),
            stripe_footer.pax_col_encodings(index), is_vec_));
        break;
      }
      default:
        // should't be here
        Assert(!"should't be here, non-implemented type");
        break;
    }

    // fill nulls data buffer
    Assert(pax_columns->GetColumns() > 0);
    const auto &last_column = (*pax_columns)[pax_columns->GetColumns() - 1];
    Assert(last_column);
    last_column->SetRows(stripe_info.numberofrows());
    if (has_null) {
      Assert(non_null_bitmap);
      last_column->SetBitmap(std::move(non_null_bitmap));
    }

    if (!column_attrs_[index].empty()) {
      last_column->SetAttributes(column_attrs_[index]);
    }

    if (has_toast) {
      Assert(toast_indexes);
      last_column->SetToastIndexes(toast_indexes);
    }
  }

#ifdef ENABLE_DEBUG
  auto storage_tyep_verify = is_vec_ ? PaxStorageFormat::kTypeStoragePorcVec
                                     : PaxStorageFormat::kTypeStoragePorcNonVec;

  Assert(storage_tyep_verify == pax_columns->GetStorageFormat());
  for (size_t index = 0; index < column_types_.size(); index++) {
    const auto &column_verify = (*pax_columns)[index];
    if (column_verify) {
      Assert(storage_tyep_verify == column_verify->GetStorageFormat());
    }
  }
#endif

  AssertImply(!toast_file_, stripe_info.toastlength() == 0);
  // deal the toast part
  if (toast_file_ && stripe_info.toastlength() > 0) {
    std::vector<std::pair<off64_t, size_t>> projection_no_combine, projection;
    std::vector<size_t> column_ext_sizes;
    size_t toast_file_size;
    uint64 ext_total_size = 0;
    off64_t curr_column_ext_offset = 0;
    std::unique_ptr<DataBuffer<char>> external_toast_buffer;

    Assert(stripe_info.numberoftoast() != 0);
    Assert((size_t)stripe_info.exttoastlength_size() ==
           pax_columns->GetColumns());
    toast_file_size = toast_file_->FileLength();
    Assert(ext_total_size <= toast_file_size);

    for (int i = 0; i < stripe_info.exttoastlength_size(); i++) {
      auto curr_column_ext_len = stripe_info.exttoastlength(i);
      column_ext_sizes.emplace_back(curr_column_ext_len);
      if ((*pax_columns)[i] && curr_column_ext_len) {
        ext_total_size += curr_column_ext_len;
        // must be sorted
        projection_no_combine.emplace_back(
            curr_column_ext_offset,
            curr_column_ext_offset + curr_column_ext_len);
      }

      curr_column_ext_offset += curr_column_ext_len;
    }

#ifdef ENABLE_DEBUG
    // verify the external toast datums
    pax_columns->VerifyAllExternalToasts(column_ext_sizes);
#endif

    for (size_t i = 0; i < projection_no_combine.size();) {
      uint64 t = projection_no_combine[i].second;
      uint64 j = i + 1;
      while (j < projection_no_combine.size() &&
             (size_t)projection_no_combine[j].first == t) {
        t = projection_no_combine[j].second;
        j++;
      }
      projection.emplace_back(
          std::make_pair(projection_no_combine[i].first, t));
      i = j;
    }

    external_toast_buffer = std::make_unique<DataBuffer<char>>(ext_total_size);

    for (const auto &range : projection) {
      CBDB_CHECK(
          external_toast_buffer->Available() >= (range.second - range.first) &&
              range.second <= toast_file_size &&
              (range.second - range.first) <= stripe_info.toastlength(),
          cbdb::CException::ExType::kExTypeInvalidExternalToast,
          fmt("Failed to parse current external toast, [file size=%lu, total "
              "size=%lu, stripe toast len=%lu, remain len=%lu, toast "
              "offset=%ld, toast end pos=%lu], \n pax file: %s, \n toast file: "
              "%s",
              toast_file_size, ext_total_size, stripe_info.toastlength(),
              external_toast_buffer->Available(), range.second, range.first,
              file_->DebugString().c_str(),
              toast_file_->DebugString().c_str()));

      toast_file_->PReadN(external_toast_buffer->GetAvailableBuffer(),
                          range.second - range.first,
                          stripe_info.toastoffset() + range.first);
      external_toast_buffer->Brush(range.second - range.first);
    }

    Assert(external_toast_buffer->Available() == 0);

    pax_columns->SetExternalToastDataBuffer(std::move(external_toast_buffer),
                                            column_ext_sizes);
  }

  Assert(streams_size == streams_index);
  return pax_columns;
}

}  // namespace pax
