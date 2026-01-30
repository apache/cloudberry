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
 * pax_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <stddef.h>

#include <cstring>
#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "comm/bitmap.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"
#include "storage/columns/pax_encoding_utils.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

#define DEFAULT_CAPACITY \
  MIN(2048, MAX(16, MAXALIGN(pax::pax_max_tuples_per_group)))

// Used to mapping pg_type
enum PaxColumnTypeInMem {
  kTypeInvalid = 1,
  kTypeFixed = 2,
  kTypeNonFixed = 3,
  kTypeDecimal = 4,
  kTypeVecDecimal = 5,
  kTypeBpChar = 6,
  kTypeVecBpChar = 7,
  kTypeBitPacked = 8,
  kTypeVecBitPacked = 9,
  kTypeVecNoHeader = 10,
};

class PaxColumn {
 public:
  PaxColumn();

  virtual ~PaxColumn();

  // Get the column in memory type
  virtual PaxColumnTypeInMem GetPaxColumnTypeInMem() const;

  // Get column buffer from current column
  virtual std::pair<char *, size_t> GetBuffer() = 0;

  // The interface `GetBuffer(size_t position)` and
  // `GetRangeBuffer(size_t start_pos, size_t len)`
  // will return the different values in different
  // `ColumnStorageType` + `ColumnTypeInMem`
  //
  // Also they should NEVER call in write path with encoding option!!!
  // But without encoding option, still can direct call it.
  //
  // If `storage_type_` is kTypeStoragePorcVec
  // Then data part contains `null field` which means no need use
  // `row index - null counts` to get the data.
  //
  // But If `storage_type_` is not kTypeStoragePorcVec
  // Then position should be `row index - null counts`, because
  // data part will not contains `null field`.
  //
  // Also it is kind different in fixed-length column and non-fixed-length
  // column when `storage_type_` is kTypeStoragePorcVec. For the fixed-length
  // column, If we got a `null field`, then it will return the buffer with zero
  // fill. But in non-fixed-length column, once we got  `null field`, the buffer
  // will be nullptr.
  //
  // A example to explain:
  //  std::tuple<char *, size_t, bool> GetBufferWithNull(
  //     size_t row_index,
  //     size_t null_counts) {
  //
  //    PaxColumn *column = source();
  //    char * buffer = nullptr;
  //    size_t length = 0;
  //    switch (GetPaxColumnTypeInMem()) {
  //      case kTypeFixed: {
  //        if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //          std::tie(buffer, length) = column->GetBuffer(row_index);
  //          assert(buffer);  // different return in different ColumnTypeInMem
  //          if (!length) {
  //            return {nullptr, 0, true};
  //          }
  //        } else {
  //          std::tie(buffer, length) = column->GetBuffer(
  //            row_index - null_counts);
  //        }
  //        assert(buffer && length);
  //        return {buffer, length, false};
  //      }
  //      case kTypeNonFixed: {
  //        if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //          std::tie(buffer, length) = column->GetBuffer(row_index);
  //          // different return in different ColumnTypeInMem
  //          assert((!buffer && !length) || (buffer && length));
  //          if (!buffer && !length) {
  //            return {nullptr, 0, true};
  //          }
  //        } else {
  //          std::tie(buffer, length) = column->GetBuffer(
  //            row_index - null_counts);
  //        }
  //        return {buffer, length, false};
  //        break;
  //      }
  //      default:
  //        // nothing
  //    }
  //    // should not react here!
  //    assert(false);
  //   }
  //
  // A simplest example:
  //  std::tuple<char *, size_t, bool> GetBufferWithNull(size_t row_index,
  //  size_t null_counts) {
  //    PaxColumn *column = source();
  //    char * buffer = nullptr;
  //    size_t length = 0;
  //    if (COLUMN_STORAGE_FORMAT_IS_VEC(column)) {
  //      std::tie(buffer, length) = column->GetBuffer(row_index);
  //      if (!length) {
  //        return {nullptr, 0, true};
  //      }
  //    } else {
  //      std::tie(buffer, length) = column->GetBuffer(row_index - null_counts);
  //    }
  //    assert(buffer && length);
  //    return {buffer, length, false};
  //  }
  //
  virtual std::pair<char *, size_t> GetBuffer(size_t position) = 0;

  // Get the column value by position
  //
  // It will return the different values in different `ColumnStorageType` +
  // `ColumnTypeInMem`
  // like `GetBuffer` it should NEVER call in write path with encoding option!!!
  // But without encoding option, still can direct call it.

  // The interface implements the conversion of Buffer to Datum according to
  // different combinations of `ColumnTypeInMem` + `ColumnStorageType
  //
  // If `storage_type_` is kTypeStoragePorcVec
  // Then data part contains `null field` which means the position is the
  // `row index` to get the data.
  //
  // But If `storage_type_` is not kTypeStoragePorcVec
  // Then position should be `row index - null counts`, because
  // data part will not contains `null field`.
  //
  // Also it is kind different in fixed-length column and non-fixed-length
  // column when `storage_type_` is kTypeStoragePorcVec. For the fixed-length
  // column, If we got a `null field`, then it will return zero . But in
  // non-fixed-length column, once we got  `null field`, the datum will be
  // `PointerToDatum(nullptr)`.
  virtual Datum GetDatum(size_t position) = 0;

  // Get buffer by range [start_pos, start_pos + len)
  // Should never call in write path with encoding option
  virtual std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                                   size_t len) = 0;

  // Get rows number(not null) from column
  virtual size_t GetNonNullRows() const;

  // Get all rows number(not null) from column by range [start_pos, start_pos +
  // len)
  virtual size_t GetRangeNonNullRows(size_t start_pos, size_t len);

  // Append new filed into current column
  virtual void Append(char *buffer, size_t size);

  // Append a null filed into last position
  virtual void AppendNull();

  // Append a toast datum into current column
  // current datum MUST with varlena head
  // the external_buffer already being compressed if it setting with compressed
  virtual void AppendToast(char *buffer, size_t size);

  // Estimated memory size from current column
  virtual size_t PhysicalSize() const = 0;

  // Get current storage type
  virtual PaxStorageFormat GetStorageFormat() const = 0;

  // Get the data part size without encoding/compress
  virtual int64 GetOriginLength() const = 0;

  // Get the offsets part size without encoding/compress
  virtual int64 GetOffsetsOriginLength() const = 0;

  // Get the type length, used to identify sub-class
  // - `PaxCommColumn<T>` will return the <T> length
  // - `PaxNonFixedColumn` will return -1
  virtual int32 GetTypeLength() const = 0;

  // Contain null filed or not
  inline bool HasNull() { return null_bitmap_ != nullptr; }

  // Are all values null?
  // Check whether all bits in the specified range are zero.
  // In pax_column, to avoid checking the capacity of the null bitmap, we
  // allocate memory based on pax_max_tuples_per_group. As a result, the last
  // group may contain fewer tuples than pax_max_tuples_per_group, so we need to
  // check whether all bits in the range [0, total_rows_) are zero.
  inline bool AllNull() const {
    return null_bitmap_ && null_bitmap_->Empty(total_rows_);
  }

  // Set the null bitmap
  inline void SetBitmap(std::unique_ptr<Bitmap8> null_bitmap) {
    Assert(!null_bitmap_);
    null_bitmap_ = std::move(null_bitmap);
  }

  // Get the null bitmap
  inline const std::unique_ptr<Bitmap8> &GetBitmap() const {
    return null_bitmap_;
  }

  // Set the column kv attributes
  void SetAttributes(const std::map<std::string, std::string> &attrs);

  // Get the column kv attributes
  const std::map<std::string, std::string> &GetAttributes() const;

  // Has setting the attributes
  inline bool HasAttributes() { return !attrs_map_.empty(); }

  // Set the total rows of current column
  inline void SetRows(size_t total_rows) { total_rows_ = total_rows; }

  // Get all rows number(contain null) from column
  virtual size_t GetRows() const;

  // Set the align size
  // Which require from `typalign` in `pg_type`
  // Notice that: column with encoding won't require a align size
  virtual void SetAlignSize(size_t align_size);

  // Get the align size,
  // Which require from `typalign` in `pg_type`
  virtual size_t GetAlignSize() const;

  // Get current data part encoding type
  inline ColumnEncoding_Kind GetEncodingType() const { return encoded_type_; }

  // Get current length part encoding type
  inline ColumnEncoding_Kind GetOffsetsEncodingType() const {
    return offsets_encoded_type_;
  }

  // Get current data part compress level
  inline int GetCompressLevel() const { return compress_level_; }

  // Get current length part compress level
  inline int GetOffsetsCompressLevel() const { return offsets_compress_level_; }

  // Get the counts of toast
  virtual size_t ToastCounts();

  // Test current row is a toast
  bool IsToast(size_t pos) {
    if (!toast_flat_map_) {
      return false;
    }

    CBDB_CHECK(pos < total_rows_, cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to check is toast [pos=%lu, total rows=%u], \n %s",
                   pos, total_rows_, DebugString().c_str()));
    return !toast_flat_map_->Test(pos);
  }

  // Set the toast indexes
  void SetToastIndexes(std::shared_ptr<DataBuffer<int32>> toast_indexes);

  // Get the toast indexes
  inline std::shared_ptr<DataBuffer<int32>> GetToastIndexes() const {
    return toast_indexes_;
  }

  // Set the external toast buffer
  virtual void SetExternalToastDataBuffer(
      std::shared_ptr<DataBuffer<char>> external_toast_data);

  // Get the external toast data buffer
  virtual std::shared_ptr<DataBuffer<char>> GetExternalToastDataBuffer();

 protected:
  // The encoding option should pass in sub-class
  inline void SetEncodeType(ColumnEncoding_Kind encoding_type) {
    encoded_type_ = encoding_type;
  }

  inline void SetCompressLevel(int compress_level) {
    compress_level_ = compress_level;
  }

  inline void SetOffsetsEncodeType(ColumnEncoding_Kind encoding_type) {
    offsets_encoded_type_ = encoding_type;
  }

  inline void SetOffsetsCompressLevel(int compress_level) {
    offsets_compress_level_ = compress_level;
  }

  inline std::pair<std::string, bool> GetAttribute(const std::string &key) {
    auto it = attrs_map_.find(key);
    if (it != attrs_map_.end()) {
      return std::make_pair(it->second, true);
    }
    return std::make_pair(std::string(), false);
  }

  inline bool PutAttribute(std::string key, std::string value) {
    auto ret = attrs_map_.insert(std::make_pair(key, value));
    return ret.second;
  }

  // add a index of toast
  void AddToastIndex(int32 index_of_toast);

  // Append to the external toast data
  size_t AppendExternalToastData(char *data, size_t size);

  // Use to dump the debug info
  std::string DebugString();

 private:
  void CreateNulls(size_t cap);

 protected:
  // null field bit map
  std::unique_ptr<Bitmap8> null_bitmap_;

  // Writer: write pointer
  // Reader: total rows
  uint32 total_rows_;

  // some of subclass will not implements the not null logic,
  // but can direct get not null rows by data part.
  size_t non_null_rows_;

  // the column data encoded type
  ColumnEncoding_Kind encoded_type_;

  // the column data compress level
  int compress_level_;

  // the column offsets encoded type
  ColumnEncoding_Kind offsets_encoded_type_;

  // the column offsets compress level
  int offsets_compress_level_;

  // data part align size.
  // This field only takes effect when current column is no encoding/compress.
  //
  // About `type_align` in `pg_type` what you need to know:
  // 1. address alignment: the datum which return need alignment with
  // `type_align`
  // 2. datum padding: the datum need padding with `type_align`
  //
  // The align logic in pax:
  // 1. address alignment:
  //    - write will make sure address alignment(data stream) in disk
  //    - `ReadTuple` with/without memcpy should get a alignment datum
  // 2. datum padding: deal it in column `Append`
  size_t type_align_size_;

  // the attributes which supplementary description of the column type
  // For example, attributes record `n` in `char(n)`
  std::map<std::string, std::string> attrs_map_;

  // The indexes of toast. PAX does not store too many toasts,
  // So we used a buffer rather than a Bitmap8
  std::shared_ptr<DataBuffer<int32>> toast_indexes_;

  // The flat map of toast indexes
  std::shared_ptr<Bitmap8> toast_flat_map_;

  // The number of external toast
  size_t numeber_of_external_toast_;

  // The external toast store space
  // PAX will use PaxColumns to include all columns
  // all of column inside PaxColumns will used the `external_toast_data_`
  // in PaxColumns.
  std::shared_ptr<DataBuffer<char>> external_toast_data_;

 private:
  PaxColumn(const PaxColumn &);
  PaxColumn &operator=(const PaxColumn &);
};

template <typename T>
class PaxCommColumn : public PaxColumn {
 public:
  explicit PaxCommColumn(uint32 capacity);

  ~PaxCommColumn() override;

  PaxCommColumn();

  virtual void Set(std::unique_ptr<DataBuffer<T>> data);

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  void Append(char *buffer, size_t size) override;

  void AppendToast(char *buffer, size_t size) override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  Datum GetDatum(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetOffsetsOriginLength() const override;

  std::pair<char *, size_t> GetBuffer() override;

  int32 GetTypeLength() const override;

 protected:
  std::unique_ptr<DataBuffer<T>> data_;
};

extern template class PaxCommColumn<char>;
extern template class PaxCommColumn<int8>;
extern template class PaxCommColumn<int16>;
extern template class PaxCommColumn<int32>;
extern template class PaxCommColumn<int64>;
extern template class PaxCommColumn<float>;
extern template class PaxCommColumn<double>;

class PaxNonFixedColumn : public PaxColumn {
 public:
  PaxNonFixedColumn(uint32 data_capacity, uint32 offsets_capacity);

  PaxNonFixedColumn();

  ~PaxNonFixedColumn() override;

  virtual void Set(std::unique_ptr<DataBuffer<char>> data,
                   std::unique_ptr<DataBuffer<int32>> offsets,
                   size_t total_size);

  void Append(char *buffer, size_t size) override;

  PaxColumnTypeInMem GetPaxColumnTypeInMem() const override;

  PaxStorageFormat GetStorageFormat() const override;

  std::pair<char *, size_t> GetBuffer() override;

  size_t PhysicalSize() const override;

  int64 GetOriginLength() const override;

  int64 GetOffsetsOriginLength() const override;

  int32 GetTypeLength() const override;

  std::pair<char *, size_t> GetBuffer(size_t position) override;

  Datum GetDatum(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  size_t GetNonNullRows() const override;

  virtual std::pair<char *, size_t> GetOffsetBuffer(bool append_last);

 protected:
  void AppendLastOffset();

  void BuildOffsets();

  void AppendAlign(char *buffer, size_t size);

 protected:
  size_t estimated_size_;
  std::unique_ptr<DataBuffer<char>> data_;

  // orc needs to serialize int32 array
  // the length of a single tuple field will not exceed 2GB,
  // so a variable-length element of the offsets stream can use int32
  // to represent the length
  std::unique_ptr<DataBuffer<int32>> offsets_;

  // used to record next offset in write path
  // in read path, next_offsets_ always be -1
  int32 next_offsets_;

  std::vector<std::shared_ptr<MemoryObject>> buffer_holders_;
};

};  // namespace pax
