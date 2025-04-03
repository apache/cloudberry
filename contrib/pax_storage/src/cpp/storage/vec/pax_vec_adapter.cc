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
 * pax_vec_adapter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_adapter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

extern "C" {
#include "utils/tuptable_vec.h"  // for vec tuple
}

#pragma GCC diagnostic pop

#include "comm/vec_numeric.h"
#ifdef BUILD_RB_RET_DICT
#include "storage/columns/pax_dict_encoding.h"  // GetRawDictionary
#endif
#include "storage/orc/orc_type.h"
#include "storage/pax_buffer.h"
#include "storage/pax_itemptr.h"
#include "storage/vec/arrow_wrapper.h"
#include "storage/vec/pax_vec_comm.h"
#include "storage/vec_parallel_common.h"

namespace pax {

#define DECIMAL_BUFFER_SIZE 16
#define DECIMAL_BUFFER_BITS 128

int VecAdapter::GetMaxBatchSizeFromStr(char *max_batch_size_str,
                                       int default_value) {
  return max_batch_size_str ? atoi(max_batch_size_str) : default_value;
}

static void ConvSchemaAndDataToVec(
    Oid pg_type_oid, const char *attname, size_t all_nums_of_row,
    VecAdapter::VecBatchBuffer *vec_batch_buffer,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names);

static std::tuple<std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  int64>
ExtractBuffers(VecAdapter::VecBatchBuffer *batch);

static void AppendColumnBuffer(arrow::ArrayVector &array_vector,
                               int64 all_num_rows,
                               Oid pg_typid,
                               const std::shared_ptr<arrow::Buffer> &data_buffer,
                               const std::shared_ptr<arrow::Buffer> &null_bitmap_buffer = nullptr,
                               const std::shared_ptr<arrow::Buffer> &offset_buffer = nullptr
                               );

VecAdapter::VecBatchBuffer::VecBatchBuffer()
    : vec_buffer(0),
      null_bits_buffer(0),
      offset_buffer(0),
      null_counts(0)
#ifdef BUILD_RB_RET_DICT
      ,
      is_dict(false),
      dict_offset_buffer(0),
      dict_entry_buffer(0)
#endif
{
  SetMemoryTakeOver(true);
};

void VecAdapter::VecBatchBuffer::Reset() {
  // Current `DataBuffer` will not hold the buffers.
  // And the buffers will be trans to `ArrayVector` which will hold it.
  // Released in `release` callback or Memory context reset in `EndScan`
  SetMemoryTakeOver(false);
  vec_buffer.Reset();
  null_bits_buffer.Reset();
  offset_buffer.Reset();
  null_counts = 0;
#ifdef BUILD_RB_RET_DICT
  is_dict = false;
  dict_offset_buffer.Reset();
  dict_entry_buffer.Reset();
#endif
  SetMemoryTakeOver(true);
}

void VecAdapter::VecBatchBuffer::SetMemoryTakeOver(bool take) {
  vec_buffer.SetMemTakeOver(take);
  null_bits_buffer.SetMemTakeOver(take);
  offset_buffer.SetMemTakeOver(take);
#ifdef BUILD_RB_RET_DICT
  dict_offset_buffer.SetMemTakeOver(take);
  dict_entry_buffer.SetMemTakeOver(take);
#endif
}

static std::tuple<std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>>
ConvToVecBuffer(VecAdapter::VecBatchBuffer *vec_batch_buffer) {
  std::shared_ptr<arrow::Buffer> arrow_buffer = nullptr;
  std::shared_ptr<arrow::Buffer> arrow_null_buffer = nullptr;
  std::shared_ptr<arrow::Buffer> arrow_offset_buffer = nullptr;

  arrow_buffer = std::make_shared<arrow::Buffer>(
      (uint8 *)vec_batch_buffer->vec_buffer.GetBuffer(),
      (int64)vec_batch_buffer->vec_buffer.Capacity());

  Assert(vec_batch_buffer->vec_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);

  if (vec_batch_buffer->null_bits_buffer.GetBuffer()) {
    arrow_null_buffer = std::make_shared<arrow::Buffer>(
        (uint8 *)vec_batch_buffer->null_bits_buffer.GetBuffer(),
        (int64)vec_batch_buffer->null_bits_buffer.Capacity());

    Assert(vec_batch_buffer->null_bits_buffer.Capacity() % MEMORY_ALIGN_SIZE ==
           0);
  }

  if (vec_batch_buffer->offset_buffer.GetBuffer()) {
    arrow_offset_buffer = std::make_shared<arrow::Buffer>(
        (uint8 *)vec_batch_buffer->offset_buffer.GetBuffer(),
        (int64)vec_batch_buffer->offset_buffer.Capacity());
    Assert(vec_batch_buffer->offset_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);
  }
  return std::make_tuple(arrow_buffer, arrow_null_buffer, arrow_offset_buffer);
}

template <typename ArrayType, typename Tuple, std::size_t... I>
static inline std::shared_ptr<ArrayType> makeSharedArrayImpl(
    Tuple &&tuple, std::index_sequence<I...>) {
  return std::make_shared<ArrayType>(
      std::get<I>(std::forward<Tuple>(tuple))...);
}

template <typename ArrayType, typename... Args>
static inline std::shared_ptr<ArrayType> makeSharedArray(Args &&...args) {
  return makeSharedArrayImpl<ArrayType>(
      std::forward_as_tuple(std::forward<Args>(args)...),
      std::make_index_sequence<sizeof...(Args)>{});
}

template <typename ArrayType, typename... Args>
static void ConvArrowSchemaAndBuffer(
    const std::string &field_name, std::shared_ptr<arrow::DataType> data_type,
    VecAdapter::VecBatchBuffer *vec_batch_buffer, size_t all_nums_of_row,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names,
    Args &&...args) {
  std::shared_ptr<arrow::Buffer> arrow_buffer;
  std::shared_ptr<arrow::Buffer> arrow_null_buffer;

  auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
  arrow_buffer = std::get<0>(arrow_buffers);
  arrow_null_buffer = std::get<1>(arrow_buffers);

  schema_types.emplace_back(arrow::field(field_name, data_type));

  // if C++17 or later, we can use `std::apply` to simplify the code
  auto array = makeSharedArray<ArrayType>(
      std::forward<Args>(args)..., all_nums_of_row, arrow_buffer,
      arrow_null_buffer, vec_batch_buffer->null_counts);

  array_vector.emplace_back(array);
  field_names.emplace_back(field_name);
}

static void ConvSchemaAndDataToVec(
    Oid pg_type_oid, const char *attname, size_t all_nums_of_row,
    VecAdapter::VecBatchBuffer *vec_batch_buffer,
    std::vector<std::shared_ptr<arrow::Field>> &schema_types,
    arrow::ArrayVector &array_vector, std::vector<std::string> &field_names) {
#ifdef BUILD_RB_RET_DICT
  if (vec_batch_buffer->is_dict) {
    DataBuffer<char> *index_buffer;
    DataBuffer<char> *null_buffer;
    DataBuffer<int32> *desc_offset_buffer;
    DataBuffer<char> *desc_entry_buffer;
    Assert(vec_batch_buffer->vec_buffer.GetBuffer());

    index_buffer = &(vec_batch_buffer->vec_buffer);
    desc_offset_buffer = &(vec_batch_buffer->dict_offset_buffer);
    desc_entry_buffer = &(vec_batch_buffer->dict_entry_buffer);
    null_buffer = &(vec_batch_buffer->null_bits_buffer);

    std::shared_ptr<arrow::Int32Array> index_array;
    {
      auto index_data_buffer = std::make_shared<arrow::Buffer>(
          (uint8 *)index_buffer->GetBuffer(), (int64)index_buffer->Capacity());

      std::shared_ptr<arrow::Buffer> arrow_null_buffer = nullptr;
      if (vec_batch_buffer->null_counts > 0) {
        arrow_null_buffer = std::make_shared<arrow::Buffer>(
            (uint8 *)null_buffer->GetBuffer(), (int64)null_buffer->Capacity());
      }

      index_array = std::make_shared<arrow::Int32Array>(
          index_buffer->GetSize() / sizeof(int32), index_data_buffer,
          arrow_null_buffer, vec_batch_buffer->null_counts);
    }

    std::shared_ptr<arrow::StringArray> dict_desc_array;
    {
      auto arrow_desc_entry_buffer = std::make_shared<arrow::Buffer>(
          (uint8 *)desc_entry_buffer->GetBuffer(),
          (int64)desc_entry_buffer->Capacity());

      auto arrow_desc_offset_buffer = std::make_shared<arrow::Buffer>(
          (uint8 *)desc_offset_buffer->GetBuffer(),
          (int64)desc_offset_buffer->Capacity());

      dict_desc_array = std::make_shared<arrow::StringArray>(
          desc_offset_buffer->GetSize() - 1, arrow_desc_offset_buffer,
          arrow_desc_entry_buffer, nullptr, 0);
    }

    auto dict_array = std::make_shared<arrow::DictionaryArray>(
        arrow::dictionary(arrow::int32(), arrow::utf8()), index_array,
        dict_desc_array);

    schema_types.emplace_back(
        arrow::field(std::string(attname),
                     arrow::dictionary(arrow::int32(), arrow::utf8())));
    array_vector.emplace_back(dict_array);
    field_names.emplace_back(std::string(attname));

    return;
  }
#endif

  switch (pg_type_oid) {
    case BOOLOID: {
      ConvArrowSchemaAndBuffer<arrow::BooleanArray>(
          std::string(attname), arrow::boolean(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case CHAROID: {
      ConvArrowSchemaAndBuffer<arrow::Int8Array>(
          std::string(attname), arrow::int8(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case TIMEOID:
    case TIMESTAMPOID:
    case TIMESTAMPTZOID: {
      ConvArrowSchemaAndBuffer<arrow::TimestampArray>(
          std::string(attname), arrow::timestamp(arrow::TimeUnit::MICRO),
          vec_batch_buffer, all_nums_of_row, schema_types, array_vector,
          field_names, arrow::timestamp(arrow::TimeUnit::MICRO));
      break;
    }
    case TIDOID:
    case INT8OID: {
      ConvArrowSchemaAndBuffer<arrow::Int64Array>(
          std::string(attname), arrow::int64(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case INT2OID: {
      ConvArrowSchemaAndBuffer<arrow::Int16Array>(
          std::string(attname), arrow::int16(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case DATEOID: {
      ConvArrowSchemaAndBuffer<arrow::Date32Array>(
          std::string(attname), arrow::date32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case INT4OID: {
      ConvArrowSchemaAndBuffer<arrow::Int32Array>(
          std::string(attname), arrow::int32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case BPCHAROID:
    case VARCHAROID:
    case TEXTOID: {
      std::shared_ptr<arrow::Buffer> arrow_buffer;
      std::shared_ptr<arrow::Buffer> arrow_null_buffer;
      std::shared_ptr<arrow::Buffer> arrow_offset_buffer;

      auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
      arrow_buffer = std::get<0>(arrow_buffers);
      arrow_null_buffer = std::get<1>(arrow_buffers);
      arrow_offset_buffer = std::get<2>(arrow_buffers);

      schema_types.emplace_back(
          arrow::field(std::string(attname), arrow::utf8()));
      auto array = std::make_shared<arrow::StringArray>(
          all_nums_of_row, arrow_offset_buffer, arrow_buffer, arrow_null_buffer,
          vec_batch_buffer->null_counts);

      array_vector.emplace_back(array);
      field_names.emplace_back(std::string(attname));
      break;
    }
    case FLOAT4OID: {
      ConvArrowSchemaAndBuffer<arrow::FloatArray>(
          std::string(attname), arrow::float32(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case FLOAT8OID: {
      ConvArrowSchemaAndBuffer<arrow::DoubleArray>(
          std::string(attname), arrow::float64(), vec_batch_buffer,
          all_nums_of_row, schema_types, array_vector, field_names);
      break;
    }
    case NUMERICOID: {
      std::string field_name = std::string(attname);
      std::shared_ptr<arrow::Buffer> arrow_buffer;
      std::shared_ptr<arrow::Buffer> arrow_null_buffer;
      std::shared_ptr<arrow::DataType> data_type;

      data_type = arrow::numeric128();

      auto arrow_buffers = ConvToVecBuffer(vec_batch_buffer);
      arrow_buffer = std::get<0>(arrow_buffers);
      arrow_null_buffer = std::get<1>(arrow_buffers);
      Assert(std::get<2>(arrow_buffers) == nullptr);
      Assert(arrow_buffer);
      AssertImply(vec_batch_buffer->null_counts > 0, arrow_null_buffer);

      schema_types.emplace_back(arrow::field(field_name, data_type));
      std::shared_ptr<arrow::ArrayData> decimal_array_data =
          arrow::ArrayData::Make(
              data_type, all_nums_of_row,
              {arrow_null_buffer, arrow_buffer},  // 16bytes array
              vec_batch_buffer->null_counts);
      auto array = std::make_shared<arrow::Numeric128Array>(decimal_array_data);

      array_vector.emplace_back(array);
      field_names.emplace_back(field_name);
      break;
    }
    case INT2ARRAYOID:
    case INT4ARRAYOID:
    case INT8ARRAYOID:
    case FLOAT4ARRAYOID:
    case FLOAT8ARRAYOID:
    case TEXTARRAYOID:
    case BPCHARARRAYOID: {
      Assert(false);
    }
    case NAMEOID:
    case XIDOID:
    case CIDOID:
    case OIDVECTOROID:
    case JSONOID:
    case OIDOID:
    case REGPROCOID:
    default: {
      Assert(false);
    }
  }
}

template <typename ArrayType>
static void AppendArrowArray(
    arrow::ArrayVector &array_vector,
    size_t all_nums_of_row,
    const std::shared_ptr<arrow::Buffer> &data_batch_buffer,
    const std::shared_ptr<arrow::Buffer> &null_bitmap_buffer = nullptr) {

  auto array = std::make_shared<ArrayType>(all_nums_of_row,
                                           data_batch_buffer,
                                           null_bitmap_buffer);

  array_vector.emplace_back(array);
}

template <typename ArrayType>
static inline void
AppendArrowArray(arrow::ArrayVector &array_vector,
                std::shared_ptr<arrow::DataType> data_type,
                int64 all_num_rows,
                std::vector<std::shared_ptr<arrow::Buffer>> &&array_buffer) {
  auto array_data = std::make_shared<arrow::ArrayData>(data_type, all_num_rows,
                                                       std::move(array_buffer));
  auto array = std::make_shared<ArrayType>(array_data);
  array_vector.emplace_back(array);
}

VecAdapter::VecAdapter(TupleDesc tuple_desc, bool build_ctid)
    : rel_tuple_desc_(tuple_desc),
      cached_batch_lens_(0),
      vec_cache_buffer_(nullptr),
      vec_cache_buffer_lens_(0),
      process_columns_(nullptr),
      current_index_(0),
      build_ctid_(build_ctid),
      group_base_offset_(0) {
  Assert(rel_tuple_desc_);
};

VecAdapter::~VecAdapter() {
  if (vec_cache_buffer_) {
    for (int i = 0; i < vec_cache_buffer_lens_; i++) {
      vec_cache_buffer_[i].SetMemoryTakeOver(false);
    }
    PAX_DELETE_ARRAY(vec_cache_buffer_);
  }
}

void VecAdapter::SetDataSource(std::shared_ptr<PaxColumns> columns, int group_base_offset) {
  Assert(columns);
  Assert(group_base_offset >= 0);
  Assert(group_base_offset < static_cast<int>(PAX_MAX_NUM_TUPLES_PER_FILE));

  process_columns_ = std::move(columns);
  group_base_offset_ = group_base_offset;
  current_index_ = 0;
  cached_batch_lens_ = 0;
  AssertImply(vec_cache_buffer_,
              process_columns_->GetColumns() <= (size_t)vec_cache_buffer_lens_);
  if (!vec_cache_buffer_) {
    vec_cache_buffer_lens_ = rel_tuple_desc_->natts;
    vec_cache_buffer_ = PAX_NEW_ARRAY<VecBatchBuffer>(vec_cache_buffer_lens_);
  }
}

TupleDesc VecAdapter::GetRelationTupleDesc() const { return rel_tuple_desc_; }

int VecAdapter::AppendToVecBuffer() {
  bool porc_vec_format;
  size_t number_of_append;
  size_t total_rows = process_columns_->GetRows();

  // There are three cases to direct return
  // 1. no call `DataSource`, then no source setup
  // 2. already have cached vec batch without flush
  // 3. all of data in pax columns have been comsume
  if (!process_columns_ || cached_batch_lens_ != 0 ||
      current_index_ == total_rows) {
    return -1;
  }

  porc_vec_format = COLUMN_STORAGE_FORMAT_IS_VEC(process_columns_);

  Assert(current_index_ == 0);
  std::tie(number_of_append, current_index_) =
      porc_vec_format
          ? AppendPorcVecFormat(process_columns_.get())
          : AppendPorcFormat(process_columns_.get(), 0, total_rows);
  Assert(number_of_append <= total_rows);

  // In this time cached_batch_lens_ always be 0
  // It's ok to direct set it, rather than use the +=
  cached_batch_lens_ = number_of_append;
  if (build_ctid_) {
    BuildCtidOffset(0, total_rows);
  }

  return number_of_append;
}

void VecAdapter::BuildCtidOffset(size_t range_begin, size_t range_lens) {
  auto buffer_len = sizeof(int32) * cached_batch_lens_;
  ctid_offset_in_current_range_ = std::make_shared<DataBuffer<int32>>(
      BlockBuffer::Alloc<int32>(buffer_len), buffer_len, false);

  size_t range_row_index = 0;
  size_t offset = group_base_offset_ + range_begin;
  if (micro_partition_visibility_bitmap_) {
    for (size_t i = 0; i < cached_batch_lens_; i++) {
      while (range_row_index < range_lens &&
             micro_partition_visibility_bitmap_->Test(offset)) {
        range_row_index++;
        offset++;
      }

      // has loop all visibility map
      if (unlikely(range_row_index >= range_lens)) break;

      (*ctid_offset_in_current_range_)[i] = static_cast<int32>(offset++);
      ctid_offset_in_current_range_->Brush(sizeof(int32));
    }
  } else {
    for (size_t i = 0; i < cached_batch_lens_; i++) {
      (*ctid_offset_in_current_range_)[i] = static_cast<int32>(offset++);
      ctid_offset_in_current_range_->Brush(sizeof(int32));
    }
  }

  //
  Assert(ctid_offset_in_current_range_->GetSize() == cached_batch_lens_);
}

bool VecAdapter::ShouldBuildCtid() const { return build_ctid_; }

void VecAdapter::FullWithCTID(TupleTableSlot *slot,
                              VecBatchBuffer *batch_buffer) {
  auto buffer_len = sizeof(int64) * cached_batch_lens_;
  DataBuffer<int64> ctid_data_buffer(BlockBuffer::Alloc<int64>(buffer_len),
                                     buffer_len, false);
  auto ctid = slot->tts_tid;

  for (size_t i = 0; i < cached_batch_lens_; i++) {
    SetTupleOffset(&ctid, (*ctid_offset_in_current_range_)[i]);
    ctid_data_buffer[i] = CTIDToUint64(ctid);
  }
  batch_buffer->vec_buffer.Set(ctid_data_buffer.Start(),
                               ctid_data_buffer.Capacity());
  batch_buffer->vec_buffer.SetMemTakeOver(false);
  batch_buffer->vec_buffer.BrushAll();
  ctid_data_buffer.SetMemTakeOver(false);
  ctid_offset_in_current_range_ = nullptr;
}

void VecAdapter::FillMissColumn(int index) {
  Datum tts_default_value;
  Datum tts_isnull;
  AttrMissing *attrmiss = nullptr;

  DataBuffer<char> *vec_data_buffer = nullptr;
  DataBuffer<char> *null_bits_buffer = nullptr;
  DataBuffer<int32> *offset_buffer = nullptr;

  Assert(index < rel_tuple_desc_->natts);

  if (rel_tuple_desc_->constr) attrmiss = rel_tuple_desc_->constr->missing;

  if (!attrmiss) {
    // no missing values array at all, so just fill everything in as NULL
    tts_default_value = 0;
    tts_isnull = true;
  } else {
    // fill with default value
    tts_default_value = attrmiss[index].am_value;
    tts_isnull = !attrmiss[index].am_present;
  }

  auto column_type_kind =
      ConvertPgTypeToPorcType(&rel_tuple_desc_->attrs[index], false);

  vec_data_buffer = &(vec_cache_buffer_[index].vec_buffer);
  offset_buffer = &(vec_cache_buffer_[index].offset_buffer);
  Assert(vec_data_buffer->GetBuffer() == nullptr);
  Assert(offset_buffer->GetBuffer() == nullptr);

  // copy null bitmap
  if (tts_isnull) {
    null_bits_buffer = &(vec_cache_buffer_[index].null_bits_buffer);
    Assert(null_bits_buffer->GetBuffer() == nullptr);
    vec_cache_buffer_[index].null_counts = cached_batch_lens_;

    auto null_align_bytes =
        TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(cached_batch_lens_));

    // all nulls
    null_bits_buffer->Set(BlockBuffer::Alloc0<char>(null_align_bytes),
                          null_align_bytes);
    null_bits_buffer->Brush(null_align_bytes);
  } else {
    vec_cache_buffer_[index].null_counts = 0;
  }

  switch (column_type_kind) {
    // bitpacked layout
    case pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN: {
      auto buffer_size =
          TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(cached_batch_lens_));
      auto bitpacked_buffer = BlockBuffer::Alloc<char>(buffer_size);
      if (DatumGetBool(tts_default_value)) {
        memset(bitpacked_buffer, 0xff, buffer_size);
      } else {
        memset(bitpacked_buffer, 0, buffer_size);
      }
      vec_data_buffer->Set(bitpacked_buffer, buffer_size);
      break;
    }
    // fixed-length layout
    case pax::porc::proto::Type_Kind::Type_Kind_BYTE:
    case pax::porc::proto::Type_Kind::Type_Kind_SHORT:
    case pax::porc::proto::Type_Kind::Type_Kind_INT:
    case pax::porc::proto::Type_Kind::Type_Kind_LONG: {
      auto buffer_size =
          TYPEALIGN(MEMORY_ALIGN_SIZE,
                    cached_batch_lens_ * rel_tuple_desc_->attrs[index].attlen);
      auto data_buffer = BlockBuffer::Alloc<char>(buffer_size);
      vec_data_buffer->Set(data_buffer, buffer_size);
      if (tts_isnull) {
        vec_data_buffer->Brush(buffer_size);
      } else {
        for (size_t i = 0; i < cached_batch_lens_; i++) {
          vec_data_buffer->Write((char *)&tts_default_value,
                                 rel_tuple_desc_->attrs[index].attlen);
          vec_data_buffer->Brush(rel_tuple_desc_->attrs[index].attlen);
        }
      }
      break;
    }
    // decimal layout
    case pax::porc::proto::Type_Kind::Type_Kind_DECIMAL: {
      int32 type_len = VEC_SHORT_NUMERIC_STORE_BYTES;
      auto buffer_size =
          TYPEALIGN(MEMORY_ALIGN_SIZE, cached_batch_lens_ * type_len);
      auto data_buffer = BlockBuffer::Alloc<char>(buffer_size);

      vec_data_buffer->Set(data_buffer, buffer_size);

      if (tts_isnull) {
        vec_data_buffer->Brush(buffer_size);
      } else {
        for (size_t i = 0; i < cached_batch_lens_; i++) {
          Numeric numeric;
          size_t num_len = 0;
          auto vl = (struct varlena *)DatumGetPointer(tts_default_value);
          num_len = VARSIZE_ANY_EXHDR(vl);
          numeric = cbdb::DatumToNumeric(PointerGetDatum(vl));

          char *dest_buff = vec_data_buffer->GetAvailableBuffer();
          Assert(vec_data_buffer->Available() >= (size_t)type_len);
          pg_short_numeric_to_vec_short_numeric(
              numeric, num_len, (int64 *)dest_buff,
              (int64 *)(dest_buff + sizeof(int64)));
          vec_data_buffer->Brush(type_len);
        }
      }

      break;
    }
    // non-fixed layout
    // bpchar is special and the trailing space character should be removed
    case pax::porc::proto::Type_Kind::Type_Kind_BPCHAR:
    case pax::porc::proto::Type_Kind::Type_Kind_STRING: {
      auto offset_align_bytes = TYPEALIGN(
          MEMORY_ALIGN_SIZE, (cached_batch_lens_ + 1) * sizeof(int32));
      offset_buffer->Set(BlockBuffer::Alloc0<char>(offset_align_bytes),
                         offset_align_bytes);

      if (!tts_isnull) {
        size_t read_len = 0;
        char *read_data;
        int default_len = 0;
        auto default_vl =
            cbdb::PointerAndLenFromDatum(tts_default_value, &default_len);

        VarlenaToRawBuffer((char *)default_vl, default_len, &read_data,
                           &read_len);

        auto buffer_len = cached_batch_lens_ * default_len;
        auto raw_data_size = buffer_len - (cached_batch_lens_ * VARHDRSZ_SHORT);
        auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE, raw_data_size);
        vec_data_buffer->Set(BlockBuffer::Alloc0<char>(align_size),
                             align_size);
        for (size_t i = 0; i < cached_batch_lens_; i++) {
          // In vec, bpchar not allow store empty char after the actual
          // characters
          if (column_type_kind ==
              pax::porc::proto::Type_Kind::Type_Kind_BPCHAR) {
            read_len = bpchartruelen(read_data, read_len);
          }
          vec_data_buffer->Write(read_data, read_len);
          vec_data_buffer->Brush(read_len);
          offset_buffer->Write(i * read_len);
          offset_buffer->Brush(sizeof(int32));
        }
        offset_buffer->Write(cached_batch_lens_ * read_len);
        offset_buffer->Brush(sizeof(int32));
      }

      break;
    }
    default:
      CBDB_RAISE(cbdb::CException::kExTypeInvalid,
                 fmt("Invalid porc [type=%d]", column_type_kind));
  }
}

size_t VecAdapter::FlushVecBuffer(TupleTableSlot *slot) {
  // when visibility map is enabled, all rows of a vec batch may be filtered
  // out. this time cached_batch_length is 0
  if (cached_batch_lens_ == 0) {
    return 0;
  }

  std::vector<std::shared_ptr<arrow::Field>> schema_types;
  arrow::ArrayVector array_vector;
  std::vector<std::string> field_names;
  VecTupleTableSlot *vslot = nullptr;
  VecBatchBuffer *vec_batch_buffer = nullptr;
  PaxColumns *columns = nullptr;

  TupleDesc target_desc;

  // column size from current pax columns(which is same size with disk stored)
  // may not equal with `rel_tuple_desc_->natts`, but must LE with
  // `rel_tuple_desc_->natts`
  size_t column_size = 0;
  size_t rc = 0;

  columns = process_columns_.get();
  Assert(columns);

  vslot = VECSLOT(slot);
  Assert(vslot);

  target_desc = slot->tts_tupleDescriptor;
  column_size = columns->GetColumns();

  Assert(column_size <= (size_t)rel_tuple_desc_->natts);

  // Vec executor is different with cbdb executor
  // if select single column in multi column defined relation
  // then `target_desc->natts` will be one, rather then actually column
  // numbers So we need use `rel_tuple_desc_` which own full relation tuple
  // desc to fill target arrow data
  for (size_t index = 0; index < column_size; index++) {
    auto attr = &rel_tuple_desc_->attrs[index];
    char *column_name = NameStr(attr->attname);

    if ((*columns)[index] == nullptr || attr->attisdropped) {
      continue;
    }

    vec_batch_buffer = &vec_cache_buffer_[index];

    ConvSchemaAndDataToVec(attr->atttypid, column_name, cached_batch_lens_,
                           vec_batch_buffer, schema_types, array_vector,
                           field_names);
    vec_batch_buffer->Reset();
  }

  Assert(schema_types.size() <= (size_t)target_desc->natts);

  // The reason why use we can put null column into `target_desc` is that
  // this situation will only happen when the column is missing in disk.
  // `add column` will make this happen
  // for example
  // 1. CREATE TABLE aa(a int4, b int4) using pax;
  // 2. insert into aa values(...);    // it will generate pax file1 with
  // column a,b
  // 3. alter table aa add c int4;
  // 4. insert into aa values(...);    // it will generate pax file2 with
  // column a,b,c
  // 5. select * from aa;
  //
  // In step5, file1 missing the column c, `schema_types.size()` is 2.
  // So we need full null in it. But in file2, `schema_types.size()` is 3,
  // so do nothing.
  //
  // Notice that: `drop column` will not effect this logic. Because we already
  // deal the `drop column` above(using the relation tuple desc filter the
  // column).
  //
  // A example about `drop column` + `add column`:
  // 1. CREATE TABLE aa(a int4, b int4) using pax;
  // 2. insert into aa values(...);    // it will generate pax file1 with
  // column a,b
  // 3. alter table aa drop b;
  // 4. alter table aa add c int4;
  // 5. insert into aa values(...);    // it will generate pax file2 with
  // column a,c
  // 6. select * from aa; // need column a + column c
  //
  // In step6, file 1 missing the column c, column b in file1 will be filter
  // by `attisdropped` so `schema_types.size()` is 1, we need full null in it.
  // But in file2, `schema_types.size()` is 3, so do nothing.
  auto natts = build_ctid_ ? target_desc->natts - 1 : target_desc->natts;

  // TODO(gongxun): should fill the missing column at here to reduce the
  // unnessary fill
  for (size_t index = schema_types.size(); index < (size_t)natts; index++) {
    auto attr = &target_desc->attrs[index];
    char *column_name = NameStr(attr->attname);
    size_t index_in_rel;

    // attrs is order by column index, so we can start from the last index
    for (index_in_rel = index; index_in_rel < (size_t)rel_tuple_desc_->natts;
         index_in_rel++) {
      if (strcmp(column_name,
                 NameStr(rel_tuple_desc_->attrs[index_in_rel].attname)) == 0) {
        FillMissColumn(index_in_rel);
        vec_batch_buffer = &vec_cache_buffer_[index_in_rel];

        ConvSchemaAndDataToVec(attr->atttypid, column_name, cached_batch_lens_,
                               vec_batch_buffer, schema_types, array_vector,
                               field_names);
        vec_batch_buffer->Reset();
        break;
      }
    }

    // must the missing column found in relation tuple desc
    CBDB_CHECK(index_in_rel != (size_t)rel_tuple_desc_->natts,
               cbdb::CException::kExTypeInvalid,
               fmt("Fail to found missing column in TupleDesc [missing column "
                   "name=%s, index=%lu, size in desc=%d]",
                   column_name, index, rel_tuple_desc_->natts));
  }

  Assert((int)schema_types.size() == natts);

  // The CTID will be full with int64(table no(16) + block number(16) +
  // offset(32)) The current value of CTID is accurate, But we cannot get the
  // row data through this CTID. For vectorization, we need to assign CTID
  // datas to the last column of target_list
  if (build_ctid_) {
    Assert((int)schema_types.size() == target_desc->natts - 1);
    VecBatchBuffer ctid_batch_buffer;

    FullWithCTID(slot, &ctid_batch_buffer);
    char *target_column_name =
        NameStr(target_desc->attrs[target_desc->natts - 1].attname);

    ConvSchemaAndDataToVec(target_desc->attrs[target_desc->natts - 1].atttypid,
                           target_column_name, cached_batch_lens_,
                           &ctid_batch_buffer, schema_types, array_vector,
                           field_names);
  }

  Assert(schema_types.size() == (size_t)target_desc->natts);
  Assert(array_vector.size() == schema_types.size());
  Assert(field_names.size() == array_vector.size());

  // `ArrowRecordBatch/ArrowSchema/ArrowArray` alloced by pax memory context.
  // Can not possible to hold the lifecycle of these three objects in pax.
  // It will be freed after memory context reset.
  auto arrow_rb = (ArrowRecordBatch *)pax::PAX_NEW<ArrowRecordBatch>();

  auto export_status = arrow::ExportType(
      *arrow::struct_(std::move(schema_types)), &arrow_rb->schema);

  CBDB_CHECK(export_status.ok(),
             cbdb::CException::ExType::kExTypeArrowExportError,
             "Fail to export arrow schema");

  // Don't use the `arrow::ExportArray`
  // Because it will cause memory leak when release call
  // The defualt `release` method won't free the `buffers`,
  // but can free the `private_data` (ExportedArrayPrivateData)
  // After we replace the `release` function. the `private_data` won't be
  // freed.
  auto array = *arrow::StructArray::Make(std::move(array_vector), field_names);
  arrow::ExportArrayRoot(array->data(), &arrow_rb->batch);

  vslot->tts_recordbatch = arrow_rb;

  rc = cached_batch_lens_;
  cached_batch_lens_ = 0;

  return rc;
}

bool VecAdapter::IsInitialized() const { return !!process_columns_; }

class PaxBuffer : public arrow::Buffer {
 public:
  PaxBuffer(const uint8_t* data, int64_t size): Buffer(data, size) {}
  template <typename T>
  PaxBuffer(T *buffer, size_t size): Buffer(
    reinterpret_cast<uint8_t *>(buffer), size
  ) { }
  PaxBuffer(int64_t size): Buffer(const_cast<const uint8_t *>(BlockBuffer::Alloc<uint8_t>(size)), size) { }
  ~PaxBuffer() {
    BlockBuffer::Free(data_);
    data_ = nullptr;
  }

  template<typename T>
  T *As() {
    auto p = const_cast<uint8_t *>(data_);
    return reinterpret_cast<T *>(p);
  }
};

template <typename intN>
static inline
void FillMissingByValColumn(VecAdapter::VecBatchBuffer *batch_buffer, Oid atttypid, intN val, size_t batch_len, arrow::ArrayVector &array_vector) {
  auto buffer_len = TYPEALIGN(MEMORY_ALIGN_SIZE, batch_len * sizeof(intN));
  auto pbuffer = std::make_shared<PaxBuffer>(buffer_len);
  auto buffer = pbuffer->As<intN>();
  for (size_t i = 0, n = batch_len; i < n; i++) {
    buffer[i] = val;
  }

  AppendColumnBuffer(array_vector, batch_len, atttypid, pbuffer);
}

static inline void FillMissingByValColumn(Form_pg_attribute attr, Datum datum, size_t batch_len, arrow::ArrayVector &array_vector) {
  VecAdapter::VecBatchBuffer batch_buffer;
  switch (attr->attlen) {
    case 1: {
      size_t buffer_len;
      int8 v = cbdb::DatumToInt8(datum);
      if (attr->atttypid == BOOLOID) {
        if (v) v = -1;
        buffer_len = TYPEALIGN(MEMORY_ALIGN_SIZE, (batch_len + 7) / 8);
      } else {
        buffer_len = TYPEALIGN(MEMORY_ALIGN_SIZE, batch_len);
      }

      auto pbuffer = std::make_shared<PaxBuffer>(buffer_len);
      memset(pbuffer->As<char>(), v, buffer_len);

      AppendColumnBuffer(array_vector, batch_len, attr->atttypid, pbuffer);
      break;
    }
    case 2: {
      auto v = cbdb::DatumToInt16(datum);
      FillMissingByValColumn<int16>(&batch_buffer, attr->atttypid, v, batch_len, array_vector);
      break;
    }
    case 4: {
      auto v = cbdb::DatumToInt32(datum);
      FillMissingByValColumn<int32>(&batch_buffer, attr->atttypid, v, batch_len, array_vector);
      break;
    }
    case 8: {
      auto v = cbdb::DatumToInt64(datum);
      FillMissingByValColumn<int64>(&batch_buffer, attr->atttypid, v, batch_len, array_vector);
      break;
    }
    default: Assert(false);
  }
}

static inline void FillMissingFixedLenVarColumn(Form_pg_attribute attr, Datum datum, size_t batch_len, arrow::ArrayVector &array_vector) {
  VecAdapter::VecBatchBuffer batch_buffer;

  int attlen = attr->attlen;

  Assert(!attr->attisdropped && !attr->attbyval);
  Assert(attlen > 0);

  void *ptr = cbdb::DatumToPointer(datum);

  size_t buffer_len = TYPEALIGN(MEMORY_ALIGN_SIZE, attlen * batch_len);
  auto pbuffer = std::make_shared<PaxBuffer>(buffer_len);
  char *pdata = pbuffer->As<char>();

  for (size_t i = 0; i < batch_len; i++) {
    memcpy(pdata, ptr, attlen);
    pdata += attlen;
  }

  AppendColumnBuffer(array_vector, batch_len, attr->atttypid, pbuffer);
}

static inline int NumericGetPrecision(int typmod) {
  return ((typmod - VARHDRSZ) >> 16) & 0xFFFF;
}

static inline void FillMissingVarColumn(Form_pg_attribute attr, Datum datum, size_t batch_len, arrow::ArrayVector &array_vector) {
  VecAdapter::VecBatchBuffer batch_buffer;

  int attlen = attr->attlen;
  int64 numerics[2];
  std::shared_ptr<PaxBuffer> pdata_buffer;
  std::shared_ptr<PaxBuffer> poffset_buffer;

  Assert(!attr->attisdropped && !attr->attbyval);
  Assert(attlen == -1);

  void *ptr = cbdb::DatumToPointer(datum);
  {
    auto varlena = static_cast<struct varlena *>(ptr);
    attlen = VARSIZE_ANY_EXHDR(varlena);
    ptr = VARDATA_ANY(varlena);

    // default value is storaged in pg_attrdef, the value is detoasted yet
    // before saving to attrmissing
    Assert(!VARATT_IS_EXTENDED(varlena));
    if (attr->atttypid == BPCHAROID)
      attlen = bpchartruelen((char *)ptr, attlen);
    else if (attr->atttypid == NUMERICOID) {
      Numeric numeric;

      // check precision
      Assert(attr->atttypmod >= VARHDRSZ &&
             NumericGetPrecision(attr->atttypmod) <= VEC_SHORT_NUMERIC_MAX_PRECISION);

      numeric = cbdb::DatumToNumeric(datum);
      Assert(reinterpret_cast<struct varlena *>(numeric) == varlena);

      pg_short_numeric_to_vec_short_numeric(
        numeric, attlen, &numerics[0], &numerics[1]
      );
      ptr = reinterpret_cast<void*>(&numerics[0]);
      attlen = sizeof(numerics);
    }
  }

  size_t buffer_len = TYPEALIGN(MEMORY_ALIGN_SIZE, attlen * batch_len);
  size_t offset_len = TYPEALIGN(MEMORY_ALIGN_SIZE, (batch_len + 1) * sizeof(int32));

  pdata_buffer = std::make_shared<PaxBuffer>(buffer_len);
  char *pdata = pdata_buffer->As<char>();

  for (size_t i = 0; i < batch_len; i++) {
    memcpy(pdata, ptr, attlen);
    pdata += attlen;
  }

  if (attr->atttypid != NUMERICOID) {
    // number of offset is 1 greater than the number of data
    poffset_buffer = std::make_shared<PaxBuffer>(offset_len);
    auto offset_buffer = poffset_buffer->As<int32>();

    if (batch_len > 0) offset_buffer[0] = 0;
    for (size_t i = 1; i <= batch_len; i++) {
      offset_buffer[i] = offset_buffer[i - 1] + attlen;
    }
  }

  AppendColumnBuffer(array_vector, batch_len, attr->atttypid, pdata_buffer, nullptr, poffset_buffer);
}


std::shared_ptr<arrow::Buffer> VecAdapter::FullWithCTID2(int block_num, int offset) {
  auto batch_len = cached_batch_lens_;
  auto buffer_len = sizeof(int64) * batch_len;
  auto buffer = std::make_shared<PaxBuffer>(buffer_len);
  int64 *p64 = buffer->As<int64>();

  Assert(block_num >= 0);
  Assert(offset >= 0);

  auto ctid = MakeCTID(block_num, offset);
  for (size_t i = 0; i < batch_len; i++) {
    SetTupleOffset(&ctid, (*ctid_offset_in_current_range_)[i]);
    p64[i] = CTIDToUint64(ctid);
  }
  ctid_offset_in_current_range_ = nullptr;
  return buffer;
}

std::shared_ptr<arrow::RecordBatch> VecAdapter::FlushVecBuffer(int ctid_offset, PaxFragmentInterface *frag, size_t &num_rows) {
  arrow::ArrayVector array_vector;
  std::shared_ptr<arrow::Buffer> data_buffer;
  std::shared_ptr<arrow::Buffer> null_bitmap_buffer;
  std::shared_ptr<arrow::Buffer> offset_buffer;
  int64 null_counts;

  if (cached_batch_lens_ == 0) return nullptr;

  auto const &target_index = frag->ScanColumns();
  int block_num = frag->BlockNum();


  // column size from current pax columns(which is same size with disk stored)
  // may not equal with `rel_tuple_desc_->natts`, but must LE with
  // `rel_tuple_desc_->natts`
  size_t column_size = 0;

  auto columns = process_columns_;
  Assert(columns);

  column_size = columns->GetColumns();

  Assert(column_size <= (size_t)rel_tuple_desc_->natts);

  AttrMissing *attrmiss = NULL;
  if (rel_tuple_desc_->constr)
    attrmiss = rel_tuple_desc_->constr->missing;

  // Vec executor is different with cbdb executor
  // if select single column in multi column defined relation
  // then `target_desc->natts` will be one, rather then actually column numbers
  // So we need use `rel_tuple_desc_` which own full relation tuple desc
  // to fill target arrow data
  for (auto index : target_index) {
    Assert(index >= 0 || index == SelfItemPointerAttributeNumber);
    Assert(index < rel_tuple_desc_->natts);

    if (unlikely(index == SelfItemPointerAttributeNumber)) {
      auto data_buffer = FullWithCTID2(block_num, ctid_offset);
      AppendColumnBuffer(array_vector, cached_batch_lens_, TIDOID, data_buffer);
      continue;
    }

    auto attr = &rel_tuple_desc_->attrs[index];
    Assert(!attr->attisdropped);

    if (likely(index < static_cast<int>(column_size))) {
      // normal values
      Assert((*columns)[index]);

      std::tie(data_buffer, null_bitmap_buffer, offset_buffer, null_counts) =
            ExtractBuffers(&vec_cache_buffer_[index]);
      AppendColumnBuffer(array_vector, cached_batch_lens_, attr->atttypid, data_buffer, null_bitmap_buffer, offset_buffer);
    } else if (!attrmiss || !attrmiss[index].am_present) {
      // all missing values are null
      auto null_array = std::make_shared<arrow::NullArray>(cached_batch_lens_);
      array_vector.emplace_back(null_array);
    } else if (attr->attbyval) {
      // fill missing value attrmiss[index].am_value
      // attlen should be one of 1, 2, 4, 8
      Assert(attr->attlen > 0 && attr->attlen <= 8);
      Assert(((attr->attlen - 1) & attr->attlen) == 0);

      FillMissingByValColumn(attr, attrmiss[index].am_value, cached_batch_lens_, array_vector);
    } else if (attr->attlen > 0) {
      FillMissingFixedLenVarColumn(attr, attrmiss[index].am_value, cached_batch_lens_, array_vector);
    } else {
      Assert(attr->attlen == -1);
      FillMissingVarColumn(attr, attrmiss[index].am_value, cached_batch_lens_, array_vector);
    }
  }

  // `ArrowRecordBatch/ArrowSchema/ArrowArray` alloced by pax memory context.
  // Can not possible to hold the lifecycle of these three objects in pax.
  // It will be freed after memory context reset.

  auto result = arrow::RecordBatch::Make(frag->ScanSchema(), cached_batch_lens_, array_vector);

  num_rows += cached_batch_lens_;
  cached_batch_lens_ = 0;

  return result;
}

static void AppendColumnBuffer(arrow::ArrayVector &array_vector,
                               int64 all_num_rows,
                               Oid pg_typid,
                               const std::shared_ptr<arrow::Buffer> &data_buffer,
                               const std::shared_ptr<arrow::Buffer> &null_bitmap_buffer,
                               const std::shared_ptr<arrow::Buffer> &offset_buffer
                               ) {
  switch (pg_typid)
  {
  case BOOLOID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::BooleanArray>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case CHAROID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::Int8Array>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case INT2OID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::Int16Array>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case INT4OID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::Int32Array>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case INT8OID:
  case TIDOID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::Int64Array>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case TIMEOID:
  case TIMESTAMPOID:
  case TIMESTAMPTZOID: {
    Assert(offset_buffer == nullptr);
    auto datatype = arrow::timestamp(arrow::TimeUnit::MICRO);
    std::vector<std::shared_ptr<arrow::Buffer>> list = {{null_bitmap_buffer, data_buffer}};

    AppendArrowArray<arrow::TimestampArray>(array_vector, datatype, all_num_rows, std::move(list));
    break;
  }
  case DATEOID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::Date32Array>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case FLOAT4OID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::FloatArray>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case FLOAT8OID:
    Assert(offset_buffer == nullptr);
    AppendArrowArray<arrow::DoubleArray>(array_vector, all_num_rows, data_buffer, null_bitmap_buffer);
    break;
  case BPCHAROID:
  case VARCHAROID:
  case TEXTOID: {
    auto array = std::make_shared<arrow::StringArray>(all_num_rows, offset_buffer, data_buffer, null_bitmap_buffer);
    array_vector.emplace_back(array);
    break;
  }

  case NUMERICOID: {
    Assert(offset_buffer == nullptr);

    std::vector<std::shared_ptr<arrow::Buffer>> list = {{null_bitmap_buffer, data_buffer}};
    AppendArrowArray<arrow::Numeric128Array>(array_vector, arrow::numeric128(), all_num_rows, std::move(list));
    break;
  }

  case INT2ARRAYOID:
  case INT4ARRAYOID:
  case INT8ARRAYOID:
  case FLOAT4ARRAYOID:
  case FLOAT8ARRAYOID:
  case TEXTARRAYOID:
  case BPCHARARRAYOID:
  case NAMEOID:
  case XIDOID:
  case CIDOID:
  case OIDVECTOROID:
  case JSONOID:
  case OIDOID:
  case REGPROCOID:
  default:
    Assert(false);
    break;
  }
}

static std::tuple<std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  std::shared_ptr<arrow::Buffer>,
                  int64>
ExtractBuffers(VecAdapter::VecBatchBuffer *batch) {
  std::shared_ptr<PaxBuffer> data_buffer;
  std::shared_ptr<PaxBuffer> null_bitmap_buffer;
  std::shared_ptr<PaxBuffer> offset_buffer;
  int64 null_counts = batch->null_counts;

  Assert(batch->vec_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);

  data_buffer = std::make_shared<PaxBuffer>(batch->vec_buffer.GetBuffer(),
  batch->vec_buffer.Capacity());
  batch->vec_buffer.SetMemTakeOver(false);

  if (batch->null_bits_buffer.GetBuffer()) {
    Assert(batch->null_bits_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);

    null_bitmap_buffer = std::make_shared<PaxBuffer>(batch->null_bits_buffer.GetBuffer(), batch->null_bits_buffer.Capacity());
    batch->null_bits_buffer.SetMemTakeOver(false);
  }

  if (batch->offset_buffer.GetBuffer()) {
    Assert(batch->offset_buffer.Capacity() % MEMORY_ALIGN_SIZE == 0);

    offset_buffer = std::make_shared<PaxBuffer>(batch->offset_buffer.GetBuffer(), batch->offset_buffer.Capacity());
    batch->offset_buffer.SetMemTakeOver(false);
  }

  batch->Reset();
  return {data_buffer, null_bitmap_buffer, offset_buffer, null_counts};
}

}  // namespace pax

#endif  // VEC_BUILD
