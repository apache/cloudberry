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
 * pax_porc_vec_adpater.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_porc_vec_adpater.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_adapter.h"

#ifdef VEC_BUILD

#include "comm/vec_numeric.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/orc/orc_type.h"
#include "storage/toast/pax_toast.h"
#include "storage/vec/pax_vec_comm.h"

namespace pax {

template <typename T>
static std::pair<bool, size_t> ColumnTransMemory(PaxColumn *column) {
  Assert(column->GetStorageFormat() == PaxStorageFormat::kTypeStoragePorcVec);

  auto vec_column = static_cast<T*>(column);
  auto data_buffer = vec_column->GetDataBuffer();
  auto data_cap = data_buffer->Capacity();

  if (data_buffer->IsMemTakeOver() && data_cap != 0) {
    Assert(data_cap % MEMORY_ALIGN_SIZE == 0);

    data_buffer->SetMemTakeOver(false);
    return {true, data_cap};
  }

  return {false, 0};
}

static void CopyNonFixedBuffer(PaxVecNonFixedColumn *column,
                               std::shared_ptr<Bitmap8> visibility_map_bitset,
                               size_t group_base_offset, size_t invisible_rows,
                               size_t total_rows,
                               DataBuffer<int32> *out_offset_buffer,
                               DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t buffer_len;
  char *offset_buffer = nullptr;
  size_t offset_buffer_len = 0;
  Assert(visibility_map_bitset && invisible_rows > 0);
  std::tie(buffer, buffer_len) = column->GetBuffer();
  std::tie(offset_buffer, offset_buffer_len) = column->GetOffsetBuffer(false);

  // offsets are not continuous
  auto const typlen = sizeof(int32);
  auto visible_num = total_rows - invisible_rows;
  auto visible_len = typlen * (visible_num + 1);
  size_t data_len = 0;
  int adjust_offset = 0;
  auto offset_array = reinterpret_cast<int32 *>(offset_buffer);

  visible_len = TYPEALIGN(MEMORY_ALIGN_SIZE, visible_len);
  out_offset_buffer->Set(BlockBuffer::Alloc<char>(visible_len), visible_len);
  for (size_t i = 0; i < total_rows; i++) {
    if (!visibility_map_bitset->Test(group_base_offset + i)) {
      out_offset_buffer->Write(&adjust_offset, typlen);
      adjust_offset += offset_array[i + 1] - offset_array[i];
      out_offset_buffer->Brush(typlen);
    }
  }

  out_offset_buffer->Write(&adjust_offset, typlen);
  out_offset_buffer->Brush(typlen);

  // append data buffer
  data_len = TYPEALIGN(MEMORY_ALIGN_SIZE, adjust_offset);
  out_data_buffer->Set(BlockBuffer::Alloc<char>(data_len), data_len);
  for (size_t i = 0; i < total_rows; i++) {
    auto value_size = offset_array[i + 1] - offset_array[i];
    if (value_size > 0 && !visibility_map_bitset->Test(group_base_offset + i)) {
      out_data_buffer->Write(&buffer[offset_array[i]], value_size);
      out_data_buffer->Brush(value_size);
    }
  }
}

static void CopyFixedBuffer(PaxColumn *column,
                            std::shared_ptr<Bitmap8> visibility_map_bitset,
                            size_t group_base_offset, size_t invisible_rows,
                            size_t total_rows,
                            DataBuffer<char> *out_data_buffer) {
  char *buffer;
  size_t pg_attribute_unused() buffer_len;
  Assert(invisible_rows > 0);
  Assert(visibility_map_bitset);

  std::tie(buffer, buffer_len) = column->GetBuffer();

  const auto typlen = column->GetTypeLength();
  auto visible_len = (total_rows - invisible_rows) * typlen;
  visible_len = TYPEALIGN(MEMORY_ALIGN_SIZE, visible_len);
  out_data_buffer->Set(BlockBuffer::Alloc<char>(visible_len), visible_len);

  for (size_t i = 0; i < total_rows; i++) {
    if (!visibility_map_bitset->Test(group_base_offset + i)) {
      out_data_buffer->Write(buffer, typlen);
      out_data_buffer->Brush(typlen);
    }
    buffer += typlen;
  }
}

std::pair<size_t, size_t> VecAdapter::AppendPorcVecFormat(PaxColumns *columns) {
  size_t invisible_rows;
  size_t total_rows;

  total_rows = columns->GetRows();
  invisible_rows = GetInvisibleNumber(0, total_rows);
  Assert(invisible_rows <= total_rows);

  if (invisible_rows == total_rows) {
    return {0, total_rows};
  }

  for (size_t index = 0; index < columns->GetColumns(); index++) {
    auto column = (*columns)[index].get();
    if (column == nullptr) {
      continue;
    }

    DataBuffer<char> *vec_buffer = &(vec_cache_buffer_[index].vec_buffer);
    DataBuffer<int32> *offset_buffer =
        &(vec_cache_buffer_[index].offset_buffer);

    Assert(index < (size_t)vec_cache_buffer_lens_ && vec_cache_buffer_);

    char *buffer = nullptr;
    size_t buffer_len = 0;
    bool trans_succ = false;
    size_t cap_len = 0;

    vec_cache_buffer_[index].null_counts = 0;
    CopyBitmapBuffer(column, micro_partition_visibility_bitmap_,
                     group_base_offset_, 0, total_rows,
                     column->GetRangeNonNullRows(0, total_rows),
                     total_rows - invisible_rows,
                     &(vec_cache_buffer_[index].null_bits_buffer),
                     &(vec_cache_buffer_[index].null_counts));

    switch (column->GetPaxColumnTypeInMem()) {
      case PaxColumnTypeInMem::kTypeVecBpChar:
      case PaxColumnTypeInMem::kTypeVecNoHeader:
      case PaxColumnTypeInMem::kTypeNonFixed: {
        Assert(!vec_buffer->GetBuffer());
        Assert(!offset_buffer->GetBuffer());

        // no need try transfer memory buffer
        if (invisible_rows != 0) {
          CopyNonFixedBuffer(dynamic_cast<PaxVecNonFixedColumn*>(column),
                             micro_partition_visibility_bitmap_,
                             group_base_offset_, invisible_rows, total_rows,
                             offset_buffer, vec_buffer);

          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxVecNonFixedColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(BlockBuffer::Alloc<char>(
                              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                          TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }

        std::tie(buffer, buffer_len) =
            static_cast<PaxVecNonFixedColumn*>(column)->GetOffsetBuffer(false);
        // TODO(jiaqizho): this buffer can also be transferred
        offset_buffer->Set(BlockBuffer::Alloc<char>(
                               TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
                           TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
        offset_buffer->Write((int *)buffer, buffer_len);
        offset_buffer->BrushAll();
        break;
      }
      case PaxColumnTypeInMem::kTypeVecDecimal: {
        Assert(!vec_buffer->GetBuffer());
        if (invisible_rows != 0) {
          CopyFixedBuffer(column, micro_partition_visibility_bitmap_,
                          group_base_offset_, invisible_rows, total_rows,
                          vec_buffer);
          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();
        std::tie(trans_succ, cap_len) =
            ColumnTransMemory<PaxShortNumericColumn>(column);

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          vec_buffer->Set(
              (char *)BlockBuffer::Alloc0(TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len)),
              TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len));
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      case PaxColumnTypeInMem::kTypeVecBitPacked:
      case PaxColumnTypeInMem::kTypeFixed: {
        Assert(!vec_buffer->GetBuffer());

        if (invisible_rows != 0) {
          CopyFixedBuffer(column, micro_partition_visibility_bitmap_,
                          group_base_offset_, invisible_rows, total_rows,
                          vec_buffer);
          break;
        }

        std::tie(buffer, buffer_len) = column->GetBuffer();

        switch (column->GetTypeLength()) {
          case 1:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int8>>(column);
            break;
          case 2:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int16>>(column);
            break;
          case 4:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int32>>(column);
            break;
          case 8:
            std::tie(trans_succ, cap_len) =
                ColumnTransMemory<PaxVecCommColumn<int64>>(column);
            break;
          default:
            Assert(false);
        }

        if (trans_succ) {
          vec_buffer->Set(buffer, cap_len);
          vec_buffer->BrushAll();
        } else {
          auto align_size = TYPEALIGN(MEMORY_ALIGN_SIZE, buffer_len);

          vec_buffer->Set(BlockBuffer::Alloc<char>(align_size), align_size);
          if (column->GetPaxColumnTypeInMem() ==
              PaxColumnTypeInMem::kTypeVecBitPacked) {
            memset(vec_buffer->Start(), 0, align_size);
          }
          vec_buffer->Write(buffer, buffer_len);
          vec_buffer->BrushAll();
        }
        break;
      }
      default: {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                   fmt("Invalid column [type=%d], PORC_VEC format won't create "
                       "this type of column.",
                       column->GetPaxColumnTypeInMem()));
      }
    }
  }

  return std::make_pair(total_rows - invisible_rows, total_rows);
}

}  // namespace pax

#endif  // VEC_BUILD
