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
 * pax_vec_comm.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_comm.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/vec/pax_vec_comm.h"
#include "storage/toast/pax_toast.h"
#ifdef VEC_BUILD

namespace pax {

void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin, size_t range_lens,
                DataBuffer<char> *null_bits_buffer) {
  // So the `range_begin % 8` must be 0
  Assert(range_begin % 8 == 0);

  auto null_buffer = reinterpret_cast<char *>(bitmap->Raw().bitmap);
  auto write_size = BITS_TO_BYTES(range_lens);
  auto bitmap_raw_size = bitmap->Raw().size;

  if ((range_begin / 8) >= bitmap_raw_size) {  // all nulls in current range
    null_bits_buffer->WriteZero(write_size);
    null_bits_buffer->Brush(write_size);
  } else {
    auto remain_size = bitmap_raw_size - (range_begin / 8);
    if (remain_size >= write_size) {  // full bitmap in current range
      null_bits_buffer->Write(null_buffer + range_begin / 8, write_size);
      null_bits_buffer->Brush(write_size);
    } else {  // part of non-null range with a continuous all nulls range
      auto write_size_gap = write_size - remain_size;
      null_bits_buffer->Write(null_buffer + range_begin / 8, remain_size);
      null_bits_buffer->Brush(remain_size);
      null_bits_buffer->WriteZero(write_size_gap);
      null_bits_buffer->Brush(write_size_gap);
    }
  }
}

void VarlenaToRawBuffer(char *buffer, size_t buffer_len, char **out_data,
                        size_t *out_len) {
  struct varlena *vl;

  vl = (struct varlena *)(buffer);

#ifdef ENABLE_DEBUG
  // should not exist no toast here
  auto tunpacked = pg_detoast_exp_short(vl);
  Assert((Pointer)vl == (Pointer)tunpacked);
#endif

  *out_len = VARSIZE_ANY_EXHDR(vl);
  *out_data = VARDATA_ANY(vl);
}

void CopyBitmapBuffer(PaxColumn *column,
                      std::shared_ptr<Bitmap8> visibility_map_bitset,
                      size_t bitset_index_begin, size_t range_begin,
                      size_t range_lens, size_t data_range_lens,
                      size_t out_range_lens, DataBuffer<char> *null_bits_buffer,
                      size_t *out_visable_null_counts) {
  if (column->HasNull()) {
    if (visibility_map_bitset == nullptr) {
      // null length depends on `range_lens`
      auto null_align_bytes =
          TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(range_lens));
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set(BlockBuffer::Alloc<char>(null_align_bytes),
                            null_align_bytes);
      auto bitmap = column->GetBitmap();
      Assert(bitmap);
      CopyBitmap(bitmap.get(), range_begin, range_lens, null_bits_buffer);
      *out_visable_null_counts = range_lens - data_range_lens;
    } else {
      auto bitmap = column->GetBitmap();
      Assert(bitmap);

      Bitmap8 null_bitmap(out_range_lens);
      size_t null_count = 0;
      size_t null_index = 0;
      for (size_t i = range_begin; i < range_begin + range_lens; i++) {
        // only calculate the null bitmap corresponding to the unmarked
        // deleted tuple.
        if (!visibility_map_bitset->Test(i - range_begin +
                                         bitset_index_begin)) {
          // is null
          if (!bitmap->Test(i)) {
            null_count++;
          } else {
            // not null
            null_bitmap.Set(null_index);
          }
          null_index++;
        }
      }

      auto null_bytes =
          TYPEALIGN(MEMORY_ALIGN_SIZE, BITS_TO_BYTES(out_range_lens));
      Assert(!null_bits_buffer->GetBuffer());
      null_bits_buffer->Set(BlockBuffer::Alloc0<char>(null_bytes), null_bytes);
      CopyBitmap(&null_bitmap, 0, out_range_lens, null_bits_buffer);
      *out_visable_null_counts = null_count;
      CBDB_CHECK(out_range_lens == null_index,
                 cbdb::CException::ExType::kExTypeOutOfRange,
                 fmt("The required range len not match the null counts [range "
                     "len=%lu, null count=%lu]",
                     out_range_lens, null_index));
    }
  }
}

}  // namespace pax

#endif  // VEC_BUILD
