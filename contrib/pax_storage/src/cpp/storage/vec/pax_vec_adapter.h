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
 * pax_vec_adapter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_adapter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#ifdef VEC_BUILD

#include "storage/columns/pax_columns.h"
#include "storage/micro_partition.h"
#include "storage/pax_defined.h"

namespace arrow {
  class RecordBatch;
  class Buffer;
}

namespace pax {

class PaxFragmentInterface;
class VecAdapter final {
 public:
  struct VecBatchBuffer {
    DataBuffer<char> vec_buffer;
    DataBuffer<char> null_bits_buffer;
    DataBuffer<int32> offset_buffer;
    size_t null_counts;
#ifdef BUILD_RB_RET_DICT
    bool is_dict;
    DataBuffer<int32> dict_offset_buffer;
    DataBuffer<char> dict_entry_buffer;
#endif

    VecBatchBuffer();

    void Reset();

    void SetMemoryTakeOver(bool take);
  };

  VecAdapter(TupleDesc tuple_desc, bool build_ctid);

  ~VecAdapter();

  void SetDataSource(std::shared_ptr<PaxColumns> columns, int group_base_offset);

  bool IsInitialized() const;

  // return -1,0 or batch_cache_lens
  // return -1: no tuples left in the current working group
  // return 0: no tuples left in the current batch, should process remaining
  // tuples in the current group return > 0: this batch of data has N tuples
  // that need to be converted into vec record batch
  int AppendToVecBuffer();

  size_t FlushVecBuffer(TupleTableSlot *slot);

  std::shared_ptr<arrow::RecordBatch> FlushVecBuffer(int ctid_offset, PaxFragmentInterface *frag, size_t &num_rows);

  TupleDesc GetRelationTupleDesc() const;

  bool ShouldBuildCtid() const;

  inline void SetVisibitilyMapInfo(std::shared_ptr<Bitmap8> visibility_bitmap) {
    micro_partition_visibility_bitmap_ = std::move(visibility_bitmap);
  }

  inline void Reset() {
    // keep some fields unchanged
    // rel_tuple_desc_ build_ctid_

    cached_batch_lens_ = 0;
    //vec_cache_buffer_ = nullptr;
    //vec_cache_buffer_lens_ = 0;
    process_columns_ = nullptr;
    current_index_ = 0;
    group_base_offset_ = 0;
    micro_partition_visibility_bitmap_ = nullptr;
  }

  static int GetMaxBatchSizeFromStr(char *max_batch_size_str,
                                    int default_value);

 private:
  void FullWithCTID(TupleTableSlot *slot, VecBatchBuffer *batch_buffer);
  void FillMissColumn(int attr_index);

  std::pair<size_t, size_t> AppendPorcFormat(PaxColumns *columns,
                                             size_t range_begin,
                                             size_t range_lens);
  std::pair<size_t, size_t> AppendPorcVecFormat(PaxColumns *columns);

  inline size_t GetInvisibleNumber(size_t range_begin, size_t range_lens) {
    if (micro_partition_visibility_bitmap_ == nullptr) {
      return 0;
    }

    // The number of bits which set to 1 in visibility map is the number of
    // invisible tuples
    auto begin = group_base_offset_ + range_begin;
    return micro_partition_visibility_bitmap_->CountBits(
        begin, begin + range_lens - 1);
  }

  void BuildCtidOffset(size_t range_begin, size_t range_lengs);

  std::shared_ptr<arrow::Buffer> FullWithCTID2(int block_num, int offset);
  void FullWithNulls(VecBatchBuffer *batch_buffer);


 private:
  TupleDesc rel_tuple_desc_;
  size_t cached_batch_lens_;
  VecBatchBuffer *vec_cache_buffer_;
  int vec_cache_buffer_lens_;

  std::shared_ptr<PaxColumns> process_columns_;
  size_t current_index_;
  bool build_ctid_;

  int group_base_offset_;

  // current block number
  int32 block_no_;
  // only referenced
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_ = nullptr;

  // ctid offset in current batch range
  std::shared_ptr<DataBuffer<int32>> ctid_offset_in_current_range_;
};
}  // namespace pax

#endif  // VEC_BUILD
