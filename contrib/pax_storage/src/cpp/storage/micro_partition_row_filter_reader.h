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
 * micro_partition_row_filter_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_row_filter_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionRowFilterReader : public MicroPartitionReaderProxy {
 public:
  static std::unique_ptr<MicroPartitionReader> New(
      std::unique_ptr<MicroPartitionReader> &&reader,
      std::shared_ptr<PaxFilter> filter,
      std::shared_ptr<Bitmap8> visibility_bitmap = nullptr);

  MicroPartitionRowFilterReader() = default;
  ~MicroPartitionRowFilterReader() override = default;
  bool ReadTuple(TupleTableSlot *slot) override;

 private:
  inline void SetVisibilityBitmap(std::shared_ptr<Bitmap8> visibility_bitmap) {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

  std::shared_ptr<MicroPartitionReader::Group> GetNextGroup(TupleDesc desc);
  bool TestRowScanInternal(TupleTableSlot *slot, ExprState *estate,
                           AttrNumber attno);

  // filter is referenced only, the reader doesn't own it.
  std::shared_ptr<PaxFilter> filter_;
  std::shared_ptr<MicroPartitionReader::Group> group_;
  size_t current_group_row_index_ = 0;
  size_t group_index_ = 0;
  // only referenced
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_;
};
}  // namespace pax
