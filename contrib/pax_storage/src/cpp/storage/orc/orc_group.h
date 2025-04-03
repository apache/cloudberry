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
 * orc_group.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_group.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition.h"

namespace pax {

namespace tools {
class OrcDumpReader;
}

class OrcGroup : public MicroPartitionReader::Group {
 public:
  OrcGroup(
      std::unique_ptr<PaxColumns> &&pax_column, size_t row_offset,
      const std::vector<int> *proj_col_index,
      std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap = nullptr);

  ~OrcGroup() override;

  size_t GetRows() const override;

  size_t GetRowOffset() const override;

  const std::shared_ptr<PaxColumns> &GetAllColumns() const override;

  virtual std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) override;

  virtual bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  std::pair<Datum, bool> GetColumnValue(TupleDesc desc, size_t column_index,
                                        size_t row_index) override;
  void SetVisibilityMap(std::shared_ptr<Bitmap8> visibility_bitmap) override {
    micro_partition_visibility_bitmap_ = visibility_bitmap;
  }

  std::shared_ptr<Bitmap8> GetVisibilityMap() const override {
    return micro_partition_visibility_bitmap_;
  }

 protected:
  void CalcNullShuffle(PaxColumn *column, size_t column_index);

  // Used to get the no missing column
  std::pair<Datum, bool> GetColumnValueNoMissing(size_t column_index,
                                                 size_t row_index);

 protected:
  std::shared_ptr<PaxColumns> pax_columns_;
  std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap_;
  size_t row_offset_;
  size_t current_row_index_;
  std::vector<std::shared_ptr<MemoryObject>> buffer_holders_;
  // only a reference, owner by pax_filter
  const std::vector<int> *proj_col_index_;

 private:
  friend class tools::OrcDumpReader;
  std::vector<uint32> current_nulls_;
  std::vector<uint32 *> nulls_shuffle_;
};

class OrcVecGroup final : public OrcGroup {
 public:
  OrcVecGroup(std::unique_ptr<PaxColumns> &&pax_column, size_t row_offset,
              const std::vector<int> *proj_col_index);

  // We want to inline the `GetColumnValue` function so we override
  // `ReadTuple`
  virtual std::pair<bool, size_t> ReadTuple(TupleTableSlot *slot) override;

  virtual bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  // Used to get the no missing column
  std::pair<Datum, bool> GetColumnValueNoMissing(size_t column_index,
                                                 size_t row_index) override;
};

}  // namespace pax
