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
 * orc_group.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_group.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/orc_group.h"

#include "comm/pax_memory.h"
#include "storage/toast/pax_toast.h"

namespace pax {

// Used in `ReadTuple`
// Different from the other `GetColumnDatum` function, in this function, if a
// null row is encountered, then we will perform an accumulation operation on
// `null_counts`. If no null row is encountered, the offset of the row data
// will be calculated through `null_counts`. The other `GetColumnDatum`
// function are less efficient in `foreach` because they have to calculate the
// offset of the row data from scratch every time.
//
// column is not owned
inline static std::pair<Datum, bool> GetColumnDatum(PaxColumn *column,
                                                    size_t row_index,
                                                    uint32 *null_counts) {
  Assert(column);
  Assert(row_index < column->GetRows());
  Datum rc;

  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      *null_counts += 1;
      return {0, true};
    }
    Assert(row_index >= *null_counts);
    rc = column->GetDatum(row_index - *null_counts);
  } else {
    Assert(*null_counts == 0);
    rc = column->GetDatum(row_index);
  }
  return {rc, false};
}

OrcGroup::OrcGroup(std::unique_ptr<PaxColumns> &&pax_column, size_t row_offset,
                   const std::vector<int> *proj_col_index,
                   std::shared_ptr<Bitmap8> micro_partition_visibility_bitmap)
    : pax_columns_(std::move(pax_column)),
      micro_partition_visibility_bitmap_(micro_partition_visibility_bitmap),
      row_offset_(row_offset),
      current_row_index_(0),
      proj_col_index_(proj_col_index),
      current_nulls_(pax_columns_->GetColumns(), 0),
      nulls_shuffle_(pax_columns_->GetColumns(), nullptr) {}

OrcGroup::~OrcGroup() {
  auto numb_of_column = pax_columns_->GetColumns();
  for (size_t i = 0; i < numb_of_column; i++) {
    if (nulls_shuffle_[i]) {
      PAX_DELETE_ARRAY(nulls_shuffle_[i]);
    }
  }
}

size_t OrcGroup::GetRows() const { return pax_columns_->GetRows(); }

size_t OrcGroup::GetRowOffset() const { return row_offset_; }

const std::shared_ptr<PaxColumns> &OrcGroup::GetAllColumns() const {
  return pax_columns_;
}

std::pair<bool, size_t> OrcGroup::ReadTuple(TupleTableSlot *slot) {
  int index = 0;
  int natts = 0;
  int column_nums = 0;

  Assert(pax_columns_);

  auto &pax_columns = *pax_columns_;

  Assert(slot);

  // already consumed
  if (current_row_index_ >= pax_columns.GetRows()) {
    return {false, current_row_index_};
  }

  natts = slot->tts_tupleDescriptor->natts;
  column_nums = pax_columns.GetColumns();

  if (micro_partition_visibility_bitmap_) {
    // skip invisible rows in micro partition
    while (micro_partition_visibility_bitmap_->Test(row_offset_ +
                                                    current_row_index_)) {
      for (index = 0; index < column_nums; index++) {
        auto column = pax_columns[index].get();

        if (!column) {
          continue;
        }

        if (column->HasNull()) {
          auto bm = column->GetBitmap();
          Assert(bm);
          if (!bm->Test(current_row_index_)) {
            current_nulls_[index]++;
          }
        }
      }
      current_row_index_++;
    }

    if (current_row_index_ >= pax_columns.GetRows()) {
      return {false, current_row_index_};
    }
  }

  // proj_col_index_ is not empty
  if (proj_col_index_ && !proj_col_index_->empty()) {
    for (size_t i = 0; i < proj_col_index_->size(); i++) {
      // filter with projection
      index = (*proj_col_index_)[i];

      // handle PAX columns number inconsistent with pg catalog natts in case
      // data not been inserted yet or read pax file conserved before last add
      // column DDL is done, for these cases it is normal that pg catalog schema
      // is not match with that in PAX file.
      if (index >= column_nums) {
        cbdb::SlotGetMissingAttrs(slot, index, index + 1);
        continue;
      }

      // In case column is droped, then set its value as null without reading
      // data tuples.
      if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
        slot->tts_isnull[index] = true;
        continue;
      }

      auto column = pax_columns[index].get();
      Assert(column);

      std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
          GetColumnDatum(column, current_row_index_, &(current_nulls_[index]));
    }
  } else {
    for (index = 0; index < column_nums; index++) {
      // Still need filter with old projection
      // If current proj_col_index_ no build or empty
      // It means current tuple only need return CTID
      if (!pax_columns[index]) {
        continue;
      }

      // In case column is droped, then set its value as null without reading
      // data tuples.
      if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
        slot->tts_isnull[index] = true;
        continue;
      }

      auto column = pax_columns[index].get();
      std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
          GetColumnDatum(column, current_row_index_, &(current_nulls_[index]));
    }

    for (index = column_nums; index < natts; index++) {
      // handle PAX columns number inconsistent with pg catalog natts in case
      // data not been inserted yet or read pax file conserved before last add
      // column DDL is done, for these cases it is normal that pg catalog schema
      // is not match with that in PAX file.
      cbdb::SlotGetMissingAttrs(slot, index, natts);
    }
  }
  return {true, current_row_index_++};
}

bool OrcGroup::GetTuple(TupleTableSlot *slot, size_t row_index) {
  size_t index = 0;
  size_t natts = 0;
  size_t column_nums = 0;

  Assert(pax_columns_);
  Assert(slot);

  if (row_index >= pax_columns_->GetRows()) {
    return false;
  }

  // if tuple has been deleted, return false;
  if (micro_partition_visibility_bitmap_ &&
      micro_partition_visibility_bitmap_->Test(row_offset_ + row_index)) {
    return false;
  }

  natts = static_cast<size_t>(slot->tts_tupleDescriptor->natts);
  column_nums = pax_columns_->GetColumns();

  for (index = 0; index < natts; index++) {
    // Same logic with `ReadTuple`
    if (index >= column_nums) {
      cbdb::SlotGetMissingAttrs(slot, index, natts);
      break;
    }

    auto column = ((*pax_columns_)[index]).get();

    if (!column) {
      continue;
    }

    if (unlikely(slot->tts_tupleDescriptor->attrs[index].attisdropped)) {
      slot->tts_isnull[index] = true;
      continue;
    }

    if (column->HasNull() && !nulls_shuffle_[index]) {
      CalcNullShuffle(column, index);
    }

    uint32 null_counts = 0;
    if (nulls_shuffle_[index]) {
      null_counts = nulls_shuffle_[index][row_index];
    }

    // different with `ReadTuple`
    std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
        GetColumnDatum(column, row_index, &null_counts);
  }

  return true;
}

std::pair<Datum, bool> OrcGroup::GetColumnValue(TupleDesc desc,
                                                size_t column_index,
                                                size_t row_index) {
  Assert(row_index < pax_columns_->GetRows());
  Assert(column_index < static_cast<size_t>(desc->natts));

  auto is_dropped = desc->attrs[column_index].attisdropped;
  if (is_dropped) {
    return {0, true};
  }

  if (column_index < pax_columns_->GetColumns()) {
    return GetColumnValueNoMissing(column_index, row_index);
  }

  AttrMissing *attrmiss = nullptr;
  if (desc->constr) attrmiss = desc->constr->missing;

  if (!attrmiss) {
    // no missing values array at all, so just fill everything in as NULL
    return {0, true};
  } else {
    // fill with default value
    return {attrmiss[column_index].am_value,
            !attrmiss[column_index].am_present};
  }
}

std::pair<Datum, bool> OrcGroup::GetColumnValueNoMissing(size_t column_index,
                                                         size_t row_index) {
  uint32 null_counts = 0;
  Assert(column_index < pax_columns_->GetColumns());
  auto column = (*pax_columns_)[column_index].get();

  // dropped column
  if (!column) {
    return {0, true};
  }

  if (column->HasNull() && !nulls_shuffle_[column_index]) {
    CalcNullShuffle(column, column_index);
  }

  if (nulls_shuffle_[column_index]) {
    null_counts = nulls_shuffle_[column_index][row_index];
  }

  return GetColumnDatum(column, row_index, &null_counts);
}

void OrcGroup::CalcNullShuffle(PaxColumn *column, size_t column_index) {
  auto rows = column->GetRows();
  uint32 n_counts = 0;
  auto bm = column->GetBitmap();

  Assert(bm);
  Assert(column->HasNull() && !nulls_shuffle_[column_index]);

  nulls_shuffle_[column_index] = PAX_NEW_ARRAY<uint32>(rows);

  for (size_t i = 0; i < rows; i++) {
    if (!bm->Test(i)) {
      n_counts += 1;
    }
    nulls_shuffle_[column_index][i] = n_counts;
  }
}

}  // namespace pax
