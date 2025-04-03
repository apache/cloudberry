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
 * orc_vec_group.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_vec_group.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/orc_group.h"
#include "storage/toast/pax_toast.h"
namespace pax {

inline static std::pair<Datum, bool> GetColumnDatum(PaxColumn *column,
                                                    size_t row_index) {
  if (column->HasNull()) {
    auto bm = column->GetBitmap();
    Assert(bm);
    if (!bm->Test(row_index)) {
      return {0, true};
    }
  }
  return {column->GetDatum(row_index), false};
}

OrcVecGroup::OrcVecGroup(std::unique_ptr<PaxColumns> &&pax_column,
                         size_t row_offset,
                         const std::vector<int> *proj_col_index)
    : OrcGroup(std::move(pax_column), row_offset, proj_col_index) {
  Assert(COLUMN_STORAGE_FORMAT_IS_VEC(pax_columns_));
}

std::pair<bool, size_t> OrcVecGroup::ReadTuple(TupleTableSlot *slot) {
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
          GetColumnDatum(column, current_row_index_);
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
          GetColumnDatum(column, current_row_index_);
    }

    for (index = column_nums; index < natts; index++) {
      // handle PAX columns number inconsistent with pg catalog natts in case
      // data not been inserted yet or read pax file conserved before last add
      // column DDL is done, for these cases it is normal that pg catalog schema
      // is not match with that in PAX file.
      cbdb::SlotGetMissingAttrs(slot, index, natts);
    }
  }

  current_row_index_++;
  return {true, current_row_index_ - 1};
}

bool OrcVecGroup::GetTuple(TupleTableSlot *slot, size_t row_index) {
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

    // different with `ReadTuple`
    std::tie(slot->tts_values[index], slot->tts_isnull[index]) =
        GetColumnDatum(column, row_index);
  }

  return true;
}


std::pair<Datum, bool> OrcVecGroup::GetColumnValueNoMissing(size_t column_index,
                                                         size_t row_index) {
  Assert(column_index < pax_columns_->GetColumns());
  auto column = (*pax_columns_)[column_index].get();

  // dropped column
  if (!column) {
    return {0, true};
  }

  return GetColumnDatum(column, row_index);
}

}  // namespace pax
