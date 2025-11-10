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
 * micro_partition_stats.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_stats.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_stats.h"

#include "comm/cbdb_api.h"

#include "comm/bloomfilter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/proto/proto_wrappers.h"

// STATUS_UNINITIALIZED: all is uninitialized
// STATUS_NOT_SUPPORT: column doesn't support min-max/sum
// STATUS_MISSING_INIT_VAL: oids are initialized, but min-max value is missing
// STATUS_NEED_UPDATE: min-max/sum is set, needs update.
#define STATUS_UNINITIALIZED 'u'
#define STATUS_NOT_SUPPORT 'x'
#define STATUS_MISSING_INIT_VAL 'n'
#define STATUS_NEED_UPDATE 'y'

namespace pax {

class MicroPartitionStatsData final {
 public:
  MicroPartitionStatsData(int natts) : info_() {
    Assert(natts >= 0);

    for (int i = 0; i < natts; i++) {
      auto col pg_attribute_unused() = info_.add_columnstats();
      Assert(col->allnull() && !col->hasnull());
    }
  }

  inline void Reset() {
    auto n = info_.columnstats_size();

    for (int i = 0; i < n; i++) {
      // no clean the basic stats
      info_.mutable_columnstats(i)->clear_datastats();
      info_.mutable_columnstats(i)->clear_allnull();
      info_.mutable_columnstats(i)->clear_hasnull();
      info_.mutable_columnstats(i)->clear_nonnullrows();
    }
  }

  inline void CopyFrom(MicroPartitionStatsData *stats, int column_index) {
    Assert(stats);
    Assert(typeid(this) == typeid(stats));

    info_.mutable_columnstats(column_index)
        ->CopyFrom(stats->info_.columnstats(column_index));
  }

  inline void CopyFrom(::pax::stats::MicroPartitionStatisticsInfo *info,
                       int column_index) {
    Assert(info);

    info_.mutable_columnstats(column_index)
        ->CopyFrom(info->columnstats(column_index));
  }

  inline ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index) {
    return info_.mutable_columnstats(column_index)->mutable_info();
  }

  inline ::pax::stats::BloomFilterBasicInfo *GetBloomFilterBasicInfo(
      int column_index) {
    return info_.mutable_columnstats(column_index)->mutable_bloomfilterinfo();
  }

  inline bool HasBloomFilterBasicInfo(int column_index) {
    return info_.mutable_columnstats(column_index)->has_bloomfilterinfo();
  }

  inline std::string *GetColumnBloomFilterStats(int column_index) {
    return info_.mutable_columnstats(column_index)->mutable_columnbfstats();
  }

  inline void SetColumnBloomFilterStats(int column_index,
                                        const std::string &bfstats) {
    info_.mutable_columnstats(column_index)->set_columnbfstats(bfstats);
  }

  inline ::pax::stats::ColumnDataStats *GetColumnDataStats(int column_index) {
    return info_.mutable_columnstats(column_index)->mutable_datastats();
  }

  inline void SetAllNull(int column_index, bool allnull) {
    info_.mutable_columnstats(column_index)->set_allnull(allnull);
  }

  inline void SetHasNull(int column_index, bool hasnull) {
    info_.mutable_columnstats(column_index)->set_hasnull(hasnull);
  }

  inline void SetNonNullRows(int column_index, uint32 non_null_rows) {
    info_.mutable_columnstats(column_index)->set_nonnullrows(non_null_rows);
  }

  inline bool GetAllNull(int column_index) {
    return info_.columnstats(column_index).allnull();
  }

  inline bool GetHasNull(int column_index) {
    return info_.columnstats(column_index).hasnull();
  }

  inline uint32 GetNonNullRows(int column_index) {
    return info_.columnstats(column_index).nonnullrows();
  }

  inline ::pax::stats::MicroPartitionStatisticsInfo *GetStatsInfoRef() {
    return &info_;
  }

 private:
  ::pax::stats::MicroPartitionStatisticsInfo info_;
};

static bool PrepareStatisticsInfoCombine(
    stats::MicroPartitionStatisticsInfo *left,
    stats::MicroPartitionStatisticsInfo *right, TupleDesc desc,
    bool allow_fallback_to_pg,
    std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> &funcs,
    std::vector<std::pair<FmgrInfo, FmgrInfo>> &finfos,
    std::vector<FmgrInfo> &sum_finfos, FmgrInfo &empty_func) {
  Assert(left->columnstats_size() <= desc->natts);
  if (left->columnstats_size() != right->columnstats_size()) {
    // exist drop/add column, schema not match
    return false;
  }

  Assert(funcs.empty());
  Assert(finfos.empty());
  Assert(sum_finfos.empty());

  funcs.reserve(desc->natts);
  finfos.reserve(desc->natts);
  sum_finfos.reserve(desc->natts);

  for (int i = 0; i < left->columnstats_size(); i++) {
    auto left_column_stats = left->columnstats(i);
    auto right_column_stats = right->columnstats(i);
    auto left_column_data_stats = left_column_stats.datastats();
    auto right_column_data_stats = right_column_stats.datastats();

    auto attr = TupleDescAttr(desc, i);
    auto collation = attr->attcollation;
    bool get_pg_oper_succ = false;

    funcs.emplace_back(std::make_pair(nullptr, nullptr));
    finfos.emplace_back(std::make_pair(empty_func, empty_func));
    sum_finfos.emplace_back(empty_func);

    // allnull/hasnull/nonnullrows must be exist.
    // And we must not check has_allnull/has_hasnull, cause it may return false
    // if we have not update allnull/hasnull it in `AddRows`.
    // The current stats may not have been serialized in disk.

    // if current column stats do have collation but
    // not same, then we can't combine two of stats
    if (left_column_stats.info().collation() !=
        right_column_stats.info().collation()) {
      return false;
    }

    // Current relation collation changed, the min/max may not work
    if (collation != left_column_stats.info().collation() &&
        left_column_stats.info().collation() != 0) {
      return false;
    }

    // the column_stats.info() can be null, if current column is allnull
    // So don't assert typeoid/collation here
    if (right_column_data_stats.has_minimal() &&
        left_column_data_stats.has_minimal()) {
      Assert(right_column_data_stats.has_maximum() &&
             left_column_data_stats.has_maximum());

      GetStrategyProcinfo(attr->atttypid, attr->atttypid, funcs[i]);
      if (allow_fallback_to_pg) {
        get_pg_oper_succ =
            GetStrategyProcinfo(attr->atttypid, attr->atttypid, finfos[i]);
      }

      // if current min/max from pg_oper, but now allow_fallback_to_pg is false
      if (!(funcs[i].first && CollateIsSupport(collation)) &&
          !get_pg_oper_succ) {
        return false;
      }
    }

    // Check `SUM` can combine or not
    if (left_column_data_stats.has_sum() && right_column_data_stats.has_sum()) {
      Assert(left_column_stats.info().prorettype() ==
             right_column_stats.info().prorettype());
      Oid addrettype;

      if (!cbdb::PGGetAddOperator(left_column_stats.info().prorettype(),
                                  right_column_stats.info().prorettype(),
                                  PG_CATALOG_NAMESPACE, &addrettype,
                                  &sum_finfos[i])) {
        return false;
      }

      // Will not generate the `SUM` if addrettype not same with aggfinaltype
      Assert(addrettype == left_column_stats.info().prorettype());
    }
  }

  Assert(funcs.size() == (size_t)desc->natts);
  Assert(finfos.size() == (size_t)desc->natts);
  Assert(sum_finfos.size() == (size_t)desc->natts);

  return true;
}

static void CommStatisticsInfoCombine(
    ::pax::stats::ColumnStats *left_column_stats,
    ::pax::stats::ColumnStats *right_column_stats) {
  if (left_column_stats->allnull() && !right_column_stats->allnull()) {
    left_column_stats->set_allnull(false);
  }

  if (!left_column_stats->hasnull() && right_column_stats->hasnull()) {
    left_column_stats->set_hasnull(true);
  }

  left_column_stats->set_nonnullrows(left_column_stats->nonnullrows() +
                                     right_column_stats->nonnullrows());
}

static void MinMaxStatisticsInfoCombine(
    int column_index, TupleDesc desc,
    std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs_pair,
    std::pair<FmgrInfo, FmgrInfo> &finfo_pair,
    ::pax::stats::ColumnStats *left_column_stats,
    ::pax::stats::ColumnStats *right_column_stats,
    ::pax::stats::ColumnDataStats *left_column_data_stats,
    ::pax::stats::ColumnDataStats *right_column_data_stats) {
  auto attr = TupleDescAttr(desc, column_index);

  auto typlen = attr->attlen;
  auto typbyval = attr->attbyval;
  auto collation = right_column_stats->info().collation();

  if (right_column_data_stats->has_minimal()) {
    if (left_column_data_stats->has_minimal()) {
      auto left_min_datum = MicroPartitionStats::FromValue(
          left_column_data_stats->minimal(), typlen, typbyval, column_index);
      auto right_min_datum = MicroPartitionStats::FromValue(
          right_column_data_stats->minimal(), typlen, typbyval, column_index);
      bool min_rc = false;

      // can direct call the oper, no need check exist again
      if (funcs_pair.first) {
        min_rc = funcs_pair.first(&right_min_datum, &left_min_datum, collation);
      } else {
        min_rc = DatumGetBool(cbdb::FunctionCall2Coll(
            &finfo_pair.first, collation, right_min_datum, left_min_datum));
      }

      if (min_rc) {
        left_column_data_stats->set_minimal(
            MicroPartitionStats::ToValue(right_min_datum, typlen, typbyval));
      }
    } else {
      left_column_data_stats->set_minimal(right_column_data_stats->minimal());
    }
  }

  if (right_column_data_stats->has_maximum()) {
    if (left_column_data_stats->has_maximum()) {
      auto left_max_datum = MicroPartitionStats::FromValue(
          left_column_data_stats->maximum(), typlen, typbyval, column_index);
      auto right_max_datum = MicroPartitionStats::FromValue(
          right_column_data_stats->maximum(), typlen, typbyval, column_index);
      bool max_rc = false;

      if (funcs_pair.second != nullptr) {
        max_rc =
            funcs_pair.second(&right_max_datum, &left_max_datum, collation);
      } else {  // no need check again
        max_rc = DatumGetBool(cbdb::FunctionCall2Coll(
            &finfo_pair.second, collation, right_max_datum, left_max_datum));
      }

      if (max_rc) {
        left_column_data_stats->set_maximum(
            MicroPartitionStats::ToValue(right_max_datum, typlen, typbyval));
      }

    } else {
      left_column_data_stats->set_maximum(right_column_data_stats->maximum());
    }
  }
}

static void SumStatisticsInfoCombine(
    int column_index, FmgrInfo &sum_finfo,
    ::pax::stats::ColumnStats *left_column_stats,
    ::pax::stats::ColumnStats *right_column_stats,
    ::pax::stats::ColumnDataStats *left_column_data_stats,
    ::pax::stats::ColumnDataStats *right_column_data_stats) {
  if (right_column_data_stats->has_sum()) {
    if (left_column_data_stats->has_sum()) {
      auto sumtyplen = cbdb::GetTyplen(left_column_stats->info().prorettype());
      auto sumtypbyval =
          cbdb::GetTypbyval(left_column_stats->info().prorettype());
      Datum left_sum = MicroPartitionStats::FromValue(
          left_column_data_stats->sum(), sumtyplen, sumtypbyval, column_index);
      Datum right_sum = MicroPartitionStats::FromValue(
          right_column_data_stats->sum(), sumtyplen, sumtypbyval, column_index);
      Datum newval =
          cbdb::FunctionCall2Coll(&sum_finfo, InvalidOid, left_sum, right_sum);

      left_column_data_stats->set_sum(
          MicroPartitionStats::ToValue(newval, sumtyplen, sumtypbyval));

      if (!sumtypbyval &&
          cbdb::DatumToPointer(newval) != cbdb::DatumToPointer(left_sum)) {
        if (newval) cbdb::Pfree(cbdb::DatumToPointer(newval));
      }

    } else {
      left_column_data_stats->set_sum(right_column_data_stats->sum());
    }
  }
}

bool MicroPartitionStats::MicroPartitionStatisticsInfoCombine(
    stats::MicroPartitionStatisticsInfo *left,
    stats::MicroPartitionStatisticsInfo *right, TupleDesc desc,
    bool allow_fallback_to_pg) {
  std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> funcs;
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos;
  std::vector<FmgrInfo> sum_finfos;
  FmgrInfo empty_info;

  memset(&empty_info, 0, sizeof(empty_info));
  Assert(left);
  Assert(right);

  // check before update left stats
  // also get the min/max operators
  if (!PrepareStatisticsInfoCombine(left, right, desc, allow_fallback_to_pg,
                                    funcs, finfos, sum_finfos, empty_info)) {
    return false;
  }

  for (int i = 0; i < left->columnstats_size(); i++) {
    auto left_column_stats = left->mutable_columnstats(i);
    auto right_column_stats = right->mutable_columnstats(i);

    auto left_column_data_stats = left_column_stats->mutable_datastats();
    auto right_column_data_stats = right_column_stats->mutable_datastats();

    CommStatisticsInfoCombine(left_column_stats, right_column_stats);
    MinMaxStatisticsInfoCombine(i, desc, funcs[i], finfos[i], left_column_stats,
                                right_column_stats, left_column_data_stats,
                                right_column_data_stats);
    SumStatisticsInfoCombine(i, sum_finfos[i], left_column_stats,
                             right_column_stats, left_column_data_stats,
                             right_column_data_stats);
  }

  // There is no need to perform bloom filtering in segmainfest
  // the effect may be very poor
  // Just must sure the bloom filter been cleared if current min/max
  // combined success.
  for (int i = 0; i < left->columnstats_size(); i++) {
    auto left_column_stats = left->mutable_columnstats(i);

    if (left_column_stats->has_bloomfilterinfo()) {
      left_column_stats->clear_bloomfilterinfo();
      left_column_stats->clear_columnbfstats();
    }
  }

  return true;
}

// We place the information of each column in a nearby layout, so that for
// each column status check, there is only one memory access, and other
// accesses can hit the cache.

struct NullCountStats {
  // whether the column has been set the has_null to true, if done, we should
  // not set it again
  bool has_null = false;
  // whether the column has been set the all_null to false, if done, we should
  // not set it again
  bool all_null = true;
  // the count of not null rows
  int32 not_null_count = 0;
  void Reset() {
    has_null = false;
    all_null = true;
    not_null_count = 0;
  }
};

struct MinMaxStats {
  // status to indicate whether the oids are initialized
  // or the min-max values are initialized
  char status = STATUS_UNINITIALIZED;
  Datum min_in_mem = 0;
  Datum max_in_mem = 0;

  void Reset() {
    Assert(status == STATUS_MISSING_INIT_VAL || status == STATUS_NEED_UPDATE ||
           status == STATUS_NOT_SUPPORT);
    if (status == STATUS_NEED_UPDATE) status = STATUS_MISSING_INIT_VAL;
    min_in_mem = 0;
    max_in_mem = 0;
  }
};

struct SumStatsInMem {
  // same as min-max 'status'
  char status = STATUS_UNINITIALIZED;

  FmgrInfo trans_func;
  FmgrInfo final_func;
  FmgrInfo add_func;
  Oid argtype = 0;
  Oid rettype = 0;
  Oid transtype = 0;
  int16 transtyplen = -3;
  bool transtypbyval = false;
  int16 rettyplen = -3;
  bool rettypbyval = false;
  bool final_func_exist = false;

  bool final_func_called = false;
  // The result pointed memory is allocated by palloc in pg function.
  Datum result = 0;
  void Reset() {
    Assert(status == STATUS_MISSING_INIT_VAL || status == STATUS_NEED_UPDATE ||
           status == STATUS_NOT_SUPPORT);
    if (status == STATUS_NEED_UPDATE) {
      result = 0;
      final_func_called = false;
      status = STATUS_MISSING_INIT_VAL;
    }
  };
};

// We place the information of each column in a nearby layout, so that for
// each column status check, there is only one memory access, and other
// accesses can hit the cache.
struct ColumnMemStats {
  NullCountStats null_count_stats_;
  MinMaxStats min_max_stats_;
  // the stats to describe column
  SumStatsInMem sum_stats_;
};

MicroPartitionStats::MicroPartitionStats(TupleDesc desc,
                                         bool allow_fallback_to_pg)
    : tuple_desc_(desc), allow_fallback_to_pg_(allow_fallback_to_pg) {
  FmgrInfo finfo;
  int natts;

  Assert(tuple_desc_);
  natts = tuple_desc_->natts;

  stats_ = std::make_unique<MicroPartitionStatsData>(natts);

  memset(&finfo, 0, sizeof(finfo));
  memset(&expr_context_, 0, sizeof(expr_context_));
  finfos_.clear();
  local_funcs_.clear();
  column_mem_stats_.resize(natts);

  for (int i = 0; i < natts; i++) {
    finfos_.emplace_back(std::pair<FmgrInfo, FmgrInfo>({finfo, finfo}));
    local_funcs_.emplace_back(
        std::pair<OperMinMaxFunc, OperMinMaxFunc>({nullptr, nullptr}));
    bf_status_.emplace_back(STATUS_UNINITIALIZED);
    bf_stats_.emplace_back(std::move(BloomFilter()));
    required_stats_.emplace_back(false);
  }
}

MicroPartitionStats::~MicroPartitionStats() {}

::pax::stats::MicroPartitionStatisticsInfo *MicroPartitionStats::Serialize() {
  auto n = tuple_desc_->natts;
  stats::ColumnDataStats *data_stats;

  for (auto column_index = 0; column_index < n; column_index++) {
    // set const stats
    stats_->SetAllNull(
        column_index,
        column_mem_stats_[column_index].null_count_stats_.all_null);
    stats_->SetHasNull(
        column_index,
        column_mem_stats_[column_index].null_count_stats_.has_null);
    stats_->SetNonNullRows(
        column_index,
        column_mem_stats_[column_index].null_count_stats_.not_null_count);

    // clear the in-memory stats
    column_mem_stats_[column_index].null_count_stats_.Reset();

    data_stats = stats_->GetColumnDataStats(column_index);
    // only STATUS_NEED_UPDATE need set to the stats_
    if (column_mem_stats_[column_index].min_max_stats_.status ==
        STATUS_NEED_UPDATE) {
      auto att = TupleDescAttr(tuple_desc_, column_index);
      auto typlen = att->attlen;
      auto typbyval = att->attbyval;

      data_stats->set_minimal(
          ToValue(column_mem_stats_[column_index].min_max_stats_.min_in_mem,
                  typlen, typbyval));
      data_stats->set_maximum(
          ToValue(column_mem_stats_[column_index].min_max_stats_.max_in_mem,
                  typlen, typbyval));

      // after serialize to pb, clear the memory
      if (!typbyval && typlen == -1) {
        buffer_holders_.erase(
            column_mem_stats_[column_index].min_max_stats_.min_in_mem);
        buffer_holders_.erase(
            column_mem_stats_[column_index].min_max_stats_.max_in_mem);
      }
      column_mem_stats_[column_index].min_max_stats_.min_in_mem = 0;
      column_mem_stats_[column_index].min_max_stats_.max_in_mem = 0;
    }

    if (column_mem_stats_[column_index].sum_stats_.status ==
        STATUS_NEED_UPDATE) {
      if (column_mem_stats_[column_index].sum_stats_.final_func_exist &&
          !column_mem_stats_[column_index].sum_stats_.final_func_called) {
        auto newval = cbdb::FunctionCall1Coll(
            &column_mem_stats_[column_index].sum_stats_.final_func, InvalidOid,
            column_mem_stats_[column_index].sum_stats_.result);

#ifdef USE_ASSERT_CHECKING
        auto newvalue_vl = (struct varlena *)cbdb::DatumToPointer(newval);
        auto detoast_newval = cbdb::PgDeToastDatum(newvalue_vl);
        Assert(newval == PointerGetDatum(detoast_newval));
#endif  // USE_ASSERT_CHECKING

        if (!column_mem_stats_[column_index].sum_stats_.transtypbyval &&
            cbdb::DatumToPointer(newval) !=
                cbdb::DatumToPointer(
                    column_mem_stats_[column_index].sum_stats_.result)) {
          // The reason why we not use the `Copydatum` is that
          // 1. newval won't be a toast
          // 2. the `newval` alloc in `final_func` which not used the
          // PAX_NEW to alloc, can't use the PAX_DELETE to delete it
          if (column_mem_stats_[column_index].sum_stats_.result)
            cbdb::Pfree(cbdb::DatumToPointer(
                column_mem_stats_[column_index].sum_stats_.result));
          column_mem_stats_[column_index].sum_stats_.result = cbdb::datumCopy(
              newval, column_mem_stats_[column_index].sum_stats_.rettypbyval,
              column_mem_stats_[column_index].sum_stats_.rettyplen);
        } else {
          column_mem_stats_[column_index].sum_stats_.result = newval;
        }
        column_mem_stats_[column_index].sum_stats_.final_func_called = true;
      }

      data_stats->set_sum(
          ToValue(column_mem_stats_[column_index].sum_stats_.result,
                  column_mem_stats_[column_index].sum_stats_.rettyplen,
                  column_mem_stats_[column_index].sum_stats_.rettypbyval));
      column_mem_stats_[column_index].sum_stats_.result = 0;
    }
  }

  // Serialize the bloom filter
  for (auto column_index = 0; column_index < n; column_index++) {
    switch (bf_status_[column_index]) {
      case STATUS_NOT_SUPPORT:
      case STATUS_MISSING_INIT_VAL:
        break;
      case STATUS_NEED_UPDATE: {
        unsigned char *bf;
        uint64 bf_bits;
        std::tie(bf, bf_bits) = bf_stats_[column_index].GetBitSet();
        // TODO(jiaqizho): compress the bloom filter data
        // safe to call the BITS_TO_BYTES
        stats_->SetColumnBloomFilterStats(
            column_index, std::string((char *)bf, BITS_TO_BYTES(bf_bits)));
        break;
      }
    }
  }

  return stats_->GetStatsInfoRef();
}

::pax::stats::ColumnBasicInfo *MicroPartitionStats::GetColumnBasicInfo(
    int column_index) const {
  return stats_->GetColumnBasicInfo(column_index);
}

MicroPartitionStats *MicroPartitionStats::Reset() {
  for (auto &column_status : column_mem_stats_) {
    column_status.sum_stats_.Reset();
    column_status.null_count_stats_.Reset();
    column_status.min_max_stats_.Reset();
  }

  for (size_t i = 0; i < bf_status_.size(); i++) {
    if (bf_status_[i] == STATUS_NEED_UPDATE ||
        bf_status_[i] == STATUS_MISSING_INIT_VAL) {
      bf_stats_[i].Reset();
      bf_status_[i] = STATUS_MISSING_INIT_VAL;
    }
  }

  stats_->Reset();
  return this;
}

void MicroPartitionStats::AddRow(TupleTableSlot *slot) {
  auto n = tuple_desc_->natts;

  Assert(initialized_);
  CBDB_CHECK(column_mem_stats_.size() == static_cast<size_t>(n),
             cbdb::CException::ExType::kExTypeSchemaNotMatch,
             fmt("Current stats initialized [N=%lu], in tuple desc [natts=%d] ",
                 column_mem_stats_.size(), n));
  for (auto i = 0; i < n; i++) {
    if (slot->tts_isnull[i])
      AddNullColumn(i);
    else
      AddNonNullColumn(i, slot->tts_values[i]);
  }
}

void MicroPartitionStats::MergeRawInfo(
    ::pax::stats::MicroPartitionStatisticsInfo *stats_info) {
  auto merge_const_stats = [](std::vector<ColumnMemStats> &origin,
                              ::pax::stats::MicroPartitionStatisticsInfo *info,
                              size_t column_index) {
    if (origin[column_index].null_count_stats_.all_null &&
        !info->columnstats(column_index).allnull()) {
      origin[column_index].null_count_stats_.all_null = false;
    }

    if (!origin[column_index].null_count_stats_.has_null &&
        info->columnstats(column_index).hasnull()) {
      origin[column_index].null_count_stats_.has_null = true;
    }

    // won't be overflow
    origin[column_index].null_count_stats_.not_null_count +=
        info->columnstats(column_index).nonnullrows();
  };

  Datum minimal, maximum, sum_result;
  for (size_t column_index = 0; column_index < column_mem_stats_.size();
       column_index++) {
    auto att = TupleDescAttr(tuple_desc_, column_index);
    auto collation = att->attcollation;
    auto typlen = att->attlen;
    auto typbyval = att->attbyval;

    // always update all_null/has_null/nonnull_count
    merge_const_stats(column_mem_stats_, stats_info, column_index);

    if (column_mem_stats_[column_index].min_max_stats_.status ==
        STATUS_NOT_SUPPORT) {
      // still need update hasnull/allnull

      goto update_sum_stats;
    } else if (column_mem_stats_[column_index].min_max_stats_.status ==
               STATUS_NEED_UPDATE) {
      auto col_basic_stats_merge pg_attribute_unused() =
          stats_info->mutable_columnstats(column_index)->mutable_info();

      auto col_basic_stats pg_attribute_unused() =
          stats_->GetColumnBasicInfo(column_index);

      Assert(col_basic_stats->typid() == col_basic_stats_merge->typid());
      Assert(col_basic_stats->collation() ==
             col_basic_stats_merge->collation());
      Assert(col_basic_stats->collation() == collation);

      minimal =
          FromValue(stats_info->columnstats(column_index).datastats().minimal(),
                    typlen, typbyval, column_index);
      maximum =
          FromValue(stats_info->columnstats(column_index).datastats().maximum(),
                    typlen, typbyval, column_index);

      UpdateMinMaxValue(column_index, minimal, collation, typlen, typbyval);
      UpdateMinMaxValue(column_index, maximum, collation, typlen, typbyval);
    } else if (column_mem_stats_[column_index].min_max_stats_.status ==
               STATUS_MISSING_INIT_VAL) {
      stats_->CopyFrom(
          stats_info,
          column_index);  // do the copy, no need call merge_const_stats

      minimal =
          FromValue(stats_info->columnstats(column_index).datastats().minimal(),
                    typlen, typbyval, column_index);
      maximum =
          FromValue(stats_info->columnstats(column_index).datastats().maximum(),
                    typlen, typbyval, column_index);

      CopyDatum(minimal,
                &column_mem_stats_[column_index].min_max_stats_.min_in_mem,
                typlen, typbyval);
      CopyDatum(maximum,
                &column_mem_stats_[column_index].min_max_stats_.max_in_mem,
                typlen, typbyval);

      column_mem_stats_[column_index].min_max_stats_.status =
          STATUS_NEED_UPDATE;
    } else {
      Assert(false);
    }
  update_sum_stats:
    // begin update sum
    auto sum_stat = &column_mem_stats_[column_index].sum_stats_;
    Assert(sum_stat->status != STATUS_UNINITIALIZED);
    Assert(sum_stat->rettype ==
           stats_info->columnstats(column_index).info().prorettype());
    sum_stat->final_func_called = true;  // no need call final function

    if (sum_stat->status == STATUS_NOT_SUPPORT) {
      // do nothing
      continue;
    } else if (sum_stat->status == STATUS_NEED_UPDATE) {
      sum_result =
          FromValue(stats_info->columnstats(column_index).datastats().sum(),
                    typlen, typbyval, column_index);
      Datum newval = cbdb::FunctionCall2Coll(&sum_stat->add_func, InvalidOid,
                                             sum_stat->result, sum_result);
      if (!sum_stat->rettypbyval && newval != sum_stat->result &&
          sum_stat->result) {
        cbdb::Pfree(cbdb::DatumToPointer(sum_stat->result));
      }
      sum_stat->result =
          cbdb::datumCopy(newval, sum_stat->rettyplen, sum_stat->rettypbyval);
    } else if (sum_stat->status == STATUS_MISSING_INIT_VAL) {
      sum_result =
          FromValue(stats_info->columnstats(column_index).datastats().sum(),
                    typlen, typbyval, column_index);
      sum_stat->result = cbdb::datumCopy(sum_result, sum_stat->rettypbyval,
                                         sum_stat->rettyplen);
      sum_stat->status = STATUS_NEED_UPDATE;
    } else {
      Assert(false);
    }
  }

  for (size_t column_index = 0; column_index < bf_status_.size();
       column_index++) {
    if (!stats_info->mutable_columnstats(column_index)->has_bloomfilterinfo()) {
      continue;
    }

    auto right_bf_basic_info =
        stats_info->mutable_columnstats(column_index)->bloomfilterinfo();

    if (!stats_->HasBloomFilterBasicInfo(column_index)) {
      stats_->GetBloomFilterBasicInfo(column_index)
          ->CopyFrom(right_bf_basic_info);
    } else {
      Assert(stats_->GetBloomFilterBasicInfo(column_index)->bf_hash_funcs() ==
             right_bf_basic_info.bf_hash_funcs());
      Assert(stats_->GetBloomFilterBasicInfo(column_index)->bf_m() ==
             right_bf_basic_info.bf_m());
      Assert(stats_->GetBloomFilterBasicInfo(column_index)->bf_seed() ==
             right_bf_basic_info.bf_seed());
    }

    if ((bf_status_[column_index] == STATUS_MISSING_INIT_VAL ||
         bf_status_[column_index] == STATUS_NEED_UPDATE) &&
        !stats_info->mutable_columnstats(column_index)->has_columnbfstats()) {
      auto right_bf = BloomFilter();
      right_bf.Create(stats_info->mutable_columnstats(column_index)
                          ->columnbfstats()
                          .c_str(),
                      right_bf_basic_info.bf_m(), right_bf_basic_info.bf_seed(),
                      right_bf_basic_info.bf_hash_funcs());
      bf_stats_[column_index].MergeFrom(&right_bf);
    }
  }
}

void MicroPartitionStats::MergeTo(MicroPartitionStats *stats) {
  Assert(column_mem_stats_.size() == stats->column_mem_stats_.size());

  // Used to merge `allnull`/`hasnull`/`nunnullrows`
  // These pb struct will exist whether minmaxopt is set or not(expect drop
  // column).
  auto merge_const_stats = [](std::vector<ColumnMemStats> &left,
                              std::vector<ColumnMemStats> &right,
                              size_t column_index) {
    if (left[column_index].null_count_stats_.all_null &&
        !right[column_index].null_count_stats_.all_null) {
      left[column_index].null_count_stats_.all_null = false;
    }

    if (!left[column_index].null_count_stats_.has_null &&
        right[column_index].null_count_stats_.has_null) {
      left[column_index].null_count_stats_.has_null = true;
    }

    // won't be overflow
    left[column_index].null_count_stats_.not_null_count +=
        right[column_index].null_count_stats_.not_null_count;
  };

  for (size_t column_index = 0; column_index < column_mem_stats_.size();
       column_index++) {
    auto att = TupleDescAttr(tuple_desc_, column_index);
    auto collation = att->attcollation;
    auto typlen = att->attlen;
    auto typbyval = att->attbyval;

    // always update all_null/has_null/nonnull_count
    merge_const_stats(column_mem_stats_, stats->column_mem_stats_,
                      column_index);

    Assert(column_mem_stats_[column_index].min_max_stats_.status !=
               STATUS_UNINITIALIZED &&
           stats->column_mem_stats_[column_index].min_max_stats_.status !=
               STATUS_UNINITIALIZED);
    AssertImply(stats->column_mem_stats_[column_index].min_max_stats_.status ==
                    STATUS_NOT_SUPPORT,
                column_mem_stats_[column_index].min_max_stats_.status ==
                    STATUS_NOT_SUPPORT);

    if (stats->column_mem_stats_[column_index].min_max_stats_.status ==
            STATUS_MISSING_INIT_VAL ||
        stats->column_mem_stats_[column_index].min_max_stats_.status ==
            STATUS_NOT_SUPPORT) {
      goto update_sum_stats;
    }

    // Now `stats->status_` will only be STATUS_NEED_UPDATE
    // `status_` will only be STATUS_NEED_UPDATE or STATUS_MISSING_INIT_VAL
    Assert(stats->column_mem_stats_[column_index].min_max_stats_.status ==
           STATUS_NEED_UPDATE);
    Assert(column_mem_stats_[column_index].min_max_stats_.status !=
           STATUS_NOT_SUPPORT);
    if (column_mem_stats_[column_index].min_max_stats_.status ==
        STATUS_NEED_UPDATE) {
      {  // check the basic info match
        auto col_basic_stats_merge pg_attribute_unused() =
            stats->stats_->GetColumnBasicInfo(column_index);

        auto col_basic_stats pg_attribute_unused() =
            stats_->GetColumnBasicInfo(column_index);

        Assert(col_basic_stats->typid() == col_basic_stats_merge->typid());
        Assert(col_basic_stats->collation() ==
               col_basic_stats_merge->collation());
        Assert(col_basic_stats->collation() == collation);
      }

      UpdateMinMaxValue(
          column_index,
          stats->column_mem_stats_[column_index].min_max_stats_.min_in_mem,
          collation, typlen, typbyval);
      UpdateMinMaxValue(
          column_index,
          stats->column_mem_stats_[column_index].min_max_stats_.max_in_mem,
          collation, typlen, typbyval);
    } else if (column_mem_stats_[column_index].min_max_stats_.status ==
               STATUS_MISSING_INIT_VAL) {
      stats_->CopyFrom(stats->stats_.get(), column_index);
      CopyDatum(
          stats->column_mem_stats_[column_index].min_max_stats_.min_in_mem,
          &column_mem_stats_[column_index].min_max_stats_.min_in_mem, typlen,
          typbyval);
      CopyDatum(
          stats->column_mem_stats_[column_index].min_max_stats_.max_in_mem,
          &column_mem_stats_[column_index].min_max_stats_.max_in_mem, typlen,
          typbyval);

      column_mem_stats_[column_index].min_max_stats_.status =
          STATUS_NEED_UPDATE;
    } else {
      Assert(false);
    }

  update_sum_stats:
    auto left_sum_stat = &column_mem_stats_[column_index].sum_stats_;
    auto right_sum_stat = &stats->column_mem_stats_[column_index].sum_stats_;

    Assert(left_sum_stat->status != STATUS_UNINITIALIZED &&
           right_sum_stat->status != STATUS_UNINITIALIZED);
    AssertImply(left_sum_stat->status == STATUS_NOT_SUPPORT,
                right_sum_stat->status == STATUS_NOT_SUPPORT);

    if (right_sum_stat->status == STATUS_MISSING_INIT_VAL ||
        right_sum_stat->status == STATUS_NOT_SUPPORT) {
      // nothing todo
      continue;
    }

    // Now `right_sum_stat status` will only be STATUS_NEED_UPDATE
    // `left_sum_stat status` will only be STATUS_NEED_UPDATE or
    // STATUS_MISSING_INIT_VAL
    Assert(right_sum_stat->status == STATUS_NEED_UPDATE);
    Assert(left_sum_stat->status != STATUS_NOT_SUPPORT);

    Assert(left_sum_stat->transtyplen == right_sum_stat->transtyplen);
    Assert(left_sum_stat->transtypbyval == right_sum_stat->transtypbyval);
    Assert(left_sum_stat->rettyplen == right_sum_stat->rettyplen);
    Assert(left_sum_stat->rettypbyval == right_sum_stat->rettypbyval);

    if (right_sum_stat->final_func_exist &&
        !right_sum_stat->final_func_called) {
      auto newval = cbdb::FunctionCall1Coll(&right_sum_stat->final_func,
                                            InvalidOid, right_sum_stat->result);

      if (!left_sum_stat->transtypbyval && newval != right_sum_stat->result) {
        if (right_sum_stat->result)
          cbdb::Pfree(cbdb::DatumToPointer(right_sum_stat->result));
        right_sum_stat->result = cbdb::datumCopy(
            newval, right_sum_stat->rettypbyval, right_sum_stat->rettyplen);
      } else {
        right_sum_stat->result = newval;
      }

      right_sum_stat->final_func_called = true;
    }

    left_sum_stat->final_func_exist = right_sum_stat->final_func_exist;
    left_sum_stat->final_func_called = right_sum_stat->final_func_called;

    if (left_sum_stat->status == STATUS_NEED_UPDATE) {
      Datum newval = cbdb::FunctionCall2Coll(&left_sum_stat->add_func,
                                             InvalidOid, left_sum_stat->result,
                                             right_sum_stat->result);
      if (!left_sum_stat->rettypbyval && newval != left_sum_stat->result &&
          left_sum_stat->result) {
        cbdb::Pfree(cbdb::DatumToPointer(left_sum_stat->result));
      }
      left_sum_stat->result = cbdb::datumCopy(newval, left_sum_stat->rettyplen,
                                              left_sum_stat->rettypbyval);
    } else if (left_sum_stat->status == STATUS_MISSING_INIT_VAL) {
      left_sum_stat->result =
          cbdb::datumCopy(right_sum_stat->result, left_sum_stat->rettypbyval,
                          left_sum_stat->rettyplen);
      left_sum_stat->status = STATUS_NEED_UPDATE;
    } else {
      Assert(false);
    }
  }

  // merge the bloom filter
  Assert(bf_status_.size() == stats->bf_status_.size());
  for (size_t column_index = 0; column_index < bf_status_.size();
       column_index++) {
    auto left_bf_status = bf_status_[column_index];
    auto right_bf_status = stats->bf_status_[column_index];

    if ((left_bf_status == STATUS_MISSING_INIT_VAL ||
         left_bf_status == STATUS_NEED_UPDATE) &&
        right_bf_status == STATUS_NEED_UPDATE) {
      bf_stats_[column_index].MergeFrom(&stats->bf_stats_[column_index]);
      bf_status_[column_index] = STATUS_NEED_UPDATE;
    }
  }
}

inline void MicroPartitionStats::AddNullColumn(int column_index) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(column_mem_stats_.size()));
  column_mem_stats_[column_index].null_count_stats_.has_null = true;
}

inline void MicroPartitionStats::AddNonNullColumn(int column_index,
                                                  Datum value) {
  Assert(column_index >= 0);
  Assert(column_index < static_cast<int>(column_mem_stats_.size()));

  auto att = TupleDescAttr(tuple_desc_, column_index);
  Oid &collation = att->attcollation;
  int16 &typlen = att->attlen;
  bool &typbyval = att->attbyval;

  column_mem_stats_[column_index].null_count_stats_.all_null = false;
  ++column_mem_stats_[column_index].null_count_stats_.not_null_count;

  // update min/max
  switch (column_mem_stats_[column_index].min_max_stats_.status) {
    case STATUS_NOT_SUPPORT:
      break;
    case STATUS_NEED_UPDATE: {
#ifdef USE_ASSERT_CHECKING
      auto info = stats_->GetColumnBasicInfo(column_index);
#endif
      Assert(info->has_typid());
      Assert(info->typid() == att->atttypid);
      Assert(info->collation() == collation);

      UpdateMinMaxValue(column_index, value, collation, typlen, typbyval);
      break;
    }
    case STATUS_MISSING_INIT_VAL: {
#ifdef USE_ASSERT_CHECKING
      auto info = stats_->GetColumnBasicInfo(column_index);
#endif
      AssertImply(info->has_typid(), info->typid() == att->atttypid);
      AssertImply(info->has_collation(), info->collation() == collation);

      CopyDatum(value,
                &column_mem_stats_[column_index].min_max_stats_.min_in_mem,
                typlen, typbyval);
      CopyDatum(value,
                &column_mem_stats_[column_index].min_max_stats_.max_in_mem,
                typlen, typbyval);

      column_mem_stats_[column_index].min_max_stats_.status =
          STATUS_NEED_UPDATE;
      break;
    }
    default:
      Assert(false);
  }

  // update sum
  switch (column_mem_stats_[column_index].sum_stats_.status) {
    case STATUS_NOT_SUPPORT:
      break;
    case STATUS_MISSING_INIT_VAL: {
      if (column_mem_stats_[column_index].sum_stats_.trans_func.fn_strict) {
        /*
         * transValue has not been initialized. This is the first non-NULL
         * input value. We use it as the initial value for transValue. (We
         * already checked that the agg's input type is binary-compatible
         * with its transtype, so straight copy here is OK.)
         *
         * We must copy the datum into aggcontext if it is pass-by-ref. We
         * do not need to pfree the old transValue, since it's NULL.
         */
        column_mem_stats_[column_index].sum_stats_.result = cbdb::datumCopy(
            value, column_mem_stats_[column_index].sum_stats_.transtypbyval,
            column_mem_stats_[column_index].sum_stats_.transtyplen);
        column_mem_stats_[column_index].sum_stats_.status = STATUS_NEED_UPDATE;
        break;
      }

      column_mem_stats_[column_index].sum_stats_.status = STATUS_NEED_UPDATE;
      [[fallthrough]];
    }
    case STATUS_NEED_UPDATE: {
      AssertImply(
          column_mem_stats_[column_index].sum_stats_.final_func_exist,
          !column_mem_stats_[column_index].sum_stats_.final_func_called);
      auto newval = cbdb::SumFuncCall(
          &column_mem_stats_[column_index].sum_stats_.trans_func, agg_state_,
          column_mem_stats_[column_index].sum_stats_.result, value);

      if (!column_mem_stats_[column_index].sum_stats_.transtypbyval &&
          cbdb::DatumToPointer(newval) !=
              cbdb::DatumToPointer(
                  column_mem_stats_[column_index].sum_stats_.result)) {
        if (column_mem_stats_[column_index].sum_stats_.result)
          cbdb::Pfree(cbdb::DatumToPointer(
              column_mem_stats_[column_index].sum_stats_.result));
        column_mem_stats_[column_index].sum_stats_.result = newval;
      } else {
        column_mem_stats_[column_index].sum_stats_.result = newval;
      }

      break;
    }
    default:
      Assert(false);
  }

  // update bloomfilter
  switch (bf_status_[column_index]) {
    case STATUS_NOT_SUPPORT:
      break;
    case STATUS_MISSING_INIT_VAL:
    case STATUS_NEED_UPDATE: {
      if (typbyval) {
        switch (typlen) {
          case 1: {
            auto val_no_ptr = cbdb::DatumToInt8(value);
            bf_stats_[column_index].Add((unsigned char *)&val_no_ptr, typlen);
            break;
          }
          case 2: {
            auto val_no_ptr = cbdb::DatumToInt16(value);
            bf_stats_[column_index].Add((unsigned char *)&val_no_ptr, typlen);
            break;
          }
          case 4: {
            auto val_no_ptr = cbdb::DatumToInt32(value);
            bf_stats_[column_index].Add((unsigned char *)&val_no_ptr, typlen);
            break;
          }
          case 8: {
            auto val_no_ptr = cbdb::DatumToInt64(value);
            bf_stats_[column_index].Add((unsigned char *)&val_no_ptr, typlen);
            break;
          }
          default:
            Assert(false);
        }
      } else if (typlen == -1) {
        auto val_ptr =
            reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(value));
        char *val_data = VARDATA_ANY(val_ptr);
        auto val_len = VARSIZE_ANY_EXHDR(val_ptr);
        // ignore trailing spaces so it can filter text/varchar correctly
        if (att->atttypid == BPCHAROID)
          val_len = bpchartruelen(val_data, val_len);

        // safe to direct call, cause no toast here
        bf_stats_[column_index].Add((unsigned char *)val_data, val_len);
      } else {
        Assert(typlen > 0);
        auto val_ptr = (unsigned char *)cbdb::DatumToPointer(value);
        Size real_size;

        // Pass by reference, but not varlena, so not toasted
        real_size = datumGetSize(value, typbyval, typlen);
        bf_stats_[column_index].Add(val_ptr, real_size);
      }
      bf_status_[column_index] = STATUS_NEED_UPDATE;
      break;
    }
    default:
      Assert(false);
  }
}

void MicroPartitionStats::UpdateMinMaxValue(int column_index, Datum datum,
                                            Oid collation, int typlen,
                                            bool typbyval) {
  Datum min_datum, max_datum;
  bool umin = false, umax = false;

  Assert(initialized_);
  Assert(column_index >= 0 &&
         static_cast<size_t>(column_index) < column_mem_stats_.size());
  Assert(column_mem_stats_[column_index].min_max_stats_.status ==
         STATUS_NEED_UPDATE);

  min_datum = column_mem_stats_[column_index].min_max_stats_.min_in_mem;
  max_datum = column_mem_stats_[column_index].min_max_stats_.max_in_mem;

  // If do have local oper here, then direct call it
  // But if local oper is null, it must be fallback to pg
  auto &lfunc = local_funcs_[column_index];
  if (lfunc.first && CollateIsSupport(collation)) {
    Assert(lfunc.second);

    umin = lfunc.first(&datum, &min_datum, collation);
    umax = lfunc.second(&datum, &max_datum, collation);
  } else if (allow_fallback_to_pg_) {  // may not support collation,
    auto &finfos = finfos_[column_index];
    umin = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.first, collation, datum, min_datum));
    umax = DatumGetBool(
        cbdb::FunctionCall2Coll(&finfos.second, collation, datum, max_datum));
  } else {
    // unreachable
    Assert(false);
  }

  if (umin)
    CopyDatum(datum, &column_mem_stats_[column_index].min_max_stats_.min_in_mem,
              typlen, typbyval);
  if (umax)
    CopyDatum(datum, &column_mem_stats_[column_index].min_max_stats_.max_in_mem,
              typlen, typbyval);
}

void MicroPartitionStats::CopyDatum(Datum src, Datum *dst, int typlen,
                                    bool typbyval) {
  if (typbyval) {
    Assert(typlen == 1 || typlen == 2 || typlen == 4 || typlen == 8);
    *dst = src;
  } else if (typlen == -1) {
    struct varlena *val;
    int len;

    val = (struct varlena *)cbdb::PointerAndLenFromDatum(src, &len);
    Assert(val && len != -1);

    auto alloc_new_datum = [this](struct varlena *vl, int vl_len, Datum *dest) {
      ByteBuffer buffer(vl_len, vl_len);
      auto p = buffer.Addr();
      memcpy(p, vl, vl_len);
      *dest = PointerGetDatum(p);
      buffer_holders_[*dest] = std::move(buffer);
    };

    // check the source datum
    if (*dst == 0) {
      alloc_new_datum(val, len, dst);
    } else {
      int dst_len;
      char *result = (char *)cbdb::PointerAndLenFromDatum(*dst, &dst_len);
      if (dst_len > len) {
        memcpy(result, (char *)val, len);
      } else {
        buffer_holders_.erase(*dst);
        alloc_new_datum(val, len, dst);
      }
    }
  } else {
    /* Pass by reference, but not varlena, so not toasted */
    Size real_size;
    char *resultptr;

    real_size = datumGetSize(src, typbyval, typlen);
    resultptr = (char *)PAX_NEW_ARRAY<char>(real_size);
    memcpy(resultptr, DatumGetPointer(src), real_size);

    if (*dst == 0) {
      *dst = PointerGetDatum(resultptr);
    } else {
      auto old_datum = (char *)cbdb::DatumToPointer(*dst);
      PAX_DELETE(old_datum);
      *dst = PointerGetDatum(resultptr);
    }
  }
}

void MicroPartitionStats::Initialize(const std::vector<int> &minmax_columns,
                                     const std::vector<int> &bf_columns) {
  auto natts = tuple_desc_->natts;

  std::vector<bool> mm_mask;
  std::vector<bool> bf_mask;

  Assert(natts == static_cast<int>(column_mem_stats_.size()));
  Assert(column_mem_stats_.size() == finfos_.size());
  Assert(column_mem_stats_.size() == required_stats_.size());

  Assert(static_cast<int>(minmax_columns.size()) <= natts);
  Assert(static_cast<int>(bf_columns.size()) <= natts);

  if (initialized_) {
    return;
  }

  mm_mask.resize(natts, false);
  bf_mask.resize(natts, false);

  for (size_t j = 0; j < minmax_columns.size(); j++) {
    Assert(minmax_columns[j] < natts);
    mm_mask[minmax_columns[j]] = true;
  }

  for (size_t j = 0; j < bf_columns.size(); j++) {
    Assert(bf_columns[j] < natts);
    bf_mask[bf_columns[j]] = true;
  }

  for (int i = 0; i < natts; i++) {
    auto att = TupleDescAttr(tuple_desc_, i);
    auto info = stats_->GetColumnBasicInfo(i);

    if (att->attisdropped || !mm_mask[i]) {
      column_mem_stats_[i].min_max_stats_.status = STATUS_NOT_SUPPORT;
      column_mem_stats_[i].sum_stats_.status = STATUS_NOT_SUPPORT;
      continue;
    }

    // init_minmax_status: (only use to mark)
    if (GetStrategyProcinfo(att->atttypid, att->atttypid, local_funcs_[i])) {
      column_mem_stats_[i].min_max_stats_.status = STATUS_MISSING_INIT_VAL;

      info->set_typid(att->atttypid);
      info->set_collation(att->attcollation);
      goto init_sum_status;
    }

    if (!allow_fallback_to_pg_ ||
        !GetStrategyProcinfo(att->atttypid, att->atttypid, finfos_[i])) {
      column_mem_stats_[i].min_max_stats_.status = STATUS_NOT_SUPPORT;
      goto init_sum_status;
    }

    info->set_typid(att->atttypid);
    info->set_collation(att->attcollation);

    column_mem_stats_[i].min_max_stats_.status = STATUS_MISSING_INIT_VAL;
  init_sum_status:
    if (cbdb::SumAGGGetProcinfo(
            att->atttypid, &column_mem_stats_[i].sum_stats_.rettype,
            &column_mem_stats_[i].sum_stats_.transtype,
            &column_mem_stats_[i].sum_stats_.trans_func,
            &column_mem_stats_[i].sum_stats_.final_func,
            &column_mem_stats_[i].sum_stats_.final_func_exist,
            &column_mem_stats_[i].sum_stats_.add_func)) {
      column_mem_stats_[i].sum_stats_.status = STATUS_MISSING_INIT_VAL;
      column_mem_stats_[i].sum_stats_.argtype = att->atttypid;
      column_mem_stats_[i].sum_stats_.transtyplen =
          cbdb::GetTyplen(column_mem_stats_[i].sum_stats_.transtype);
      column_mem_stats_[i].sum_stats_.transtypbyval =
          cbdb::GetTypbyval(column_mem_stats_[i].sum_stats_.transtype);
      column_mem_stats_[i].sum_stats_.rettyplen =
          cbdb::GetTyplen(column_mem_stats_[i].sum_stats_.rettype);
      column_mem_stats_[i].sum_stats_.rettypbyval =
          cbdb::GetTypbyval(column_mem_stats_[i].sum_stats_.rettype);
      info->set_prorettype(column_mem_stats_[i].sum_stats_.rettype);
    } else {
      column_mem_stats_[i].sum_stats_.status = STATUS_NOT_SUPPORT;
    }

    Assert(column_mem_stats_[i].min_max_stats_.status == STATUS_NOT_SUPPORT ||
           column_mem_stats_[i].min_max_stats_.status ==
               STATUS_MISSING_INIT_VAL);
    Assert(column_mem_stats_[i].sum_stats_.status == STATUS_NOT_SUPPORT ||
           column_mem_stats_[i].sum_stats_.status == STATUS_MISSING_INIT_VAL);

    if (column_mem_stats_[i].min_max_stats_.status == STATUS_MISSING_INIT_VAL ||
        column_mem_stats_[i].sum_stats_.status == STATUS_MISSING_INIT_VAL) {
      required_stats_[i] = true;
    }
  }

  // init the bloom filter stats
  for (int i = 0; i < natts; i++) {
    auto att = TupleDescAttr(tuple_desc_, i);

    if (att->attisdropped || !bf_mask[i]) {
      bf_status_[i] = STATUS_NOT_SUPPORT;
      continue;
    }

    bf_stats_[i].CreateFixed();
    bf_status_[i] = STATUS_MISSING_INIT_VAL;

    // still need set the basic info
    auto info = stats_->GetColumnBasicInfo(i);
    info->set_typid(att->atttypid);
    info->set_collation(att->attcollation);

    auto bf_info = stats_->GetBloomFilterBasicInfo(i);
    bf_info->set_bf_hash_funcs(bf_stats_[i].GetKHashFuncs());
    bf_info->set_bf_seed(bf_stats_[i].GetSeed());
    bf_info->set_bf_m(bf_stats_[i].GetM());

    Assert(bf_status_[i] == STATUS_NOT_SUPPORT ||
           bf_status_[i] == STATUS_MISSING_INIT_VAL);

    if (bf_status_[i] == STATUS_MISSING_INIT_VAL) {
      required_stats_[i] = true;
    }
  }

  agg_state_ = makeNode(AggState);
  agg_state_->curaggcontext = &expr_context_;
  expr_context_.ecxt_per_tuple_memory = CurrentMemoryContext;

  initialized_ = true;
}

Datum MicroPartitionStats::FromValue(const std::string &s, int typlen,
                                     bool typbyval, int column_index) {
  const char *p = s.data();
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = *reinterpret_cast<const int8 *>(p);
        return cbdb::Int8ToDatum(i);
      }
      case 2: {
        int16 i = *reinterpret_cast<const int16 *>(p);
        return cbdb::Int16ToDatum(i);
      }
      case 4: {
        int32 i = *reinterpret_cast<const int32 *>(p);
        return cbdb::Int32ToDatum(i);
      }
      case 8: {
        int64 i = *reinterpret_cast<const int64 *>(p);
        return cbdb::Int64ToDatum(i);
      }
      default:
        CBDB_RAISE(cbdb::CException::kExTypeLogicError,
                   fmt("Fail to parse the MIN/MAX datum in pb [typbyval=%d, "
                       "typlen=%d, column_index=%d]",
                       typbyval, typlen, column_index));
        break;
    }
    return 0;
  }

  Assert(typlen == -1 || typlen > 0);
  return PointerGetDatum(p);
}

std::string MicroPartitionStats::ToValue(Datum datum, int typlen,
                                         bool typbyval) {
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = cbdb::DatumToInt8(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 2: {
        int16 i = cbdb::DatumToInt16(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 4: {
        int32 i = cbdb::DatumToInt32(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      case 8: {
        int64 i = cbdb::DatumToInt64(datum);
        return std::string(reinterpret_cast<char *>(&i), sizeof(i));
      }
      default:
        Assert(!"unexpected typbyval, len not in 1,2,4,8");
        break;
    }
    CBDB_RAISE(cbdb::CException::kExTypeLogicError,
               fmt("Invalid typlen %d", typlen));
  }

  if (typlen == -1) {
    void *v;
    int len;

    v = cbdb::PointerAndLenFromDatum(datum, &len);
    Assert(v && len != -1);
    return std::string(reinterpret_cast<char *>(v), len);
  }
  // byref but fixed size
  Assert(typlen > 0);
  Assert(datum);
  return std::string(reinterpret_cast<char *>(cbdb::DatumToPointer(datum)),
                     typlen);
}

MicroPartitionStatsProvider::MicroPartitionStatsProvider(
    const ::pax::stats::MicroPartitionStatisticsInfo &stats)
    : stats_(stats) {}

int MicroPartitionStatsProvider::ColumnSize() const {
  return stats_.columnstats_size();
}

bool MicroPartitionStatsProvider::AllNull(int column_index) const {
  return stats_.columnstats(column_index).allnull();
}

bool MicroPartitionStatsProvider::HasNull(int column_index) const {
  return stats_.columnstats(column_index).hasnull();
}

uint64 MicroPartitionStatsProvider::NonNullRows(int column_index) const {
  return stats_.columnstats(column_index).nonnullrows();
}

const ::pax::stats::ColumnBasicInfo &MicroPartitionStatsProvider::ColumnInfo(
    int column_index) const {
  return stats_.columnstats(column_index).info();
}

const ::pax::stats::ColumnDataStats &MicroPartitionStatsProvider::DataStats(
    int column_index) const {
  return stats_.columnstats(column_index).datastats();
}

bool MicroPartitionStatsProvider::HasBloomFilter(int column_index) const {
  return stats_.columnstats(column_index).has_columnbfstats();
}

const ::pax::stats::BloomFilterBasicInfo &
MicroPartitionStatsProvider::BloomFilterBasicInfo(int column_index) const {
  return stats_.columnstats(column_index).bloomfilterinfo();
}

std::string MicroPartitionStatsProvider::GetBloomFilter(
    int column_index) const {
  return stats_.columnstats(column_index).columnbfstats();
}

}  // namespace pax

namespace paxc {

static inline const char *BoolToString(bool b) { return b ? "true" : "false"; }

Datum FromValue(const char *p, int typlen, bool typbyval, int column_index) {
  if (typbyval) {
    Assert(typlen > 0);
    switch (typlen) {
      case 1: {
        int8 i = *reinterpret_cast<const int8 *>(p);
        return Int8GetDatum(i);
      }
      case 2: {
        int16 i = *reinterpret_cast<const int16 *>(p);
        return Int16GetDatum(i);
      }
      case 4: {
        int32 i = *reinterpret_cast<const int32 *>(p);
        return Int32GetDatum(i);
      }
      case 8: {
        int64 i = *reinterpret_cast<const int64 *>(p);
        return Int64GetDatum(i);
      }
      default:
        elog(ERROR,
             "Fail to parse the MIN/MAX datum in pb [typbyval=%d, typlen=%d, "
             "column_index=%d]",
             typbyval, typlen, column_index);
    }
    return 0;
  }

  Assert(typlen == -1 || typlen > 0);
  return PointerGetDatum(p);
}

static char *TypeValueToCString(Oid typid, Oid collation,
                                const std::string &value) {
  FmgrInfo finfo;
  HeapTuple tuple;
  Form_pg_type form;
  Datum datum;

  tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for type %u", typid);

  form = (Form_pg_type)GETSTRUCT(tuple);
  Assert(OidIsValid(form->typoutput));

  datum = FromValue(value.c_str(), form->typlen, form->typbyval, -1);
  fmgr_info_cxt(form->typoutput, &finfo, CurrentMemoryContext);
  datum = FunctionCall1Coll(&finfo, collation, datum);
  ReleaseSysCache(tuple);

  return DatumGetCString(datum);
}

void MicroPartitionStatsToString(
    pax::stats::MicroPartitionStatisticsInfo *stats, StringInfoData *str) {
  initStringInfo(str);
  for (int i = 0, n = stats->columnstats_size(); i < n; i++) {
    const auto &column = stats->columnstats(i);
    const auto &info = column.info();
    const auto &data_stats = column.datastats();

    // header
    if (i > 0) {
      appendStringInfoString(str, ",[");
    } else {
      appendStringInfoChar(str, '[');
    }

    // hasnull/allnull information
    appendStringInfo(str, "(%s,%s),", BOOL_TOSTRING(column.allnull()),
                     BOOL_TOSTRING(column.hasnull()));

    // count(column) information
    appendStringInfo(str, "(%ld),", column.nonnullrows());

    // min/max information
    if (!column.has_datastats()) {
      appendStringInfoString(str, "None,None");
      goto tail;
    }

    Assert(data_stats.has_minimal() == data_stats.has_maximum());
    if (!data_stats.has_minimal()) {
      appendStringInfoString(str, "None,");
      goto sum_info;
    }

    appendStringInfo(str, "(%s,%s),",
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.minimal()),
                     TypeValueToCString(info.typid(), info.collation(),
                                        data_stats.maximum()));

  sum_info:
    // sum(column) information
    if (!data_stats.has_sum()) {
      appendStringInfoString(str, "None");
      goto tail;
    }

    appendStringInfo(
        str, "(%s)",
        TypeValueToCString(info.prorettype(), InvalidOid, data_stats.sum()));

  tail:
    // tail
    appendStringInfoChar(str, ']');
  }
}

}  // namespace paxc

// define stat type for custom output
extern "C" {
extern Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS);
extern Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(MicroPartitionStatsInput);
PG_FUNCTION_INFO_V1(MicroPartitionStatsOutput);
}

Datum MicroPartitionStatsInput(PG_FUNCTION_ARGS) {
  ereport(ERROR, (errmsg("unsupport MicroPartitionStatsInput")));
  (void)fcinfo;
  PG_RETURN_POINTER(NULL);
}

Datum MicroPartitionStatsOutput(PG_FUNCTION_ARGS) {
  struct varlena *v = PG_GETARG_VARLENA_PP(0);
  pax::stats::MicroPartitionStatisticsInfo stats;
  StringInfoData str;

  bool ok = stats.ParseFromArray(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
  if (!ok) ereport(ERROR, (errmsg("micropartition stats is corrupt")));

  paxc::MicroPartitionStatsToString(&stats, &str);
  PG_RETURN_CSTRING(str.data);
}
