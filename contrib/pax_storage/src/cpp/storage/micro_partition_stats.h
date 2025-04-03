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
 * micro_partition_stats.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_stats.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "comm/byte_buffer.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "storage/filter/pax_filter.h"
#include "storage/oper/pax_stats.h"

namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
class ColumnBasicInfo;
class ColumnDataStats;
}  // namespace stats
class MicroPartitionStatsData;
class BloomFilter;
struct ColumnMemStats;

class MicroPartitionStats final {
 public:
  MicroPartitionStats(TupleDesc desc, bool allow_fallback_to_pg = false);
  ~MicroPartitionStats();

  void Initialize(const std::vector<int> &minmax_columns,
                  const std::vector<int> &bf_columns);
  void AddRow(TupleTableSlot *slot);
  MicroPartitionStats *Reset();
  ::pax::stats::MicroPartitionStatisticsInfo *Serialize();

  inline const std::vector<bool> &GetRequiredStatsColsMask() {
    Assert(initialized_);
    return required_stats_;
  }

  void MergeRawInfo(::pax::stats::MicroPartitionStatisticsInfo *stats_info);
  void MergeTo(MicroPartitionStats *stats);
  ::pax::stats::ColumnBasicInfo *GetColumnBasicInfo(int column_index) const;

  // used to encode/decode datum
  static std::string ToValue(Datum datum, int typlen, bool typbyval);
  static Datum FromValue(const std::string &s, int typlen, bool typbyval,
                         int column_index);

  static bool MicroPartitionStatisticsInfoCombine(
      stats::MicroPartitionStatisticsInfo *left,
      stats::MicroPartitionStatisticsInfo *right, TupleDesc desc,
      bool allow_fallback_to_pg = false);

 private:
  void AddNullColumn(int column_index);
  void AddNonNullColumn(int column_index, Datum value);
  void UpdateMinMaxValue(int column_index, Datum datum, Oid collation,
                         int typlen, bool typbyval);
  void CopyDatum(Datum src, Datum *dst, int typlen, bool typbyval);

 private:
  TupleDesc tuple_desc_;
  // stats_: only references the info object by pointer
  std::unique_ptr<MicroPartitionStatsData> stats_;
  std::unordered_map<Datum, ByteBuffer> buffer_holders_;

  AggState *agg_state_;
  ExprContext expr_context_;

  // the stats to desc bloom filter
  std::vector<BloomFilter> bf_stats_;
  std::vector<char> bf_status_;

  // The mask of columns for which statistics are requested
  std::vector<bool> required_stats_;

  // less: pair[0], greater: pair[1]
  std::vector<std::pair<FmgrInfo, FmgrInfo>> finfos_;
  std::vector<std::pair<OperMinMaxFunc, OperMinMaxFunc>> local_funcs_;
  bool allow_fallback_to_pg_ = false;  // only effect min/max

  std::vector<ColumnMemStats> column_mem_stats_;

  bool initialized_ = false;
};

class MicroPartitionStatsProvider final : public ColumnStatsProvider {
 public:
  explicit MicroPartitionStatsProvider(
      const ::pax::stats::MicroPartitionStatisticsInfo &stats);
  int ColumnSize() const override;
  bool AllNull(int column_index) const override;
  bool HasNull(int column_index) const override;
  uint64 NonNullRows(int column_index) const override;
  const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const override;
  const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const override;
  bool HasBloomFilter(int column_index) const override;
  const ::pax::stats::BloomFilterBasicInfo &BloomFilterBasicInfo(
      int column_index) const override;
  std::string GetBloomFilter(int column_index) const override;

 private:
  const ::pax::stats::MicroPartitionStatisticsInfo &stats_;
};

}  // namespace pax

namespace paxc {
void MicroPartitionStatsToString(
    pax::stats::MicroPartitionStatisticsInfo *stats, StringInfoData *str);
}
