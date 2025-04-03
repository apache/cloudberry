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
 * pax_scanner.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_scanner.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <unordered_set>

#include "comm/pax_memory.h"
#include "storage/filter/pax_filter.h"
#include "storage/pax.h"
#ifdef VEC_BUILD
#include "storage/vec/pax_vec_adapter.h"
#endif

namespace paxc {
bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool *all_dead);
}

namespace pax {
class PaxIndexScanDesc final {
 public:
  explicit PaxIndexScanDesc(Relation rel);
  ~PaxIndexScanDesc();
  bool FetchTuple(ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                  bool *call_again, bool *all_dead);

  // release internal reader
  void Release();
  inline IndexFetchTableData *ToBase() { return &base_; }
  inline Relation GetRelation() { return base_.rel; }
  static inline PaxIndexScanDesc *FromBase(IndexFetchTableData *base) {
    return reinterpret_cast<PaxIndexScanDesc *>(base);
  }

 private:
  bool OpenMicroPartition(BlockNumber block, Snapshot snapshot);

  IndexFetchTableData base_;
  BlockNumber current_block_ = InvalidBlockNumber;
  std::unique_ptr<MicroPartitionReader> reader_;
  std::string rel_path_;
};

class PaxScanDesc {
 public:
  PaxScanDesc() = default;
  TableScanDesc BeginScan(Relation relation, Snapshot snapshot, int nkeys,
                          struct ScanKeyData *key, ParallelTableScanDesc pscan,
                          uint32 flags, std::shared_ptr<PaxFilter> &&pax_filter,
                          bool build_bitmap);

  TableScanDesc BeginScanExtractColumns(Relation rel, Snapshot snapshot,
                                        int nkeys, struct ScanKeyData *key,
                                        ParallelTableScanDesc parallel_scan,
                                        struct PlanState *ps, uint32 flags);

  void EndScan();
  void ReScan(ScanKey key, bool set_params, bool allow_strat, bool allow_sync,
              bool allow_pagemode);

  bool GetNextSlot(TupleTableSlot *slot);

  bool ScanAnalyzeNextBlock(BlockNumber blockno,
                            BufferAccessStrategy bstrategy);
  bool ScanAnalyzeNextTuple(TransactionId oldest_xmin, double *liverows,
                            double *deadrows, TupleTableSlot *slot);

  bool ScanSampleNextBlock(SampleScanState *scanstate);

  bool ScanSampleNextTuple(SampleScanState *scanstate, TupleTableSlot *slot);

  bool BitmapNextBlock(struct TBMIterateResult *tbmres);
  bool BitmapNextTuple(struct TBMIterateResult *tbmres, TupleTableSlot *slot);

  ~PaxScanDesc();

  static inline PaxScanDesc *ToDesc(TableScanDesc scan) {
    auto desc = reinterpret_cast<PaxScanDesc *>(scan);
    return desc;
  }

  inline Relation GetRelation() { return rs_base_.rs_rd; }

 private:
  TableScanDescData rs_base_{};

  std::unique_ptr<TableReader> reader_;

  std::shared_ptr<DataBuffer<char>> reused_buffer_;

  MemoryContext memory_context_ = nullptr;

  // Only used by `scan analyze` and `scan sample`
  uint64 next_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 prev_target_tuple_id_ = 0;
  // Only used by `scan analyze`
  uint64 target_tuple_id_ = 0;
  // Only used by `scan sample`
  uint64 fetch_tuple_id_ = 0;
  uint64 total_tuples_ = 0;

  // filter used to do column projection
  std::shared_ptr<PaxFilter> filter_ = nullptr;
#ifdef VEC_BUILD
  std::unique_ptr<VecAdapter> vec_adapter_;
#endif

  // used only by bitmap index scan
  std::unique_ptr<PaxIndexScanDesc> index_desc_;
  int cindex_ = 0;
};  // class PaxScanDesc

}  // namespace pax
