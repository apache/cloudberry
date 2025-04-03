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
 * pax_scanner.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_scanner.cc
 *
 *-------------------------------------------------------------------------
 */

#include "access/pax_scanner.h"

#include "access/pax_access_handle.h"
#include "access/pax_dml_state.h"
#include "access/pax_visimap.h"
#include "catalog/pax_catalog.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "comm/pax_resource.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_stats.h"
#include "storage/orc/porc.h"
#include "storage/pax.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"
#include "storage/filter/pax_sparse_filter.h"
#ifdef VEC_BUILD
#include "utils/am_vec.h"
#endif


namespace pax {

// select count(*) from p1; or select count(1) from p1
// For the above query, any column could be returned,
// to minimize the cost, we choose the column with smallest
// type length, even if the column is dropped.
static inline int ChooseCheapColumn(TupleDesc desc) {
  int i;
  int natts = desc->natts;

  // dropped index
  int drop_i = -1;

  // fixed-length index and length
  int fix_i = -1;
  int fix_n = -1;

  // byref index and length;
  int var_i = -1;
  int var_n = -1;

  for (i = 0; i < natts; i++) {
    auto att = TupleDescAttr(desc, i);
    auto attlen = att->attlen;
    if (attlen < 0) continue;

    if (att->attisdropped) {
      if (drop_i < 0) drop_i = i;
      continue;
    }

    if (att->attbyval) {
      Assert((fix_i >= 0) == (fix_n > 0));

      if (fix_i < 0 || fix_n > attlen) {
        fix_i = i;
        fix_n = attlen;
      }
    } else if (var_i < 0 || var_n > attlen) {
      Assert((var_i >= 0) == (var_n > 0));
      var_i = i;
      var_n = attlen;
    }
  }

  // return the smallest fixed-length column
  if (fix_i >= 0) return fix_i;
  if (var_i >= 0) return var_i;
  if (drop_i >= 0) return drop_i;

  return 0;
}

static inline bool CheckExists(Relation rel, ItemPointer tid, Snapshot snapshot,
                               bool *all_dead) {
  CBDB_WRAP_START;
  { return paxc::IndexUniqueCheck(rel, tid, snapshot, all_dead); }
  CBDB_WRAP_END;
}

PaxIndexScanDesc::PaxIndexScanDesc(Relation rel) : base_{.rel = rel} {
  Assert(rel);
  Assert(&base_ == reinterpret_cast<IndexFetchTableData *>(this));
  rel_path_ = cbdb::BuildPaxDirectoryPath(
      rel->rd_node, rel->rd_backend);
}

PaxIndexScanDesc::~PaxIndexScanDesc() {}

bool PaxIndexScanDesc::FetchTuple(ItemPointer tid, Snapshot snapshot,
                                  TupleTableSlot *slot, bool *call_again,
                                  bool *all_dead) {
  BlockNumber block = pax::GetBlockNumber(*tid);
  if (block != current_block_ || !reader_) {
    if (!OpenMicroPartition(block, snapshot)) return false;
  }

  Assert(current_block_ == block && reader_);
  if (call_again) *call_again = false;
  if (all_dead) *all_dead = false;

  try {
    ExecClearTuple(slot);
    if (CheckExists(GetRelation(), tid, snapshot, all_dead) &&
        reader_->GetTuple(slot, pax::GetTupleOffset(*tid))) {
      SetBlockNumber(&slot->tts_tid, block);
      ExecStoreVirtualTuple(slot);

      return true;
    }
  } catch (cbdb::CException &e) {
    e.AppendDetailMessage(
        fmt("\n FetchTuple [tid=%s]", ItemPointerToString(tid)));
    CBDB_RERAISE(e);
  }

  return false;
}

bool PaxIndexScanDesc::OpenMicroPartition(BlockNumber block,
                                          Snapshot snapshot) {
  bool ok;

  Assert(block != current_block_);

  ok = cbdb::IsMicroPartitionVisible(base_.rel, block, snapshot);
  if (ok) {
    MicroPartitionReader::ReaderOptions options;

    auto block_name = std::to_string(block);
    auto file_name = cbdb::BuildPaxFilePath(rel_path_, block_name);
    auto file = Singleton<LocalFileSystem>::GetInstance()->Open(file_name,
                                                                fs::kReadMode);
    auto reader = std::make_unique<OrcReader>(std::move(file));
    reader->Open(options);
    if (reader_) {
      reader_->Close();
    }
    reader_ = std::move(reader);
    current_block_ = block;
  }

  return ok;
}

void PaxIndexScanDesc::Release() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
}

bool PaxScanDesc::BitmapNextBlock(struct TBMIterateResult *tbmres) {
  cindex_ = 0;
  if (!index_desc_) {
    index_desc_ = std::make_unique<PaxIndexScanDesc>(rs_base_.rs_rd);
  }
  return true;
}

// should skip invisible tuple, and fetch next tuple like
// appendonly_scan_bitmap_next_tuple
bool PaxScanDesc::BitmapNextTuple(struct TBMIterateResult *tbmres,
                                  TupleTableSlot *slot) {
  ItemPointerData tid;
  if (tbmres->ntuples < 0) {
    // lossy bitmap. The maximum value of the last 16 bits in CTID is
    // 0x7FFF + 1, i.e. 0x8000. See layout of ItemPointerData in PAX
    if (cindex_ > 0X8000) elog(ERROR, "unexpected offset in pax");

    ItemPointerSet(&tid, tbmres->blockno, cindex_);
  }
  while (cindex_ < tbmres->ntuples) {
    // The maximum value of the last 16 bits in CTID is 0x7FFF + 1,
    // i.e. 0x8000. See layout of ItemPointerData in PAX
    if (tbmres->offsets[cindex_] > 0X8000)
      elog(ERROR, "unexpected offset in pax");
    ItemPointerSet(&tid, tbmres->blockno, tbmres->offsets[cindex_]);

    ++cindex_;
    // invisible tuple should be skipped and fetch next tuple
    if (index_desc_->FetchTuple(&tid, rs_base_.rs_snapshot, slot, nullptr,
                                nullptr)) {
      return true;
    }
  }
  return false;
}

TableScanDesc PaxScanDesc::BeginScan(Relation relation, Snapshot snapshot,
                                     int nkeys, struct ScanKeyData * /*key*/,
                                     ParallelTableScanDesc pscan, uint32 flags,
                                     std::shared_ptr<PaxFilter> &&pax_filter,
                                     bool build_bitmap) {
  MemoryContext old_ctx;
  std::shared_ptr<PaxFilter> filter;
  TableReader::ReaderOptions reader_options{};

  StaticAssertStmt(
      offsetof(PaxScanDesc, rs_base_) == 0,
      "rs_base should be the first field and aligned to the object address");

  auto desc = this;

  desc->memory_context_ = cbdb::AllocSetCtxCreate(
      CurrentMemoryContext, "Pax Storage", PAX_ALLOCSET_DEFAULT_SIZES);

  memset(&desc->rs_base_, 0, sizeof(desc->rs_base_));
  desc->rs_base_.rs_rd = relation;
  desc->rs_base_.rs_snapshot = snapshot;
  desc->rs_base_.rs_nkeys = nkeys;
  desc->rs_base_.rs_flags = flags;
  desc->rs_base_.rs_parallel = pscan;
  desc->reused_buffer_ =
      std::make_shared<DataBuffer<char>>(pax_scan_reuse_buffer_size);
  if (pax_filter)
    desc->filter_ = std::move(pax_filter);
  else
    desc->filter_ = std::make_shared<PaxFilter>();

  filter = desc->filter_;
  if (filter->GetColumnProjection().empty()) {
    auto natts = cbdb::RelationGetAttributesNumber(relation);
    filter->SetColumnProjection(std::vector<bool>(natts, true));
  }

#ifdef VEC_BUILD
  if (flags & SO_TYPE_VECTOR) {
    reader_options.is_vec = true;
    reader_options.tuple_desc = cbdb::RelationGetTupleDesc(relation);
    reader_options.vec_build_ctid = build_bitmap;
  }
#endif  // VEC_BUILD

  old_ctx = MemoryContextSwitchTo(desc->memory_context_);

  // build reader
  reader_options.reused_buffer = desc->reused_buffer_;
  reader_options.table_space_id = relation->rd_rel->reltablespace;
  reader_options.filter = filter;

  // aux_snapshot is used to get the micro partition metadata
  // from the micro partition files.
  Snapshot aux_snapshot = (snapshot && snapshot->snapshot_type == SNAPSHOT_ANY)
                              ? GetCatalogSnapshot(InvalidOid)
                              : snapshot;
  std::unique_ptr<pax::IteratorBase<pax::MicroPartitionMetadata>> iter;
  if (desc->rs_base_.rs_parallel) {
    ParallelBlockTableScanDesc pt_scan = (ParallelBlockTableScanDesc)pscan;

    iter = MicroPartitionIterator::NewParallelIterator(relation, aux_snapshot,
                                                       pt_scan);
  } else {
    iter = MicroPartitionIterator::New(relation, aux_snapshot);
  }

  if (filter->SparseFilterEnabled()) {
    auto wrap = std::make_unique<FilterIterator<MicroPartitionMetadata>>(
        std::move(iter), [filter, relation](const auto &x) {
          MicroPartitionStatsProvider provider(x.GetStats());
          auto ok =
              filter->ExecSparseFilter(provider, RelationGetDescr(relation),
                                       PaxSparseFilter::StatisticsKind::kFile);
          return ok;
        });
    iter = std::move(wrap);
  }
  desc->reader_ =
      std::make_unique<TableReader>(std::move(iter), reader_options);

  MemoryContextSwitchTo(old_ctx);
  pgstat_count_heap_scan(relation);

  return &desc->rs_base_;
}

PaxScanDesc::~PaxScanDesc() {}
void PaxScanDesc::EndScan() {
  if (pax_enable_debug && filter_) {
    filter_->LogStatistics();
  }

  auto memory_context = memory_context_;
  Assert(memory_context_);
  Assert(reader_);
  reader_->Close();
  reader_ = nullptr;

  // optional index_scan should close internal file
  if (index_desc_) index_desc_->Release();

  if (rs_base_.rs_flags & SO_TEMP_SNAPSHOT)
    UnregisterSnapshot(rs_base_.rs_snapshot);

  cbdb::MemoryCtxDelete(memory_context);
}

TableScanDesc PaxScanDesc::BeginScanExtractColumns(
    Relation rel, Snapshot snapshot, int /*nkeys*/,
    struct ScanKeyData * /*key*/, ParallelTableScanDesc parallel_scan,
    struct PlanState *ps, uint32 flags) {
  std::shared_ptr<PaxFilter> filter;
  List *targetlist = ps->plan->targetlist;
  List *qual = ps->plan->qual;
  auto natts = cbdb::RelationGetAttributesNumber(rel);
  bool found = false;
  bool build_bitmap = true;
  std::vector<bool> col_bits(natts, false);
  {
    PaxcExtractcolumnContext extract_column(col_bits);

    found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(targetlist),
                                         &extract_column);
    found = cbdb::ExtractcolumnsFromNode(reinterpret_cast<Node *>(qual),
                                         col_bits) ||
            found;
    build_bitmap = cbdb::IsSystemAttrNumExist(&extract_column,
                                              SelfItemPointerAttributeNumber);
  }
  filter = std::make_shared<PaxFilter>();

  // In some cases (for example, count(*)), targetlist and qual may be null,
  // extractcolumns_walker will return immediately, so no columns are specified.
  // We always scan the first column.
  if (!found && !build_bitmap && natts > 0) {
    int i = ChooseCheapColumn(RelationGetDescr(rel));
    col_bits[i] = true;
  }

  // The `cols` life cycle will be bound to `PaxFilter`
  filter->SetColumnProjection(std::move(col_bits));

  if (pax_enable_sparse_filter) {
    filter->InitSparseFilter(rel, qual);

    // FIXME: enable predicate pushdown can filter rows immediately without
    // assigning all columns. But it may mess the filter orders for multiple
    // conditions. For example: ... where a = 2 and f_leak(b) the second
    // condition may be executed before the first one.
    if (gp_enable_predicate_pushdown && pax_enable_row_filter
#ifdef VEC_BUILD
        && !(flags & SO_TYPE_VECTOR)
#endif
    ) {
      filter->InitRowFilter(rel, ps, filter->GetColumnProjection());
    }
  }
  return BeginScan(rel, snapshot, 0, nullptr, parallel_scan, flags,
                   std::move(filter), build_bitmap);
}

// FIXME: shall we take these parameters into account?
void PaxScanDesc::ReScan(ScanKey /*key*/, bool /*set_params*/,
                         bool /*allow_strat*/, bool /*allow_sync*/,
                         bool /*allow_pagemode*/) {
  MemoryContext old_ctx;
  Assert(reader_);

  old_ctx = MemoryContextSwitchTo(memory_context_);
  reader_->ReOpen();
  MemoryContextSwitchTo(old_ctx);
}

bool PaxScanDesc::GetNextSlot(TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);

  Assert(reader_);
  ok = reader_->ReadTuple(slot);

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanAnalyzeNextBlock(BlockNumber blockno,
                                       BufferAccessStrategy /*bstrategy*/) {
  target_tuple_id_ = blockno;
  return true;
}

bool PaxScanDesc::ScanAnalyzeNextTuple(TransactionId /*oldest_xmin*/,
                                       double *liverows, double *deadrows,
                                       TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  if (next_tuple_id_ > target_tuple_id_) {
    return false;
  }

  old_ctx = MemoryContextSwitchTo(memory_context_);
  try {
    ExecClearTuple(slot);
    ok = reader_->GetTuple(slot, ForwardScanDirection,
                           target_tuple_id_ - prev_target_tuple_id_);
    next_tuple_id_ = target_tuple_id_ + 1;
    prev_target_tuple_id_ = target_tuple_id_;
    if (ok) {
      ExecStoreVirtualTuple(slot);
      *liverows += 1;
    } else {
      *deadrows += 1;
    }
  } catch (cbdb::CException &e) {
    e.AppendDetailMessage(
        fmt("\n ScanAnalyzeNextTuple [target tuple=%lu, next tuple=%lu, prev "
            "target tuple=%lu]",
            target_tuple_id_, next_tuple_id_, prev_target_tuple_id_));
    CBDB_RERAISE(e);
  }

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextBlock(SampleScanState *scanstate) {
  MemoryContext old_ctx;
  TsmRoutine *tsm = scanstate->tsmroutine;
  BlockNumber blockno = 0;
  BlockNumber pages = 0;
  double total_tuples = 0;
  int32 attrwidths = 0;
  double allvisfrac = 0;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);

  if (total_tuples_ == 0) {
    paxc::PaxAccessMethod::EstimateRelSize(rs_base_.rs_rd, &attrwidths, &pages,
                                           &total_tuples, &allvisfrac);
    total_tuples_ = total_tuples;
  }

  if (tsm->NextSampleBlock)
    blockno = tsm->NextSampleBlock(scanstate, total_tuples_);
  else
    blockno = system_nextsampleblock(scanstate, total_tuples_);

  ok = BlockNumberIsValid(blockno);
  if (ok) fetch_tuple_id_ = blockno;

  MemoryContextSwitchTo(old_ctx);
  return ok;
}

bool PaxScanDesc::ScanSampleNextTuple(SampleScanState * /*scanstate*/,
                                      TupleTableSlot *slot) {
  MemoryContext old_ctx;
  bool ok = false;

  old_ctx = MemoryContextSwitchTo(memory_context_);
  while (next_tuple_id_ < fetch_tuple_id_) {
    ok = GetNextSlot(slot);
    if (!ok) break;
    next_tuple_id_++;
  }

  if (next_tuple_id_ == fetch_tuple_id_) {
    ok = GetNextSlot(slot);
    next_tuple_id_++;
  }
  MemoryContextSwitchTo(old_ctx);
  return ok;
}

}  // namespace pax
