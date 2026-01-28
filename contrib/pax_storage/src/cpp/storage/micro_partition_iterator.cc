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
 * micro_partition_iterator.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_iterator.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_iterator.h"

#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {
namespace internal {
class MicroPartitionInfoIterator final
    : public MicroPartitionIterator {
 public:
  MicroPartitionInfoIterator(Relation pax_rel, Snapshot snapshot,
                             const std::string &rel_path);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override { End(true); }
  ~MicroPartitionInfoIterator() = default;

 private:
  // paxc function
  void paxc_begin();
  void paxc_end(bool close_aux);

  // pax function, wrap paxc_xxx in c++
  void Begin();
  void End(bool close_aux);

  MicroPartitionMetadata ToValue(HeapTuple tuple);

  std::string rel_path_;
  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
};

class MicroPartitionInfoParallelIterator final
    : public MicroPartitionIterator {
 public:
  MicroPartitionInfoParallelIterator(Relation pax_rel, Snapshot snapshot,
                                     ParallelBlockTableScanDesc pscan,
                                     std::string rel_path);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan);

  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override;
  ~MicroPartitionInfoParallelIterator() = default;

 private:
  MicroPartitionMetadata ToValue(HeapTuple tuple);

  // paxc function
  void paxc_begin();
  void paxc_end(bool close_aux);

  Relation pax_rel_ = nullptr;
  Relation aux_rel_ = nullptr;
  Snapshot snapshot_ = nullptr;
  ParallelBlockTableScanDesc pscan_ = nullptr;
  SysScanDesc desc_ = nullptr;
  HeapTuple tuple_ = nullptr;
  std::string rel_path_;

  Oid index_oid_ = InvalidOid;

  uint64 batch_allocated_ = 2;
  int64 allocated_block_id_ = -1;
};

MicroPartitionInfoIterator::MicroPartitionInfoIterator(Relation pax_rel,
                                                       Snapshot snapshot,
                                                       const std::string &rel_path)
    : rel_path_(rel_path), pax_rel_(pax_rel), snapshot_(snapshot) {}

void MicroPartitionInfoIterator::paxc_begin() {
  Assert(pax_rel_);
  Assert(!desc_);
  Assert(!tuple_);

  if (!aux_rel_) {
    auto aux_oid = paxc::GetPaxAuxRelid(RelationGetRelid(pax_rel_));
    aux_rel_ = table_open(aux_oid, AccessShareLock);
  }

  desc_ = systable_beginscan(aux_rel_, InvalidOid, false, snapshot_, 0, NULL);
}

void MicroPartitionInfoIterator::Begin() {
  CBDB_WRAP_START;
  { paxc_begin(); }
  CBDB_WRAP_END;
}

void MicroPartitionInfoIterator::paxc_end(bool close_aux) {
  if (desc_) {
    auto desc = desc_;
    desc_ = nullptr;
    tuple_ = nullptr;
    systable_endscan(desc);

    Assert(aux_rel_);
    if (close_aux) {
      auto aux_rel = aux_rel_;
      aux_rel_ = nullptr;

      table_close(aux_rel, NoLock);
    }
  }
  Assert(!tuple_);
}

void MicroPartitionInfoIterator::End(bool close_aux) {
  CBDB_WRAP_START;
  { paxc_end(close_aux); }
  CBDB_WRAP_END;
}

bool MicroPartitionInfoIterator::HasNext() {
  if (!tuple_ && desc_) {
    tuple_ = cbdb::SystableGetNext(desc_);
  }
  return tuple_ != nullptr;
}

MicroPartitionMetadata MicroPartitionInfoIterator::Next() {
  auto tuple = tuple_;
  Assert(tuple);

  tuple_ = nullptr;
  return ToValue(tuple);
}

void MicroPartitionInfoIterator::Rewind() {
  CBDB_WRAP_START;
  {
    paxc_end(false);
    paxc_begin();
  }
  CBDB_WRAP_END;
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionInfoIterator::New(Relation pax_rel, Snapshot snapshot) {
  auto it = std::make_unique<MicroPartitionInfoIterator>(
      pax_rel, snapshot,
      cbdb::BuildPaxDirectoryPath(
          pax_rel->rd_node, pax_rel->rd_backend));

  it->Begin();
  return it;
}

MicroPartitionMetadata MicroPartitionInfoIterator::ToValue(HeapTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  bool is_null;
  auto tup_desc = RelationGetDescr(aux_rel_);

  {
    auto block_id = DatumGetInt32(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    auto file_name =
        cbdb::BuildPaxFilePath(rel_path_, std::to_string(block_id));
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(block_id);
  }

  auto tup_count = cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                                     tup_desc, &is_null);
  CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
  v.SetTupleCount(cbdb::DatumToInt32(tup_count));

  {
    auto stats = reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(
        cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                          tup_desc, &is_null)));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    auto flat_stats = cbdb::PgDeToastDatumPacked(stats);
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(flat_stats),
                                        VARSIZE_ANY_EXHDR(flat_stats));
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError,
               ::pax::fmt("Invalid pb structure in the aux table [rd_id=%d]",
                          aux_rel_->rd_id));
    v.SetStats(std::move(stats_info));

    if (flat_stats != stats) cbdb::Pfree(flat_stats);
  }

  {
    auto visibility_map_filename = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME, tup_desc, &is_null);

    if (!is_null) {
      auto name = NameStr(*DatumGetName(visibility_map_filename));
      auto v_file_name = cbdb::BuildPaxFilePath(rel_path_, name);
      v.SetVisibilityBitmapFile(std::move(v_file_name));
    }
  }

  {
    auto existexttoast = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST, tup_desc, &is_null);
    Assert(!is_null);

    v.SetExistToast(DatumGetBool(existexttoast));

    auto is_cluster = cbdb::DatumToBool(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    v.SetClustered(is_cluster);
  }

  // deserialize protobuf message
  return v;
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionInfoParallelIterator::New(Relation pax_rel, Snapshot snapshot,
                                        ParallelBlockTableScanDesc pscan) {
  auto it = std::make_unique<MicroPartitionInfoParallelIterator>(
      pax_rel, snapshot, pscan,
      cbdb::BuildPaxDirectoryPath(
          pax_rel->rd_node, pax_rel->rd_backend));

  return it;
}

MicroPartitionInfoParallelIterator::MicroPartitionInfoParallelIterator(
    Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan,
    std::string rel_path)
    : pax_rel_(pax_rel),
      snapshot_(snapshot),
      pscan_(pscan),
      rel_path_(rel_path) {
}

/**
 * Lazily call this function to only change the atomic block counter
 * when scanning tuples.
 */
void MicroPartitionInfoParallelIterator::paxc_begin() {
  Assert(pax_rel_);
  Assert(pscan_);
  Assert(!desc_);
  Assert(!tuple_);

  ScanKeyData scan_key[1];

  if (!aux_rel_) {
    auto aux_oid = paxc::GetPaxAuxRelid(RelationGetRelid(pax_rel_));
    aux_rel_ = table_open(aux_oid, AccessShareLock);
    index_oid_ = paxc::FindAuxIndexOid(aux_rel_->rd_id, snapshot_);
  }

  allocated_block_id_ =
      (int64)pg_atomic_fetch_add_u64(&pscan_->phs_nallocated, batch_allocated_);

  ScanKeyInit(&scan_key[0], ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
              BTGreaterEqualStrategyNumber, F_INT4GE, allocated_block_id_);

  desc_ = systable_beginscan(aux_rel_, index_oid_, true, snapshot_, 1,
                             &scan_key[0]);
}

void MicroPartitionInfoParallelIterator::paxc_end(bool close_aux) {
  if (desc_) {
    auto desc = desc_;
    desc_ = nullptr;
    tuple_ = nullptr;
    systable_endscan(desc);

    Assert(aux_rel_);
    if (close_aux) {
      auto aux_rel = aux_rel_;
      aux_rel_ = nullptr;

      table_close(aux_rel, NoLock);
    }
    // do not reset pscan_, when rewind, we need to keep pscan_ to continue
  }
  Assert(!tuple_);
}

void MicroPartitionInfoParallelIterator::Release() {
  if (allocated_block_id_ >= 0) {
    CBDB_WRAP_START;
    { paxc_end(true); }
    CBDB_WRAP_END;
  }
}

bool MicroPartitionInfoParallelIterator::HasNext() {
  if (tuple_) return true;

  bool has_more;
  CBDB_WRAP_START;
  {
    /* lazily to get the block number in parallel scan */
    if (unlikely(allocated_block_id_ < 0))
      paxc_begin();

    Assert(allocated_block_id_ >= 0);
    do {
      bool is_null;

      tuple_ = systable_getnext(desc_);
      has_more = tuple_ != nullptr;
      // has no more blocks to scan
      if (!has_more) break;

      auto block_id = heap_getattr(tuple_, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                                   RelationGetDescr(aux_rel_), &is_null);
      Assert(!is_null);
      // check block_id is in the range of [allocated_block_id_,
      // allocated_block_id_ + batch_allocated_)
      if (block_id < allocated_block_id_ + batch_allocated_) break;

      paxc_end(false);
      // block_id is great than phs_nallocated, update pscan_->phs_nallocated to
      // block_id and switch to next batch
      uint64 old_value = pg_atomic_read_u64(&pscan_->phs_nallocated);
      while (old_value < block_id &&
             !pg_atomic_compare_exchange_u64(&pscan_->phs_nallocated,
                                             &old_value, block_id));
      paxc_begin();
    } while (true);
  }
  CBDB_WRAP_END;

  // has no more blocks to scan
  return has_more;
}

MicroPartitionMetadata MicroPartitionInfoParallelIterator::Next() {
  auto tuple = tuple_;
  Assert(tuple);

  tuple_ = nullptr;
  return ToValue(tuple);
}

void MicroPartitionInfoParallelIterator::Rewind() {
  CBDB_RAISE(cbdb::CException::kExTypeUnImplements,
             "parallel scan should not call the Rewind function");
}

MicroPartitionMetadata MicroPartitionInfoParallelIterator::ToValue(
    HeapTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  bool is_null;
  auto tup_desc = RelationGetDescr(aux_rel_);

  {
    auto block_id = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, tup_desc, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    std::string block_name = std::to_string(DatumGetInt32(block_id));
    auto file_name = cbdb::BuildPaxFilePath(rel_path_, block_name);
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(DatumGetInt32(block_id));
  }

  auto tup_count = cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                                     tup_desc, &is_null);
  CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
  v.SetTupleCount(cbdb::DatumToInt32(tup_count));

  {
    auto stats = reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(
        cbdb::HeapGetAttr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                          tup_desc, &is_null)));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    auto flat_stats = cbdb::PgDeToastDatumPacked(stats);
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(flat_stats),
                                        VARSIZE_ANY_EXHDR(flat_stats));
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError,
               ::pax::fmt("Invalid pb structure in the aux table [rd_id=%d]",
                          aux_rel_->rd_id));
    v.SetStats(std::move(stats_info));

    if (flat_stats != stats) cbdb::Pfree(flat_stats);
  }

  {
    auto visibility_map_filename = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME, tup_desc, &is_null);

    if (!is_null) {
      auto name = NameStr(*DatumGetName(visibility_map_filename));
      auto v_file_name = cbdb::BuildPaxFilePath(rel_path_, name);
      v.SetVisibilityBitmapFile(std::move(v_file_name));
    }
  }

  {
    auto existexttoast = cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST, tup_desc, &is_null);
    Assert(!is_null);

    v.SetExistToast(DatumGetBool(existexttoast));

    auto is_cluster = cbdb::DatumToBool(cbdb::HeapGetAttr(
        tuple, ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED, tup_desc, &is_null));
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    v.SetClustered(is_cluster);
  }

  // deserialize protobuf message
  return v;
}

}  // namespace internal

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionIterator::New(Relation pax_rel, Snapshot snapshot) {
  return internal::MicroPartitionInfoIterator::New(pax_rel, snapshot);
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionIterator::NewParallelIterator(Relation pax_rel, Snapshot snapshot,
                                        ParallelBlockTableScanDesc pscan) {
  return internal::MicroPartitionInfoParallelIterator::New(pax_rel, snapshot, pscan);
}

}  // namespace pax
