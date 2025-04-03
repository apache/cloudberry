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
 * micro_partition_iterator_manifest.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_iterator_manifest.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_iterator.h"

#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"

namespace pax {
static inline Datum ManifestGetTupleValue(ManifestTuple tuple,
                                          ManifestRelation mrel,
                                          const char *attr_name,
                                          bool *is_null) {
  CBDB_WRAP_START;
  {
    return get_manifesttuple_value(tuple, mrel, attr_name, is_null);
  }
  CBDB_WRAP_END;
}

MicroPartitionMetadata ManifestTupleToValue(
    const std::string &rel_path, ManifestRelation mrel, ManifestTuple tuple) {
  MicroPartitionMetadata v;
  ::pax::stats::MicroPartitionStatisticsInfo stats_info;
  Datum datum;
  bool is_null;

  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTBLOCKNAME, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    std::string block_name = std::to_string(DatumGetInt32(datum));
    auto file_name = cbdb::BuildPaxFilePath(rel_path, block_name);
    v.SetFileName(std::move(file_name));
    v.SetMicroPartitionId(DatumGetInt32(datum));
  }
  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTTUPCOUNT, &is_null);
    Assert(!is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    v.SetTupleCount(DatumGetInt32(datum));
  }
  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTSTATISITICS, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);
    
    auto stats = reinterpret_cast<struct varlena*>(cbdb::DatumToPointer(datum));
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(stats),
                                        VARSIZE_ANY_EXHDR(stats));
    CBDB_CHECK(ok, cbdb::CException::kExTypeIOError);
    v.SetStats(std::move(stats_info));
  }
  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTVISIMAPNAME, &is_null);
    if (!is_null) {
      auto name = DatumGetCString(datum);
      auto vname = cbdb::BuildPaxFilePath(rel_path, name);
      v.SetVisibilityBitmapFile(std::move(vname));
    }
  }
  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTEXISTEXTTOAST, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    v.SetExistToast(DatumGetBool(datum));
  }
  {
    datum = ManifestGetTupleValue(tuple, mrel, PAX_AUX_PTISCLUSTERED, &is_null);
    CBDB_CHECK(!is_null, cbdb::CException::kExTypeLogicError);

    v.SetClustered(DatumGetBool(datum));
  }
  return v;
}

namespace internal {
class MicroPartitionManifestIterator : public MicroPartitionIterator {
 public:
  MicroPartitionManifestIterator(Relation pax_rel, Snapshot snapshot,
                                 const std::string &rel_path);
  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override;
  ~MicroPartitionManifestIterator() = default;

  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot, const std::string &rel_path);

 private:
  void begin();
  void end(bool close_aux);
  void Release(bool null_ok);

 private:
  std::string rel_path_;
  Relation pax_rel_;
  Snapshot snapshot_;
  ManifestRelation mrel_;
  ManifestScan mscan_;
  ManifestTuple tuple_; // current fetched tuple, not consumed.
};

MicroPartitionManifestIterator::MicroPartitionManifestIterator(
    Relation pax_rel, Snapshot snapshot, const std::string &rel_path)
    : rel_path_(rel_path)
    , pax_rel_(pax_rel)
    , snapshot_(snapshot)
    , mrel_(nullptr)
    , mscan_(nullptr)
    , tuple_(nullptr)
    {}

void MicroPartitionManifestIterator::begin() {
  Assert(pax_rel_);
  Assert(!tuple_);

  if (!mrel_) {
    mrel_ = manifest_open(pax_rel_);
  }
  mscan_ = manifest_beginscan(mrel_, snapshot_);
}

void MicroPartitionManifestIterator::end(bool close_aux) {
  if (mrel_) {
    Assert(mscan_);
    manifest_endscan(mscan_);
    mscan_ = nullptr;
    tuple_ = nullptr;
    if (close_aux) {
      manifest_close(mrel_);
      mrel_ = nullptr;
    }
  }
}

bool MicroPartitionManifestIterator::HasNext() {
  if (!tuple_) {
    auto tuple = manifest_getnext(mscan_, nullptr);
    if (!tuple) return false;

    tuple_ = tuple;
  }
  return true;
}

MicroPartitionMetadata MicroPartitionManifestIterator::Next() {
  auto tuple = tuple_;
  Assert(tuple);

  tuple_ = nullptr;
  return pax::ManifestTupleToValue(rel_path_, mrel_, tuple);
}

void MicroPartitionManifestIterator::Release() {
  end(true);
}
void MicroPartitionManifestIterator::Rewind() {
  end(false);
  begin();
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>> MicroPartitionManifestIterator::New(
      Relation pax_rel, Snapshot snapshot, const std::string &rel_path) {
  auto iter = std::make_unique<MicroPartitionManifestIterator>(pax_rel, snapshot, rel_path);
  iter->begin();
  return iter;
}

class MicroPartitionManifestParallelIterator : public MicroPartitionIterator {
 public:
  MicroPartitionManifestParallelIterator(Relation pax_rel,
                                         Snapshot snapshot,
                                         ParallelBlockTableScanDesc pscan,
                                         const std::string &rel_path);
  bool HasNext() override;
  MicroPartitionMetadata Next() override;
  void Rewind() override;
  void Release() override;

  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot,
      ParallelBlockTableScanDesc pscan, 
      const std::string &rel_path);

 private:
  void fetch_range();
  void cache();
 private:
  Relation pax_rel_;
  Snapshot snapshot_;
  ParallelBlockTableScanDesc pscan_;
  std::vector<MicroPartitionMetadata> micro_partitions_;
  int64 start_block_; // inclusive: >= start_block_
  int64 end_block_; // exclusive: < end_block_
  size_t current_index_;
  bool next_;
};

MicroPartitionManifestParallelIterator::MicroPartitionManifestParallelIterator(
    Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan,
    const std::string &rel_path)
    : pax_rel_(pax_rel)
    , snapshot_(snapshot)
    , pscan_(pscan)
    , start_block_(-1)
    , end_block_(-1)
    , current_index_(0)
    , next_(false)
     { }

static bool compare_mpd(const MicroPartitionMetadata &x, const MicroPartitionMetadata &y) {
  return x.GetMicroPartitionId() < y.GetMicroPartitionId();
}

void MicroPartitionManifestParallelIterator::cache() {
  // FIXME: The data size of manifest could be large, we may need to
  //        use a more efficient way to manage the memory.
  auto iter = MicroPartitionIterator::New(pax_rel_, snapshot_);
  while (iter->HasNext()) {
    micro_partitions_.emplace_back(iter->Next());
  }
  std::sort(micro_partitions_.begin(), micro_partitions_.end(), compare_mpd);
  iter->Release();
}

#define NUM_PARALLEL_FILES  2
void MicroPartitionManifestParallelIterator::fetch_range() {
  start_block_ = (int64) pg_atomic_fetch_add_u64(&pscan_->phs_nallocated, NUM_PARALLEL_FILES);
  end_block_ = start_block_ + NUM_PARALLEL_FILES;
}


bool MicroPartitionManifestParallelIterator::HasNext() {
  if (next_) return true;

  if (unlikely(start_block_ < 0))
    fetch_range();

  auto n = micro_partitions_.size();
  for (; current_index_ < n; ++current_index_) {
    const auto &m = micro_partitions_[current_index_];
    auto block = m.GetMicroPartitionId();
    if (block < start_block_) continue;
    if (block >= end_block_) {
      fetch_range();
      continue;
    }
    next_ = true;
    break;
  }
  return next_;
}

MicroPartitionMetadata MicroPartitionManifestParallelIterator::Next() {
  Assert(next_);
  next_ = false;
  return micro_partitions_[current_index_++];
}

void MicroPartitionManifestParallelIterator::Release() {
}

void MicroPartitionManifestParallelIterator::Rewind() {
  start_block_ = end_block_ = -1;
  current_index_ = 0;
  next_ = false;
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>> MicroPartitionManifestParallelIterator::New(
      Relation pax_rel, Snapshot snapshot,
      ParallelBlockTableScanDesc pscan, 
      const std::string &rel_path) {
  auto it = std::make_unique<MicroPartitionManifestParallelIterator>(pax_rel, snapshot, pscan, rel_path);
  it->cache();
  return it;
}

} // namespace internal

std::unique_ptr<IteratorBase<MicroPartitionMetadata>> MicroPartitionIterator::New(
      Relation pax_rel, Snapshot snapshot) {
  auto rel_path = cbdb::BuildPaxDirectoryPath(
                    pax_rel->rd_node, pax_rel->rd_backend);
  return internal::MicroPartitionManifestIterator::New(pax_rel, snapshot, rel_path);
}

std::unique_ptr<IteratorBase<MicroPartitionMetadata>>
MicroPartitionIterator::NewParallelIterator(
      Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan) {
  auto rel_path = cbdb::BuildPaxDirectoryPath(
                    pax_rel->rd_node, pax_rel->rd_backend);
  return internal::MicroPartitionManifestParallelIterator::New(pax_rel,
                                             snapshot, pscan, rel_path);
}

} // namespace pax
