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
 * pax_vacuum_compactor.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_vacuum_compactor.cc
 *
 *-------------------------------------------------------------------------
 */
#include "storage/pax_vacuum_compactor.h"

#include <unordered_set>

#include "access/genam.h"
#include "access/pax_visimap.h"
#include "comm/iterator.h"  // VectorIterator
#include "comm/pax_memory.h"
#include "commands/vacuum.h"
#include "exceptions/CException.h"
#include "executor/executor.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/pax.h"
#include "storage/pax_index_update.h"
#include "storage/pax_itemptr.h"

namespace pax {

static void InsertOrUpdateClusteredMicroPartitionEntry(
    const pax::WriteSummary &summary) {
  pax::WriteSummary clusterd_summary(summary);
  clusterd_summary.is_stats_valid = true;
  cbdb::InsertOrUpdateMicroPartitionEntry(clusterd_summary);
}
}  // namespace pax

namespace pax {

PaxVacuumBatcher::PaxVacuumBatcher(Relation rel) : rel_(rel) {}

void PaxVacuumBatcher::Process(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&it,
    const std::vector<int> &minmax_col_idxs,
    const std::vector<int> &bf_col_idxs) {
  pax::IndexUpdater index_updater;
  index_updater.Begin(rel_);
  Assert(rel_->rd_rel->relhasindex == index_updater.HasIndex());
  auto slot = index_updater.GetSlot();
  slot->tts_tableOid = RelationGetRelid(rel_);

  // Track processed micro partition IDs
  std::unordered_set<int> processed_mp_ids;

  // First, collect all micro partition metadata and IDs
  // We need to do this before reading tuples because we'll delete micro
  // partitions from catalog to make them invisible before inserting new index
  // entries
  std::vector<MicroPartitionMetadata> metadata_list;
  while (it->HasNext()) {
    auto meta = it->Next();
    metadata_list.push_back(meta);
    processed_mp_ids.insert(meta.GetMicroPartitionId());
  }

  // Delete all processed micro partition entries from catalog BEFORE compaction
  // This makes them invisible so that when we insert new index entries,
  // B-tree's uniqueness check will find old entries pointing to dead micro
  // partitions and mark them as DEAD instead of raising an error
  Snapshot snapshot = GetActiveSnapshot();
  Oid relid = RelationGetRelid(rel_);
  for (auto mp_id : processed_mp_ids) {
    cbdb::DeleteMicroPartitionEntry(relid, snapshot, mp_id);
  }

  // Clean up old index entries pointing to processed micro partitions
  // This is similar to vacuum_appendonly_index() in vacuum_ao.c
  if (index_updater.HasIndex() && !processed_mp_ids.empty()) {
    index_updater.CleanIndex(processed_mp_ids);
  }

  // CommandCounterIncrement is needed to ensure the deletions are visible
  // in the current transaction before we start inserting new index entries
  CommandCounterIncrement();

  TableWriter tw(rel_);
  tw.SetFileSplitStrategy(std::make_unique<PaxDefaultSplitStrategy>());
  tw.SetWriteSummaryCallback(cbdb::InsertOrUpdateMicroPartitionEntry);
  tw.SetEnableStats(true);
  tw.Open();

  // Recreate iterator from the collected metadata for reading tuples
  auto metadata_iter = std::make_unique<VectorIterator<MicroPartitionMetadata>>(
      std::move(metadata_list));

  // Create TableReader similar to PaxScanDesc
  TableReader::ReaderOptions reader_options{};
  reader_options.filter = std::make_shared<PaxFilter>();
  auto natts = cbdb::RelationGetAttributesNumber(rel_);
  reader_options.filter->SetColumnProjection(std::vector<bool>(natts, true));
  reader_options.reused_buffer = nullptr;
  reader_options.table_space_id = rel_->rd_rel->reltablespace;

  TableReader reader(std::move(metadata_iter), reader_options);
  reader.Open();

  // Read all tuples using TableReader and insert into new micro partitions
  // Note: Old micro partitions are now deleted from catalog, so when we insert
  // new index entries, B-tree's uniqueness check will find old entries pointing
  // to dead micro partitions and mark them as DEAD instead of raising an error.
  // Note: TableReader::ReadTuple already calls ExecStoreVirtualTuple
  while (reader.ReadTuple(slot)) {
    tw.WriteTuple(slot);
    if (index_updater.HasIndex()) {
      // Already store the ctid after WriteTuple
      Assert(ItemPointerIsValid(&slot->tts_tid));
      // Insert new index entries directly (similar to AO/AOCS compaction)
      // The uniqueness check will handle old entries by marking them as DEAD
      index_updater.UpdateIndex(slot);
    }
  }

  index_updater.End();
  reader.Close();
  tw.Close();
}

}  // namespace pax