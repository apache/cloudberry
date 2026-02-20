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
 * pax_index_update.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_index_update.cc
 *
 *-------------------------------------------------------------------------
 */
#include "storage/pax_index_update.h"

#include "storage/pax_itemptr.h"

namespace paxc {

/*
 * Callback function to check if an index entry should be reaped during vacuum.
 * Returns true if the index entry points to a micro partition that was
 * processed (and will be deleted) during compaction.
 *
 * This has the right signature to be an IndexBulkDeleteCallback.
 * Must be defined outside namespace to ensure proper C linkage.
 */
static bool pax_tid_reaped(ItemPointer itemptr, void *state) {
  std::unordered_set<int> *processed_mp_ids =
      static_cast<std::unordered_set<int> *>(state);
  uint32 block_number = pax::GetBlockNumber(*itemptr);
  return processed_mp_ids->find(block_number) != processed_mp_ids->end();
}

void IndexUpdaterInternal::Begin(Relation rel) {
  Assert(rel);

  rel_ = rel;
  slot_ = MakeTupleTableSlot(rel->rd_att, &TTSOpsVirtual);

  if (HasIndex()) {
    estate_ = CreateExecutorState();

    relinfo_ = makeNode(ResultRelInfo);
    relinfo_->ri_RelationDesc = rel;
    ExecOpenIndices(relinfo_, false);
  }
}

void IndexUpdaterInternal::UpdateIndex(TupleTableSlot *slot) {
  Assert(slot == slot_);
  Assert(HasIndex());
  /* Ensure tts_nvalid is set correctly for virtual slot before index
   * insertion */
  /* This is similar to what aocs_getnext does (setting tts_nvalid = natts) */
  slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
  auto recheck_index =
      ExecInsertIndexTuples(relinfo_, slot_, estate_, false, false, NULL, NIL);
  list_free(recheck_index);
}

void IndexUpdaterInternal::End() {
  if (HasIndex()) {
    Assert(relinfo_ && estate_);

    ExecCloseIndices(relinfo_);
    pfree(relinfo_);
    relinfo_ = nullptr;

    FreeExecutorState(estate_);
    estate_ = nullptr;
  }
  Assert(relinfo_ == nullptr && estate_ == nullptr);

  ExecDropSingleTupleTableSlot(slot_);
  slot_ = nullptr;

  rel_ = nullptr;
}

void IndexUpdaterInternal::CleanIndex(
    std::unordered_set<int> &processed_mp_ids) {
  Assert(HasIndex());
  int nindexes;
  Relation *Irel;
  vac_open_indexes(rel_, RowExclusiveLock, &nindexes, &Irel);

  if (Irel != NULL && nindexes > 0) {
    IndexVacuumInfo ivinfo = {0};
    ivinfo.analyze_only = false;
    ivinfo.message_level = DEBUG2;
    ivinfo.num_heap_tuples = rel_->rd_rel->reltuples;
    ivinfo.estimated_count = true;
    ivinfo.strategy = NULL;  // No buffer strategy needed

    // Process all indexes
    for (int i = 0; i < nindexes; i++) {
      ivinfo.index = Irel[i];

      // Bulk delete index entries pointing to processed micro partitions
      IndexBulkDeleteResult *stats =
          index_bulk_delete(&ivinfo, NULL, pax_tid_reaped,
                            static_cast<void *>(&processed_mp_ids));

      // Always call index_vacuum_cleanup to clean up all dead entries
      // This includes entries deleted by index_bulk_delete and entries
      // marked as DEAD during index insertion (when old micro partitions
      // were found to be invisible)
      // Pass stats from index_bulk_delete if available, otherwise pass NULL
      // to clean up all remaining dead entries
      stats = index_vacuum_cleanup(&ivinfo, stats);

      if (stats) {
        // Optionally update index statistics if needed
        // Similar to vacuum_appendonly_index(), we could update relstats here
        // but for now we just free the stats
        pfree(stats);
      }
    }
  }

  vac_close_indexes(nindexes, Irel, NoLock);
}
}  // namespace paxc
