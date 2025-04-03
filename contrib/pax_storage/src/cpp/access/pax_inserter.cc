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
 * pax_inserter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_inserter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "access/pax_dml_state.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_stats.h"
#include "storage/strategy.h"

namespace pax {

CPaxInserter::CPaxInserter(Relation rel)
    : rel_(rel), insert_count_(0), writer_(nullptr) {
  writer_ = std::make_unique<TableWriter>(rel);

  writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(std::make_unique<PaxDefaultSplitStrategy>())
      ->Open();
}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                               CommandId /*cid*/, int /*options*/,
                               BulkInsertState /*bistate*/) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);

  if (!TTS_IS_VIRTUAL(slot)) {
    cbdb::SlotGetAllAttrs(slot);
  }

  writer_->WriteTuple(slot);
}

void CPaxInserter::MultiInsert(Relation relation, TupleTableSlot **slots,
                               int ntuples, CommandId cid, int options,
                               BulkInsertState bistate) {
  auto inserter =
      pax::CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  for (int i = 0; i < ntuples; i++) {
    inserter->InsertTuple(relation, slots[i], cid, options, bistate);
  }
}

void CPaxInserter::FinishBulkInsert(Relation relation, int /*options*/) {
  pax::CPaxDmlStateLocal::Instance()->FinishDmlState(relation, CMD_INSERT);
}

void CPaxInserter::FinishInsert() {
  writer_->Close();
  writer_ = nullptr;
}

void CPaxInserter::TupleInsert(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  auto inserter = CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

}  // namespace pax
