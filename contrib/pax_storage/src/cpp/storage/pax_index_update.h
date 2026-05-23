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
 * pax_index_update.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_index_update.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once
#include "comm/cbdb_api.h"

#include <unordered_set>

#include "comm/cbdb_wrappers.h"

namespace paxc {
class IndexUpdaterInternal {
 public:
  void Begin(Relation rel);
  void UpdateIndex(TupleTableSlot *slot);
  void CleanIndex(std::unordered_set<int> &processed_mp_ids);
  void End();
  inline TupleTableSlot *GetSlot() { return slot_; }
  inline bool HasIndex() const { return rel_->rd_rel->relhasindex; }

 private:
  Relation rel_ = nullptr;
  TupleTableSlot *slot_ = nullptr;
  EState *estate_ = nullptr;
  ResultRelInfo *relinfo_ = nullptr;
};
}  // namespace paxc

namespace pax {
class IndexUpdater final {
 public:
  void Begin(Relation rel) {
    CBDB_WRAP_START;
    { stub_.Begin(rel); }
    CBDB_WRAP_END;
  }

  void UpdateIndex(TupleTableSlot *slot) {
    CBDB_WRAP_START;
    { stub_.UpdateIndex(slot); }
    CBDB_WRAP_END;
  }

  void CleanIndex(std::unordered_set<int> &processed_mp_ids) {
    CBDB_WRAP_START;
    { stub_.CleanIndex(processed_mp_ids); }
    CBDB_WRAP_END;
  }

  void End() {
    CBDB_WRAP_START;
    { stub_.End(); }
    CBDB_WRAP_END;
  }
  inline TupleTableSlot *GetSlot() { return stub_.GetSlot(); }
  inline bool HasIndex() const { return stub_.HasIndex(); }

 private:
  paxc::IndexUpdaterInternal stub_;
};
}  // namespace pax