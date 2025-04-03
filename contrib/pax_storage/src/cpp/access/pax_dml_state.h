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
 * pax_dml_state.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_dml_state.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <memory>

#include "access/pax_deleter.h"
#include "access/pax_inserter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"

namespace pax {
struct PaxDmlState {
  Oid oid;
  CPaxInserter *inserter;
  CPaxDeleter *deleter;
};

class CPaxDmlStateLocal final {
  friend class Singleton<CPaxDmlStateLocal>;

 public:
  static CPaxDmlStateLocal *Instance() {
    return Singleton<CPaxDmlStateLocal>::GetInstance();
  }

  ~CPaxDmlStateLocal() = default;

  void InitDmlState(Relation rel, CmdType operation);
  void FinishDmlState(Relation rel, CmdType operation);

  bool IsInitialized() const { return cbdb::pax_memory_context != nullptr; }
  CPaxInserter *GetInserter(Relation rel);
  CPaxDeleter *GetDeleter(Relation rel, Snapshot snapshot,
                          bool missing_null = false);

  void Reset();

  CPaxDmlStateLocal(const CPaxDmlStateLocal &) = delete;
  CPaxDmlStateLocal &operator=(const CPaxDmlStateLocal &) = delete;

 private:
  struct DmlStateValue {
    std::unique_ptr<CPaxInserter> inserter;
    std::unique_ptr<CPaxDeleter> deleter;
  };

  CPaxDmlStateLocal();
  static void DmlStateResetCallback(void * /*arg*/);

  std::shared_ptr<DmlStateValue> FindDmlState(const Oid &oid);
  std::shared_ptr<DmlStateValue> RemoveDmlState(const Oid &oid);

 private:
  std::unordered_map<Oid, std::shared_ptr<DmlStateValue>> dml_descriptor_tab_;
  Oid last_oid_;
  std::shared_ptr<DmlStateValue> last_state_;

  MemoryContextCallback cb_;
};

}  // namespace pax
