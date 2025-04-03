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
 * pax_updater.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_updater.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxUpdater final {
 public:
  static TM_Result UpdateTuple(const Relation relation, const ItemPointer otid,
                               TupleTableSlot *slot, const CommandId cid,
                               const Snapshot snapshot,
                               const Snapshot crosscheck, const bool wait,
                               TM_FailureData *tmfd, LockTupleMode *lockmode,
                               bool *update_indexes);
};
}  // namespace pax
