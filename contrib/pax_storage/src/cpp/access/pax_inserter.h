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
 * pax_inserter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_inserter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include "storage/micro_partition_metadata.h"
#include "storage/pax.h"
namespace pax {
class PartitionObject;
class CPaxInserter {
 public:
  explicit CPaxInserter(Relation rel);
  virtual ~CPaxInserter() = default;

  static void TupleInsert(Relation relation, TupleTableSlot *slot,
                          CommandId cid, int options, BulkInsertState bistate);

  static void MultiInsert(Relation relation, TupleTableSlot **slots,
                          int ntuples, CommandId cid, int options,
                          BulkInsertState bistate);

  static void FinishBulkInsert(Relation relation, int options);

  void InsertTuple(Relation relation, TupleTableSlot *slot, CommandId cid,
                   int options, BulkInsertState bistate);
  void FinishInsert();

 private:
  Relation rel_;
  uint32 insert_count_;

  std::unique_ptr<TableWriter> writer_;
};  // class CPaxInserter

}  // namespace pax
