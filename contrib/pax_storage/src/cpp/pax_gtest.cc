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
 * pax_gtest.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/pax_gtest.cc
 *
 *-------------------------------------------------------------------------
 */

#include <cstdio>
#include "comm/gtest_wrappers.h"

#include "access/paxc_rel_options.h"
#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "storage/oper/pax_stats.h"
#include "stub.h"

bool MockMinMaxGetStrategyProcinfo(Oid, Oid, Oid *, FmgrInfo *,
                                   StrategyNumber) {
  return false;
}

int32 MockGetFastSequences(Oid) {
  static int32 mock_id = 0;
  return mock_id++;
}

void MockInsertMicroPartitionPlaceHolder(Oid, int) {
  // do nothing
}

void MockDeleteMicroPartitionEntry(Oid, Snapshot, int) {
  // do nothing
}

void MockExecStoreVirtualTuple(TupleTableSlot *) {
  // do nothing
}

std::string MockBuildPaxDirectoryPath(RelFileNode, BackendId) {
  return std::string(".");
}

std::vector<int> MockGetMinMaxColumnIndexes(Relation rel) {
  return std::vector<int>();
}

std::vector<int> MockBloomFilterColumnIndexes(Relation rel) {
  return std::vector<int>();
}

std::vector<std::tuple<pax::ColumnEncoding_Kind, int>> MockGetRelEncodingOptions(Relation rel) {
  return std::vector<std::tuple<pax::ColumnEncoding_Kind, int>>();
}

// Mock global method which is not link from another libarays
void GlobalMock(Stub *stub) {
  stub->set(pax::MinMaxGetPgStrategyProcinfo, MockMinMaxGetStrategyProcinfo);
  stub->set(CPaxGetFastSequences, MockGetFastSequences);
  stub->set(cbdb::BuildPaxDirectoryPath, MockBuildPaxDirectoryPath);
  stub->set(cbdb::InsertMicroPartitionPlaceHolder,
            MockInsertMicroPartitionPlaceHolder);
  stub->set(cbdb::DeleteMicroPartitionEntry, MockDeleteMicroPartitionEntry);
  stub->set(cbdb::GetMinMaxColumnIndexes, MockGetMinMaxColumnIndexes);
  stub->set(cbdb::GetBloomFilterColumnIndexes, MockBloomFilterColumnIndexes);
  stub->set(cbdb::GetRelEncodingOptions, MockGetRelEncodingOptions);
  stub->set(ExecStoreVirtualTuple, MockExecStoreVirtualTuple);
}

int main(int argc, char **argv) {
  Stub *stub_global;
  int rc;
  MemoryContextInit();
  stub_global = new Stub();

  testing::InitGoogleTest(&argc, argv);
  GlobalMock(stub_global);

  rc = RUN_ALL_TESTS();
  MemoryContextReset(TopMemoryContext);
  return rc;
}
