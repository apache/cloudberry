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
 * comm_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/comm_test.cc
 *
 *-------------------------------------------------------------------------
 */


#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"

namespace pax::tests {
class CommTest : public ::testing::Test {
 public:
  void SetUp() override {
    MemoryContext comm_test_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "CommTestMemoryContext", 1 * 1024 * 1024,
        1 * 1024 * 1024, 1 * 1024 * 1024);
    MemoryContextSwitchTo(comm_test_memory_context);
  }
};

TEST_F(CommTest, TestDeleteOperator) {
  // In standard C++, we allow the delete null operation
  // In Pax, we overloaded the delete operator which used
  // `pfree` to free the memory. `pfree` not allow pass NULL.
  // But we still need to conform the overloaded delete operation
  // to the semantics of c++ which achieve a complete semantic replacement.
  auto obj = new int32();
  delete obj;
  obj = nullptr;
  delete obj;

  auto array_obj = new int32[10];
  delete[] array_obj;
  array_obj = nullptr;
  delete[] array_obj;
}


TEST_F(CommTest, TestNewOperator) {
  auto obj = new bool[0];
  ASSERT_NE(obj, nullptr);
  delete[] obj;

  auto obj2 = cbdb::Palloc(0);
  ASSERT_NE(obj2, nullptr);
  cbdb::Pfree(obj2);

  auto obj3 = cbdb::Palloc0(0);
  ASSERT_NE(obj3, nullptr);
  cbdb::Pfree(obj3);
}

}  // namespace pax::tests