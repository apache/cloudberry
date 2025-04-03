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
 * pax_itemptr_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_itemptr_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include <gtest/gtest.h>

#include "storage/pax_itemptr.h"

#include "comm/cbdb_api.h"

namespace pax::tests {
class PaxItemPtrTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

#define MAX_BIT_NUMBER(nbits) ((1ULL << (nbits)) - 1)

TEST_F(PaxItemPtrTest, ItemPointerLocalIndexBlockNumber) {
  uint32 block;
  uint32 tuple_offsets[] = {0, 1, MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE)};
  for (auto tuple_offset : tuple_offsets) {
    for (block = 0; block <= 0xFFFF; block++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));

      SetBlockNumber(&ctid, block);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
    }
    for (block = MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE) - 0xFFFF;
         block <= MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE); block++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));

      SetBlockNumber(&ctid, block);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
    }
  }
}

TEST_F(PaxItemPtrTest, ItemPointerLocalIndexTupleNumber) {
  uint32 blocks[] = {0, 1, 0xff, 0xfff, MAX_BIT_NUMBER(PAX_BLOCK_BIT_SIZE)};
  uint32 tuple_offset;
  for (auto block : blocks) {
    for (tuple_offset = 0; tuple_offset <= 0xFFFF; tuple_offset++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
    for (tuple_offset = MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE) - 0xFFFF;
         tuple_offset <= MAX_BIT_NUMBER(PAX_TUPLE_BIT_SIZE); tuple_offset++) {
      auto ctid = pax::MakeCTID(block, tuple_offset);
      EXPECT_EQ(block, pax::GetBlockNumber(ctid));
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));

      SetTupleOffset(&ctid, tuple_offset);
      EXPECT_EQ(tuple_offset, pax::GetTupleOffset(ctid));
    }
  }
}

}  // namespace pax::tests
