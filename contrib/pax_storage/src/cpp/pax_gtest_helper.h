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
 * pax_gtest_helper.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/pax_gtest_helper.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <vector>

#include "storage/proto/proto_wrappers.h"

namespace pax::tests {

// 3 clomun - string(len 100), string(len 100), int(len 4)
#define COLUMN_NUMS 3
#define COLUMN_SIZE 100
#define INT32_COLUMN_VALUE 0x123
#define INT32_COLUMN_VALUE_DEFAULT 0x001

#define TOAST_COLUMN_NUMS 4
#define NO_TOAST_COLUMN_SIZE 14
#define COMPRESS_TOAST_COLUMN_SIZE 516
#define EXTERNAL_TOAST_COLUMN_SIZE 772
#define EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE 1028

extern void CreateMemoryContext();
extern void CreateTestResourceOwner();
extern void ReleaseTestResourceOwner();
extern TupleTableSlot *CreateTestTupleTableSlot(bool with_value = true, int ncols = COLUMN_NUMS);
extern TupleTableSlot *CreateTestToastTupleTableSlot();
#ifdef VEC_BUILD
extern TupleTableSlot *CreateVecEmptyTupleSlot(TupleDesc tuple_desc);
#endif
extern bool VerifyTestTupleTableSlot(TupleTableSlot *tuple_slot);
extern bool VerifyTestTupleTableSlot(TupleTableSlot *tuple_slot, int attrno);
extern void DeleteTestTupleTableSlot(TupleTableSlot *tuple_slot);

extern void GenTextBuffer(char *buffer, size_t length);
extern std::vector<pax::porc::proto::Type_Kind> CreateTestSchemaTypes();

extern void InitAttribute_text(Form_pg_attribute attr);
extern void InitAttribute_int4(Form_pg_attribute attr);
extern void InitAttribute_int8(Form_pg_attribute attr);
}  // namespace pax::tests
