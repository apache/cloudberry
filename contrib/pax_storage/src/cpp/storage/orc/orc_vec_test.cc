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
 * orc_vec_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_vec_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/porc.h"  // NOLINT
#include <cstdio>
#include <random>
#include <string>
#include <utility>

#include "access/tupdesc_details.h"
#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/local_file_system.h"
#include "storage/columns/pax_column_traits.h"
#include "pax_gtest_helper.h"
namespace pax::tests {

#ifdef VEC_BUILD

class OrcVecTest : public ::testing::Test {
 public:
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);

    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

  static TupleTableSlot *CreateFakeTupleSlot() {
    TupleTableSlot *tuple_slot = nullptr;
    auto tuple_desc = reinterpret_cast<TupleDescData *>(cbdb::Palloc0(
        sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

    tuple_desc->natts = 2;
    tuple_desc->attrs[0] = {
        .attlen = -1,
        .attbyval = false,
        .attalign = TYPALIGN_DOUBLE,
        .attstorage = TYPSTORAGE_PLAIN,
    };

    tuple_desc->attrs[1] = {
        .attlen = 4,
        .attbyval = true,
        .attalign = TYPALIGN_INT,
        .attstorage = TYPSTORAGE_PLAIN,
    };

    tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
    bool *fake_is_null =
        reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * 2));
    tuple_slot->tts_isnull = fake_is_null;
    return tuple_slot;
  }

  static void DeleteTupleSlot(TupleTableSlot *tuple_table_slot) {
    ExecDropSingleTupleTableSlot(tuple_table_slot);
  }

 protected:
  const std::string file_name_ = "./test_vec_orc.file";
};

TEST_F(OrcVecTest, WriteReadGroup) {
  TupleTableSlot *tuple_slot = CreateFakeTupleSlot();

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_INT);
  OrcWriter::WriterOptions writer_options;

  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  writer_options.storage_format = PaxStorageFormat::kTypeStoragePorcVec;

  auto writer = OrcWriter::CreateWriter(writer_options, types, file_ptr);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      tuple_slot->tts_isnull[0] = true;
      tuple_slot->tts_isnull[1] = true;
    } else {
      tuple_slot->tts_isnull[0] = false;
      tuple_slot->tts_isnull[1] = false;
    }

    tuple_slot->tts_values[0] =
        cbdb::DatumFromCString((char *)&i, sizeof(uint16));
    tuple_slot->tts_values[1] = Int64GetDatum(i);
    writer->WriteTuple(tuple_slot);
  }

  writer->Flush();

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;

  uint16 i = 10000;
  tuple_slot->tts_values[0] =
      cbdb::DatumFromCString((char *)&i, sizeof(uint16));
  tuple_slot->tts_values[1] = Int64GetDatum(10000);
  writer->WriteTuple(tuple_slot);

  writer->Close();

  DeleteTupleSlot(tuple_slot);

  MicroPartitionReader::ReaderOptions reader_options;

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2UL, reader->GetGroupNums());

  // verify group1
  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns();

  EXPECT_EQ(2UL, columns1->GetColumns());

  auto column1 = static_cast<PaxVecNonFixedColumn*>((*columns1)[0].get());
  auto column2 = static_cast<PaxVecCommColumn<int32>*>((*columns1)[1].get());

  auto column1_data = column1->GetDataBuffer();
  auto column2_data = column2->GetDataBuffer();
  auto column1_offsets = column1->GetOffsetDataBuffer();

  ASSERT_EQ(10000UL, column1->GetRows());
  ASSERT_EQ(10000UL, column2->GetRows());

  ASSERT_TRUE(column1->HasNull());
  ASSERT_TRUE(column2->HasNull());

  ASSERT_EQ(6666UL, column1->GetNonNullRows());
  ASSERT_EQ(6666UL, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0UL, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(10001UL, column1_offsets->GetSize());
  ASSERT_LE(
      static_cast<size_t>((*column1_offsets)[column1_offsets->GetSize() - 1]),
      column1_data->GetSize());

  ASSERT_EQ(10000UL, column2_data->GetSize());

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ((*column2_data)[i], 0);
    } else {
      ASSERT_EQ((*column2_data)[i], i);
    }
  }

  // verify group 2
  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns();

  column1 = static_cast<PaxVecNonFixedColumn*>((*columns2)[0].get());
  column2 = static_cast<PaxVecCommColumn<int32>*>((*columns2)[1].get());

  column1_data = column1->GetDataBuffer();
  column2_data = column2->GetDataBuffer();
  column1_offsets = column1->GetOffsetDataBuffer();

  ASSERT_FALSE(column1->HasNull());
  ASSERT_FALSE(column2->HasNull());

  ASSERT_EQ(1UL, column1->GetNonNullRows());
  ASSERT_EQ(1UL, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);

  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0UL, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(2UL, column1_offsets->GetSize());
  ASSERT_LE(
      static_cast<size_t>((*column1_offsets)[column1_offsets->GetSize() - 1]),
      column1_data->GetSize());
  // still contain memalign size
  ASSERT_EQ(2UL, column2_data->GetSize());

  ASSERT_EQ((*column2_data)[0], 10000);

  delete reader;
}

TEST_F(OrcVecTest, WriteReadGroupWithEncoding) {
  TupleTableSlot *tuple_slot = CreateFakeTupleSlot();

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_INT);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(std::make_tuple(
      ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD, 5));
  types_encoding.emplace_back(
      std::make_tuple(ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2, 0));

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  writer_options.encoding_opts = types_encoding;
  writer_options.storage_format = PaxStorageFormat::kTypeStoragePorcVec;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      tuple_slot->tts_isnull[0] = true;
      tuple_slot->tts_isnull[1] = true;
    } else {
      tuple_slot->tts_isnull[0] = false;
      tuple_slot->tts_isnull[1] = false;
    }

    tuple_slot->tts_values[0] =
        cbdb::DatumFromCString((char *)&i, sizeof(uint16));
    tuple_slot->tts_values[1] = Int64GetDatum(i);
    writer->WriteTuple(tuple_slot);
  }

  writer->Flush();

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;

  uint16 i = 10000;
  tuple_slot->tts_values[0] =
      cbdb::DatumFromCString((char *)&i, sizeof(uint16));
  tuple_slot->tts_values[1] = Int64GetDatum(10000);
  writer->WriteTuple(tuple_slot);

  writer->Close();

  DeleteTupleSlot(tuple_slot);
  delete writer;

  MicroPartitionReader::ReaderOptions reader_options;

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2UL, reader->GetGroupNums());

  // verify group1
  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns();

  EXPECT_EQ(2UL, columns1->GetColumns());

  auto column1 = static_cast<PaxVecNonFixedColumn*>((*columns1)[0].get());
  auto column2 = static_cast<PaxVecCommColumn<int32>*>((*columns1)[1].get());

  auto column1_data = column1->GetDataBuffer();
  auto column2_data = column2->GetDataBuffer();
  auto column1_offsets = column1->GetOffsetDataBuffer();

  ASSERT_EQ(10000UL, column1->GetRows());
  ASSERT_EQ(10000UL, column2->GetRows());

  ASSERT_TRUE(column1->HasNull());
  ASSERT_TRUE(column2->HasNull());

  ASSERT_EQ(6666UL, column1->GetNonNullRows());
  ASSERT_EQ(6666UL, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0UL, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(10001UL, column1_offsets->GetSize());
  ASSERT_LE(
      static_cast<size_t>((*column1_offsets)[column1_offsets->GetSize() - 1]),
      column1_data->GetSize());

  column2->GetBuffer(0);

  for (uint16 i = 0; i < 10000; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(*(int32 *)column2->GetBuffer(i).first, 0);
    } else {
      ASSERT_EQ(*(int32 *)column2->GetBuffer(i).first, i);
    }
  }

  // verify group 2
  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns();

  column1 = static_cast<PaxVecNonFixedColumn*>((*columns2)[0].get());
  column2 = static_cast<PaxVecCommColumn<int32>*>((*columns2)[1].get());

  column1_data = column1->GetDataBuffer();
  column2_data = column2->GetDataBuffer();
  column1_offsets = column1->GetOffsetDataBuffer();

  ASSERT_FALSE(column1->HasNull());
  ASSERT_FALSE(column2->HasNull());

  ASSERT_EQ(1UL, column1->GetNonNullRows());
  ASSERT_EQ(1UL, column2->GetNonNullRows());

  ASSERT_TRUE(column1_data);
  ASSERT_TRUE(column2_data);

  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_TRUE(column1_offsets);

  ASSERT_EQ(0UL, column1_offsets->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column1_data->Capacity() % MEMORY_ALIGN_SIZE);
  ASSERT_EQ(0UL, column2_data->Capacity() % MEMORY_ALIGN_SIZE);

  ASSERT_EQ(2UL, column1_offsets->GetSize());
  ASSERT_LE(
      static_cast<size_t>((*column1_offsets)[column1_offsets->GetSize() - 1]),
      column1_data->GetSize());

  ASSERT_EQ((*column2_data)[0], 10000);

  delete reader;
}

#endif  // VEC_BUILD

}  // namespace pax::tests
