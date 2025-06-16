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
 * orc_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_test.cc
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
#include "pax_gtest_helper.h"
#include "storage/local_file_system.h"
#include "storage/filter/pax_filter.h"
#include "storage/toast/pax_toast.h"
#include "stub.h"

namespace pax::tests {

using namespace pax;
#define PROJECTION_COLUMN 2
#define PROJECTION_COLUMN_SINGLE 1

class OrcTest : public ::testing::Test {
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

  static void VerifySingleStripe(PaxColumns *columns,
                                 const std::vector<bool> &proj_map=std::vector<bool>()) {
    char column_buff[COLUMN_SIZE];
    struct varlena *vl = nullptr;
    struct varlena *tunpacked = nullptr;
    int read_len = -1;
    char *read_data = nullptr;

    GenTextBuffer(column_buff, COLUMN_SIZE);

    EXPECT_EQ(static_cast<size_t>(COLUMN_NUMS), columns->GetColumns());

    if (proj_map.empty() || proj_map[0]) {
      auto column1 = static_cast<PaxNonFixedColumn*>((*columns)[0].get());
      EXPECT_EQ(1UL, column1->GetNonNullRows());
      char *column1_buffer = column1->GetBuffer(0).first;
      EXPECT_EQ(
          0, std::memcmp(column1_buffer + VARHDRSZ, column_buff, COLUMN_SIZE));
      vl = (struct varlena *)DatumGetPointer(column1_buffer);
      tunpacked = pg_detoast_datum_packed(vl);
      EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);
      read_len = VARSIZE(tunpacked);
      read_data = VARDATA_ANY(tunpacked);
      // read_len is COLUMN_SIZE + VARHDRSZ
      // because DatumFromCString set it
      EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
      EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
      // read_data should pass the pointer rather than memcpy
      EXPECT_EQ(read_data, column1_buffer + VARHDRSZ);
    }

    if (proj_map.empty() || proj_map[1]) {
      auto column2 = static_cast<PaxNonFixedColumn*>((*columns)[1].get());
      char *column2_buffer = column2->GetBuffer(0).first;
      EXPECT_EQ(1UL, column2->GetNonNullRows());
      EXPECT_EQ(
          0, std::memcmp(column2_buffer + VARHDRSZ, column_buff, COLUMN_SIZE));
      vl = (struct varlena *)DatumGetPointer(column2_buffer);
      tunpacked = pg_detoast_datum_packed(vl);
      EXPECT_EQ((Pointer)vl, (Pointer)tunpacked);
      read_len = VARSIZE(tunpacked);
      read_data = VARDATA_ANY(tunpacked);
      EXPECT_EQ(read_len, COLUMN_SIZE + VARHDRSZ);
      EXPECT_EQ(0, std::memcmp(read_data, column_buff, COLUMN_SIZE));
      EXPECT_EQ(read_data, column2_buffer + VARHDRSZ);
    }

    if (proj_map.empty() || proj_map[2]) {
      auto column3 = static_cast<PaxCommColumn<int32>*>((*columns)[2].get());
      std::tie(read_data, read_len) = column3->GetBuffer();
      EXPECT_EQ(4, read_len);
      EXPECT_EQ(INT32_COLUMN_VALUE, *(int32 *)read_data);
    }
  }

 protected:
  const std::string file_name_ = "./test.file";
};

class OrcTestProjection
    : public ::testing::TestWithParam<::testing::tuple<uint8, bool>> {
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

 protected:
  const std::string file_name_ = "./test.file";
};

TEST_F(OrcTest, WriteTuple) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  OrcWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), std::move(file_ptr));

  writer->WriteTuple(tuple_slot);
  writer->Close();

  DeleteTestTupleTableSlot(tuple_slot);
}

TEST_F(OrcTest, OpenOrc) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), std::move(file_ptr));

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(std::move(file_ptr));
  reader->Open(reader_options);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadStripes) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  // file_ptr in orc writer will be freed when writer do destruct
  // current OrcWriter::CreateWriter only for test
  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  // file_ptr in orc reader will be freed when reader do destruct
  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  auto group = reader->ReadGroup(0);
  auto columns = group->GetAllColumns().get();

  OrcTest::VerifySingleStripe(columns);
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadStripesTwice) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  auto group = reader->ReadGroup(0);
  auto columns = group->GetAllColumns().get();

  reader->Close();
  char column_buff[COLUMN_SIZE];

  GenTextBuffer(column_buff, COLUMN_SIZE);

  EXPECT_EQ(static_cast<size_t>(COLUMN_NUMS), columns->GetColumns());
  auto column1 = static_cast<PaxNonFixedColumn*>((*columns)[0].get());
  auto column2 = static_cast<PaxNonFixedColumn*>((*columns)[1].get());

  EXPECT_EQ(2UL, column1->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(0).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(1).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(2UL, column2->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column2->GetBuffer(0).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column2->GetBuffer(1).first + VARHDRSZ, column_buff,
                           COLUMN_SIZE));

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadMultiStripes) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2UL, reader->GetGroupNums());
  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns().get();
  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns().get();
  OrcTest::VerifySingleStripe(columns1);
  OrcTest::VerifySingleStripe(columns2);
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadCloseEmptyOrc) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  writer->WriteTuple(tuple_slot);
  writer->Flush();

  // close without any data
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  auto group = reader->ReadGroup(0);
  auto columns = group->GetAllColumns().get();
  OrcTest::VerifySingleStripe(columns);
  reader->Close();

  delete reader;
}

TEST_F(OrcTest, WriteReadEmptyOrc) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  // flush empty
  writer->Flush();
  // direct close
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(0UL, reader->GetGroupNums());
  reader->Close();

  delete reader;
}

TEST_F(OrcTest, ReadTuple) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());
  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty));
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, GetTuple) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  writer_options.group_limit = 100;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  for (int i = 0; i < 1000; i++) {
    if (i % 5 == 0) {
      tuple_slot->tts_isnull[0] = true;
      tuple_slot->tts_isnull[1] = true;
    } else {
      tuple_slot->tts_isnull[0] = false;
      tuple_slot->tts_isnull[1] = false;
    }
    tuple_slot->tts_values[2] = Int32GetDatum(i);
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  reader->Open(reader_options);
  EXPECT_EQ(10UL, reader->GetGroupNums());

  for (int i = 0; i < 1000; i++) {
    ASSERT_TRUE(reader->GetTuple(tuple_slot_empty, i));
    if (i % 5 == 0) {
      EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
      EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
    } else {
      EXPECT_FALSE(tuple_slot_empty->tts_isnull[0]);
      EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
    }
    EXPECT_EQ(DatumGetInt32(tuple_slot_empty->tts_values[2]), i);
  }

  ASSERT_FALSE(reader->GetTuple(tuple_slot_empty, 1000));
  ASSERT_FALSE(reader->GetTuple(tuple_slot_empty, 10000));
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadTupleWithToast) {
  std::string toast_file_name = std::string(file_name_) + ".toast";
  Singleton<LocalFileSystem>::GetInstance()->Delete(toast_file_name);
  TupleTableSlot *tuple_slot = nullptr, *tuple_slot_empty = nullptr;
  TupleDesc tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) +
                    sizeof(FormData_pg_attribute) * TOAST_COLUMN_NUMS));

  char no_toast_buff[NO_TOAST_COLUMN_SIZE] = {0};
  char compress_toast_buff[COMPRESS_TOAST_COLUMN_SIZE] = {0};
  char external_toast_buff[EXTERNAL_TOAST_COLUMN_SIZE] = {0};
  char external_compress_toast_buff[EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE] = {0};

  int origin_pax_min_size_of_compress_toast = pax_min_size_of_compress_toast;
  int origin_pax_min_size_of_external_toast = pax_min_size_of_external_toast;

  pax_min_size_of_compress_toast = 512;
  pax_min_size_of_external_toast = 1024;

  tuple_desc->natts = TOAST_COLUMN_NUMS;
  InitAttribute_text(&tuple_desc->attrs[0]);
  InitAttribute_text(&tuple_desc->attrs[1]);
  InitAttribute_text(&tuple_desc->attrs[2]);
  InitAttribute_text(&tuple_desc->attrs[3]);
  tuple_desc->attrs[0].attstorage = TYPSTORAGE_EXTENDED;
  tuple_desc->attrs[1].attstorage = TYPSTORAGE_EXTENDED;
  tuple_desc->attrs[2].attstorage = TYPSTORAGE_EXTENDED;
  // column 4 is external but no compress
  tuple_desc->attrs[3].attstorage = TYPSTORAGE_EXTERNAL;

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  GenTextBuffer(no_toast_buff + VARHDRSZ, NO_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(compress_toast_buff + VARHDRSZ,
                COMPRESS_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(external_toast_buff + VARHDRSZ,
                EXTERNAL_TOAST_COLUMN_SIZE - VARHDRSZ);
  GenTextBuffer(external_compress_toast_buff + VARHDRSZ,
                EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE - VARHDRSZ);

  SET_VARSIZE(no_toast_buff, NO_TOAST_COLUMN_SIZE);
  SET_VARSIZE(compress_toast_buff, COMPRESS_TOAST_COLUMN_SIZE);
  SET_VARSIZE(external_toast_buff, EXTERNAL_TOAST_COLUMN_SIZE);
  SET_VARSIZE(external_compress_toast_buff,
              EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE);

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;
  tuple_slot->tts_isnull[2] = false;
  tuple_slot->tts_isnull[3] = false;

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::shared_ptr<File> toast_file_ptr = local_fs->Open(toast_file_name, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  writer_options.group_limit = 20;

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  std::vector<pax::porc::proto::Type_Kind> types_for_read = types;

  auto writer = OrcWriter::CreateWriter(writer_options, std::move(types),
                                        file_ptr, toast_file_ptr);
  for (int i = 0; i < 106; i++) {
    switch (i % 3) {
      case 0: {
        tuple_slot->tts_values[0] = PointerGetDatum(no_toast_buff);
        tuple_slot->tts_values[1] = PointerGetDatum(compress_toast_buff);
        tuple_slot->tts_values[2] =
            PointerGetDatum(external_compress_toast_buff);
        break;
      }
      case 1: {
        tuple_slot->tts_values[0] = PointerGetDatum(compress_toast_buff);
        tuple_slot->tts_values[1] =
            PointerGetDatum(external_compress_toast_buff);
        tuple_slot->tts_values[2] = PointerGetDatum(no_toast_buff);
        break;
      }
      case 2: {
        tuple_slot->tts_values[0] =
            PointerGetDatum(external_compress_toast_buff);
        tuple_slot->tts_values[1] = PointerGetDatum(no_toast_buff);
        tuple_slot->tts_values[2] = PointerGetDatum(compress_toast_buff);
        break;
      }
      default:
        ASSERT_FALSE(true);
    }

    tuple_slot->tts_values[3] = PointerGetDatum(external_toast_buff);
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  // begin full read without projection
  file_ptr = local_fs->Open(file_name_, fs::kReadMode);
  EXPECT_NE(nullptr, file_ptr);

  toast_file_ptr = local_fs->Open(toast_file_name, fs::kReadMode);
  EXPECT_NE(nullptr, file_ptr);
  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr, toast_file_ptr);
  tuple_slot_empty = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);

  reader->Open(reader_options);
  EXPECT_EQ(6UL, reader->GetGroupNums());

  for (int i = 0; i < 106; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot_empty));
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[0]);
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[2]);
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[3]);

    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[0]));
    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[1]));
    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[2]));
    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[3]));
    switch (i % 3) {
      case 0: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[0]), 10UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 512UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 1024UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[0]),
                           no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           compress_toast_buff,
                           COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           external_compress_toast_buff,
                           EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      case 1: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[0]), 512UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 1024UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 10UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[0]),
                           compress_toast_buff,
                           COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           external_compress_toast_buff,
                           EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      case 2: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[0]), 1024UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 10UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 512UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[0]),
                           external_compress_toast_buff,
                           EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           compress_toast_buff,
                           COMPRESS_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      default:
        ASSERT_FALSE(true);
    }

    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[3]), 768UL + 4);
    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[3]),
                       external_toast_buff, EXTERNAL_TOAST_COLUMN_SIZE) == 0);
  }
  ASSERT_FALSE(reader->ReadTuple(tuple_slot_empty));

  reader->Close();
  delete reader;

  // begin read with projection
  file_ptr = local_fs->Open(file_name_, fs::kReadMode);
  EXPECT_NE(nullptr, file_ptr);

  toast_file_ptr = local_fs->Open(toast_file_name, fs::kReadMode);
  EXPECT_NE(nullptr, file_ptr);
  std::vector<bool> projection = {false, true, true, true};
  std::shared_ptr<PaxFilter> filter = std::make_shared<PaxFilter>();

  filter->SetColumnProjection(std::move(projection));
  reader_options.filter = filter;
  reader = new OrcReader(file_ptr, toast_file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(6UL, reader->GetGroupNums());

  for (int i = 0; i < 106; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot_empty));
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[2]);
    EXPECT_FALSE(tuple_slot_empty->tts_isnull[3]);

    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[1]));
    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[2]));
    ASSERT_FALSE(VARATT_IS_PAX_SUPPORT_TOAST(tuple_slot_empty->tts_values[3]));
    switch (i % 3) {
      case 0: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 512UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 1024UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           compress_toast_buff,
                           COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           external_compress_toast_buff,
                           EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      case 1: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 1024UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 10UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           external_compress_toast_buff,
                           EXTERNAL_COMPRESS_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      case 2: {
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[1]), 10UL + 4);
        ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[2]), 512UL + 4);

        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[1]),
                           no_toast_buff, NO_TOAST_COLUMN_SIZE) == 0);
        ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[2]),
                           compress_toast_buff,
                           COMPRESS_TOAST_COLUMN_SIZE) == 0);
        break;
      }
      default:
        ASSERT_FALSE(true);
    }

    ASSERT_EQ(VARSIZE_ANY(tuple_slot_empty->tts_values[3]), 768UL + 4);
    ASSERT_TRUE(memcmp(DatumGetPointer(tuple_slot_empty->tts_values[3]),
                       external_toast_buff, EXTERNAL_TOAST_COLUMN_SIZE) == 0);
  }
  ASSERT_FALSE(reader->ReadTuple(tuple_slot_empty));

  ExecDropSingleTupleTableSlot(tuple_slot_empty);
  ExecDropSingleTupleTableSlot(tuple_slot);
  delete reader;

  pax_min_size_of_compress_toast = origin_pax_min_size_of_compress_toast;
  pax_min_size_of_external_toast = origin_pax_min_size_of_external_toast;
  Singleton<LocalFileSystem>::GetInstance()->Delete(toast_file_name);
}

class OrcEncodingTest : public ::testing::TestWithParam<ColumnEncoding_Kind> {
  void SetUp() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);

    CreateMemoryContext();
    CreateTestResourceOwner();
  }

  void TearDown() override {
    Singleton<LocalFileSystem>::GetInstance()->Delete(file_name_);
    ReleaseTestResourceOwner();
  }

 protected:
  const std::string file_name_ = "./test_encoding.file";
};

class OrcCompressTest : public OrcEncodingTest {};

TEST_P(OrcEncodingTest, ReadTupleWithEncoding) {
  TupleTableSlot *tuple_slot = nullptr;
  auto encoding_kind = GetParam();

  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

  tuple_desc->natts = 2;
  InitAttribute_int8(&tuple_desc->attrs[0]);
  InitAttribute_int8(&tuple_desc->attrs[1]);

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  bool *fake_is_null =
      reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
  fake_is_null[0] = false;
  fake_is_null[1] = false;

  tuple_slot->tts_values[0] = Int64GetDatum(0);
  tuple_slot->tts_values[1] = Int64GetDatum(1);
  tuple_slot->tts_isnull = fake_is_null;

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_LONG);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_LONG);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 0));
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 0));
  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.encoding_opts = types_encoding;
  writer_options.rel_tuple_desc = tuple_desc;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (size_t i = 0; i < 10000; i++) {
    tuple_slot->tts_values[0] = Int64GetDatum(i);
    tuple_slot->tts_values[1] = Int64GetDatum(i + 1);
    writer->WriteTuple(tuple_slot);
  }

  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());
  for (size_t i = 0; i < 10000; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot));
    ASSERT_EQ(tuple_slot->tts_values[0], i);
    ASSERT_EQ(tuple_slot->tts_values[1], i + 1);
  }
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete writer;
  delete reader;
}

INSTANTIATE_TEST_SUITE_P(
    OrcEncodingTestCombine, OrcEncodingTest,
    testing::Values(ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED,
                    ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED,
                    ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2,
                    ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD));

TEST_P(OrcCompressTest, ReadTupleWithCompress) {
  TupleTableSlot *tuple_slot = nullptr;
  auto encoding_kind = GetParam();
  char column_buff_str[COLUMN_SIZE];

  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

  tuple_desc->natts = 2;
  InitAttribute_text(&tuple_desc->attrs[0]);
  InitAttribute_text(&tuple_desc->attrs[1]);

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  bool *fake_is_null =
      reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
  fake_is_null[0] = false;
  fake_is_null[1] = false;

  tuple_slot->tts_isnull = fake_is_null;

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_STRING);
  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 5));
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 5));
  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.encoding_opts = types_encoding;
  writer_options.rel_tuple_desc = tuple_desc;

  auto writer = new OrcWriter(writer_options, types, file_ptr);

  for (size_t i = 0; i < COLUMN_SIZE; i++) {
    column_buff_str[i] = i;
  }

  for (size_t i = 0; i < 1000; i++) {
    tuple_slot->tts_values[0] =
        cbdb::DatumFromCString(column_buff_str, COLUMN_SIZE);
    tuple_slot->tts_values[1] =
        cbdb::DatumFromCString(column_buff_str, COLUMN_SIZE);
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  ASSERT_EQ(1UL, reader->GetGroupNums());
  auto group = reader->ReadGroup(0);
  auto columns = group->GetAllColumns().get();

  ASSERT_EQ(2UL, columns->GetColumns());

  auto column1 = static_cast<PaxNonFixedColumn*>((*columns)[0].get());
  ASSERT_EQ(1000UL, column1->GetNonNullRows());

  auto column2 = static_cast<PaxNonFixedColumn*>((*columns)[1].get());
  ASSERT_EQ(1000UL, column2->GetNonNullRows());

  for (size_t i = 0; i < 1000; i++) {
    EXPECT_EQ(0, std::memcmp(column1->GetBuffer(i).first + VARHDRSZ,
                             column_buff_str, COLUMN_SIZE));

    EXPECT_EQ(0, std::memcmp(column2->GetBuffer(i).first + VARHDRSZ,
                             column_buff_str, COLUMN_SIZE));
  }

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete writer;
  delete reader;
}

INSTANTIATE_TEST_SUITE_P(
    OrcEncodingTestCombine, OrcCompressTest,
    testing::Values(ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD,
                    ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB));

TEST_F(OrcTest, ReadTupleDefaultColumn) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());

  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false, 4);
  InitAttribute_int4(&tuple_slot_empty->tts_tupleDescriptor->attrs[3]);

  tuple_slot_empty->tts_tupleDescriptor->natts = COLUMN_NUMS + 1;

  tuple_slot_empty->tts_tupleDescriptor->attrs[3].atthasmissing = true;
  tuple_slot_empty->tts_tupleDescriptor->constr =
      reinterpret_cast<TupleConstr *>(cbdb::Palloc0(sizeof(TupleConstr)));
  tuple_slot_empty->tts_tupleDescriptor->constr->missing =
      reinterpret_cast<AttrMissing *>(
          cbdb::Palloc0((COLUMN_NUMS + 1) * sizeof(AttrMissing)));

  tuple_slot_empty->tts_tupleDescriptor->constr->missing[3].am_value =
      cbdb::Int32ToDatum(INT32_COLUMN_VALUE_DEFAULT);
  tuple_slot_empty->tts_tupleDescriptor->constr->missing[3].am_present = true;
  reader->ReadTuple(tuple_slot_empty);

  ASSERT_EQ(tuple_slot_empty->tts_values[3],
            static_cast<size_t>(INT32_COLUMN_VALUE_DEFAULT));

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, ReadTupleDroppedColumn) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);
  tuple_slot_empty->tts_tupleDescriptor->attrs[2].attisdropped = true;
  reader->ReadTuple(tuple_slot_empty);
  ASSERT_EQ(tuple_slot_empty->tts_isnull[2], true);

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, ReadTupleDroppedColumnWithProjection) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot(true);
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  tuple_slot_empty->tts_tupleDescriptor->attrs[2].attisdropped = true;

  reader->ReadTuple(tuple_slot_empty);

  ASSERT_EQ(tuple_slot_empty->tts_isnull[2], true);

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadBigTuple) {
  TupleTableSlot *tuple_slot = nullptr;
  auto tuple_desc = reinterpret_cast<TupleDescData *>(
      cbdb::Palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2));

  tuple_desc->natts = 2;
  InitAttribute_int4(&tuple_desc->attrs[0]);
  InitAttribute_int4(&tuple_desc->attrs[1]);

  tuple_slot = MakeTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  bool *fake_is_null =
      reinterpret_cast<bool *>(cbdb::Palloc0(sizeof(bool) * COLUMN_NUMS));
  fake_is_null[0] = false;
  fake_is_null[1] = false;

  tuple_slot->tts_values[0] = Int32GetDatum(0);
  tuple_slot->tts_values[1] = Int32GetDatum(1);
  tuple_slot->tts_isnull = fake_is_null;

  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  std::vector<pax::porc::proto::Type_Kind> types;
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_INT);
  types.emplace_back(pax::porc::proto::Type_Kind::Type_Kind_INT);
  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_desc;

  auto writer = OrcWriter::CreateWriter(writer_options, types, file_ptr);

  for (size_t i = 0; i < 10000; i++) {
    tuple_slot->tts_values[0] = Int32GetDatum(i);
    tuple_slot->tts_values[1] = Int32GetDatum(i + 1);
    writer->WriteTuple(tuple_slot);
  }

  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  EXPECT_EQ(1UL, reader->GetGroupNums());
  for (size_t i = 0; i < 10000; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot));
    ASSERT_EQ(tuple_slot->tts_values[0], i);
    ASSERT_EQ(tuple_slot->tts_values[1], i + 1);
  }
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadNoFixedColumnInSameTuple) {
  char column_buff_origin[COLUMN_SIZE];
  char column_buff_reset[COLUMN_SIZE];

  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);

  // using the same tuple slot with different data
  cbdb::Pfree(cbdb::DatumToPointer(tuple_slot->tts_values[0]));
  memset(&column_buff_reset, 0, COLUMN_SIZE);
  tuple_slot->tts_values[0] =
      cbdb::DatumFromCString(column_buff_reset, COLUMN_SIZE);

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  auto group = reader->ReadGroup(0);
  auto columns = group->GetAllColumns().get();

  EXPECT_EQ(static_cast<size_t>(COLUMN_NUMS), columns->GetColumns());
  auto column1 = static_cast<PaxNonFixedColumn*>((*columns)[0].get());

  GenTextBuffer(column_buff_origin, COLUMN_SIZE);

  EXPECT_EQ(2UL, column1->GetNonNullRows());
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(0).first + VARHDRSZ,
                           column_buff_origin, COLUMN_SIZE));
  EXPECT_EQ(0, std::memcmp(column1->GetBuffer(1).first + VARHDRSZ,
                           column_buff_reset, COLUMN_SIZE));

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadWithNullField) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  OrcWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  // str str int
  // null null int
  // str str null
  // null null null
  writer->WriteTuple(tuple_slot);

  tuple_slot->tts_isnull[0] = true;
  tuple_slot->tts_isnull[1] = true;
  tuple_slot->tts_isnull[2] = false;
  writer->WriteTuple(tuple_slot);

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;
  tuple_slot->tts_isnull[2] = true;
  writer->WriteTuple(tuple_slot);

  tuple_slot->tts_isnull[0] = true;
  tuple_slot->tts_isnull[1] = true;
  tuple_slot->tts_isnull[2] = true;
  writer->WriteTuple(tuple_slot);

  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  EXPECT_EQ(1UL, reader->GetGroupNums());

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[2]);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty));

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[2]);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty, 3));

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[2]);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty, 1));
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty, 2));

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[2]);

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadWithBoundNullField) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  OrcWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  // null null null
  // str str int
  // null null null
  tuple_slot->tts_isnull[0] = true;
  tuple_slot->tts_isnull[1] = true;
  tuple_slot->tts_isnull[2] = true;
  writer->WriteTuple(tuple_slot);

  tuple_slot->tts_isnull[0] = false;
  tuple_slot->tts_isnull[1] = false;
  tuple_slot->tts_isnull[2] = false;
  writer->WriteTuple(tuple_slot);

  tuple_slot->tts_isnull[0] = true;
  tuple_slot->tts_isnull[1] = true;
  tuple_slot->tts_isnull[2] = true;
  writer->WriteTuple(tuple_slot);

  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  EXPECT_EQ(1UL, reader->GetGroupNums());

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[2]);

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_FALSE(tuple_slot_empty->tts_isnull[2]);
  EXPECT_TRUE(VerifyTestTupleTableSlot(tuple_slot_empty));

  reader->ReadTuple(tuple_slot_empty);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
  EXPECT_TRUE(tuple_slot_empty->tts_isnull[2]);

  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_F(OrcTest, WriteReadWithALLNullField) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  OrcWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  tuple_slot->tts_isnull[0] = true;
  tuple_slot->tts_isnull[1] = true;
  tuple_slot->tts_isnull[2] = true;
  for (size_t i = 0; i < 1000; i++) {
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  MicroPartitionReader::ReaderOptions reader_options;
  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);

  EXPECT_EQ(1UL, reader->GetGroupNums());
  for (size_t i = 0; i < 1000; i++) {
    reader->ReadTuple(tuple_slot_empty);
    EXPECT_TRUE(tuple_slot_empty->tts_isnull[0]);
    EXPECT_TRUE(tuple_slot_empty->tts_isnull[1]);
    EXPECT_TRUE(tuple_slot_empty->tts_isnull[2]);
  }
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot_empty);
  DeleteTestTupleTableSlot(tuple_slot);
  delete reader;
}

TEST_P(OrcTestProjection, ReadTupleWithProjectionColumn) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);
  size_t proj_index = ::testing::get<0>(GetParam());
  auto reversal = ::testing::get<1>(GetParam());
  std::vector<bool> proj_map(COLUMN_NUMS, false);

  ASSERT_LE(proj_index, static_cast<size_t>(COLUMN_NUMS));
  ASSERT_GE(proj_index, 0UL);
  if (reversal) {
    proj_map = std::vector<bool>(COLUMN_NUMS, true);
  }

  if (proj_index < COLUMN_NUMS) {
    proj_map[proj_index] = !proj_map[proj_index];
  }

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);

  writer->WriteTuple(tuple_slot);
  writer->Flush();

  writer->WriteTuple(tuple_slot);
  writer->Close();

  file_ptr = local_fs->Open(file_name_, fs::kReadMode);

  auto pax_filter = std::make_shared<PaxFilter>();
  pax_filter->SetColumnProjection(std::vector<bool>(proj_map));
  MicroPartitionReader::ReaderOptions reader_options;
  reader_options.filter = pax_filter;

  auto reader = new OrcReader(file_ptr);
  reader->Open(reader_options);

  EXPECT_EQ(2UL, reader->GetGroupNums());

  auto group1 = reader->ReadGroup(0);
  auto columns1 = group1->GetAllColumns().get();

  auto group2 = reader->ReadGroup(1);
  auto columns2 = group2->GetAllColumns().get();

  OrcTest::VerifySingleStripe(columns1, proj_map);
  OrcTest::VerifySingleStripe(columns2, proj_map);
  reader->Close();

  DeleteTestTupleTableSlot(tuple_slot);

  delete reader;
}

INSTANTIATE_TEST_SUITE_P(OrcTestProjectionCombine, OrcTestProjection,
                         testing::Combine(testing::Values(0, 1, 2, 3),
                                          testing::Values(false, true)));

TEST_P(OrcEncodingTest, WriterMerge) {
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  const std::string file1_name = "./test1.file";
  const std::string file2_name = "./test2.file";
  const std::string file3_name = "./test3.file";
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);
  auto encoding_kind = GetParam();

  remove(file1_name.c_str());
  remove(file2_name.c_str());
  remove(file3_name.c_str());

  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file1_ptr = local_fs->Open(file1_name, fs::kReadWriteMode);
  std::shared_ptr<File> file2_ptr = local_fs->Open(file2_name, fs::kReadWriteMode);
  std::shared_ptr<File> file3_ptr = local_fs->Open(file3_name, fs::kReadWriteMode);
  EXPECT_NE(nullptr, file1_ptr);
  EXPECT_NE(nullptr, file2_ptr);
  EXPECT_NE(nullptr, file3_ptr);

  OrcWriter::WriterOptions writer_options;
  std::vector<std::tuple<ColumnEncoding_Kind, int>> types_encoding;
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 0));
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 0));
  types_encoding.emplace_back(std::make_tuple(encoding_kind, 0));
  writer_options.encoding_opts = types_encoding;
  writer_options.group_limit = 100;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;

  auto *writer1 = new OrcWriter(writer_options,
                                std::move(CreateTestSchemaTypes()), file1_ptr);
  auto *writer2 = new OrcWriter(writer_options,
                                std::move(CreateTestSchemaTypes()), file2_ptr);
  auto *writer3 = new OrcWriter(writer_options,
                                std::move(CreateTestSchemaTypes()), file3_ptr);

  // two group + 51 rows in memory
  for (size_t i = 0; i < 251; i++) {
    if (i % 3 == 0) {
      tuple_slot->tts_isnull[0] = true;
      tuple_slot->tts_isnull[1] = true;
      tuple_slot->tts_isnull[2] = true;
    } else {
      tuple_slot->tts_isnull[0] = false;
      tuple_slot->tts_isnull[1] = false;
      tuple_slot->tts_isnull[2] = false;
    }
    writer1->WriteTuple(tuple_slot);
    writer2->WriteTuple(tuple_slot);
  }

  for (size_t i = 0; i < 20; i++) {
    writer3->WriteTuple(tuple_slot);
  }

  // writer1 merge writer2, writer3 merge writer1
  // writer3 must contains all of datas
  writer1->MergeTo(writer2);
  writer3->MergeTo(writer1);
  writer3->Close();

  delete writer1;
  delete writer2;
  delete writer3;

  ASSERT_NE(0, access(file1_name.c_str(), 0));
  ASSERT_NE(0, access(file2_name.c_str(), 0));

  MicroPartitionReader::ReaderOptions reader_options;
  file3_ptr = local_fs->Open(file3_name, fs::kReadMode);

  auto reader = new OrcReader(file3_ptr);
  reader->Open(reader_options);

  // no memory merge
  ASSERT_EQ(7UL, reader->GetGroupNums());

  size_t total_rows = (251 * 2) + 20;

  for (size_t i = 0; i < total_rows; i++) {
    ASSERT_TRUE(reader->ReadTuple(tuple_slot_empty));
    if (!tuple_slot_empty->tts_isnull[0]) {
      VerifyTestTupleTableSlot(tuple_slot_empty);
    }
  }

  ASSERT_FALSE(reader->ReadTuple(tuple_slot_empty));

  DeleteTestTupleTableSlot(tuple_slot);
  DeleteTestTupleTableSlot(tuple_slot_empty);

  reader->Close();
  delete reader;

  // only remain file3
  remove(file3_name.c_str());
}

namespace exceptions {

static int current_pb_func_call_times = 0;
static int target_pb_func_call_times = 0;

static inline bool MockPbFuncCallTimes() {
  if (target_pb_func_call_times <= current_pb_func_call_times) {
    current_pb_func_call_times = 0;
    return false;
  }

  current_pb_func_call_times++;
  return true;
}

bool MockSerializeToZeroCopyStream(
    ::google::protobuf::io::ZeroCopyOutputStream *output) {
  return MockPbFuncCallTimes();
}

TEST_F(OrcTest, WriteException) {
  bool get_exception = false;
  Stub *stub;
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();

  stub = new Stub();
  auto create_test_writer = [&]() {
    OrcWriter::WriterOptions writer_options;
    auto local_fs = Singleton<LocalFileSystem>::GetInstance();
    local_fs->Delete(file_name_);
    auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);

    writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
    auto writer = OrcWriter::CreateWriter(
        writer_options, std::move(CreateTestSchemaTypes()), std::move(file_ptr));

    return writer;
  };

  stub->set(ADDR(::google::protobuf::MessageLite, SerializeToZeroCopyStream),
            MockSerializeToZeroCopyStream);

  // 1. check serialize STRIPE FOOTER
  auto writer = create_test_writer();
  target_pb_func_call_times = 0;
  try {
    writer->WriteTuple(tuple_slot);
    writer->Close();
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to serialize the STRIPE FOOTER"),
              std::string::npos);
    ASSERT_NE(exception_str.find("mem used="), std::string::npos);
    ASSERT_NE(exception_str.find("mem avail="), std::string::npos);
    ASSERT_NE(exception_str.find("path="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 2. check serialize FOOTER
  writer = create_test_writer();
  target_pb_func_call_times = 1;

  try {
    writer->WriteTuple(tuple_slot);
    writer->Close();
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to serialize the FOOTER"),
              std::string::npos);
    ASSERT_NE(exception_str.find("mem used="), std::string::npos);
    ASSERT_NE(exception_str.find("mem avail="), std::string::npos);
    ASSERT_NE(exception_str.find("path="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 3. check serialize POSTSCRIPT
  writer = create_test_writer();
  target_pb_func_call_times = 2;

  try {
    writer->WriteTuple(tuple_slot);
    writer->Close();
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to serialize the POSTSCRIPT"),
              std::string::npos);
    ASSERT_NE(exception_str.find("mem used="), std::string::npos);
    ASSERT_NE(exception_str.find("mem avail="), std::string::npos);
    ASSERT_NE(exception_str.find("path="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  delete stub;
  DeleteTestTupleTableSlot(tuple_slot);
}

bool MockParseFromArray(const void *data, int size) {
  return MockPbFuncCallTimes();
}

bool MockParseFromZeroCopyStream(
    ::google::protobuf::io::ZeroCopyInputStream *input) {
  return MockPbFuncCallTimes();
}

TEST_F(OrcTest, ReadException) {
  bool get_exception = false;
  Stub *stub;
  MicroPartitionReader::ReaderOptions reader_options;
  TupleTableSlot *tuple_slot = CreateTestTupleTableSlot();
  TupleTableSlot *tuple_slot_empty = CreateTestTupleTableSlot(false);
  auto local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  std::shared_ptr<File> file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  current_pb_func_call_times = 0;
  target_pb_func_call_times = 0;

  MicroPartitionWriter::WriterOptions writer_options;
  writer_options.rel_tuple_desc = tuple_slot->tts_tupleDescriptor;
  writer_options.group_limit = 10;

  auto writer = OrcWriter::CreateWriter(
      writer_options, std::move(CreateTestSchemaTypes()), file_ptr);
  for (int i = 0; i < 50; i++) {
    writer->WriteTuple(tuple_slot);
  }
  writer->Close();

  DeleteTestTupleTableSlot(tuple_slot);

  auto create_test_reader = [&]() {
    auto local_fs = Singleton<LocalFileSystem>::GetInstance();
    ;
    auto file_ptr = local_fs->Open(file_name_, fs::kReadMode);

    return std::make_unique<OrcReader>(std::move(file_ptr));
  };

  stub = new Stub();

  // 1. failed to parse POSTSCRIPT
  auto reader = create_test_reader();
  stub->set(ADDR(::google::protobuf::MessageLite, ParseFromArray),
            MockParseFromArray);
  try {
    reader->Open(reader_options);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to parse the POSTSCRIPT"),
              std::string::npos);
    ASSERT_NE(exception_str.find("offset="), std::string::npos);
    ASSERT_NE(exception_str.find("len="), std::string::npos);
    ASSERT_NE(exception_str.find("file="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  stub->reset(ADDR(::google::protobuf::MessageLite, ParseFromArray));

  // 2. failed to parse FOOTER
  reader = create_test_reader();
  stub->set(ADDR(::google::protobuf::MessageLite, ParseFromZeroCopyStream),
            MockParseFromZeroCopyStream);
  target_pb_func_call_times = 0;
  try {
    reader->Open(reader_options);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to parse the FOOTER"),
              std::string::npos);
    ASSERT_NE(exception_str.find("offset="), std::string::npos);
    ASSERT_NE(exception_str.find("len="), std::string::npos);
    ASSERT_NE(exception_str.find("file="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  stub->reset(ADDR(::google::protobuf::MessageLite, ParseFromZeroCopyStream));

  // 3. failed to parse STRIPE FOOTER
  reader = create_test_reader();
  reader->Open(reader_options);

  // mock pb function after file opened
  stub->set(ADDR(::google::protobuf::MessageLite, ParseFromZeroCopyStream),
            MockParseFromZeroCopyStream);

  try {
    reader->GetTuple(tuple_slot_empty, 25);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to parse the STRIPE FOOTER"),
              std::string::npos);
    ASSERT_NE(exception_str.find("group index="), std::string::npos);
    ASSERT_NE(exception_str.find("offset="), std::string::npos);
    ASSERT_NE(exception_str.find("len="), std::string::npos);
    ASSERT_NE(exception_str.find("file="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  stub->reset(ADDR(::google::protobuf::MessageLite, ParseFromZeroCopyStream));

  delete stub;
  DeleteTestTupleTableSlot(tuple_slot_empty);
}

}  // namespace exceptions

}  // namespace pax::tests
