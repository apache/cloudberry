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
 * file_system_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/file_system_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include <gtest/gtest.h>

#include "comm/singleton.h"
#include "storage/local_file_system.h"
#include "stub.h"

namespace pax::tests {
#define PAX_TEST_CMD_LENGTH 2048
#define PAX_TEST_LIST_FILE_NUM 10

class LocalFileSystemTest : public ::testing::Test {
 public:
  void SetUp() override {
    remove(file_name_.c_str());
    remove(file_path_.c_str());
  }

  void TearDown() override {
    remove(file_name_.c_str());
    remove(file_path_.c_str());
  }

 protected:
  const std::string file_name_ = "./test.file";
  const std::string file_path_ = "./test_path";
  const std::string file_full_path_ =
      "./test_path/16400/GPDB_1_302206171/13261/16394";
};

TEST_F(LocalFileSystemTest, Open) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  file_ptr->Close();
}

TEST_F(LocalFileSystemTest, BuildPath) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  auto path = local_fs->BuildPath(file_ptr.get());
  ASSERT_EQ(path, "./test.file");

  file_ptr->Close();
}

TEST_F(LocalFileSystemTest, WriteRead) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  auto write_size = file_ptr->Write("abc", 3);
  ASSERT_EQ(3, write_size);

  file_ptr->Flush();
  file_ptr->Close();
  file_ptr = local_fs->Open(file_name_, fs::kReadMode);
  ASSERT_NE(nullptr, file_ptr);

  char buff[10] = {0};
  auto read_size = file_ptr->Read(buff, 3);
  ASSERT_EQ(3, read_size);
  ASSERT_EQ(strncmp("abc", buff, 3), 0);
}

TEST_F(LocalFileSystemTest, ListDirectory) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    auto f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));
}

TEST_F(LocalFileSystemTest, CopyFile) {
  CBDB_TRY();
  {
    static const char *pax_copy_test_dir = "./copytest";
    static const char *pax_copy_src_path = file_name_.c_str();
    static const char *pax_copy_dst_path = "./copytest/test_dst";

    int result = 0;
    FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

    fs->DeleteDirectory(pax_copy_test_dir, true);
    ASSERT_NE(access(pax_copy_test_dir, F_OK), 0);

    cbdb::MakedirRecursive(pax_copy_test_dir);
    auto src_file = fs->Open(pax_copy_src_path, pax::fs::kWriteMode);

    src_file->Write("abc", 3);
    src_file->Flush();
    src_file->Close();

    src_file = fs->Open(pax_copy_src_path, pax::fs::kReadMode);

    auto dst_file = fs->Open(pax_copy_dst_path, pax::fs::kWriteMode);

    InitFileAccess();
    fs->CopyFile(src_file.get(), dst_file.get());

    src_file->Close();
    dst_file->Close();

    result = access(pax_copy_dst_path, F_OK);
    ASSERT_NE(result, -1);

    struct stat src_stat, dst_stat;

    stat(pax_copy_src_path, &src_stat);
    stat(pax_copy_dst_path, &dst_stat);
    ASSERT_EQ(src_stat.st_size, dst_stat.st_size);

    fs->DeleteDirectory(pax_copy_test_dir, true);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

TEST_F(LocalFileSystemTest, MakedirRecursive) {
  int result = 0;
  struct stat st {};
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  fs->DeleteDirectory(file_full_path_, true);
  ASSERT_NE(access(file_full_path_.c_str(), F_OK), 0);

  cbdb::MakedirRecursive(file_full_path_.c_str());
  result = stat(file_full_path_.c_str(), &st);
  ASSERT_EQ(result, 0);
}

TEST_F(LocalFileSystemTest, CreateDeleteDirectory) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    auto f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);
}

TEST_F(LocalFileSystemTest, DeleteDirectoryReserveToplevel) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    auto f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));

  fs->DeleteDirectory(file_path_, false);
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), 0UL);
}

namespace exceptions {

ssize_t MockWrite(int __fd, const void *__buf, size_t __n) { return -1; }

ssize_t MockWrite2(int __fd, const void *__buf, size_t __n) { return __n - 1; }

ssize_t MockRead(int __fd, void *__buf, size_t __nbytes) { return -1; }

ssize_t MockRead2(int __fd, void *__buf, size_t __nbytes) {
  return __nbytes - 1;
}

ssize_t MockPWrite(int __fd, const void *__buf, size_t __n, off_t __offset) {
  return -1;
}

ssize_t MockPWrite2(int __fd, const void *__buf, size_t __n, off_t __offset) {
  return __n - 1;
}

ssize_t MockPRead(int __fd, void *__buf, size_t __nbytes, off_t __offset) {
  return -1;
}

ssize_t MockPRead2(int __fd, void *__buf, size_t __nbytes, off_t __offset) {
  return __nbytes - 1;
}

int MockFSync(int __fd) { return -1; }

int MockClose(int __fd) { return -1; }

TEST_F(LocalFileSystemTest, TestFileException) {
  Stub *stub;
  char buffer[50] = {0};
  bool get_exception = false;
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  stub = new Stub();

  // 1. check the pwrite
  stub->set(pwrite, MockPWrite);

  try {
    file_ptr->PWriteN(buffer, 50, -1);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to pwrite"), std::string::npos);
    ASSERT_NE(exception_str.find("offset=-1"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 2. check the pwriten
  stub->reset(pwrite);
  stub->set(pwrite, MockPWrite2);

  try {
    file_ptr->PWriteN(buffer, 50, -1);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to PWriteN"), std::string::npos);
    ASSERT_NE(exception_str.find("offset=-1"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("written="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 3. check the pread
  stub->set(pread, MockPRead);

  try {
    file_ptr->PReadN(buffer, 50, -1);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to pread"), std::string::npos);
    ASSERT_NE(exception_str.find("offset=-1"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 4. check the preadN
  stub->reset(pread);
  stub->set(pread, MockPRead2);

  try {
    file_ptr->PReadN(buffer, 50, -1);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to PReadN"), std::string::npos);
    ASSERT_NE(exception_str.find("offset=-1"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("read="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 5. check the write
  stub->set(write, MockWrite);

  try {
    file_ptr->WriteN(buffer, 50);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to write"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 6. check the pwriten
  stub->reset(write);
  stub->set(write, MockWrite2);

  try {
    file_ptr->WriteN(buffer, 50);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to WriteN"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("written="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 7. check the pread
  stub->set(read, MockRead);

  try {
    file_ptr->ReadN(buffer, 50);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to read"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 8. check the preadN
  stub->reset(read);
  stub->set(read, MockRead2);

  try {
    file_ptr->ReadN(buffer, 50);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to ReadN"), std::string::npos);
    ASSERT_NE(exception_str.find("require=50"), std::string::npos);
    ASSERT_NE(exception_str.find("read="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 9. check the flush
  stub->set(fsync, MockFSync);

  try {
    file_ptr->Flush();
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to fsync"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 10. check the close
  stub->set(close, MockClose);

  try {
    file_ptr->Close();
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to close"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  stub->reset(close);
  file_ptr->Close();
  delete stub;
}

int MockOpen(const char *__path, int __oflag, ...) { return -1; }

TEST_F(LocalFileSystemTest, TestFileSystemException) {
  Stub *stub;
  bool get_exception = false;
  auto *local_fs = Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  remove(std::string(file_name_ + ".1").c_str());
  remove(std::string(file_name_ + ".2").c_str());

  stub = new Stub();

  // 1. check the open
  stub->set(open, MockOpen);

  try {
    local_fs->Open(file_name_, fs::kWriteMode);
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to open"), std::string::npos);
    ASSERT_NE(exception_str.find("rc="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    ASSERT_NE(exception_str.find("path="), std::string::npos);
    ASSERT_NE(exception_str.find("flags="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  // 2. check the copy
  stub->reset(open);

  auto file_ptr1 = local_fs->Open(file_name_ + ".1", fs::kReadWriteMode);
  ASSERT_NE(nullptr, file_ptr1);

  auto file_ptr2 = local_fs->Open(file_name_ + ".2", fs::kReadWriteMode);
  ASSERT_NE(nullptr, file_ptr2);

  char buffer[50] = {0};
  file_ptr1->Write(buffer, 50);
  file_ptr1->Flush();

  stub->set(write, MockWrite2);
  try {
    local_fs->CopyFile(file_ptr1.get(), file_ptr2.get());
  } catch (cbdb::CException &e) {
    std::string exception_str(e.What());
    std::cout << exception_str << std::endl;
    ASSERT_NE(exception_str.find("Fail to copy LOCAL file from"),
              std::string::npos);
    ASSERT_NE(exception_str.find("require="), std::string::npos);
    ASSERT_NE(exception_str.find("written="), std::string::npos);
    ASSERT_NE(exception_str.find("errno="), std::string::npos);
    get_exception = true;
  }

  ASSERT_TRUE(get_exception);
  get_exception = false;

  stub->reset(write);

  file_ptr1->Close();
  file_ptr2->Close();

  remove(std::string(file_name_ + ".1").c_str());
  remove(std::string(file_name_ + ".2").c_str());

  delete stub;
}

}  // namespace exceptions

}  // namespace pax::tests
