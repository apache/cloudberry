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
 * local_file_system.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/local_file_system.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

#include "access/pax_access_handle.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fast_io.h"
#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "comm/pax_resource.h"
#include "exceptions/CException.h"

namespace pax {

class LocalFile final : public File {

 public:
  LocalFile(int fd, const std::string &file_path);

  ssize_t Read(void *ptr, size_t n) const override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, off_t offset) override;
  ssize_t PRead(void *ptr, size_t n, off_t offset) const override;
  void ReadBatch(const std::vector<IORequest> &requests) const override;
  size_t FileLength() const override;
  void Flush() override;
  void Delete() override;
  void Close() override;
  std::string GetPath() const override;
  std::string DebugString() const override;

 private:
  int fd_;
  std::string file_path_;
};

static void LocalFileReleaseFile(Datum arg) {
  int rc;
  auto fd = cbdb::DatumToInt32(arg);
  Assert(fd >= 0);

  do {
    rc = close(fd);
  } while (unlikely(rc == -1 && errno == EINTR));
}

LocalFile::LocalFile(int fd, const std::string &file_path)
    : File(), fd_(fd), file_path_(file_path) {
  Assert(fd_ >= 0);
}

ssize_t LocalFile::Read(void *ptr, size_t n) const {
  ssize_t num;

  do {
    num = read(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to read [require=%lu, rc=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
  return num;
}

ssize_t LocalFile::Write(const void *ptr, size_t n) {
  ssize_t num;

  do {
    num = write(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to write [require=%lu, rc=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
  return num;
}

ssize_t LocalFile::PRead(void *ptr, size_t n, off_t offset) const {
  ssize_t num;

  do {
    num = pread(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(
      num >= 0, cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to pread [offset=%ld, require=%lu, rc=%ld, errno=%d], %s",
          offset, n, num, errno, DebugString().c_str()));
  return num;
}

ssize_t LocalFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t num;

  do {
    num = pwrite(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(
      num >= 0, cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to pwrite [offset=%ld, require=%lu, rc=%ld, errno=%d], %s",
          offset, n, num, errno, DebugString().c_str()));
  return num;
}

void LocalFile::ReadBatch(const std::vector<IORequest> &requests) const {
  if (unlikely(requests.empty())) return;

  if (IOUringFastIO::available()) {
    IOUringFastIO fast_io(requests.size());
    std::vector<bool> result(requests.size(), false);
    auto res = fast_io.read(fd_, const_cast<std::vector<IORequest>&>(requests), result);
    CBDB_CHECK(res.first == 0, cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to ReadBatch with io_uring [successful=%d, total=%lu], %s",
                   res.second, requests.size(), DebugString().c_str()));
  } else {
    SyncFastIO fast_io;
    std::vector<bool> result(requests.size(), false);
    auto res = fast_io.read(fd_, const_cast<std::vector<IORequest>&>(requests), result);
    CBDB_CHECK(res.first == 0, cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to ReadBatch with sync read [successful=%d, total=%lu], %s",
                   res.second, requests.size(), DebugString().c_str()));
  }
}

size_t LocalFile::FileLength() const {
  struct stat file_stat {};
  int rc;
  rc = fstat(fd_, &file_stat);

  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to fstat [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
  return static_cast<size_t>(file_stat.st_size);
}

void LocalFile::Flush() {
  int rc;
  rc = fsync(fd_);

  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to fsync [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
}

void LocalFile::Delete() {
  int rc;
  rc = remove(file_path_.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to remove [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
}

void LocalFile::Close() {
  int rc;

  do {
    rc = close(fd_);
  } while (unlikely(rc == -1 && errno == EINTR));
  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to close [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));

  pax::common::ForgetResourceCallback(LocalFileReleaseFile, cbdb::Int32ToDatum(fd_));
  fd_ = -1;
}

std::string LocalFile::GetPath() const { return file_path_; }

std::string LocalFile::DebugString() const {
  return fmt("LOCAL file [path=%s]", file_path_.c_str());
}

std::unique_ptr<File> LocalFileSystem::Open(const std::string &file_path, int flags,
                            const std::shared_ptr<FileSystemOptions> & /*options*/) {
  int fd;

  if (flags & O_CREAT) {
    fd = open(file_path.c_str(), flags, fs::kDefaultWritePerm);
  } else {
    fd = open(file_path.c_str(), flags);
  }

  CBDB_CHECK(fd >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to open [rc=%d, errno=%d, path=%s, flags=%d]", fd, errno,
                 file_path.c_str(), flags));

  auto ok = pax::common::RememberResourceCallback(LocalFileReleaseFile, cbdb::Int32ToDatum(fd));
  if (!ok) {
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    CBDB_CHECK(
        ok, cbdb::CException::ExType::kExTypeIOError,
        fmt("Fail to do remember local fd [fd=%d, errno=%m, path=%s, flags=%d]",
            fd, file_path.c_str(), flags));
  }

  return std::make_unique<LocalFile>(fd, file_path);
}

void LocalFileSystem::Delete(const std::string &file_path,
                             const std::shared_ptr<FileSystemOptions> & /*options*/) const {
  int rc;

  rc = remove(file_path.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to remove [rc=%d, errno=%d, path=%s]", rc, errno,
                 file_path.c_str()));
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

int LocalFileSystem::CopyFile(const File *src_file, File *dst_file) {
  const size_t buf_size = 32 * 1024;
  char buf[buf_size];
  off_t read_off = 0;
  ssize_t num_write = 0;
  ssize_t num_read = 0;

  while ((num_read = src_file->PRead(buf, buf_size, read_off)) > 0) {
    read_off += num_read;
    num_write = dst_file->Write(buf, num_read);
    CBDB_CHECK(
        num_write == num_read, cbdb::CException::kExTypeIOError,
        fmt("Fail to copy LOCAL file from %s to %s. \n"
            "Write failed [read off=%ld, require=%ld, written=%ld, errno=%d]",
            src_file->DebugString().c_str(), dst_file->DebugString().c_str(),
            read_off, num_read, num_write, errno));
  }

  // no need check num_read >= 0 again
  // already checked in `src_file->Read`

  return 0;
}

std::vector<std::string> LocalFileSystem::ListDirectory(
    const std::string &path, const std::shared_ptr<FileSystemOptions> & /*options*/) const {
  DIR *dir;
  std::vector<std::string> filelist;
  const char *filepath = path.c_str();

  Assert(filepath != NULL && filepath[0] != '\0');

  dir = opendir(filepath);
  CBDB_CHECK(dir, cbdb::CException::ExType::kExTypeFileOperationError,
             fmt("Fail to opendir [path=%s, errno=%d]", filepath, errno));

  try {
    struct dirent *direntry;
    while ((direntry = readdir(dir)) != NULL) {
      char *filename = &direntry->d_name[0];
      // skip to add '.' or '..' direntry for file enumerating under folder on
      // linux OS.
      if (*filename == '.' &&
          (!strcmp(filename, ".") || !strcmp(filename, "..")))
        continue;
      filelist.push_back(std::string(filename));
    }
  } catch (std::exception &ex) {
    closedir(dir);
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError,
               fmt("List directory failed. [path=%s]", path.c_str()));
  }

  return filelist;
}

int LocalFileSystem::CreateDirectory(const std::string &path,
                                     const std::shared_ptr<FileSystemOptions> &options) const {
  return cbdb::PathNameCreateDir(path.c_str());
}

void LocalFileSystem::DeleteDirectory(const std::string &path,
                                      bool delete_topleveldir,
                                      const std::shared_ptr<FileSystemOptions> &options) const {
  cbdb::PathNameDeleteDir(path.c_str(), delete_topleveldir);
}

}  // namespace pax
