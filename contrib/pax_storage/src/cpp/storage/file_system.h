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
 * file_system.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/file_system.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <fcntl.h>

#include <functional>
#include <string>
#include <vector>

#include "comm/common_io.h"
#include "comm/pax_memory.h"

namespace pax {

namespace fs {
const int kWriteMode = O_CREAT | O_WRONLY | O_EXCL;
const int kWriteWithTruncMode = O_CREAT | O_WRONLY | O_TRUNC;
const int kReadMode = O_RDONLY;
const int kReadWriteMode = O_CREAT | O_RDWR | O_EXCL;
const int kDefaultWritePerm = 0640;
};  // namespace fs

/*
 * The IO functions may have error that have two different ways
 * to handle errors. In C style, the function returns -1 and set
 * the errno. The other style likes Java that any error will throw
 * an exception.
 * The IO functions provided by postgres will raise an ERROR
 * if unexpected behavior happens.
 *
 * The following IO functions use the same behavior like postgres,
 * but we throw an exception in C++ code.
 */
class File {
 public:
  virtual ~File() = default;

  // The following [P]Read/[P]Write may partially read/write
  virtual ssize_t Read(void *ptr, size_t n) const = 0;
  virtual ssize_t Write(const void *ptr, size_t n) = 0;
  virtual ssize_t PWrite(const void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t PRead(void *buf, size_t count, off_t offset) const = 0;

  // The *N version of Read/Write means that R/W must read/write complete
  // number of bytes, or the function should throw an exception.
  // These 4 methods have default implementation that simply calls  read/write
  // and check the returned number of bytes.
  virtual void ReadN(void *ptr, size_t n) const;
  virtual void WriteN(const void *ptr, size_t n);
  virtual void PWriteN(const void *buf, size_t count, off_t offset);
  virtual void PReadN(void *buf, size_t count, off_t offset) const;
  virtual void ReadBatch(const std::vector<IORequest> &requests) const;

  virtual void Flush() = 0;
  virtual void Delete() = 0;
  virtual void Close() = 0;
  virtual size_t FileLength() const = 0;
  virtual std::string GetPath() const = 0;
  virtual std::string DebugString() const = 0;
};

class FileSystemOptions {
 public:
  FileSystemOptions() = default;
  virtual ~FileSystemOptions() = default;
};

class FileSystem {
 public:
  virtual ~FileSystem() = default;

  // operate with path
  virtual std::unique_ptr<File> Open(const std::string &file_path, int flags,
                     const std::shared_ptr<FileSystemOptions> &options = nullptr) = 0;
  virtual std::vector<std::string> ListDirectory(
      const std::string &path, const std::shared_ptr<FileSystemOptions> &options = nullptr) const = 0;

  virtual int CreateDirectory(const std::string &path,
                              const std::shared_ptr<FileSystemOptions> &options = nullptr) const = 0;
  virtual void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                               const std::shared_ptr<FileSystemOptions> &options = nullptr) const = 0;
  virtual void Delete(const std::string &file_path,
                      const std::shared_ptr<FileSystemOptions> &options = nullptr) const = 0;
  // operate with file
  virtual std::string BuildPath(const File *file) const = 0;
  virtual int CopyFile(const File *src_file, File *dst_file) = 0;
};

}  //  namespace pax
