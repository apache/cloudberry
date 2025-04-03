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
 * local_file_system.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/local_file_system.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <sys/stat.h>

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {

class LocalFileSystem final : public FileSystem {
  friend class Singleton<LocalFileSystem>;
  friend class ClassCreator;

 public:
  LocalFileSystem() = default;
  std::unique_ptr<File> Open(const std::string &file_path, int flags,
             const std::shared_ptr<FileSystemOptions> &options = nullptr) override;
  void Delete(const std::string &file_path,
              const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  std::vector<std::string> ListDirectory(
      const std::string &path,
      const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  int CreateDirectory(const std::string &path,
                      const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                       const std::shared_ptr<FileSystemOptions> &options = nullptr) const override;
  // operate with file
  std::string BuildPath(const File *file) const override;

  int CopyFile(const File *src_file, File *dst_file) override;
};
}  // namespace pax
