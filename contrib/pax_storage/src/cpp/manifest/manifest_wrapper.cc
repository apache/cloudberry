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
 * manifest_wrapper.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/manifest/manifest_wrapper.cc
 *
 *-------------------------------------------------------------------------
 */

#include "manifest_wrapper.h"
extern "C" {
#include "catalog/storage.h"
}
#include "comm/paxc_wrappers.h"
#include "exceptions/CException.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/wal/paxc_wal.h"

bool paxc_need_wal(Relation rel) {
  return paxc::NeedWAL(rel);
}

char *paxc_get_pax_dir(RelFileNode rnode, BackendId backend) {
  return paxc::BuildPaxDirectoryPath(rnode, backend);
}
char *paxc_build_path(const char *filename);

void paxc_create_pax_directory(Relation rel, RelFileNode newrnode) {
  // create relfilenode file for pax table
  auto srel = paxc::PaxRelationCreateStorage(newrnode, rel);
  smgrclose(srel);

  char *path = paxc::BuildPaxDirectoryPath(newrnode, rel->rd_backend);
  Assert(path);

  int rc;
  CBDB_TRY();
  {
    pax::FileSystem *fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    rc = fs->CreateDirectory(path);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  if (rc != 0)
    elog(ERROR, "create data dir failed for %u/%u/%lu",
                newrnode.dbNode, newrnode.spcNode, newrnode.relNode);
}

void paxc_store_file(const char *filename, const void *data, size_t size) {
  CBDB_TRY();
  {
    auto fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    auto file = fs->Open(filename, pax::fs::kWriteMode);
    file->WriteN(data, size);
    file->Close();
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();
}

void paxc_read_all(const char *filename, void (*func)(const void *ptr, size_t size, void *opaque), void *opaque) {
  CBDB_TRY();
  {
    std::vector<uint8> v;
    auto fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    auto file = fs->Open(filename, pax::fs::kReadMode);
    auto filesize = file->FileLength();
    v.resize(filesize);
    file->ReadN(v.data(), filesize);
    file->Close();
    CBDB_WRAP_START;
    {
      func(v.data(), v.size(), opaque);
    }
    CBDB_WRAP_END;
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

}

void paxc_wal_insert_if_required(Relation rel, const char *filename, const void *data, size_t size, int64 offset) {
  if (paxc::NeedWAL(rel)) {
    paxc::XLogPaxInsert(rel->rd_node, filename, offset, const_cast<void*>(data), size);
  }
}

void paxc_wal_truncate_directory(RelFileNode node) {
  paxc::XLogPaxTruncate(node);
}
