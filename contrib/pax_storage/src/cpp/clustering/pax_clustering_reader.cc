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
 * pax_clustering_reader.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/pax_clustering_reader.cc
 *
 *-------------------------------------------------------------------------
 */

#include "clustering/pax_clustering_reader.h"

#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"

namespace pax {
namespace clustering {
PaxClusteringReader::PaxClusteringReader(
    Relation relation,
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator)
    : relation_(relation), iter_(std::move(iterator)) {
  file_system_ = Singleton<LocalFileSystem>::GetInstance();
}

PaxClusteringReader::~PaxClusteringReader() {}

bool PaxClusteringReader::GetNextTuple(TupleTableSlot *slot) {
  cbdb::ExecClearTuple(slot);
  while (reader_ == nullptr || !reader_->ReadTuple(slot)) {
    if (iter_->HasNext()) {
      if (reader_ != nullptr) {
        reader_->Close();
        reader_ = nullptr;
      }
      auto meta_info = iter_->Next();
      MicroPartitionReader::ReaderOptions options;

      std::string visibility_bitmap_file = meta_info.GetVisibilityBitmapFile();
      if (!visibility_bitmap_file.empty()) {
        auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode,
                                       file_system_options_);
        auto file_length = file->FileLength();
        auto bm = std::make_shared<Bitmap8>(file_length * 8);
        file->ReadN(bm->Raw().bitmap, file_length);
        options.visibility_bitmap = bm;
        file->Close();
      }

      std::shared_ptr<File> file;
      std::shared_ptr<File> toast_file;
      file =
          file_system_->Open(meta_info.GetFileName(), pax::fs::kReadMode);

      if (meta_info.GetExistToast()) {
        toast_file = file_system_->Open(
            meta_info.GetFileName() + TOAST_FILE_SUFFIX, pax::fs::kReadMode);
        ;
      }

      reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
          options, ReaderFlags::FLAGS_EMPTY, file, toast_file);
    } else {
      return false;
    }
  }
  cbdb::ExecStoreVirtualTuple(slot);
  return true;
}

void PaxClusteringReader::Close() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
  iter_->Release();
  iter_ = nullptr;
}

}  // namespace clustering

}  // namespace pax
