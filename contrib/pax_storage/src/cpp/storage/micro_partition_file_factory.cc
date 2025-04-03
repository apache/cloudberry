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
 * micro_partition_file_factory.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_file_factory.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_file_factory.h"

#include "comm/pax_memory.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_row_filter_reader.h"
#include "storage/orc/porc.h"
#include "storage/vec/pax_vec_adapter.h"
#include "storage/vec/pax_vec_reader.h"

namespace pax {
std::unique_ptr<MicroPartitionReader>
MicroPartitionFileFactory::CreateMicroPartitionReader(
    const MicroPartitionReader::ReaderOptions &options, int32 flags,
    std::shared_ptr<File> file, std::shared_ptr<File> toast_file) {
  std::unique_ptr<MicroPartitionReader> reader =
      std::make_unique<OrcReader>(file, toast_file);

#ifdef VEC_BUILD
  if (flags & ReaderFlags::FLAGS_VECTOR_PATH) {
    auto vec_adapter_ptr = std::make_shared<VecAdapter>(
        options.tuple_desc,
        (flags & ReaderFlags::FLAGS_SCAN_WITH_CTID) != 0);
    reader = std::make_unique<PaxVecReader>(std::move(reader), vec_adapter_ptr,
                                            options.filter);
  } else
#endif
      if (options.filter && options.filter->GetRowFilter()) {
    reader = MicroPartitionRowFilterReader::New(
        std::move(reader), options.filter, options.visibility_bitmap);
  }

  reader->Open(options);
  return reader;
}

std::unique_ptr<MicroPartitionWriter>
MicroPartitionFileFactory::CreateMicroPartitionWriter(
    const MicroPartitionWriter::WriterOptions &options,
    std::shared_ptr<File> file, std::shared_ptr<File> toast_file) {
  std::vector<pax::porc::proto::Type_Kind> type_kinds;
  type_kinds = OrcWriter::BuildSchema(
      options.rel_tuple_desc,
      options.storage_format == PaxStorageFormat::kTypeStoragePorcVec);
  return std::make_unique<OrcWriter>(options, std::move(type_kinds), file,
                                     toast_file);
}

}  // namespace pax
