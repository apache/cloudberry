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
 * micro_partition_file_factory.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_file_factory.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>

#include "storage/file_system.h"
#include "storage/micro_partition.h"

namespace pax {

// flags is used to control the behavior of the reader
// refer to the definition of ReaderFlags.
enum ReaderFlags {
  FLAGS_EMPTY = 0UL,
  FLAGS_VECTOR_PATH =
      1L << 0,  // use vec_adapter to read and returns the record
                // batch format required by the vectorized executor
  FLAGS_SCAN_WITH_CTID = 1L << 1,  // record batch format should build with ctid
};

#define READER_FLAG_SET_VECTOR_PATH(flags) (flags) |= FLAGS_VECTOR_PATH;

#define READER_FLAG_SET_SCAN_WITH_CTID(flags) (flags) |= FLAGS_SCAN_WITH_CTID;

class MicroPartitionFileFactory final {
 public:
  static std::unique_ptr<MicroPartitionWriter> CreateMicroPartitionWriter(
      const MicroPartitionWriter::WriterOptions &options,
      std::shared_ptr<File> file,
      std::shared_ptr<File> toast_file = nullptr);

  static std::unique_ptr<MicroPartitionReader> CreateMicroPartitionReader(
      const MicroPartitionReader::ReaderOptions &options, int32 flags,
      std::shared_ptr<File> file,
      std::shared_ptr<File> toast_file = nullptr);
};

}  // namespace pax
