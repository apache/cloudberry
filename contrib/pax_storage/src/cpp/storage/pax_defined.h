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
 * pax_defined.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax_defined.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <vector>

#include "storage/paxc_define.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

#define VEC_BATCH_LENGTH (16384)
#define MEMORY_ALIGN_SIZE (8)
#define PAX_DATA_NO_ALIGN (1)
#define BITS_TO_BYTES(bits) (((bits) + 7) / 8)

#define PAX_OFFSETS_DEFAULT_COMPRESSTYPE \
  ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD
#define PAX_OFFSETS_DEFAULT_COMPRESSLEVEL 5

#define COLUMN_STORAGE_FORMAT_IS_VEC(column) \
  (((column)->GetStorageFormat()) == PaxStorageFormat::kTypeStoragePorcVec)

enum PaxStorageFormat {
  // default non-vec store
  // which split null field and null bitmap
  kTypeStoragePorcNonVec = 1,
  // vec storage format
  // spec the storage format
  kTypeStoragePorcVec = 2,
};

}  // namespace pax
