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
 * micro_partition_iterator.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_iterator.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <utility>

#include "comm/iterator.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
class MicroPartitionIterator : public IteratorBase<MicroPartitionMetadata> {
 public:
  virtual ~MicroPartitionIterator() = default;
  // FIXME: add flags to control which part of metadata to fetch.
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> New(
      Relation pax_rel, Snapshot snapshot);
  static std::unique_ptr<IteratorBase<MicroPartitionMetadata>> NewParallelIterator(
      Relation pax_rel, Snapshot snapshot, ParallelBlockTableScanDesc pscan);
};

}  // namespace pax
