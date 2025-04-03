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
 * pax_clustering_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/pax_clustering_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>

#include "clustering/clustering_reader.h"
#include "comm/iterator.h"
#include "comm/pax_memory.h"
#include "storage/file_system.h"
#include "storage/micro_partition.h"
#include "storage/micro_partition_metadata.h"

namespace pax {
namespace clustering {
class PaxClusteringReader final : public ClusteringDataReader {
 public:
  PaxClusteringReader(
      Relation rel,
      std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator);
  virtual ~PaxClusteringReader();
  bool GetNextTuple(TupleTableSlot *) override;
  void Close() override;

 private:
  Relation relation_ = nullptr;
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> iter_;
  std::unique_ptr<MicroPartitionReader> reader_ = nullptr;
  FileSystem *file_system_ = nullptr;
  std::shared_ptr<FileSystemOptions> file_system_options_ = nullptr;
};
}  // namespace clustering
}  // namespace pax
