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
 * micro_partition_metadata.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_metadata.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <string>
#include <utility>

#include "storage/proto/proto_wrappers.h"

namespace pax {
// WriteSummary is generated after the current micro partition is flushed and
// closed.
struct WriteSummary {
  std::string file_name;
  std::string block_id;
  size_t file_size;
  size_t num_tuples;
  unsigned int rel_oid;
  bool is_clustered = false;
  pax::stats::MicroPartitionStatisticsInfo *mp_stats = nullptr;
  bool exist_ext_toast;
  WriteSummary();
  WriteSummary(const WriteSummary &summary) = default;
};

struct MicroPartitionMetadata {
 public:
  MicroPartitionMetadata() = default;
  MicroPartitionMetadata(const MicroPartitionMetadata &other) = default;

  ~MicroPartitionMetadata() = default;

  MicroPartitionMetadata(MicroPartitionMetadata &&other);

  MicroPartitionMetadata &operator=(const MicroPartitionMetadata &other) =
      default;

  MicroPartitionMetadata &operator=(MicroPartitionMetadata &&other);

  inline int GetMicroPartitionId() const { return micro_partition_id_; }

  inline const std::string &GetFileName() const { return file_name_; }

  inline uint32 GetTupleCount() const { return tuple_count_; }

  inline const ::pax::stats::MicroPartitionStatisticsInfo &GetStats() const {
    return stats_;
  }

  inline void SetMicroPartitionId(int id) { micro_partition_id_ = id; }

  inline void SetFileName(std::string &&name) { file_name_ = std::move(name); }

  inline void SetTupleCount(uint32 tuple_count) { tuple_count_ = tuple_count; }

  inline void SetStats(::pax::stats::MicroPartitionStatisticsInfo &&stats) {
    stats_ = std::move(stats);
  }

  inline const std::string &GetVisibilityBitmapFile() const {
    return visibility_bitmap_file_;
  }

  inline void SetVisibilityBitmapFile(std::string &&file) {
    visibility_bitmap_file_ = std::move(file);
  }

  inline bool GetExistToast() const { return exist_ext_toast_; }

  inline void SetExistToast(bool exist) { exist_ext_toast_ = exist; }
  inline void SetClustered(bool clustered) { is_clustered_ = clustered; }

  inline bool IsClustered() const { return is_clustered_; }

 private:
  int micro_partition_id_;

  std::string file_name_;

  std::string visibility_bitmap_file_;

  // statistics info
  uint32 tuple_count_ = 0;

  bool exist_ext_toast_ = false;
  bool is_clustered_ = false;

  ::pax::stats::MicroPartitionStatisticsInfo stats_;
};  // class MicroPartitionMetadata
}  // namespace pax
