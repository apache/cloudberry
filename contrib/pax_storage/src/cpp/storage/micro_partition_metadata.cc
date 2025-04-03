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
 * micro_partition_metadata.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_metadata.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition_metadata.h"
namespace pax {

WriteSummary::WriteSummary()
    : file_size(0), num_tuples(0), rel_oid(InvalidOid) {}

MicroPartitionMetadata::MicroPartitionMetadata(MicroPartitionMetadata &&other) {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  stats_ = std::move(other.stats_);
  visibility_bitmap_file_ = std::move(other.visibility_bitmap_file_);
  exist_ext_toast_ = other.exist_ext_toast_;
  is_clustered_ = other.is_clustered_;
}

MicroPartitionMetadata &MicroPartitionMetadata::operator=(
    MicroPartitionMetadata &&other) {
  micro_partition_id_ = std::move(other.micro_partition_id_);
  file_name_ = std::move(other.file_name_);
  tuple_count_ = other.tuple_count_;
  stats_ = std::move(other.stats_);
  visibility_bitmap_file_ = std::move(other.visibility_bitmap_file_);
  exist_ext_toast_ = other.exist_ext_toast_;
  is_clustered_ = other.is_clustered_;
  return *this;
}

}  // namespace pax
