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
 * micro_partition_stats_updater.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_stats_updater.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/micro_partition.h"

namespace pax {
class MicroPartitionStatsUpdater {
 public:
  // visibility_bitmap
  // v1: 111000
  // v2: 000100(updater) -> 111100(reader)
  MicroPartitionStatsUpdater(MicroPartitionReader *reader,
                             std::shared_ptr<Bitmap8> visibility_bitmap);
  std::shared_ptr<MicroPartitionStats> Update(
      TupleTableSlot *slot, const std::vector<int> &minmax_columns,
      const std::vector<int> &bf_columns);

 private:
  MicroPartitionReader *reader_;
  std::vector<bool> exist_invisible_tuples_;
};
}  // namespace pax
