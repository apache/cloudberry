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
 * pax_table_cluster.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_table_cluster.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"
namespace pax {
void DeleteClusteringFiles(
    Relation rel, Snapshot snapshot,
    std::shared_ptr<IteratorBase<MicroPartitionMetadata>> iter);

void Cluster(Relation rel, Snapshot snapshot, bool is_incremental_cluster);

void IndexCluster(Relation old_rel, Relation new_rel, Relation index,
                  Snapshot snapshot);
}  // namespace pax
