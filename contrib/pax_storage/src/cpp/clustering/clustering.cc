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
 * clustering.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/clustering.cc
 *
 *-------------------------------------------------------------------------
 */

#include "clustering/clustering.h"

#include "clustering/index_clustering.h"
#include "clustering/lexical_clustering.h"
#include "clustering/zorder_clustering.h"
#include "comm/pax_memory.h"

namespace pax {
namespace clustering {

DataClustering *DataClustering::CreateDataClustering(
    const DataClustering::ClusterType type) {
  switch (type) {
    case DataClustering::kClusterTypeZOrder:
      static ZOrderClustering zorder_clustering;
      return &zorder_clustering;
    case DataClustering::kClusterTypeIndex:
      static IndexClustering index_clustering;
      return &index_clustering;
    case DataClustering::kClusterTypeLexical:
      static LexicalClustering lexical_clustering;
      return &lexical_clustering;
    default:
      return nullptr;
  }
}

}  // namespace clustering
}  // namespace pax
