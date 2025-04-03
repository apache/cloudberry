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
 * index_clustering.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/index_clustering.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "clustering/clustering.h"
namespace pax {
namespace clustering {
class IndexClustering final : public DataClustering {
 public:
  struct IndexClusteringOptions : public DataClusteringOptions {
    IndexClusteringOptions() { type = kClusterTypeIndex; }
    TupleDesc tup_desc;
    Relation index_rel;
    int work_mem;
  };

 public:
  IndexClustering();
  virtual ~IndexClustering();
  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;
};

}  // namespace clustering

}  // namespace pax
