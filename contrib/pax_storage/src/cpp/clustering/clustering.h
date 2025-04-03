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
 * clustering.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/clustering.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "clustering/clustering_reader.h"
#include "clustering/clustering_writer.h"
#include "clustering/sorter.h"

namespace pax {

namespace clustering {

class DataClustering {
 public:
  enum ClusterType {
    kClusterTypeZOrder,
    kClusterTypeIndex,
    kClusterTypeLexical,
  };

  struct DataClusteringOptions {
    ClusterType type;
  };

 public:
  DataClustering() = default;
  virtual ~DataClustering() = default;
  virtual void Clustering(ClusteringDataReader *reader,
                          ClusteringDataWriter *writer,
                          const DataClusteringOptions *options) = 0;
  static DataClustering *CreateDataClustering(const ClusterType type);
};

}  // namespace clustering
}  // namespace pax
