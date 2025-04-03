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
 * pax_clustering_writer.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/pax_clustering_writer.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include "clustering/clustering_writer.h"
#include "storage/pax.h"

namespace pax {
namespace clustering {
class PaxClusteringWriter final : public ClusteringDataWriter {
 public:
  PaxClusteringWriter(Relation rel);
  virtual ~PaxClusteringWriter();
  // If in zorder_cluster, the last column of the slot in WriteTuple is
  // zorder_value
  void WriteTuple(TupleTableSlot *tuple) override;
  void Close() override;

 private:
  Relation rel_ = nullptr;
  std::unique_ptr<TableWriter> writer_ = nullptr;
};
}  // namespace clustering

}  // namespace pax
