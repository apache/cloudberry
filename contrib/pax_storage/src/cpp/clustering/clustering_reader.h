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
 * clustering_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/clustering_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"
namespace pax {

namespace clustering {
class ClusteringDataReader {
 public:
  ClusteringDataReader() = default;
  virtual ~ClusteringDataReader() = default;
  // TODO(gongxun): support record batch
  // return false if no more tuples
  virtual bool GetNextTuple(TupleTableSlot *) = 0;
  virtual void Close() = 0;
};

}  // namespace clustering

}  // namespace pax
