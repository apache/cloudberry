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
 * zorder_clustering.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/zorder_clustering.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "clustering/clustering.h"
#include "clustering/clustering_reader.h"
#include "clustering/clustering_writer.h"
#include "comm/pax_memory.h"
namespace pax {
namespace clustering {
class ZOrderClustering final : public DataClustering {
 public:
  struct ZOrderClusteringOptions : public DataClusteringOptions {
    ZOrderClusteringOptions(TupleDesc tup_desc, int nkeys,
                            bool nulls_first_flags, int work_mem) {
      type = kClusterTypeZOrder;

      this->tup_desc = tup_desc;
      this->nkeys = nkeys;
      this->nulls_first_flags = nulls_first_flags;
      this->work_mem = work_mem;
      attr = PAX_NEW_ARRAY<AttrNumber>(nkeys);
    }
    ~ZOrderClusteringOptions() { PAX_DELETE_ARRAY(attr); }
    TupleDesc tup_desc;
    AttrNumber *attr;
    int nkeys;
    bool nulls_first_flags;
    int work_mem;
  };
  ZOrderClustering();
  virtual ~ZOrderClustering();
  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;

 private:
  void CheckOptions(const ZOrderClusteringOptions *options);
  // TODO(gongxun): support batch size
  void MakeZOrderTupleSlot(TupleTableSlot *zorder_slot,
                           const TupleTableSlot *slot,
                           const ZOrderClusteringOptions *options,
                           char *column_bytes_buffer, int buffer_len);
};

}  // namespace clustering

}  // namespace pax
