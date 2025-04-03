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
 * index_clustering.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/index_clustering.cc
 *
 *-------------------------------------------------------------------------
 */


#include "clustering/index_clustering.h"

#include "clustering/sorter_index.h"
#include "comm/cbdb_wrappers.h"

namespace pax {
namespace clustering {

IndexClustering::IndexClustering() {}
IndexClustering::~IndexClustering() {}
void IndexClustering::Clustering(ClusteringDataReader *reader,
                                 ClusteringDataWriter *writer,
                                 const DataClusteringOptions *options) {
  const IndexClusteringOptions *index_options =
      reinterpret_cast<const IndexClusteringOptions *>(options);

  CBDB_CHECK(index_options != nullptr && index_options->index_rel != nullptr,
             cbdb::CException::kExTypeInvalid, "index options is invalid");

  TupleTableSlot *origin_slot;
  TupleTableSlot *sorted_slot;
  IndexSorter::IndexTupleSorterOptions sorter_options;

  sorter_options.tup_desc = index_options->tup_desc;
  sorter_options.work_mem = index_options->work_mem;
  sorter_options.index_rel = index_options->index_rel;

  IndexSorter sorter(sorter_options);

  origin_slot =
      cbdb::MakeSingleTupleTableSlot(index_options->tup_desc, &TTSOpsVirtual);
  while (reader->GetNextTuple(origin_slot)) {
    sorter.AppendSortData(origin_slot);
  }

  cbdb::ExecDropSingleTupleTableSlot(origin_slot);

  sorter.Sort();

  sorted_slot = cbdb::MakeSingleTupleTableSlot(sorter_options.tup_desc,
                                               &TTSOpsMinimalTuple);
  while (sorter.GetSortedData(sorted_slot)) {
    cbdb::SlotGetAllAttrs(sorted_slot);
    writer->WriteTuple(sorted_slot);
  }
  cbdb::ExecDropSingleTupleTableSlot(sorted_slot);
}

}  // namespace clustering
}  // namespace pax