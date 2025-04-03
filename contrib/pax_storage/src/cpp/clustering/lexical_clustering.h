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
 * lexical_clustering.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/lexical_clustering.h
 *
 *-------------------------------------------------------------------------
 */


#pragma once

#include "clustering/clustering.h"
#include "comm/pax_memory.h"

namespace pax {
namespace clustering {
class LexicalClustering final : public DataClustering {
 public:
  struct LexicalClusteringOptions : public DataClusteringOptions {
    LexicalClusteringOptions(TupleDesc tup_desc, int nkeys,
                             bool nulls_first_flags, int work_mem) {
      type = kClusterTypeLexical;

      this->tup_desc = tup_desc;
      this->nkeys = nkeys;
      this->nulls_first_flags = nulls_first_flags;
      this->work_mem = work_mem;
      attr = PAX_NEW_ARRAY<AttrNumber>(nkeys);
      sortOperators = PAX_NEW_ARRAY<Oid>(nkeys);
      sortCollations = PAX_NEW_ARRAY<Oid>(nkeys);
    }

    ~LexicalClusteringOptions() {
      PAX_DELETE_ARRAY(attr);
      PAX_DELETE_ARRAY(sortOperators);
      PAX_DELETE_ARRAY(sortCollations);
    }
    TupleDesc tup_desc;
    AttrNumber *attr;
    Oid *sortOperators;
    Oid *sortCollations;
    int nkeys;
    bool nulls_first_flags;
    int work_mem;
  };

  LexicalClustering();
  virtual ~LexicalClustering();

  void Clustering(ClusteringDataReader *reader, ClusteringDataWriter *writer,
                  const DataClusteringOptions *options) override;
};
}  // namespace clustering

}  // namespace pax