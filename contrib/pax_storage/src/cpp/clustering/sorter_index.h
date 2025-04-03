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
 * sorter_index.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/sorter_index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "clustering/sorter.h"

namespace pax {
namespace clustering {
class IndexSorter final : public Sorter {
 public:
  struct IndexTupleSorterOptions : public TupleSorterOptions {
    IndexTupleSorterOptions() { type = kTupleSorterTypeIndex; }
    TupleDesc tup_desc;
    Relation index_rel;
    int work_mem;
  };

 public:
  IndexSorter(IndexTupleSorterOptions options);
  virtual ~IndexSorter();
  virtual void AppendSortData(TupleTableSlot *slot) override;
  virtual void Sort() override;
  virtual bool GetSortedData(TupleTableSlot *slot) override;

 private:
  void Init();
  void DeInit();

  IndexTupleSorterOptions options_;
  Tuplesortstate *sort_state_ = nullptr;
};
}  // namespace clustering

}  // namespace pax
