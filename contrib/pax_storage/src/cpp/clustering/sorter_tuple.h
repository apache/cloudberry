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
 * sorter_tuple.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/sorter_tuple.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "clustering/sorter.h"

namespace pax {
namespace clustering {
class TupleSorter final : public Sorter {
 public:
  struct HeapTupleSorterOptions {
    TupleSorterOptions base;
    TupleDesc tup_desc;
    AttrNumber *attr;
    Oid *sortOperators;
    Oid *sortCollations;
    int nkeys;
    bool nulls_first_flags;
    int work_mem;
    HeapTupleSorterOptions() { base.type = kTupleSorterTypeHeap; }
  };

 public:
  TupleSorter(HeapTupleSorterOptions options);
  virtual ~TupleSorter();
  virtual void AppendSortData(TupleTableSlot *slot) override;
  virtual void Sort() override;
  virtual bool GetSortedData(TupleTableSlot *slot) override;

 private:
  void Init();
  void DeInit();

  HeapTupleSorterOptions options_;
  Tuplesortstate *sort_state_;
};
}  // namespace clustering

}  // namespace pax