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
 * sorter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/sorter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

namespace pax {
namespace clustering {
class Sorter {
 public:
  enum TupleSorterType {
    kTupleSorterTypeHeap,
    kTupleSorterTypeIndex,
  };
  struct TupleSorterOptions {
    TupleSorterType type;
  };

 public:
  Sorter() = default;
  virtual ~Sorter() = default;
  virtual void AppendSortData(TupleTableSlot *slot) = 0;
  virtual void Sort() = 0;
  virtual bool GetSortedData(TupleTableSlot *slot) = 0;
};
}  // namespace clustering

}  // namespace pax
