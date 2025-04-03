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
 * strategy.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/strategy.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/strategy.h"

#include "comm/guc.h"
#include "storage/micro_partition.h"

namespace pax {

size_t PaxDefaultSplitStrategy::SplitTupleNumbers() const {
  // The reason why we chose 16384 as a separator value
  // is because in the vectorized version, the number of
  // rows returned by each tuple cannot be greater than 16384
  // and needs to be as close as possible to this value
  return pax_max_tuples_per_file;
}

size_t PaxDefaultSplitStrategy::SplitFileSize() const {
  return pax_max_size_per_file;
}

bool PaxDefaultSplitStrategy::ShouldSplit(size_t phy_size,
                                          size_t num_tuples) const {
  return (num_tuples >= SplitTupleNumbers()) || (phy_size >= SplitFileSize());
}

}  // namespace pax
