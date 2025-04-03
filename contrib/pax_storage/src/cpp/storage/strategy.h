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
 * strategy.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/strategy.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cstddef>

namespace pax {
class MicroPartitionWriter;
class FileSplitStrategy {
 public:
  virtual ~FileSplitStrategy() = default;

  virtual bool ShouldSplit(size_t phy_size, size_t num_tuples) const = 0;

  virtual size_t SplitTupleNumbers() const = 0;

  virtual size_t SplitFileSize() const = 0;
};

class PaxDefaultSplitStrategy final : public FileSplitStrategy {
 public:
  PaxDefaultSplitStrategy() = default;
  ~PaxDefaultSplitStrategy() override = default;

  size_t SplitTupleNumbers() const override;

  size_t SplitFileSize() const override;

  bool ShouldSplit(size_t phy_size, size_t num_tuples) const override;
};
}  // namespace pax
