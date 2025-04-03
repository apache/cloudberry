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
 * pax_column_stats.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_column_stats.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/pax_defined.h"

namespace pax {

class ColumnStatsProvider {
 public:
  virtual ~ColumnStatsProvider() = default;
  virtual int ColumnSize() const = 0;
  virtual bool AllNull(int column_index) const = 0;
  virtual bool HasNull(int column_index) const = 0;
  virtual uint64 NonNullRows(int column_index) const = 0;
  virtual const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const = 0;
  virtual const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const = 0;

  virtual bool HasBloomFilter(int column_index) const = 0;
  virtual const ::pax::stats::BloomFilterBasicInfo &BloomFilterBasicInfo(
      int column_index) const = 0;
  virtual std::string GetBloomFilter(int column_index) const = 0;
};

} //  namespace pax