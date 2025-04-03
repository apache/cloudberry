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
 * pax_vec_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/micro_partition.h"
#include "storage/vec/arrow_wrapper.h"

#ifdef VEC_BUILD

namespace pax {

class PaxFilter;
class VecAdapter;
class PaxFragmentInterface;

class PaxVecReader : public MicroPartitionReaderProxy {
 public:
  // If enable read tuple from vec reader,
  // then OrcReader will be hold by PaxVecReader,
  // current MicroPartitionReader lifecycle will be bound to the PaxVecReader)
  PaxVecReader(std::unique_ptr<MicroPartitionReader> &&reader,
               std::shared_ptr<VecAdapter> adapter,
               std::shared_ptr<PaxFilter> filter);

  ~PaxVecReader() override;

  void Open(const ReaderOptions &options) override;

  void Close() override;

  bool ReadTuple(TupleTableSlot *slot) override;

  bool GetTuple(TupleTableSlot *slot, size_t row_index) override;

  size_t GetGroupNums() override;

  size_t GetTupleCountsInGroup(size_t group_index) override;

  std::unique_ptr<ColumnStatsProvider> GetGroupStatsInfo(
      size_t group_index) override;

  std::unique_ptr<MicroPartitionReader::Group> ReadGroup(size_t index) override;

  std::shared_ptr<arrow::RecordBatch> ReadBatch(PaxFragmentInterface *frag);

 private:
  std::shared_ptr<VecAdapter> adapter_;

  std::unique_ptr<MicroPartitionReader::Group> working_group_;
  size_t current_group_index_;
  size_t ctid_offset_;
  std::shared_ptr<PaxFilter> filter_;

};

}  // namespace pax

#endif  // VEC_BUILD
