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
 * micro_partition.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/micro_partition.h"

#include <utility>

#include "comm/pax_memory.h"
#include "storage/filter/pax_filter.h"

namespace pax {

MicroPartitionWriter::MicroPartitionWriter(const WriterOptions &writer_options)
    : writer_options_(writer_options) {}

MicroPartitionWriter *MicroPartitionWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  summary_callback_ = callback;
  return this;
}

MicroPartitionWriter *MicroPartitionWriter::SetStatsCollector(
    std::shared_ptr<MicroPartitionStats> mpstats) {
  Assert(mp_stats_ == nullptr);
  mp_stats_ = mpstats;
  return this;
}

MicroPartitionReaderProxy::~MicroPartitionReaderProxy() {}

void MicroPartitionReaderProxy::Open(
    const MicroPartitionReader::ReaderOptions &options) {
  Assert(reader_);
  reader_->Open(options);
}

void MicroPartitionReaderProxy::Close() {
  Assert(reader_);
  reader_->Close();
}

bool MicroPartitionReaderProxy::ReadTuple(TupleTableSlot *slot) {
  Assert(reader_);
  return reader_->ReadTuple(slot);
}

bool MicroPartitionReaderProxy::GetTuple(TupleTableSlot *slot,
                                         size_t row_index) {
  Assert(reader_);
  return reader_->GetTuple(slot, row_index);
}

void MicroPartitionReaderProxy::SetReader(
    std::unique_ptr<MicroPartitionReader> &&reader) {
  Assert(reader);
  Assert(!reader_);
  reader_ = std::move(reader);
}

size_t MicroPartitionReaderProxy::GetGroupNums() {
  return reader_->GetGroupNums();
}

size_t MicroPartitionReaderProxy::GetTupleCountsInGroup(size_t group_index) {
  return reader_->GetTupleCountsInGroup(group_index);
}

std::unique_ptr<ColumnStatsProvider>
MicroPartitionReaderProxy::GetGroupStatsInfo(size_t group_index) {
  return reader_->GetGroupStatsInfo(group_index);
}

std::unique_ptr<MicroPartitionReader::Group>
MicroPartitionReaderProxy::ReadGroup(size_t index) {
  return reader_->ReadGroup(index);
}

}  // namespace pax
