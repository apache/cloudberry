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
 * vec_parallel_pax.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec_parallel_pax.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/vec_parallel_common.h"

namespace pax
{

class PaxDatasetInterface : public arrow::dataset::DatasetInterface {
 public:
  // Initialize should be able to call postgres functions, if error happens, will raise
  // the PG ERROR
  PaxDatasetInterface(std::shared_ptr<arrow::Schema> table_schema): DatasetInterface(table_schema) {}
  arrow::Status Initialize(uint32_t tableoid, void *context, const std::shared_ptr<arrow::dataset::ScanOptions> &scan_options) override;

  void Release() override;
  std::string type_name() const override { return "pax-parallel-scan"; }
  arrow::Result<arrow::dataset::FragmentIterator> GetFragmentsImpl(arrow::compute::Expression predicate) override;


  static std::shared_ptr<arrow::dataset::DatasetInterface> New(std::shared_ptr<arrow::Schema> table_schema) {
    return std::make_shared<PaxDatasetInterface>(table_schema);
  }

 private:
  Relation relation_ = nullptr;
  std::shared_ptr<ParallelScanDesc> desc_;
};

} // namespace pax
