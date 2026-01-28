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
 * file_system.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/file_system.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/file_system.h"

#include "comm/fmt.h"
#include "exceptions/CException.h"

namespace pax {

void File::ReadN(void *ptr, size_t n) const {
  auto num = Read(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to ReadN [require=%lu, read=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
}

void File::WriteN(const void *ptr, size_t n) {
  auto num = Write(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to WriteN [require=%lu, written=%ld, errno=%d], %s", n,
                 num, errno, DebugString().c_str()));
}

void File::PReadN(void *buf, size_t count, off64_t offset) const {
  auto num = PRead(buf, count, offset);
  CBDB_CHECK(
      static_cast<size_t>(num) == count,
      cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to PReadN [offset=%ld, require=%lu, read=%ld, errno=%d], %s",
          offset, count, num, errno, DebugString().c_str()));
}

void File::PWriteN(const void *buf, size_t count, off64_t offset) {
  auto num = PWrite(buf, count, offset);
  CBDB_CHECK(static_cast<size_t>(num) == count,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to PWriteN [offset=%ld, require=%lu, written=%ld, "
                 "errno=%d], %s",
                 offset, count, num, errno, DebugString().c_str()));
}

void File::ReadBatch(const std::vector<IORequest> &requests) const {
  if (requests.empty()) {
    return;
  }
  for (const auto &req : requests) {
    PReadN(req.buffer, req.size, req.offset);
  }
}

}  // namespace pax
