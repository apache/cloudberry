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
 * fast_io.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/fast_io.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/common_io.h"

#include <liburing.h>
#include <cstddef>
#include <cstdio>
#include <algorithm>
#include <vector>

namespace pax
{

template<typename T>
int fast_io_read(int fd, std::vector<IORequest> &request) {
  T io_handler(request.size());
  return io_handler.read(fd, request).first;
}

template<typename T>
std::pair<int, int> fast_io_read2(int fd, std::vector<IORequest> &request) {
  T io_handler(request.size());
  return io_handler.read(fd, request);
}

class SyncFastIO {
public:
  SyncFastIO(size_t dummy_queue_size = 0) {}
  std::pair<int, int> read(int fd, std::vector<IORequest> &request, std::vector<bool> &result);
};

// io_uring-based FastIO
class IOUringFastIO {
public:
  IOUringFastIO(size_t queue_size = 128) {
    int ret = io_uring_queue_init(std::max(queue_size, static_cast<size_t>(128)), &ring_, 0);

    // ret < 0: unsupported
    // otherwise initialized
    status_ = ret < 0 ? 'x' : 'i';
  }

  ~IOUringFastIO() {
    if (status_ == 'i')
      io_uring_queue_exit(&ring_);
  }

  static bool available();

  // if pair.first == 0, all read requests are successful
  // pair.second indicates the number of successful read requests
  std::pair<int, int> read(int fd, std::vector<IORequest> &request, std::vector<bool> &result);

private:
  struct io_uring ring_;

  // 'u' for uninitialized, 'i' for initialized, 'x' for unsupported
  char status_ = 'u';
};

} // namespace pax