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
 * fast_io.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/fast_io.cc
 *
 *-------------------------------------------------------------------------
 */

#include "fast_io.h"

namespace pax
{

bool IOUringFastIO::available() {
  static char support_io_uring = 0;

  if (support_io_uring == 1) return true;
  if (support_io_uring == -1) return false;

  struct io_uring ring;
  bool supported = (io_uring_queue_init(128, &ring, 0) == 0);
  if (supported) {
    io_uring_queue_exit(&ring);
  }
  support_io_uring = supported ? 1 : -1;
  return supported;
}

// if pair.first == 0, all read requests are successful
// pair.second indicates the number of successful read requests
std::pair<int, int> IOUringFastIO::read(int fd, std::vector<IORequest> &request, std::vector<bool> &result)  {
  size_t index = 0;
  int success_read = 0;
  int retcode = 0;
  size_t completed = 0;
  size_t total_requests = request.size();

  // Implementation for synchronous read using io_uring
  if (uring_likely(request.empty())) return {0, 0};
  if (status_ != 'i') return {-EINVAL, 0};

  result.resize(request.size(), false);

  while (completed < total_requests) {
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count;
    int rc;
    // Submit read requests
    while (index < total_requests) {
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) break; // No more SQEs available, retry later

      io_uring_prep_read(sqe, fd, request[index].buffer, request[index].size, request[index].offset);
      io_uring_sqe_set_data(sqe, (void*)(uintptr_t)index);
      index++;
    }

    // submit and wait for completions
    do {
      rc = io_uring_submit_and_wait(&ring_, 1);
    } while (rc == -EINTR);
    if (rc < 0) return {rc, success_read};

    count = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      size_t req_index = (size_t)(uintptr_t)io_uring_cqe_get_data(cqe);
      if (cqe->res >= 0) {
        // Successful read
        result[req_index] = true;
        success_read++;
      } else if (retcode == 0) {
        retcode = cqe->res; // capture the first error
      }
      completed++;
      count++;
    }
    io_uring_cq_advance(&ring_, count);
  }
  return {retcode, success_read}; // Placeholder
}

std::pair<int, int> SyncFastIO::read(int fd, std::vector<IORequest> &request, std::vector<bool> &result) {
  size_t total_requests = request.size();
  if (total_requests == 0) return {0, 0};

  result.resize(total_requests, false);

  int success_read = 0;
  int retcode = 0;

  for (size_t i = 0; i < total_requests; ++i) {
    ssize_t bytes_read = 0;
    ssize_t nbytes;
    auto &req = request[i];
    do {
      nbytes = pread(fd, (char *)req.buffer + bytes_read, req.size - bytes_read, req.offset + bytes_read);
      if (nbytes > 0) bytes_read += nbytes;
    } while ((nbytes == -1 && errno == EINTR) || (nbytes > 0 && static_cast<size_t>(bytes_read) < req.size));

    if (bytes_read < 0) {
      if (retcode == 0) {
          retcode = static_cast<int>(bytes_read); // capture first error
      }
    } else if (static_cast<size_t>(bytes_read) == request[i].size) {
      result[i] = true;
      success_read++;
    }
  }

  return {retcode, success_read};
}

} // namespace pax