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
 * UDSConnector.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/UDSConnector.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "UDSConnector.h"
#include "Config.h"
#include "YagpStat.h"
#include "memory/gpdbwrappers.h"
#include "log/LogOps.h"

#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <chrono>
#include <thread>

extern "C" {
#include "postgres.h"
}

static void inline log_tracing_failure(const yagpcc::SetQueryReq &req,
                                       const std::string &event) {
  ereport(LOG, (errmsg("Query {%d-%d-%d} %s tracing failed with error %m",
                       req.query_key().tmid(), req.query_key().ssid(),
                       req.query_key().ccnt(), event.c_str())));
}

bool UDSConnector::report_query(const yagpcc::SetQueryReq &req,
                                const std::string &event,
                                const Config &config) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  const auto &uds_path = config.uds_path();

  if (uds_path.size() >= sizeof(address.sun_path)) {
    ereport(WARNING, (errmsg("UDS path is too long for socket buffer")));
    YagpStat::report_error();
    return false;
  }
  strcpy(address.sun_path, uds_path.c_str());

  const auto sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sockfd == -1) {
    log_tracing_failure(req, event);
    YagpStat::report_error();
    return false;
  }

  // Close socket automatically on error path.
  struct SockGuard {
    int fd;
    ~SockGuard() { close(fd); }
  } sock_guard{sockfd};

  if (fcntl(sockfd, F_SETFL, O_NONBLOCK) == -1) {
    // That's a very important error that should never happen, so make it
    // visible to an end-user and admins.
    ereport(WARNING,
            (errmsg("Unable to create non-blocking socket connection %m")));
    YagpStat::report_error();
    return false;
  }

  if (connect(sockfd, reinterpret_cast<sockaddr *>(&address),
              sizeof(address)) == -1) {
    log_tracing_failure(req, event);
    YagpStat::report_bad_connection();
    return false;
  }

  const auto data_size = req.ByteSizeLong();
  const auto total_size = data_size + sizeof(uint32_t);
  auto *buf = static_cast<uint8_t *>(ya_gpdb::palloc(total_size));
  // Free buf automatically on error path.
  struct BufGuard {
    void *p;
    ~BufGuard() { ya_gpdb::pfree(p); }
  } buf_guard{buf};

  *reinterpret_cast<uint32_t *>(buf) = data_size;
  req.SerializeWithCachedSizesToArray(buf + sizeof(uint32_t));

  int64_t sent = 0, sent_total = 0;
  do {
    sent =
        send(sockfd, buf + sent_total, total_size - sent_total, MSG_DONTWAIT);
    if (sent > 0)
      sent_total += sent;
  } while (sent > 0 && size_t(sent_total) != total_size &&
           // the line below is a small throttling hack:
           // if a message does not fit a single packet, we take a nap
           // before sending the next one.
           // Otherwise, MSG_DONTWAIT send might overflow the UDS
           (std::this_thread::sleep_for(std::chrono::milliseconds(1)), true));

  if (sent < 0) {
    log_tracing_failure(req, event);
    YagpStat::report_bad_send(total_size);
    return false;
  }

  YagpStat::report_send(total_size);
  return true;
}
